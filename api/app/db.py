from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator
from uuid import UUID

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/tracker")
# Found by tests/test_stress_partition.py: with no timeout, a silent API<->DB partition made every
# request hang until the client gave up (60/60 client timeouts), /healthz included.
COMMAND_TIMEOUT = float(os.environ.get("DB_COMMAND_TIMEOUT", "3"))

log = logging.getLogger("db")

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """Retry instead of dying: found when the DB was unreachable at boot and the API process
    exited ("Application startup failed") and, without a restart policy, stayed down."""
    global _pool
    delay = 0.5
    for attempt in range(1, 61):
        try:
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=int(os.environ.get("DB_POOL_MIN", "2")),
                max_size=int(os.environ.get("DB_POOL_MAX", "20")),
                command_timeout=COMMAND_TIMEOUT,
            )
            return _pool
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
            log.warning("DB not reachable at startup (attempt %d): %s", attempt, type(e).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5.0)
    raise RuntimeError("database unreachable at startup")


async def close_pool() -> None:
    if _pool is not None:
        await _pool.close()


def pool() -> asyncpg.Pool:
    assert _pool is not None, "pool not initialised"
    return _pool


@asynccontextmanager
async def acquire() -> AsyncIterator[asyncpg.Connection]:
    """Bounded acquire AND release.

    asyncpg's `async with pool.acquire()` releases without a timeout, and release resets the
    connection with a query. On a silently partitioned connection that reset never returns, so
    a request hung even after its own query had timed out (found by test_stress_partition).
    release(timeout=...) terminates the connection instead of waiting forever.
    """
    con = await pool().acquire(timeout=COMMAND_TIMEOUT)
    try:
        yield con
    finally:
        await pool().release(con, timeout=COMMAND_TIMEOUT)


async def fetchval(query: str, *args: object) -> object:
    async with acquire() as con:
        return await con.fetchval(query, *args)


async def fetchrow(query: str, *args: object) -> asyncpg.Record | None:
    async with acquire() as con:
        return await con.fetchrow(query, *args)


async def execute(query: str, *args: object) -> str:
    async with acquire() as con:
        return await con.execute(query, *args)


@asynccontextmanager
async def project_tx(project_id: UUID) -> AsyncIterator[asyncpg.Connection]:
    """Transaction scoped to one project: RLS policies read app.project_id.

    set_config(..., is_local=true) is reset at transaction end, so a pooled
    connection never carries one project's scope into another request.
    """
    async with acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.project_id', $1, true)", str(project_id))
            yield conn
