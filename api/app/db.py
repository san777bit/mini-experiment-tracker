from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from collections.abc import Hashable
from typing import AsyncIterator
from uuid import UUID

import asyncpg

from .limits import FairGate
from .stats import TIMERS

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/tracker")
# Found by tests/test_stress_partition.py: with no timeout, a silent API<->DB partition made every
# request hang until the client gave up (60/60 client timeouts), /healthz included.
COMMAND_TIMEOUT = float(os.environ.get("DB_COMMAND_TIMEOUT", "3"))
POOL_MAX = int(os.environ.get("DB_POOL_MAX", "20"))          # per process, both lanes together
HUB_POOL = int(os.environ.get("DB_HUB_POOL", "4"))
MAIN_POOL = POOL_MAX - HUB_POOL
SYSTEM = "system"      # tenant for work not done on behalf of a project (notify, health, auth misses)


class Lane:
    """A pool plus the fair gate in front of it (capacity == pool size, see FairGate)."""

    def __init__(self, name: str, size: int) -> None:
        self.name, self.size = name, size
        self.gate = FairGate(size)
        self.pool: asyncpg.Pool | None = None


# Round-4 breakdown: tail fetches (3 ms queries on the live path) waited up to ~100 ms for a
# connection behind ingests that hold theirs through a commit fsync. The hub (tail fetches and
# NOTIFY) gets its own small lane, so the live path never queues behind writes.
MAIN = Lane("main", MAIN_POOL)
HUB = Lane("hub", HUB_POOL)
GATE = MAIN.gate       # diagnostics

log = logging.getLogger("db")



async def _no_session_state(_con: asyncpg.Connection) -> None:
    """Replaces asyncpg's reset query on release (UNLISTEN *, RESET ALL, advisory unlock, CLOSE ALL).

    Found by the round-4 latency breakdown: ~4 pool acquisitions per ingested event, each paying an
    extra round trip on release while still holding its fair-gate slot. Pool connections never
    LISTEN, take advisory locks, open cursors or SET session-wide (app.project_id is always
    set_config(..., is_local=true)), so there is nothing to reset. asyncpg still rolls back an open
    transaction before calling this (Connection._reset).
    """


async def init_pool() -> None:
    """Retry instead of dying: found when the DB was unreachable at boot and the API process
    exited ("Application startup failed") and, without a restart policy, stayed down."""
    delay = 0.5
    for attempt in range(1, 61):
        try:
            for lane in (MAIN, HUB):
                if lane.pool is None:
                    lane.pool = await asyncpg.create_pool(
                        DATABASE_URL,
                        # Open the whole pool at boot. With min 2, the first burst after a restart
                        # paid for ~18 connection setups (SCRAM auth) inside request latency
                        # (round-4 breakdown: the first run after every rebuild was the slow one).
                        min_size=lane.size,
                        max_size=lane.size,
                        command_timeout=COMMAND_TIMEOUT,
                        reset=_no_session_state,
                    )
            return
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
            log.warning("DB not reachable at startup (attempt %d): %s", attempt, type(e).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5.0)
    raise RuntimeError("database unreachable at startup")


async def close_pool() -> None:
    for lane in (MAIN, HUB):
        if lane.pool is not None:
            await lane.pool.close()


def pool() -> asyncpg.Pool:
    assert MAIN.pool is not None, "pool not initialised"
    return MAIN.pool


@asynccontextmanager
async def acquire(tenant: Hashable = SYSTEM, lane: Lane = MAIN) -> AsyncIterator[asyncpg.Connection]:
    """Fair, bounded acquire AND release.

    asyncpg's `async with pool.acquire()` releases without a timeout, and release resets the
    connection with a query. On a silently partitioned connection that reset never returns, so
    a request hung even after its own query had timed out (found by test_stress_partition).
    release(timeout=...) terminates the connection instead of waiting forever.
    """
    assert lane.pool is not None, "pool not initialised"
    t0 = time.perf_counter()
    await lane.gate.acquire(tenant)
    try:
        con = await lane.pool.acquire(timeout=COMMAND_TIMEOUT)
        TIMERS.add(f"db_conn_wait_{lane.name}", (time.perf_counter() - t0) * 1000)
        try:
            yield con
        finally:
            await lane.pool.release(con, timeout=COMMAND_TIMEOUT)
    finally:
        lane.gate.release(tenant)


async def fetchval(query: str, *args: object, tenant: Hashable = SYSTEM, lane: Lane = MAIN) -> object:
    async with acquire(tenant, lane) as con:
        return await con.fetchval(query, *args)


async def fetch(query: str, *args: object, tenant: Hashable = SYSTEM, lane: Lane = MAIN) -> list[asyncpg.Record]:
    async with acquire(tenant, lane) as con:
        return await con.fetch(query, *args)


async def fetchrow(query: str, *args: object, tenant: Hashable = SYSTEM, lane: Lane = MAIN) -> asyncpg.Record | None:
    async with acquire(tenant, lane) as con:
        return await con.fetchrow(query, *args)


async def execute(query: str, *args: object, tenant: Hashable = SYSTEM, lane: Lane = MAIN) -> str:
    async with acquire(tenant, lane) as con:
        return await con.execute(query, *args)


@asynccontextmanager
async def project_tx(project_id: UUID) -> AsyncIterator[asyncpg.Connection]:
    """Transaction scoped to one project: RLS policies read app.project_id.

    set_config(..., is_local=true) is reset at transaction end, so a pooled
    connection never carries one project's scope into another request.
    """
    async with acquire(project_id) as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.project_id', $1, true)", str(project_id))
            yield conn
