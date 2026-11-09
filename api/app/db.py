from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator
from uuid import UUID

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/tracker")

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=int(os.environ.get("DB_POOL_MIN", "5")),
        max_size=int(os.environ.get("DB_POOL_MAX", "20")),
    )
    return _pool


async def close_pool() -> None:
    if _pool is not None:
        await _pool.close()


def pool() -> asyncpg.Pool:
    assert _pool is not None, "pool not initialised"
    return _pool


@asynccontextmanager
async def project_tx(project_id: UUID) -> AsyncIterator[asyncpg.Connection]:
    """Transaction scoped to one project: RLS policies read app.project_id.

    set_config(..., is_local=true) is reset at transaction end, so a pooled
    connection never carries one project's scope into another request.
    """
    async with pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.project_id', $1, true)", str(project_id))
            yield conn
