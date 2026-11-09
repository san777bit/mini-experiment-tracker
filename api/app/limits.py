from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from .errors import ApiError
from .stats import STATS

# Per process. With 4 workers a project can hold at most 4 x PROJECT_DB_SLOTS connections.
PROJECT_DB_SLOTS = int(os.environ.get("PROJECT_DB_SLOTS", "6"))     # of DB_POOL_MAX=10
RUN_QUEUE_MAX = int(os.environ.get("RUN_QUEUE_MAX", "64"))           # writers waiting on one run
QUEUE_WAIT = float(os.environ.get("QUEUE_WAIT", "5"))                # max wait for a slot, seconds


class _Keyed:
    """asyncio primitives keyed by id, dropped when nobody uses them."""

    def __init__(self, factory) -> None:  # type: ignore[no-untyped-def]
        self._factory = factory
        self._items: dict[UUID, list] = {}   # id -> [primitive, users]

    @asynccontextmanager
    async def hold(self, key: UUID) -> AsyncIterator[object]:
        entry = self._items.get(key)
        if entry is None:
            entry = self._items[key] = [self._factory(), 0]
        entry[1] += 1
        try:
            yield entry[0]
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                self._items.pop(key, None)

    def users(self, key: UUID) -> int:
        entry = self._items.get(key)
        return entry[1] if entry else 0


_run_locks = _Keyed(asyncio.Lock)
_project_slots = _Keyed(lambda: asyncio.Semaphore(PROJECT_DB_SLOTS))


@asynccontextmanager
async def write_slot(project_id: UUID, run_id: UUID) -> AsyncIterator[None]:
    """Queue writers of one run in memory, not in the DB pool.

    Found by load/stress_hot_run.py: 200 writers on one run each held a pooled connection while
    waiting for the run's row lock, the pool emptied, and an unrelated project's reads took 7.4 s.
    Now waiters wait here (no connection held), one writer per run per process reaches the DB, a
    project uses at most PROJECT_DB_SLOTS connections, and an overlong queue is an explicit 429.
    """
    if _run_locks.users(run_id) >= RUN_QUEUE_MAX:
        STATS.inc("writes_rejected_run_queue_full")
        raise ApiError(429, "run_busy")
    async with _run_locks.hold(run_id) as lock:
        await _bounded(lock.acquire(), "run_lock")  # type: ignore[attr-defined]
        try:
            async with _project_slots.hold(project_id) as sem:
                await _bounded(sem.acquire(), "project_slot")  # type: ignore[attr-defined]
                try:
                    yield
                finally:
                    sem.release()  # type: ignore[attr-defined]
        finally:
            lock.release()  # type: ignore[attr-defined]


@asynccontextmanager
async def read_slot(project_id: UUID) -> AsyncIterator[None]:
    async with _project_slots.hold(project_id) as sem:
        await _bounded(sem.acquire(), "project_slot")  # type: ignore[attr-defined]
        try:
            yield
        finally:
            sem.release()  # type: ignore[attr-defined]


async def _bounded(waiter, what: str) -> None:  # type: ignore[no-untyped-def]
    """In-memory queues must not turn a slow DB into requests that hang: if the holder is stuck
    (e.g. DB partition, each query up to DB_COMMAND_TIMEOUT), give up with a retryable 503."""
    try:
        await asyncio.wait_for(waiter, timeout=QUEUE_WAIT)
    except asyncio.TimeoutError:
        STATS.inc(f"queue_timeouts_{what}")
        raise ApiError(503, "busy") from None
