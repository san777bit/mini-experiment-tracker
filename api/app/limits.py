from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager
from uuid import UUID

from .errors import ApiError
from .stats import STATS

# All limits are per API process.
RUN_QUEUE_MAX = int(os.environ.get("RUN_QUEUE_MAX", "64"))      # writers waiting on one run
QUEUE_WAIT = float(os.environ.get("QUEUE_WAIT", "5"))           # max wait for a slot, seconds
# Sustained requests/s per project PER PROCESS. Was 300/600: the kernel spreads connections
# unevenly over the 4 workers, so a legitimate 500 req/s project got 133-655 early 429s on its
# busiest worker (round-4 load runs). Abuse is handled by the run queue, the byte budget and the
# delayed 429; this budget only caps a single project's request rate.
PROJECT_RPS = float(os.environ.get("PROJECT_RPS", "1000"))
PROJECT_BURST = float(os.environ.get("PROJECT_BURST", "2000"))
# Request count alone let 1.9 MB bodies through at 126 req/s (well under the request budget) and the
# JSON parsing of 240 MB/s starved everyone (stress_bigbody). A 1000-event batch is ~250 KB and the
# 50x10 target is ~50 KB/s, so 4 MB/s per project per process is generous.
PROJECT_BYTES_PS = float(os.environ.get("PROJECT_BYTES_PS", str(4 * 1024 * 1024)))
PROJECT_BYTES_BURST = float(os.environ.get("PROJECT_BYTES_BURST", str(8 * 1024 * 1024)))
# Early 429s are answered after this delay (= Retry-After). A client that waits for each response
# before sending the next (most retry loops) is slowed to ~1 request/s per connection for free;
# only a coroutine is held. Bounded, so an open-loop flood cannot pile up sleeping requests.
REJECT_DELAY = float(os.environ.get("REJECT_DELAY", "1.0"))
TARPIT_MAX = int(os.environ.get("TARPIT_MAX", "2000"))


class FairGate:
    """Work-conserving, max-min fair admission to the DB pool.

    Round 2 used a hard cap (a project could hold at most 12 of 20 connections). It protected
    neighbours but cost a lone big project 6% throughput, because the cap applied even when the
    other 8 connections were idle. Here a tenant may use every free connection; only when tenants
    are WAITING does a released connection go to the waiting tenant that currently holds the
    fewest (ties: whoever has waited longest). So a quiet project waits for at most one release,
    not for a hot project's whole queue.

    capacity must equal the pool size and every grant must take exactly one pooled connection,
    so pool.acquire() itself never queues (db.acquire does both).
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.used = 0
        self.held: dict[Hashable, int] = {}
        self.waiting: dict[Hashable, deque[tuple[int, asyncio.Future[None]]]] = {}
        self._ticket = 0

    def _grant(self, tenant: Hashable) -> None:
        self.used += 1
        self.held[tenant] = self.held.get(tenant, 0) + 1

    async def acquire(self, tenant: Hashable) -> None:
        if self.used < self.capacity and not self.waiting:
            self._grant(tenant)
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._ticket += 1
        self.waiting.setdefault(tenant, deque()).append((self._ticket, fut))
        try:
            await asyncio.wait_for(fut, timeout=QUEUE_WAIT)
        except BaseException as e:
            if fut.done() and not fut.cancelled():
                self.release(tenant)          # granted in the same tick we gave up: hand it back
            else:
                self._forget(tenant, fut)
            if isinstance(e, asyncio.TimeoutError):
                STATS.inc("queue_timeouts_db_gate")
                raise ApiError(503, "busy") from None
            raise

    def release(self, tenant: Hashable) -> None:
        self.used -= 1
        n = self.held.get(tenant, 0) - 1
        if n > 0:
            self.held[tenant] = n
        else:
            self.held.pop(tenant, None)
        self._dispatch()

    def _forget(self, tenant: Hashable, fut: asyncio.Future[None]) -> None:
        q = self.waiting.get(tenant)
        if q is None:
            return
        for item in q:
            if item[1] is fut:
                q.remove(item)
                break
        if not q:
            del self.waiting[tenant]

    def _dispatch(self) -> None:
        while self.used < self.capacity and self.waiting:
            tenant = min(self.waiting, key=lambda t: (self.held.get(t, 0), self.waiting[t][0][0]))
            q = self.waiting[tenant]
            _, fut = q.popleft()
            if not q:
                del self.waiting[tenant]
            if fut.done():
                continue
            self._grant(tenant)
            fut.set_result(None)


class TokenBuckets:
    """Per-project request budget, checked before the body is read or the DB is touched."""

    def __init__(self, rate: float, burst: float, max_keys: int = 10_000) -> None:
        self.rate, self.burst, self.max_keys = rate, burst, max_keys
        self._b: dict[Hashable, list[float]] = {}   # key -> [tokens, last refill]

    def _refill(self, key: Hashable, now: float | None) -> list[float]:
        now = time.monotonic() if now is None else now
        b = self._b.get(key)
        if b is None:
            if len(self._b) >= self.max_keys:
                self._b.clear()               # crude bound; worst case: everyone gets a fresh burst
            b = self._b[key] = [self.burst, now]
        b[0] = min(self.burst, b[0] + (now - b[1]) * self.rate)
        b[1] = now
        return b

    def allow(self, key: Hashable, cost: float = 1.0, now: float | None = None) -> bool:
        """Take `cost` if available. cost=0 (size not known yet) still needs a positive balance."""
        b = self._refill(key, now)
        if b[0] < cost or b[0] <= 0:
            return False
        b[0] -= cost
        return True

    def charge(self, key: Hashable, cost: float, now: float | None = None) -> None:
        """Take `cost` after the fact (chunked body); the balance may go negative (debt)."""
        self._refill(key, now)[0] -= cost


BUCKETS = TokenBuckets(PROJECT_RPS, PROJECT_BURST)
BYTE_BUCKETS = TokenBuckets(PROJECT_BYTES_PS, PROJECT_BYTES_BURST)


class _RunQueues:
    """One asyncio.Lock per run, refcounted, remembering which project owns the waiters."""

    def __init__(self) -> None:
        self._items: dict[UUID, list] = {}   # run id -> [lock, users, project id]

    @asynccontextmanager
    async def hold(self, run_id: UUID, project_id: UUID) -> AsyncIterator[asyncio.Lock]:
        entry = self._items.get(run_id)
        if entry is None:
            entry = self._items[run_id] = [asyncio.Lock(), 0, project_id]
        entry[1] += 1
        try:
            yield entry[0]
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                self._items.pop(run_id, None)

    def full(self, run_id: UUID, project_id: UUID) -> bool:
        # Only the owning project learns that a run is busy: another project probing a foreign run
        # id must keep getting the same 404 as for a missing run.
        entry = self._items.get(run_id)
        return entry is not None and entry[2] == project_id and entry[1] >= RUN_QUEUE_MAX


RUN_QUEUES = _RunQueues()


@asynccontextmanager
async def write_slot(project_id: UUID, run_id: UUID) -> AsyncIterator[None]:
    """Queue writers of one run in memory, not in the DB pool.

    Found by load/stress_hot_run.py: 200 writers on one run each held a pooled connection while
    waiting for the run's row lock, the pool emptied, and an unrelated project's reads took 7.4 s.
    Waiters wait here (no connection held), one writer per run per process reaches the DB, and an
    overlong queue is an explicit 429 (also answered early, before the body is read, by
    main.RateLimitMiddleware once the key is known).
    """
    if RUN_QUEUES.full(run_id, project_id):
        STATS.inc("writes_rejected_run_queue_full")
        raise ApiError(429, "run_busy")
    async with RUN_QUEUES.hold(run_id, project_id) as lock:
        try:
            await asyncio.wait_for(lock.acquire(), timeout=QUEUE_WAIT)
        except asyncio.TimeoutError:
            STATS.inc("queue_timeouts_run_lock")
            raise ApiError(503, "busy") from None
        try:
            yield
        finally:
            lock.release()
