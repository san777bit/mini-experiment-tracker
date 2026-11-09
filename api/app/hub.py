from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from . import db
from .db import DATABASE_URL
from .limits import read_slot
from .stats import STATS

log = logging.getLogger("hub")

CHANNEL = "run_events"
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
TAIL_MAX = int(os.environ.get("TAIL_MAX", "5000"))
FETCH_PAGE = int(os.environ.get("STREAM_PAGE", "500")) * 10


@dataclass(frozen=True, slots=True)
class Row:
    seq: int
    key: str
    step: int
    value: float


class RunTail:
    """The newest rows of one run, fetched ONCE per process and shared by all its viewers.

    Found by load/stress_fanout.py: with a query per viewer per wake-up, 1000 viewers turned
    6 events/s into ~4900 DB transactions/s and e2e p99 1.8 s. Invariant: `rows` is always a
    contiguous seq range (first_seq .. last_seq); a viewer behind first_seq reads from the DB.
    """

    def __init__(self, run_id: UUID, project_id: UUID, start_seq: int) -> None:
        self.run_id = run_id
        self.project_id = project_id
        self.rows: list[Row] = []
        self.last_seq = start_seq            # rows cover (last_seq - len(rows), last_seq]
        self.status: tuple[int, int] = (start_seq, 0)
        self.wake = asyncio.Event()
        self._changed = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    @property
    def first_seq(self) -> int:
        return self.last_seq - len(self.rows) + 1

    def changed(self) -> asyncio.Event:
        """Grab BEFORE reading rows: it fires on the next change after this call."""
        return self._changed

    def after(self, cursor: int, limit: int) -> list[Row]:
        start = cursor - self.first_seq + 1
        return self.rows[start:start + limit]

    def _publish(self) -> None:
        old, self._changed = self._changed, asyncio.Event()
        old.set()

    async def run(self) -> None:
        while True:
            self.wake.clear()
            try:
                async with read_slot(self.project_id), db.project_tx(self.project_id) as conn:
                    run = await conn.fetchrow(
                        "SELECT last_seq, conflicts FROM runs WHERE id = $1 AND project_id = $2",
                        self.run_id, self.project_id)
                    recs = await conn.fetch(
                        """SELECT seq, metric_key, step, value FROM metrics
                            WHERE run_id = $1 AND project_id = $2 AND seq > $3 ORDER BY seq LIMIT $4""",
                        self.run_id, self.project_id, self.last_seq, FETCH_PAGE)
            except asyncio.CancelledError:
                raise
            except Exception:
                STATS.inc("tail_fetch_errors")
                log.exception("tail fetch failed run=%s", self.run_id)
                await asyncio.sleep(1.0)
                continue
            STATS.inc("tail_fetches")
            changed = False
            for r in recs:
                if r["seq"] != self.last_seq + 1:      # cannot happen with per-run seq; never paper over it
                    STATS.inc("tail_seq_breaks")
                    log.error("tail seq break run=%s have=%s got=%s", self.run_id, self.last_seq, r["seq"])
                    self.rows.clear()
                    self.last_seq = r["seq"] - 1
                self.rows.append(Row(r["seq"], r["metric_key"], r["step"], r["value"]))
                self.last_seq = r["seq"]
                changed = True
            if len(self.rows) > TAIL_MAX:
                del self.rows[: len(self.rows) - TAIL_MAX]
            if run is not None and (run["last_seq"], run["conflicts"]) != self.status:
                self.status = (run["last_seq"], run["conflicts"])
                changed = True
            if changed:
                self._publish()
            if len(recs) == FETCH_PAGE:
                continue
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                STATS.inc("tail_polls")


class Hub:
    """Owns one LISTEN connection per process and the shared tails of watched runs.

    NOTIFY carries no data: it only wakes a tail, which reads from the DB. Tails also poll, so a
    NOTIFY lost while the listener reconnects delays updates but never drops them.
    """

    def __init__(self) -> None:
        self._tails: dict[UUID, list] = {}      # run_id -> [RunTail, users]
        self._conn: asyncpg.Connection | None = None
        self._task: asyncio.Task[None] | None = None

    async def acquire(self, run_id: UUID, project_id: UUID, start_seq: int) -> RunTail:
        entry = self._tails.get(run_id)
        if entry is None:
            tail = RunTail(run_id, project_id, start_seq)
            tail.task = asyncio.create_task(tail.run())
            entry = self._tails[run_id] = [tail, 0]
        entry[1] += 1
        STATS.gauge("tails_open", 1 if entry[1] == 1 else 0)
        return entry[0]

    def release(self, run_id: UUID) -> None:
        entry = self._tails.get(run_id)
        if entry is None:
            return
        entry[1] -= 1
        if entry[1] == 0:
            del self._tails[run_id]
            if entry[0].task:
                entry[0].task.cancel()
            STATS.gauge("tails_open", -1)

    def wake(self, run_id: UUID) -> None:
        entry = self._tails.get(run_id)
        if entry is not None:
            entry[0].wake.set()

    def _wake_all(self) -> None:
        for tail, _ in self._tails.values():
            tail.wake.set()

    def _on_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        try:
            run_id = UUID(payload.split(":", 1)[0])
        except ValueError:
            return
        self.wake(run_id)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        for tail, _ in self._tails.values():
            if tail.task:
                tail.task.cancel()
        if self._conn and not self._conn.is_closed():
            await self._conn.close()

    async def _supervise(self) -> None:
        while True:
            try:
                if self._conn is None or self._conn.is_closed():
                    self._conn = await asyncpg.connect(
                        DATABASE_URL, timeout=5, server_settings={"application_name": "tracker-listener"})
                    await self._conn.add_listener(CHANNEL, self._on_notify)
                    STATS.inc("listener_connects")
                    # Anything notified while we were down is unknown: wake everyone.
                    self._wake_all()
                await asyncio.sleep(5.0)
                # A silently partitioned connection never reports itself closed and NOTIFYs just
                # stop. Probe it; on failure drop it and reconnect (polling covers the gap).
                try:
                    await asyncio.wait_for(self._conn.fetchval("SELECT 1"), timeout=3)
                except (asyncio.TimeoutError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError):
                    STATS.inc("listener_probe_failures")
                    self._conn.terminate()
            except asyncio.CancelledError:
                raise
            except Exception:
                STATS.inc("listener_errors")
                log.exception("listener failed, retrying")
                await asyncio.sleep(1.0)


HUB = Hub()
