from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from uuid import UUID

import asyncpg

from .db import DATABASE_URL
from .stats import STATS

log = logging.getLogger("hub")

CHANNEL = "run_events"


class Hub:
    """Wakes stream generators when a run gets new rows.

    It carries no data: NOTIFY is only a hint that the DB has something past the
    reader's cursor. Every stream also polls, so a NOTIFY lost while this listener
    reconnects delays updates but never drops them.
    """

    def __init__(self) -> None:
        self._waiters: dict[UUID, set[asyncio.Event]] = defaultdict(set)
        self._conn: asyncpg.Connection | None = None
        self._task: asyncio.Task[None] | None = None

    def subscribe(self, run_id: UUID) -> asyncio.Event:
        ev = asyncio.Event()
        self._waiters[run_id].add(ev)
        return ev

    def unsubscribe(self, run_id: UUID, ev: asyncio.Event) -> None:
        waiters = self._waiters.get(run_id)
        if waiters is None:
            return
        waiters.discard(ev)
        if not waiters:
            del self._waiters[run_id]

    def _wake_all(self) -> None:
        for waiters in self._waiters.values():
            for ev in waiters:
                ev.set()

    def wake(self, run_id: UUID) -> None:
        """Local wake-up for streams in this process (other processes get the NOTIFY)."""
        for ev in self._waiters.get(run_id, ()):
            ev.set()

    def _on_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        run_part = payload.split(":", 1)[0]
        try:
            run_id = UUID(run_part)
        except ValueError:
            return
        self.wake(run_id)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._conn and not self._conn.is_closed():
            await self._conn.close()

    async def _supervise(self) -> None:
        while True:
            try:
                if self._conn is None or self._conn.is_closed():
                    self._conn = await asyncpg.connect(DATABASE_URL)
                    await self._conn.add_listener(CHANNEL, self._on_notify)
                    STATS.inc("listener_connects")
                    # Anything notified while we were down is unknown: wake everyone.
                    self._wake_all()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                STATS.inc("listener_errors")
                log.exception("listener failed, retrying")
                await asyncio.sleep(1.0)


HUB = Hub()
