from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from . import db
from .auth import KEYS
from .db import DATABASE_URL
from .stats import STATS, TIMERS

log = logging.getLogger("hub")

CHANNEL = "run_events"
KEY_CHANNEL = "key_events"
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
TAIL_MAX = int(os.environ.get("TAIL_MAX", "5000"))
FETCH_PAGE = int(os.environ.get("STREAM_PAGE", "500")) * 10
PAYLOAD_MAX = 7000          # NOTIFY payloads must stay under 8000 bytes
PUSH_ENABLED = os.environ.get("TAIL_PUSH", "1") == "1"
PID = os.getpid()


@dataclass(frozen=True, slots=True)
class Row:
    seq: int
    key: str
    step: int
    value: float
    received: float = 0.0     # diagnostics only (epoch s): DB txn start, or commit time if pushed


class RunTail:
    """The newest rows of one run, shared by all viewers of that run in this process.

    Found by load/stress_fanout.py: with a query per viewer per wake-up, 1000 viewers turned
    6 events/s into ~4900 DB transactions/s and e2e p99 1.8 s. Invariant: `rows` is always a
    contiguous seq range (first_seq .. last_seq); a viewer behind first_seq reads from the DB.

    Rows arrive two ways. push(): the process that committed them hands them over directly
    (same process) or inside the NOTIFY payload (other processes), no DB read. run(): a DB read,
    on start, every POLL_INTERVAL, and whenever a push cannot be applied contiguously. Both paths
    only ever append seq == last_seq + 1, so the DB stays the source of truth and a pushed row
    can never create or hide a hole.
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

    def _append(self, rows: list[Row]) -> bool:
        """Append the part of `rows` that continues the tail. False if there is a hole."""
        fresh = [r for r in rows if r.seq > self.last_seq]
        if not fresh:
            return True
        for r in fresh:
            if r.seq != self.last_seq + 1:
                return False
            self.rows.append(r)
            self.last_seq = r.seq
        if len(self.rows) > TAIL_MAX:
            del self.rows[: len(self.rows) - TAIL_MAX]
        return True

    def push(self, project_id: UUID, rows: list[Row] | None, last_seq: int) -> None:
        """Rows committed by some process (None = 'something changed, read the DB').

        Pushed rows skip the DB read, and with it RLS (the second wall). So the tail checks the
        project itself: rows that claim a different project than this tail's are dropped and
        counted; that can only be a routing bug, and it must not reach a viewer.
        """
        if project_id != self.project_id:
            STATS.inc("tail_push_foreign_project")
            log.error("pushed rows for run=%s claim project=%s, tail belongs to another project",
                      self.run_id, project_id)
            return
        if rows is None:
            self.wake.set()
            return
        before = self.last_seq
        if not self._append(rows):
            STATS.inc("tail_push_gaps")      # e.g. two processes' NOTIFYs arrived out of order
            self.wake.set()
            return
        if self.last_seq != before:
            STATS.inc("tail_pushed_rows", self.last_seq - before)
            TIMERS.add("commit_to_tail", (time.time() - rows[0].received) * 1000)
            if last_seq > self.status[0]:
                self.status = (last_seq, self.status[1])
            self._publish()

    async def run(self) -> None:
        while True:
            self.wake.clear()
            t0 = time.perf_counter()
            try:
                found = await db.fetch(
                    "SELECT * FROM tail_fetch($1, $2, $3, $4)",
                    self.run_id, self.project_id, self.last_seq, FETCH_PAGE,
                    tenant=self.project_id, lane=db.HUB)
                run = found[0] if found else None
                recs = [Row(r["t_seq"], r["t_key"], r["t_step"], r["t_value"],
                            r["t_received_at"].timestamp()) for r in found if r["t_seq"] is not None]
            except asyncio.CancelledError:
                raise
            except Exception:
                STATS.inc("tail_fetch_errors")
                log.exception("tail fetch failed run=%s", self.run_id)
                await asyncio.sleep(1.0)
                continue
            STATS.inc("tail_fetches")
            TIMERS.add("tail_fetch", (time.perf_counter() - t0) * 1000)
            before = self.last_seq
            if not self._append(recs):
                # Cannot happen with per-run seq (the DB read starts at last_seq); never paper over it.
                STATS.inc("tail_seq_breaks")
                log.error("tail seq break run=%s have=%s got=%s", self.run_id, self.last_seq, recs[0].seq)
                self.rows.clear()
                self.last_seq = recs[0].seq - 1
                self._append(recs)
            changed = self.last_seq != before
            if changed:
                TIMERS.add("txn_start_to_tail", (time.time() - recs[0].received) * 1000)
            if run is not None and (run["t_last_seq"], run["t_conflicts"]) != self.status:
                self.status = (run["t_last_seq"], run["t_conflicts"])
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

    NOTIFY carries the committed rows when they fit (else just "read the DB"). Tails also poll,
    so a NOTIFY lost while the listener reconnects delays updates but never drops them.
    """

    def __init__(self) -> None:
        self._tails: dict[UUID, list] = {}      # run_id -> [RunTail, users]
        self._conn: asyncpg.Connection | None = None
        self._task: asyncio.Task[None] | None = None
        self._pending: dict[UUID, list] = {}    # run_id -> [last_seq, rows | None, project_id]
        self._notifier: asyncio.Task[None] | None = None
        self._key_watch: dict[UUID, list[tuple[asyncio.Event, RunTail]]] = {}

    # --- key revocation -------------------------------------------------------------------

    def watch_key(self, key_id: UUID, tail: RunTail) -> asyncio.Event:
        """Event set when key_id is revoked. Before: an open stream noticed only at its next
        5 s DB re-check (test measured 5.5 s of streaming with a revoked key)."""
        ev = asyncio.Event()
        self._key_watch.setdefault(key_id, []).append((ev, tail))
        return ev

    def unwatch_key(self, key_id: UUID, ev: asyncio.Event) -> None:
        lst = self._key_watch.get(key_id)
        if lst is None:
            return
        lst[:] = [x for x in lst if x[0] is not ev]
        if not lst:
            del self._key_watch[key_id]

    def key_revoked(self, key_id: UUID, announce: bool = False) -> None:
        KEYS.revoked(key_id)                          # drop cached write auth for it
        for ev, tail in self._key_watch.get(key_id, []):
            ev.set()
            tail._publish()                           # wake the stream; it sees the event first
        if announce:
            asyncio.get_running_loop().create_task(self._announce_revoked(key_id))

    async def _announce_revoked(self, key_id: UUID) -> None:
        try:
            await db.execute("SELECT pg_notify($1, $2)", KEY_CHANNEL,
                             json.dumps({"p": PID, "k": str(key_id)}), lane=db.HUB)
        except Exception:
            STATS.inc("notify_errors")      # streams still close at their periodic re-check
            log.exception("revocation notify failed")

    def _on_key_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        try:
            msg = json.loads(payload)
            if msg["p"] != PID:
                self.key_revoked(UUID(msg["k"]))
        except (ValueError, KeyError, TypeError):
            STATS.inc("notify_bad_payload")

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

    def publish(self, run_id: UUID, project_id: UUID, rows: list[Row] | None, last_seq: int) -> None:
        """Called right after an ingest commit. rows=None: changed, but read it from the DB.

        Round 4: tails read every new row from the DB (one pooled round trip per tail per
        commit). Under bursts that queued behind commits and dominated e2e p99. Now the
        committing process hands the rows to its own tail at once and to the other processes in
        the NOTIFY payload; the DB read remains the fallback for anything that does not fit.
        """
        if not PUSH_ENABLED:
            rows = None
        entry = self._tails.get(run_id)
        if entry is not None:
            entry[0].push(project_id, rows, last_seq)
        p = self._pending.get(run_id)
        if p is None:
            self._pending[run_id] = [last_seq, list(rows) if rows is not None else None, project_id]
        else:
            p[0] = max(p[0], last_seq)
            p[1] = p[1] + rows if (p[1] is not None and rows is not None) else None
        if self._notifier is None or self._notifier.done():
            self._notifier = asyncio.create_task(self._flush_notifies())

    @staticmethod
    def _item(run_id: UUID, project_id: UUID, last_seq: int, rows: list[Row] | None) -> str:
        item = {"r": str(run_id), "j": str(project_id), "s": last_seq,
                "rows": [[r.seq, r.key, r.step, r.value, r.received] for r in rows] if rows is not None else None}
        text = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
        if len(text.encode()) > PAYLOAD_MAX - 100:          # too big to carry: hint only
            STATS.inc("notify_rows_too_big")
            item["rows"] = None
            text = json.dumps(item, separators=(",", ":"))
        return text

    async def _flush_notifies(self) -> None:
        """One background sender per process: while a NOTIFY is in flight, new ones accumulate and
        go out together. Losing one (crash before it is sent) costs at most POLL_INTERVAL."""
        while self._pending:
            batch, self._pending = self._pending, {}
            items = [self._item(rid, p[2], p[0], p[1]) for rid, p in batch.items()]
            STATS.inc("notify_batches")
            chunk: list[str] = []
            size = 0
            for it in items + [None]:  # type: ignore[list-item]
                n = len(it.encode()) + 1 if it is not None else 0
                if chunk and (it is None or size + n > PAYLOAD_MAX):
                    payload = '{"p":%d,"i":[%s]}' % (PID, ",".join(chunk))
                    try:
                        await db.execute("SELECT pg_notify($1, $2)", CHANNEL, payload, lane=db.HUB)
                    except Exception:
                        STATS.inc("notify_errors")
                        log.exception("notify failed")
                    chunk, size = [], 0
                if it is not None:
                    chunk.append(it)
                    size += n

    def _on_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        try:
            msg = json.loads(payload)
            if msg["p"] == PID:
                return                       # our own: already pushed to our tails
            for it in msg["i"]:
                entry = self._tails.get(UUID(it["r"]))
                if entry is None:
                    continue
                rows = None if it["rows"] is None else [Row(*r) for r in it["rows"]]
                entry[0].push(UUID(it["j"]), rows, it["s"])
        except (ValueError, KeyError, TypeError):
            STATS.inc("notify_bad_payload")
            self._wake_all()                 # unknown content: read everything from the DB

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
                    await self._conn.add_listener(KEY_CHANNEL, self._on_key_notify)
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
