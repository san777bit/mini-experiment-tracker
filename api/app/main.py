from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
import unicodedata
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, AsyncIterator
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from . import db
from .auth import (KEYS, Auth, bearer_token, hash_key, key_is_active, new_key, recheck_key_in_tx,
                   require_admin, require_project, require_project_for_write)
from .errors import NOT_FOUND, UNAUTHORIZED, ApiError
from .hub import HUB, Row
from . import limits
from .limits import BUCKETS, BYTE_BUCKETS, RUN_QUEUES, write_slot
from .stats import STATS, TIMERS, loop_lag_monitor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

MAX_BATCH = int(os.environ.get("MAX_BATCH", "1000"))
STREAM_PAGE = int(os.environ.get("STREAM_PAGE", "500"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
KEY_RECHECK = float(os.environ.get("KEY_RECHECK", "5.0"))
HEARTBEAT = float(os.environ.get("HEARTBEAT", "15.0"))
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await db.init_pool()
    await HUB.start()
    lag = asyncio.create_task(loop_lag_monitor())
    # Pool/gate sampling every 50 ms is for load diagnostics only (PERF.md); off by default.
    gate_mon = asyncio.create_task(_gate_monitor()) if os.environ.get("DIAG") == "1" else None
    yield
    lag.cancel()
    if gate_mon:
        gate_mon.cancel()
    await HUB.stop()
    await db.close_pool()


async def _gate_monitor() -> None:
    """Diagnostics: connections in use / tenants waiting, sampled every 50 ms."""
    while True:
        await asyncio.sleep(0.05)
        TIMERS.add("gate_used", db.GATE.used)
        TIMERS.add("gate_waiters", sum(len(q) for q in db.GATE.waiting.values()))
        TIMERS.add("pool_idle", db.pool().get_idle_size())


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


# --- middleware: request id, no-store, access log without secrets -------------


class AccessMiddleware:
    def __init__(self, inner: ASGIApp) -> None:
        self.inner = inner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.inner(scope, receive, send)
            return
        rid = uuid.uuid4().hex[:12]
        scope.setdefault("state", {})["rid"] = rid
        started = time.perf_counter()
        status = 0

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = MutableHeaders(scope=message)
                headers["x-request-id"] = rid
                if scope["path"].startswith("/api"):
                    headers["cache-control"] = "no-store"
            await send(message)

        try:
            await self.inner(scope, receive, send_wrapper)
        finally:
            if scope["method"] == "POST" and scope["path"].endswith("/metrics"):
                TIMERS.add(f"request_ingest_{status}", (time.perf_counter() - started) * 1000)
            # Path only (no query string, no headers, no body): keys travel in a header.
            if ACCESS_LOG:
                log.info(
                    "%s %s %s %.1fms rid=%s",
                    scope["method"], scope["path"], status,
                    (time.perf_counter() - started) * 1000, rid,
                )


ACCESS_LOG = os.environ.get("ACCESS_LOG", "1") == "1"
MAX_BODY = int(os.environ.get("MAX_BODY_BYTES", str(2 * 1024 * 1024)))  # 1000 events need ~250 KB


class BodyLimitMiddleware:
    """Reject oversized bodies before they are buffered and parsed.

    Found by test_huge_body_is_rejected_before_it_is_parsed: a 200 MB body was read, json-decoded
    and validated (422 after 1.6 s). Content-Length is checked up front; chunked bodies are counted
    as they stream in and cut off at the limit.
    """

    def __init__(self, inner: ASGIApp) -> None:
        self.inner = inner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.inner(scope, receive, send)
            return
        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    too_big = int(value) > MAX_BODY
                except ValueError:
                    too_big = True
                if too_big:
                    STATS.inc("bodies_rejected_too_large")
                    await JSONResponse({"error": "body_too_large"}, status_code=413)(scope, receive, send)
                    return
        if scope["method"] not in ("POST", "PUT", "PATCH"):
            await self.inner(scope, receive, send)
            return
        # Chunked (no Content-Length): read up to the limit here. Raising from inside the app's
        # receive does not work: FastAPI turns any body-read error into a 400 (first attempt).
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body += message.get("body", b"")
            if len(body) > MAX_BODY:
                STATS.inc("bodies_rejected_too_large")
                await JSONResponse({"error": "body_too_large"}, status_code=413)(scope, receive, send)
                return
            if not message.get("more_body", False):
                break
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.inner(scope, replay, send)


METRICS_PATH = re.compile(r"^/api/runs/([0-9a-fA-F-]{32,36})/metrics$")


class RateLimitMiddleware:
    """Cheap rejections, before the body is read and before the DB is touched.

    Found by load/stress_hot_run.py (round 3 baseline): a client that ignores 429 still cost a
    DB key lookup, a full body read and JSON validation per request; the quiet project's reads
    went to p99 393 ms, and to 1.9 s when the ignored requests carried 1.9 MB bodies.
    Here, from the Authorization header alone: a key the DB already rejected -> 401; a key whose
    project is out of budget -> 429; a write to a run whose queue is full -> 429. A key this
    process has not seen yet passes through to the normal, DB-backed check.
    """

    def __init__(self, inner: ASGIApp) -> None:
        self.inner = inner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if scope["type"] != "http" or not path.startswith("/api/") or path.startswith("/api/admin/"):
            await self.inner(scope, receive, send)
            return
        header = next((v for k, v in scope["headers"] if k == b"authorization"), None)
        token = bearer_token(header.decode("latin-1")) if header is not None else None
        if token is not None:
            digest = hash_key(token)
            if KEYS.is_bad(digest):
                STATS.inc("rejected_early_bad_key")
                await JSONResponse({"error": UNAUTHORIZED.code}, status_code=401)(scope, receive, send)
                return
            project = KEYS.project_of(digest)
            if project is not None:
                code = None
                has_body = scope["method"] in ("POST", "PUT", "PATCH")
                length = next((v for k, v in scope["headers"] if k == b"content-length"), None)
                size = int(length) if length is not None and length.isdigit() else None
                if size is not None and size > MAX_BODY:
                    # Found by test_huge_body_is_rejected_before_it_is_parsed: the byte budget answered
                    # 429 (retry later) to a body that can never be accepted, so a client would retry it
                    # forever. Oversized is a permanent 413, decided by BodyLimitMiddleware.
                    has_body = False
                if not BUCKETS.allow(project):
                    code = "rate_limited"
                elif has_body and not BYTE_BUCKETS.allow(project, size or 0):
                    code = "rate_limited"
                elif scope["method"] == "POST" and (m := METRICS_PATH.match(path)):
                    try:
                        if RUN_QUEUES.full(UUID(m.group(1)), project):
                            code = "run_busy"
                    except ValueError:
                        pass
                if code is not None:
                    STATS.inc(f"rejected_early_{code}")
                    await self._tarpit()
                    await JSONResponse({"error": code}, status_code=429,
                                       headers={"Retry-After": "1"})(scope, receive, send)
                    return
                if has_body and size is None:
                    receive = self._metered(receive, project)   # chunked: charge what arrives
        await self.inner(scope, receive, send)

    _sleeping = 0

    async def _tarpit(self) -> None:
        cls = type(self)
        if limits.REJECT_DELAY <= 0 or cls._sleeping >= limits.TARPIT_MAX:
            return
        cls._sleeping += 1
        try:
            await asyncio.sleep(limits.REJECT_DELAY)
        finally:
            cls._sleeping -= 1

    @staticmethod
    def _metered(receive: Receive, project: UUID) -> Receive:
        async def wrapped() -> Message:
            message = await receive()
            if message["type"] == "http.request":
                BYTE_BUCKETS.charge(project, len(message.get("body", b"")))
            return message
        return wrapped


app.add_middleware(BodyLimitMiddleware)
app.add_middleware(RateLimitMiddleware)   # outside BodyLimit: decides before the body is read
app.add_middleware(AccessMiddleware)


@app.exception_handler(ApiError)
async def api_error_handler(_req: Request, exc: ApiError) -> JSONResponse:
    headers = {"Retry-After": "1"} if exc.status in (429, 503) else None
    return JSONResponse({"error": exc.code}, status_code=exc.status, headers=headers)


@app.exception_handler(RequestValidationError)
async def validation_handler(_req: Request, exc: RequestValidationError) -> JSONResponse:
    # Field locations and messages only; never echo input values back.
    details = [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse({"error": "invalid", "details": details[:20]}, status_code=422)


DB_UNAVAILABLE = (TimeoutError, OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError,
                  asyncpg.CannotConnectNowError)


async def db_unavailable_handler(req: Request, exc: Exception) -> JSONResponse:
    # A fast, explicit, retryable answer instead of a hung request or an opaque 500.
    STATS.inc("db_unavailable")
    log.warning("db unavailable rid=%s: %s", req.state.rid, type(exc).__name__)
    return JSONResponse({"error": "db_unavailable"}, status_code=503, headers={"Retry-After": "1"})


for _exc in DB_UNAVAILABLE:
    app.add_exception_handler(_exc, db_unavailable_handler)


@app.exception_handler(Exception)
async def internal_handler(req: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error rid=%s", req.state.rid)
    return JSONResponse({"error": "internal", "request_id": req.state.rid}, status_code=500)


BIGINT_MAX = 2**63 - 1   # seq is bigint; larger cursors overflowed in asyncpg as a 500


def parse_id(raw: str) -> UUID:
    try:
        return UUID(raw)
    except ValueError:
        raise NOT_FOUND from None


AuthDep = Annotated[Auth, Depends(require_project)]
# Only for endpoints that re-check the key inside their write transaction.
WriteAuthDep = Annotated[Auth, Depends(require_project_for_write)]


# --- admin / keys ----------------------------------------------------------------


class ProjectIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.post("/api/admin/projects", status_code=201, dependencies=[Depends(require_admin)])
async def create_project(body: ProjectIn) -> dict[str, str]:
    project_id = uuid.uuid4()
    plain, prefix, digest = new_key()
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("INSERT INTO projects (id, name) VALUES ($1, $2)", project_id, body.name)
        await conn.execute(
            "INSERT INTO api_keys (id, project_id, key_hash, prefix) VALUES ($1, $2, $3, $4)",
            uuid.uuid4(), project_id, digest, prefix,
        )
    return {"project_id": str(project_id), "api_key": plain}


@app.get("/api/admin/stats", dependencies=[Depends(require_admin)])
async def stats() -> dict[str, int]:
    return STATS.snapshot()


@app.get("/api/admin/missing-points", dependencies=[Depends(require_admin)])
async def missing_points(quiet_seconds: Annotated[int, Query(ge=0, le=86400)] = 60) -> dict[str, object]:
    """Alerting hook: runs whose producer reported more points than are stored and has been quiet
    for quiet_seconds (in-flight batches are not losses). Ids and counts only, no metric data."""
    rows = await db.fetch("SELECT * FROM runs_missing_points(make_interval(secs => $1))", quiet_seconds)
    return {"runs": [{"run_id": str(r["run_id"]), "project_id": str(r["project_id"]),
                      "stored": r["stored"], "reported": r["reported"],
                      "missing": r["reported"] - r["stored"],
                      "quiet_since": r["quiet_since"].isoformat()} for r in rows]}


@app.get("/api/admin/timings", dependencies=[Depends(require_admin)])
async def timings(reset: bool = False) -> dict[str, object]:
    """This worker's phase timings (call repeatedly to reach every worker; `pid` tells them apart)."""
    snap = TIMERS.snapshot()
    if reset:
        TIMERS.reset()
        return {"pid": snap["pid"], "reset": True}
    return snap


@app.post("/api/keys/rotate")
async def rotate_key(auth: AuthDep) -> dict[str, str]:
    plain, prefix, digest = new_key()
    async with db.acquire() as conn, conn.transaction():
        # Revoke first and only if still active. Found by test_concurrent_rotations...: 10 parallel
        # rotations with one key used to mint 5 valid keys. Now the row lock makes the losers see
        # revoked_at IS NOT NULL after the winner commits, and they get 401.
        revoked = await conn.fetchval(
            "UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING id",
            auth.key_id,
        )
        if revoked is None:
            raise ApiError(401, "unauthorized")
        await conn.execute(
            "INSERT INTO api_keys (id, project_id, key_hash, prefix) VALUES ($1, $2, $3, $4)",
            uuid.uuid4(), auth.project_id, digest, prefix,
        )
    STATS.inc("key_rotations")
    HUB.key_revoked(auth.key_id, announce=True)   # close this key's open streams in every process now
    return {"api_key": plain}


# --- runs ----------------------------------------------------------------------------


class RunIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


def run_json(row: object) -> dict[str, object]:
    r = dict(row)  # type: ignore[call-overload]
    return {
        "id": str(r["id"]),
        "name": r["name"],
        "last_seq": r["last_seq"],
        "conflicts": r["conflicts"],
        "created_at": r["created_at"].isoformat(),
        # Loss detection (only if the producer sends client_points): points the producer says it
        # generated but the server has not stored. Briefly > 0 while batches are in flight.
        "client_points": r["client_points"],
        "unconfirmed": max(0, r["client_points"] - r["last_seq"]) if r["client_points"] is not None else None,
    }


RUN_COLS = "id, name, last_seq, conflicts, created_at, client_points"


@app.post("/api/runs", status_code=201)
async def create_run(body: RunIn, auth: WriteAuthDep) -> dict[str, object]:
    async with db.project_tx(auth.project_id) as conn:
        await recheck_key_in_tx(conn, auth.key_id)
        row = await conn.fetchrow(
            f"INSERT INTO runs (id, project_id, name) VALUES ($1, $2, $3) RETURNING {RUN_COLS}",
            uuid.uuid4(), auth.project_id, body.name,
        )
    return run_json(row)


@app.get("/api/runs")
async def list_runs(auth: AuthDep) -> dict[str, object]:
    async with db.project_tx(auth.project_id) as conn:
        rows = await conn.fetch(
            f"SELECT {RUN_COLS} FROM runs WHERE project_id = $1 ORDER BY created_at DESC LIMIT 200",
            auth.project_id,
        )
    return {"runs": [run_json(r) for r in rows]}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str, auth: AuthDep) -> dict[str, object]:
    rid = parse_id(run_id)
    async with db.project_tx(auth.project_id) as conn:
        row = await conn.fetchrow(
            f"SELECT {RUN_COLS} FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id
        )
    if row is None:
        raise NOT_FOUND
    return run_json(row)


# --- ingestion -----------------------------------------------------------------------


class Event(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    step: int = Field(ge=0, le=2**53)
    value: float
    ts: datetime | None = None

    @field_validator("value")
    @classmethod
    def finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be finite")
        return v

    @field_validator("key")
    @classmethod
    def storable_key(cls, v: str) -> str:
        # NFC: "é" composed and decomposed render identically but were stored as two metrics
        # (test_unicode_keys_that_look_identical_are_one_metric). Homoglyphs across scripts
        # (Latin "o" vs Cyrillic "о") are NOT merged: that would need a confusables table.
        v = unicodedata.normalize("NFC", v)
        # Found by test_partial_write...: "\u0000" is valid JSON but not valid Postgres text,
        # so it failed inside the transaction as a 500. Clients retry 5xx, so one such event
        # blocked a producer forever. Reject at the boundary with a 4xx instead.
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in v):
            raise ValueError("key must not contain control characters")
        try:
            v.encode("utf-8")
        except UnicodeEncodeError:   # lone surrogates, e.g. "\ud800"
            raise ValueError("key must be valid unicode") from None
        return v


class BatchIn(BaseModel):
    batch_id: UUID
    events: list[Event] = Field(min_length=1, max_length=MAX_BATCH)
    # Optional: distinct (key, step) points this producer has generated for the run so far,
    # including this batch. Lets the server notice events that never arrived (see runs_missing_points).
    client_points: int | None = Field(default=None, ge=0, le=2**53)


@app.post("/api/runs/{run_id}/metrics")
async def ingest(run_id: str, body: BatchIn, auth: WriteAuthDep) -> dict[str, int]:
    rid = parse_id(run_id)
    t0 = time.perf_counter()
    async with write_slot(auth.project_id, rid):
        t1 = time.perf_counter()
        TIMERS.add("ingest_run_queue", (t1 - t0) * 1000)
        # One round trip (db/init.sql: ingest_batch). Round 2 used 6-7 statements per batch while
        # holding a pooled connection and the run's row lock; the function runs the same steps
        # (row lock -> key re-check -> classify -> insert -> bump last_seq) as one autocommitted
        # statement. Each statement inside a plpgsql function takes a fresh snapshot, so the reads
        # after the row lock see everything the previous writer of this run committed.
        async with db.acquire(auth.project_id) as conn:
            t2 = time.perf_counter()
            r = await conn.fetchrow(
                """SELECT * FROM ingest_batch($1, $2, $3, $4,
                       $5::text[], $6::bigint[], $7::float8[], $8::timestamptz[], $9::bigint)""",
                rid, auth.project_id, auth.key_id, body.batch_id,
                [e.key for e in body.events], [e.step for e in body.events],
                [e.value for e in body.events], [e.ts for e in body.events], body.client_points,
            )
            TIMERS.add("ingest_fn", (time.perf_counter() - t2) * 1000)
    if r["o_status"] == "not_found":
        raise NOT_FOUND
    if r["o_status"] == "revoked":
        KEYS.revoked(auth.key_id)
        STATS.inc("writes_rejected_revoked_in_flight")
        raise UNAUTHORIZED
    accepted, duplicates = r["o_accepted"], r["o_duplicates"]
    conflicts, new_conflicts, last_seq = r["o_conflicts"], r["o_new_conflicts"], r["o_last_seq"]
    new = accepted

    if new or new_conflicts:
        # Hand the committed rows to the live tails (hub.publish). Seqs: the accepted events,
        # in input order, got last_seq-accepted+1 .. last_seq. A new conflict changes the run's
        # counter, which only the DB has: send "read the DB" instead of rows.
        rows = None
        if not new_conflicts and r["o_new_ords"]:
            base, now = last_seq - accepted, time.time()
            rows = [Row(base + i + 1, body.events[o - 1].key, body.events[o - 1].step,
                        body.events[o - 1].value, now) for i, o in enumerate(r["o_new_ords"])]
        # NOTIFY after commit, in its own statement. First version notified inside the ingest
        # transaction: Postgres holds a global notify-queue lock from PreCommit until the commit
        # record is fsynced, so every ingest committed one at a time (load test: 19 sessions in
        # wait_event "Lock/object", ~300 events/s). Losing this hint (crash right here) costs at
        # most POLL_INTERVAL of latency, never data: streams read from the DB.
        HUB.publish(rid, auth.project_id, rows, last_seq)   # NOTIFY goes out off the request path, batched
    TIMERS.add("ingest_handler", (time.perf_counter() - t0) * 1000)

    STATS.inc("ingest_batches")
    STATS.inc("events_accepted", accepted)
    STATS.inc("events_duplicate", duplicates)
    STATS.inc("events_conflict", conflicts)
    if conflicts:
        log.warning("conflicting events run=%s batch=%s n=%d", rid, body.batch_id, conflicts)
    return {"accepted": accepted, "duplicates": duplicates, "conflicts": conflicts, "last_seq": last_seq}


def event_json(r: object) -> dict[str, object]:
    row = dict(r)  # type: ignore[call-overload]
    return {"seq": row["seq"], "key": row["metric_key"], "step": row["step"], "value": row["value"]}


@app.get("/api/runs/{run_id}/metrics")
async def read_metrics(
    run_id: str,
    auth: AuthDep,
    after: Annotated[int, Query(ge=0, le=BIGINT_MAX)] = 0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
) -> dict[str, object]:
    rid = parse_id(run_id)
    async with db.project_tx(auth.project_id) as conn:
        run = await conn.fetchrow(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id
        )
        if run is None:
            raise NOT_FOUND
        rows = await conn.fetch(
            """SELECT seq, metric_key, step, value FROM metrics
                WHERE run_id = $1 AND project_id = $2 AND seq > $3 ORDER BY seq LIMIT $4""",
            rid, auth.project_id, after, limit,
        )
    return {"last_seq": run["last_seq"], "events": [event_json(r) for r in rows]}


@app.get("/api/runs/{run_id}/conflicts")
async def read_conflicts(run_id: str, auth: AuthDep) -> dict[str, object]:
    """Values rejected by first-write-wins: the audit trail for 'did we drop anything?'."""
    rid = parse_id(run_id)
    async with db.project_tx(auth.project_id) as conn:
        if await conn.fetchval(
            "SELECT 1 FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id
        ) is None:
            raise NOT_FOUND
        rows = await conn.fetch(
            """SELECT c.metric_key, c.step, c.value AS rejected, m.value AS stored, c.seen_at
                 FROM metric_conflicts c
                 JOIN metrics m ON m.run_id = c.run_id AND m.metric_key = c.metric_key AND m.step = c.step
                WHERE c.run_id = $1 AND c.project_id = $2 ORDER BY c.seen_at LIMIT 1000""",
            rid, auth.project_id,
        )
    return {"conflicts": [
        {"key": r["metric_key"], "step": r["step"], "rejected": r["rejected"], "stored": r["stored"],
         "seen_at": r["seen_at"].isoformat()} for r in rows
    ]}


# --- live stream (SSE) ---------------------------------------------------------------


def row_json(r: Row) -> dict[str, object]:
    return {"seq": r.seq, "key": r.key, "step": r.step, "value": r.value}


def sse(event: str, data: object, event_id: int | None = None) -> str:
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"


@app.get("/api/runs/{run_id}/stream")
async def stream(
    run_id: str,
    auth: AuthDep,
    last_event_id: Annotated[str | None, Header()] = None,
    after: Annotated[int | None, Query(ge=0, le=BIGINT_MAX)] = None,
) -> StreamingResponse:
    rid = parse_id(run_id)
    # ASCII digits only: Python int() also accepts "+5", "1_000", " 5" and non-ASCII digits.
    if last_event_id is not None and not (last_event_id.isascii() and last_event_id.isdigit()):
        raise ApiError(422, "invalid")
    cursor = int(last_event_id) if last_event_id is not None else (after or 0)
    if cursor > BIGINT_MAX:
        raise ApiError(422, "invalid")

    async with db.project_tx(auth.project_id) as conn:
        start_seq = await conn.fetchval(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id
        )
    if start_seq is None:
        raise NOT_FOUND

    async def gen() -> AsyncIterator[str]:
        nonlocal cursor
        tail = await HUB.acquire(rid, auth.project_id, start_seq)
        revoked = HUB.watch_key(auth.key_id, tail)
        STATS.gauge("streams_open", 1)
        last_key_check = last_write = time.monotonic()
        status: tuple[int, int] | None = None
        try:
            yield sse("hello", {"after": cursor})
            while True:
                if revoked.is_set():
                    # Rotation announced (same or another process): close before sending more.
                    STATS.inc("streams_closed_revoked_push")
                    yield sse("revoked", {})
                    return
                # Grab the change signal BEFORE reading, so a change during the read is not missed.
                changed = tail.changed()
                behind = cursor < tail.first_seq - 1
                if behind:
                    # Older than the shared in-memory window: page through the DB ourselves.
                    tc = time.perf_counter()
                    async with db.project_tx(auth.project_id) as conn:
                        recs = await conn.fetch(
                            """SELECT seq, metric_key, step, value FROM metrics
                                WHERE run_id = $1 AND project_id = $2 AND seq > $3 ORDER BY seq LIMIT $4""",
                            rid, auth.project_id, cursor, STREAM_PAGE,
                        )
                    rows = [Row(r["seq"], r["metric_key"], r["step"], r["value"]) for r in recs]
                    STATS.inc("stream_db_catchup_pages")
                    TIMERS.add("catchup_page_db", (time.perf_counter() - tc) * 1000)
                else:
                    rows = tail.after(cursor, STREAM_PAGE)
                if rows:
                    if rows[0].received:   # oldest row of the batch waited longest
                        TIMERS.add("txn_start_to_sse", (time.time() - rows[0].received) * 1000)
                    ty = time.perf_counter()
                    yield "".join(sse("metric", row_json(r), r.seq) for r in rows)
                    TIMERS.add("sse_send", (time.perf_counter() - ty) * 1000)
                    cursor = rows[-1].seq
                    STATS.inc("stream_events_sent", len(rows))
                    last_write = time.monotonic()
                if tail.status != status:
                    status = tail.status
                    yield sse("status", {"last_seq": status[0], "conflicts": status[1]})
                if rows and (behind or len(rows) == STREAM_PAGE):
                    continue  # more backlog, no waiting

                try:
                    await asyncio.wait_for(changed.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    STATS.inc("stream_idle_ticks")

                now = time.monotonic()
                # Fallback for a revocation announced while this process's listener was down.
                if now - last_key_check >= KEY_RECHECK:
                    last_key_check = now
                    if not await key_is_active(auth.key_id, auth.project_id):
                        STATS.inc("streams_closed_revoked")
                        yield sse("revoked", {})
                        return
                if now - last_write >= HEARTBEAT:
                    yield ": hb\n\n"
                    last_write = now
        finally:
            HUB.unwatch_key(auth.key_id, revoked)
            HUB.release(rid)
            STATS.gauge("streams_open", -1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


# --- pages ---------------------------------------------------------------------------


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    await db.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/")
async def page_index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/runs/{run_id}")
async def page_run(run_id: str) -> FileResponse:  # data is fetched by JS with the key
    return FileResponse(STATIC / "run.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
