from __future__ import annotations

import asyncio
import json
import logging
import math
import os
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
from .auth import Auth, key_is_active, new_key, recheck_key_in_tx, require_admin, require_project
from .errors import NOT_FOUND, ApiError
from .hub import HUB, Row
from .limits import read_slot, write_slot
from .stats import STATS

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
    yield
    await HUB.stop()
    await db.close_pool()


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
            # Path only (no query string, no headers, no body): keys travel in a header.
            log.info(
                "%s %s %s %.1fms rid=%s",
                scope["method"], scope["path"], status,
                (time.perf_counter() - started) * 1000, rid,
            )


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


app.add_middleware(BodyLimitMiddleware)
app.add_middleware(AccessMiddleware)


@app.exception_handler(ApiError)
async def api_error_handler(_req: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse({"error": exc.code}, status_code=exc.status)


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
    }


RUN_COLS = "id, name, last_seq, conflicts, created_at"


@app.post("/api/runs", status_code=201)
async def create_run(body: RunIn, auth: AuthDep) -> dict[str, object]:
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


@app.post("/api/runs/{run_id}/metrics")
async def ingest(run_id: str, body: BatchIn, auth: AuthDep) -> dict[str, int]:
    rid = parse_id(run_id)
    async with write_slot(auth.project_id, rid), db.project_tx(auth.project_id) as conn:
        # Row lock serializes all writers of this run: seqs are assigned and
        # committed strictly in order, so readers never see a hole that fills later.
        last_seq = await conn.fetchval(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE",
            rid, auth.project_id,
        )
        if last_seq is None:
            raise NOT_FOUND
        await recheck_key_in_tx(conn, auth.key_id)

        existing = await conn.fetch(
            """SELECT m.metric_key, m.step, m.value
                 FROM metrics m
                 JOIN unnest($2::text[], $3::bigint[]) AS u(k, s)
                   ON m.metric_key = u.k AND m.step = u.s
                WHERE m.run_id = $1""",
            rid, [e.key for e in body.events], [e.step for e in body.events],
        )
        seen: dict[tuple[str, int], float] = {(r["metric_key"], r["step"]): r["value"] for r in existing}
        new: list[Event] = []
        rejected: list[Event] = []
        duplicates = 0
        for e in body.events:
            k = (e.key, e.step)
            if k in seen:
                if seen[k] == e.value:
                    duplicates += 1
                else:
                    rejected.append(e)
            else:
                seen[k] = e.value
                new.append(e)
        conflicts = len(rejected)          # in this request (what the client sent)
        new_conflicts = 0                  # distinct rejected values never seen before
        if rejected:
            inserted = await conn.fetch(
                """INSERT INTO metric_conflicts (run_id, project_id, metric_key, step, value, batch_id)
                   SELECT $1, $2, u.k, u.s, u.v, $3
                     FROM unnest($4::text[], $5::bigint[], $6::float8[]) AS u(k, s, v)
                   ON CONFLICT DO NOTHING RETURNING 1""",
                rid, auth.project_id, body.batch_id,
                [e.key for e in rejected], [e.step for e in rejected], [e.value for e in rejected],
            )
            new_conflicts = len(inserted)

        if new:
            await conn.execute(
                """INSERT INTO metrics (run_id, seq, project_id, metric_key, step, value, client_ts, batch_id)
                   SELECT $1, $2 + u.ord, $3, u.k, u.s, u.v, u.t, $4
                     FROM unnest($5::text[], $6::bigint[], $7::float8[], $8::timestamptz[])
                          WITH ORDINALITY AS u(k, s, v, t, ord)""",
                rid, last_seq, auth.project_id, body.batch_id,
                [e.key for e in new], [e.step for e in new],
                [e.value for e in new], [e.ts for e in new],
            )
            last_seq += len(new)
        if new or new_conflicts:
            await conn.execute(
                "UPDATE runs SET last_seq = $2, conflicts = conflicts + $3 WHERE id = $1",
                rid, last_seq, new_conflicts,
            )

    if new or new_conflicts:
        # NOTIFY after commit, in its own statement. First version notified inside the ingest
        # transaction: Postgres holds a global notify-queue lock from PreCommit until the commit
        # record is fsynced, so every ingest committed one at a time (load test: 19 sessions in
        # wait_event "Lock/object", ~300 events/s). Losing this hint (crash right here) costs at
        # most POLL_INTERVAL of latency, never data: streams read from the DB.
        HUB.wake(rid)
        try:
            await db.execute("SELECT pg_notify('run_events', $1)", f"{rid}:{last_seq}")
        except Exception:
            STATS.inc("notify_errors")
            log.exception("notify failed run=%s", rid)

    STATS.inc("ingest_batches")
    STATS.inc("events_accepted", len(new))
    STATS.inc("events_duplicate", duplicates)
    STATS.inc("events_conflict", conflicts)
    if conflicts:
        log.warning("conflicting events run=%s batch=%s n=%d", rid, body.batch_id, conflicts)
    return {"accepted": len(new), "duplicates": duplicates, "conflicts": conflicts, "last_seq": last_seq}


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
        STATS.gauge("streams_open", 1)
        last_key_check = last_write = time.monotonic()
        status: tuple[int, int] | None = None
        try:
            yield sse("hello", {"after": cursor})
            while True:
                # Grab the change signal BEFORE reading, so a change during the read is not missed.
                changed = tail.changed()
                behind = cursor < tail.first_seq - 1
                if behind:
                    # Older than the shared in-memory window: page through the DB ourselves.
                    async with read_slot(auth.project_id), db.project_tx(auth.project_id) as conn:
                        recs = await conn.fetch(
                            """SELECT seq, metric_key, step, value FROM metrics
                                WHERE run_id = $1 AND project_id = $2 AND seq > $3 ORDER BY seq LIMIT $4""",
                            rid, auth.project_id, cursor, STREAM_PAGE,
                        )
                    rows = [Row(r["seq"], r["metric_key"], r["step"], r["value"]) for r in recs]
                    STATS.inc("stream_db_catchup_pages")
                else:
                    rows = tail.after(cursor, STREAM_PAGE)
                if rows:
                    yield "".join(sse("metric", row_json(r), r.seq) for r in rows)
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
                if now - last_key_check >= KEY_RECHECK:
                    last_key_check = now
                    if not await key_is_active(auth.key_id):
                        STATS.inc("streams_closed_revoked")
                        yield sse("revoked", {})
                        return
                if now - last_write >= HEARTBEAT:
                    yield ": hb\n\n"
                    last_write = now
        finally:
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
