from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, AsyncIterator
from uuid import UUID

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from . import db
from .auth import Auth, key_is_active, new_key, require_admin, require_project
from .errors import NOT_FOUND, ApiError
from .hub import HUB
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


app.add_middleware(AccessMiddleware)


@app.exception_handler(ApiError)
async def api_error_handler(_req: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse({"error": exc.code}, status_code=exc.status)


@app.exception_handler(RequestValidationError)
async def validation_handler(_req: Request, exc: RequestValidationError) -> JSONResponse:
    # Field locations and messages only; never echo input values back.
    details = [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse({"error": "invalid", "details": details[:20]}, status_code=422)


@app.exception_handler(Exception)
async def internal_handler(req: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error rid=%s", req.state.rid)
    return JSONResponse({"error": "internal", "request_id": req.state.rid}, status_code=500)


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
    async with db.pool().acquire() as conn, conn.transaction():
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
    async with db.pool().acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO api_keys (id, project_id, key_hash, prefix) VALUES ($1, $2, $3, $4)",
            uuid.uuid4(), auth.project_id, digest, prefix,
        )
        await conn.execute("UPDATE api_keys SET revoked_at = now() WHERE id = $1", auth.key_id)
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
    async with db.project_tx(auth.project_id) as conn:
        # Row lock serializes all writers of this run: seqs are assigned and
        # committed strictly in order, so readers never see a hole that fills later.
        last_seq = await conn.fetchval(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE",
            rid, auth.project_id,
        )
        if last_seq is None:
            raise NOT_FOUND

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
        duplicates = conflicts = 0
        for e in body.events:
            k = (e.key, e.step)
            if k in seen:
                if seen[k] == e.value:
                    duplicates += 1
                else:
                    conflicts += 1
            else:
                seen[k] = e.value
                new.append(e)

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
        if new or conflicts:
            await conn.execute(
                "UPDATE runs SET last_seq = $2, conflicts = conflicts + $3 WHERE id = $1",
                rid, last_seq, conflicts,
            )

    if new or conflicts:
        # NOTIFY after commit, in its own statement. First version notified inside the ingest
        # transaction: Postgres holds a global notify-queue lock from PreCommit until the commit
        # record is fsynced, so every ingest committed one at a time (load test: 19 sessions in
        # wait_event "Lock/object", ~300 events/s). Losing this hint (crash right here) costs at
        # most POLL_INTERVAL of latency, never data: streams read from the DB.
        HUB.wake(rid)
        try:
            await db.pool().execute("SELECT pg_notify('run_events', $1)", f"{rid}:{last_seq}")
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
    after: Annotated[int, Query(ge=0)] = 0,
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


# --- live stream (SSE) ---------------------------------------------------------------


def sse(event: str, data: object, event_id: int | None = None) -> str:
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"


@app.get("/api/runs/{run_id}/stream")
async def stream(
    run_id: str,
    auth: AuthDep,
    last_event_id: Annotated[str | None, Header()] = None,
    after: Annotated[int | None, Query(ge=0)] = None,
) -> StreamingResponse:
    rid = parse_id(run_id)
    try:
        cursor = int(last_event_id) if last_event_id is not None else (after or 0)
    except ValueError:
        raise ApiError(422, "invalid") from None
    if cursor < 0:
        raise ApiError(422, "invalid")

    async with db.project_tx(auth.project_id) as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id
        )
    if exists is None:
        raise NOT_FOUND

    async def gen() -> AsyncIterator[str]:
        nonlocal cursor
        wake = HUB.subscribe(rid)
        STATS.gauge("streams_open", 1)
        last_key_check = last_write = time.monotonic()
        status: tuple[int, int] | None = None
        try:
            yield sse("hello", {"after": cursor})
            while True:
                # Clear BEFORE reading: a NOTIFY that lands during the query sets it again.
                wake.clear()
                async with db.project_tx(auth.project_id) as conn:
                    run = await conn.fetchrow(
                        "SELECT last_seq, conflicts FROM runs WHERE id = $1 AND project_id = $2",
                        rid, auth.project_id,
                    )
                    rows = await conn.fetch(
                        """SELECT seq, metric_key, step, value FROM metrics
                            WHERE run_id = $1 AND project_id = $2 AND seq > $3 ORDER BY seq LIMIT $4""",
                        rid, auth.project_id, cursor, STREAM_PAGE,
                    )
                if run is None:  # cannot happen today (no deletes), but never stream a ghost
                    return
                if rows:
                    chunk = []
                    for r in rows:
                        chunk.append(sse("metric", event_json(r), r["seq"]))
                        cursor = r["seq"]
                    yield "".join(chunk)
                    STATS.inc("stream_events_sent", len(rows))
                    last_write = time.monotonic()
                new_status = (run["last_seq"], run["conflicts"])
                if new_status != status:
                    status = new_status
                    yield sse("status", {"last_seq": status[0], "conflicts": status[1]})
                if len(rows) == STREAM_PAGE:
                    continue  # more backlog, no waiting

                try:
                    await asyncio.wait_for(wake.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    STATS.inc("stream_polls")

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
            HUB.unsubscribe(rid, wake)
            STATS.gauge("streams_open", -1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


# --- pages ---------------------------------------------------------------------------


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    await db.pool().fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/")
async def page_index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/runs/{run_id}")
async def page_run(run_id: str) -> FileResponse:  # data is fetched by JS with the key
    return FileResponse(STATIC / "run.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
