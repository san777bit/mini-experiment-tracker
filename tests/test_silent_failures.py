"""Requirement 4: failures that do not raise an error by themselves.

Each test states the failure, how it would stay silent, and how we detect it.
"""
from __future__ import annotations

import asyncio
import json
import random
import subprocess
import time
import uuid

import asyncpg
import httpx
import pytest

from helpers import (
    ADMIN, API, DB_OWNER, ProducerStats, batch, db_points, db_run, fresh_conn_client,
    new_project, new_run, read_sse, send_until_acked,
)


def assert_contiguous(events: list[dict], first: int, last: int) -> None:
    seqs = [e["seq"] for e in events]
    assert seqs == list(range(first, last + 1)), (
        f"expected {first}..{last}, got {len(seqs)} events; "
        f"missing={sorted(set(range(first, last + 1)) - set(seqs))[:10]} "
        f"repeated={len(seqs) - len(set(seqs))}"
    )


async def test_events_lost_on_reconnect_with_concurrent_writers():
    """Silent version: seq allocated by a global sequence commits out of order; a reader
    resuming from seq N misses an event N-1 that commits after it. Nothing errors.
    Detection: many concurrent writers on ONE run + a reader that keeps disconnecting and
    resuming from Last-Event-ID; the union of what it read must be exactly 1..last_seq."""
    p = await new_project("reconnect")
    run_id = await new_run(p)
    writers, per_writer = 16, 40
    received: list[dict] = []
    done = asyncio.Event()

    async def writer(w: int) -> None:
        async with httpx.AsyncClient(base_url=API, timeout=10) as c:
            st = ProducerStats()
            for i in range(per_writer):
                await send_until_acked(c, run_id, p, batch(
                    [{"key": f"w{w}", "step": i * 3 + j, "value": float(w * 1000 + i)} for j in range(3)]), st)

    async def reader() -> None:
        async with httpx.AsyncClient(base_url=API, timeout=30) as c:
            while True:
                cursor = received[-1]["seq"] if received else 0
                # Short sessions: reconnect every ~0.2 s with Last-Event-ID = last applied seq.
                received.extend(await read_sse(c, run_id, p, after=cursor, max_seconds=random.uniform(0.05, 0.3)))
                if done.is_set():
                    truth = await db_run(run_id)
                    if received and received[-1]["seq"] >= truth["last_seq"]:
                        return

    r = asyncio.create_task(reader())
    await asyncio.gather(*(writer(w) for w in range(writers)))
    done.set()
    await asyncio.wait_for(r, timeout=30)
    run = await db_run(run_id)
    print(f"[reconnect] writers={writers} events={run['last_seq']} reader_events={len(received)}")
    assert run["last_seq"] == writers * per_writer * 3 == run["n"]
    assert_contiguous(received, 1, run["last_seq"])
    stored = await db_points(run_id)
    assert {(e["key"], e["step"]): e["value"] for e in received} == stored


async def test_duplicate_batch_sent_concurrently():
    """Same batch arriving 20 times at once (client retries racing each other)."""
    p = await new_project("dups")
    run_id = await new_run(p)
    body = batch([{"key": "loss", "step": s, "value": s * 0.5} for s in range(100)])
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        rs = await asyncio.gather(*(c.post(f"/api/runs/{run_id}/metrics", json=body, headers=p.h) for _ in range(20)))
    assert all(r.status_code == 200 for r in rs)
    accepted = sum(r.json()["accepted"] for r in rs)
    dup = sum(r.json()["duplicates"] for r in rs)
    print(f"[dups] accepted={accepted} duplicates={dup}")
    assert accepted == 100 and dup == 19 * 100
    run = await db_run(run_id)
    assert run["n"] == run["last_seq"] == 100


async def test_slow_reader_gets_everything():
    """Silent version: a bounded per-client queue drops events for a client that reads slowly.
    Our server has no per-client queue (it reads from the DB at the client's pace).
    Detection: a reader that stops reading while 20k events are written, then resumes;
    it must get all of them, in order, and the API's memory must stay bounded."""
    p = await new_project("slow")
    run_id = await new_run(p)

    def api_mem() -> str:
        return subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "taskforwork-api-1"],
                              capture_output=True, text=True).stdout.strip()

    mem_before = api_mem()
    total = 20_000
    async with httpx.AsyncClient(base_url=API, timeout=60) as c:
        async with c.stream("GET", f"/api/runs/{run_id}/stream", headers={**p.h, "last-event-id": "0"}) as s:
            lines = s.aiter_lines()
            # Open the stream, then stop reading while we write.
            assert (await lines.__anext__()).startswith("event: hello")
            for i in range(0, total, 1000):
                r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch(
                    [{"key": "k", "step": s, "value": float(s)} for s in range(i, i + 1000)]))
                assert r.status_code == 200
            await asyncio.sleep(3)
            mem_stalled = api_mem()
            events, ev = [], None
            t0 = time.monotonic()
            async for line in lines:
                if line.startswith("event: "):
                    ev = line[7:]
                elif line.startswith("data: ") and ev == "metric":
                    import json
                    events.append(json.loads(line[6:]))
                    if events[-1]["seq"] == total:
                        break
                if time.monotonic() - t0 > 60:
                    break
    print(f"[slow] api mem before={mem_before} while stalled={mem_stalled}; received={len(events)}")
    assert_contiguous(events, 1, total)


@pytest.mark.parametrize("bad_key", ["bad\u0000key", "bad\ud800key"])
async def test_partial_write_is_impossible_bad_event_mid_batch(bad_key: str):
    """A batch whose 3rd event cannot be stored (NUL: valid JSON, invalid Postgres text;
    lone surrogate: valid JSON escape, not encodable as UTF-8).
    Expected: the whole batch rejected, nothing stored, last_seq unchanged, and a 4xx so the
    client does not retry forever. First run: NUL returned 500 (atomic, but a poison batch)."""
    p = await new_project("partial")
    run_id = await new_run(p)
    body = batch([{"key": "ok", "step": 1, "value": 1.0}, {"key": "ok", "step": 2, "value": 2.0},
                  {"key": bad_key, "step": 3, "value": 3.0}, {"key": "ok", "step": 4, "value": 4.0}])
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        # httpx would refuse to encode a lone surrogate; send the JSON escape as raw bytes.
        raw = json.dumps(body, ensure_ascii=True).encode()
        r = await c.post(f"/api/runs/{run_id}/metrics", content=raw,
                         headers={**p.h, "content-type": "application/json"})
    print(f"[partial] key={bad_key!r} status={r.status_code} body={r.text}")
    run = await db_run(run_id)
    assert run["n"] == 0 and run["last_seq"] == 0, "partial batch was stored"
    assert r.status_code == 422


async def test_old_key_stops_working_after_rotation():
    """Silent version: rotation issues a new key but the old one (or an already open stream
    authenticated with it) keeps working. Detection: use the old key everywhere after rotation."""
    p = await new_project("rotate")
    run_id = await new_run(p)
    async with httpx.AsyncClient(base_url=API, timeout=30) as c:
        stream_events: list[str] = []

        async def watch() -> float:
            async with c.stream("GET", f"/api/runs/{run_id}/stream", headers=p.h) as s:
                async for line in s.aiter_lines():
                    if line.startswith("event: "):
                        stream_events.append(line[7:])
            return time.monotonic()

        watcher = asyncio.create_task(watch())
        await asyncio.sleep(0.5)
        r = await c.post("/api/keys/rotate", headers=p.h)
        assert r.status_code == 200
        rotated_at = time.monotonic()
        new_h = {"authorization": "Bearer " + r.json()["api_key"]}

        for method, path, body in [
            ("GET", "/api/runs", None),
            ("GET", f"/api/runs/{run_id}", None),
            ("POST", f"/api/runs/{run_id}/metrics", batch([{"key": "k", "step": 0, "value": 1.0}])),
            ("GET", f"/api/runs/{run_id}/stream", None),
            ("POST", "/api/keys/rotate", None),
        ]:
            old = await c.request(method, path, headers=p.h, json=body)
            assert old.status_code == 401, f"old key still works on {method} {path}"
        assert (await c.get(f"/api/runs/{run_id}", headers=new_h)).status_code == 200

        closed_at = await asyncio.wait_for(watcher, timeout=20)
    print(f"[rotate] stream opened with old key closed {closed_at - rotated_at:.1f}s after rotation; events={stream_events}")
    assert "revoked" in stream_events
    assert closed_at - rotated_at < 5.0 + 2.0 + 1.0   # KEY_RECHECK + POLL_INTERVAL + slack


async def test_lost_notify_does_not_lose_events():
    """Silent version: the LISTEN connection dies, NOTIFYs are dropped, live pages freeze while
    the DB keeps growing. Detection: kill the listener backend, write, the stream must still deliver."""
    p = await new_project("listener")
    run_id = await new_run(p)
    owner = await asyncpg.connect(DB_OWNER)
    try:
        async with httpx.AsyncClient(base_url=API, timeout=30) as c:
            reader = asyncio.create_task(read_sse(c, run_id, p, after=0, stop_at_seq=50, max_seconds=20))
            await asyncio.sleep(0.5)
            killed = await owner.fetchval(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                "WHERE usename = 'app' AND query ILIKE 'LISTEN%'")
            t0 = time.monotonic()
            for s in range(50):
                await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch([{"key": "k", "step": s, "value": 1.0}]))
            events = await reader
            elapsed = time.monotonic() - t0
            stats = (await c.get("/api/admin/stats", headers=ADMIN)).json()
    finally:
        await owner.close()
    print(f"[listener] killed={killed} delivered={len(events)} in {elapsed:.1f}s "
          f"listener_connects={stats.get('listener_connects')} polls={stats.get('stream_polls')}")
    assert killed >= 1
    assert_contiguous(events, 1, 50)


async def test_api_crash_mid_ingest_leaves_no_partial_batch():
    """kill -9 the API while big batches are in flight, restart, check invariants for every batch:
    stored entirely or not at all; seq contiguous; last_seq == row count."""
    p = await new_project("crash")
    run_id = await new_run(p)
    sent: dict[str, int] = {}
    acked: set[str] = set()

    async def hammer(t: int) -> None:
        async with httpx.AsyncClient(base_url=API, timeout=5) as c:
            i = 0
            while True:
                # Unique keys per batch (first version reused keys across tasks, so later batches
                # were deduplicated into earlier ones and the test failed for the wrong reason).
                b = batch([{"key": f"t{t}b{i}", "step": s, "value": float(s)} for s in range(1000)])
                sent[b["batch_id"]] = 1000
                try:
                    r = await c.post(f"/api/runs/{run_id}/metrics", json=b, headers=p.h)
                    if r.status_code == 200:
                        acked.add(b["batch_id"])
                except httpx.TransportError:
                    return
                i += 1

    tasks = [asyncio.create_task(hammer(t)) for t in range(4)]
    await asyncio.sleep(2.0)
    subprocess.run(["docker", "kill", "--signal", "KILL", "taskforwork-api-1"], check=True, capture_output=True)
    await asyncio.gather(*tasks, return_exceptions=True)
    subprocess.run(["docker", "compose", "up", "-d", "api"], check=True, capture_output=True)
    async with httpx.AsyncClient(base_url=API, timeout=2) as c:
        for _ in range(60):
            try:
                if (await c.get("/healthz")).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.5)

    conn = await asyncpg.connect(DB_OWNER)
    try:
        per_batch = {str(r["batch_id"]): r["n"] for r in await conn.fetch(
            "SELECT batch_id, count(*) AS n FROM metrics WHERE run_id = $1 GROUP BY batch_id", uuid.UUID(run_id))}
        seqs = [r["seq"] for r in await conn.fetch(
            "SELECT seq FROM metrics WHERE run_id = $1 ORDER BY seq", uuid.UUID(run_id))]
    finally:
        await conn.close()
    run = await db_run(run_id)
    partial = {b: n for b, n in per_batch.items() if n != 1000}
    unacked_but_stored = set(per_batch) - acked
    print(f"[crash] sent={len(sent)} acked={len(acked)} stored_batches={len(per_batch)} partial={len(partial)} "
          f"stored_without_ack={len(unacked_but_stored)}")
    assert not partial
    assert acked <= set(per_batch), "a batch was acknowledged but is not stored"
    assert seqs == list(range(1, len(seqs) + 1)) and run["last_seq"] == len(seqs)
