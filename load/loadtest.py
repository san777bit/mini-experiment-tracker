"""Load test for the verdict: N concurrent runs, R events/s each, one live viewer per run.

Run it inside the compose network (see README):
    docker run --rm --network taskforwork_default -v "$PWD:/w" -w /w \
      -e API_URL=http://api:8000 -e DB_OWNER_URL=postgresql://owner:owner@db:5432/tracker \
      python:3.11-slim sh -c "pip install -q -r load/requirements.txt && python load/loadtest.py --runs 50 --rate 10"

Uses aiohttp, not httpx: the first version used httpx 0.28.1 and measured the client, not the
server (43 req/s vs ~1100 req/s with aiohttp against the same /healthz; see results/diag_*.txt).

Each event's value is its send time, so a viewer computes end-to-end latency
(producer send -> commit -> NOTIFY -> SSE -> parsed) as now - value.
At the end every viewer must hold exactly seq 1..last_seq of its run (loss check).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid

import aiohttp
import asyncpg

API = os.environ.get("API_URL", "http://localhost:8000")
DB_OWNER = os.environ.get("DB_OWNER_URL", "postgresql://owner:owner@localhost:55432/tracker")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}


def pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1)


async def main(a: argparse.Namespace) -> dict:
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(API, connector=conn, timeout=aiohttp.ClientTimeout(total=None, sock_read=60)) as s:
        async with s.post("/api/admin/projects", json={"name": "load"}, headers=ADMIN) as r:
            h = {"authorization": "Bearer " + (await r.json())["api_key"]}
        runs = []
        for i in range(a.runs):
            async with s.post("/api/runs", json={"name": f"load-{i}"}, headers=h) as r:
                runs.append((await r.json())["id"])

        ingest_ms: list[float] = []
        e2e_ms: list[float] = []
        errors: dict[str, int] = {}
        misses = 0
        seen: dict[str, list[int]] = {rid: [] for rid in runs}
        stop = asyncio.Event()

        async def producer(rid: str) -> None:
            nonlocal misses
            interval = a.batch / a.rate
            t_next = time.monotonic()
            end = t_next + a.duration
            step = 0
            while time.monotonic() < end:
                events = [{"key": "m", "step": step + j, "value": time.time()} for j in range(a.batch)]
                step += a.batch
                t0 = time.perf_counter()
                try:
                    async with s.post(f"/api/runs/{rid}/metrics", headers=h,
                                      json={"batch_id": str(uuid.uuid4()), "events": events}) as r:
                        await r.read()
                        if r.status != 200:
                            errors[str(r.status)] = errors.get(str(r.status), 0) + 1
                except aiohttp.ClientError as e:
                    errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
                ingest_ms.append((time.perf_counter() - t0) * 1000)
                t_next += interval
                delay = t_next - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                else:
                    misses += 1

        async def viewer(rid: str) -> None:
            ev = None
            async with s.get(f"/api/runs/{rid}/stream", headers=h) as r:
                async for raw in r.content:
                    line = raw.decode().rstrip("\n")
                    if line.startswith("event: "):
                        ev = line[7:]
                    elif line.startswith("data: ") and ev == "metric":
                        d = json.loads(line[6:])
                        e2e_ms.append((time.time() - d["value"]) * 1000)
                        seen[rid].append(d["seq"])

        viewers = [asyncio.create_task(viewer(rid)) for _ in range(a.viewers) for rid in runs]
        await asyncio.sleep(1.0)
        t_start = time.monotonic()
        await asyncio.gather(*(producer(rid) for rid in runs))
        wall = time.monotonic() - t_start
        await asyncio.sleep(a.drain)
        stop.set()
        for t in viewers:
            t.cancel()
        await asyncio.gather(*viewers, return_exceptions=True)

    db = await asyncpg.connect(DB_OWNER)
    try:
        stored = {str(r["id"]): r["last_seq"] for r in await db.fetch(
            "SELECT id, last_seq FROM runs WHERE id = ANY($1::uuid[])", [uuid.UUID(x) for x in runs])}
    finally:
        await db.close()
    lost = dup = 0
    if a.viewers:
        for rid in runs:
            got = seen[rid]
            # with V viewers per run each seq is expected V times
            lost += sum(1 for q in range(1, stored[rid] + 1) if got.count(q) < a.viewers) if a.viewers > 1 \
                else len(set(range(1, stored[rid] + 1)) - set(got))
            dup += len(got) - a.viewers * len(set(got))

    total = sum(stored.values())
    return {
        "config": vars(a),
        "target_events_per_s": a.runs * a.rate,
        "achieved_events_per_s": round(total / wall, 1),
        "events_sent": len(ingest_ms) * a.batch,
        "events_stored": total,
        "request_errors": errors,
        "schedule_misses": misses,
        "ingest_ms": {"p50": pct(ingest_ms, .5), "p95": pct(ingest_ms, .95), "p99": pct(ingest_ms, .99),
                      "max": round(max(ingest_ms), 1)},
        "e2e_ms": {"p50": pct(e2e_ms, .5), "p95": pct(e2e_ms, .95), "p99": pct(e2e_ms, .99),
                   "max": round(max(e2e_ms), 1) if e2e_ms else None},
        "viewer_events": len(e2e_ms),
        "viewer_lost_events": lost,
        "viewer_duplicate_events": dup,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=50)
    ap.add_argument("--rate", type=float, default=10.0, help="events per second per run")
    ap.add_argument("--batch", type=int, default=1, help="events per request (1 = worst case)")
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--drain", type=float, default=5.0)
    ap.add_argument("--viewers", type=int, default=1, help="live viewers per run")
    print(json.dumps(asyncio.run(main(ap.parse_args())), indent=2))
