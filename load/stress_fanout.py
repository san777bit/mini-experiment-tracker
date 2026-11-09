"""Fan-out: many viewers of ONE run.

Every event wakes every viewer, and each viewer runs its own DB query for the same new rows:
V viewers x E events/s = V*E queries/s for data that is identical for all of them.
Hypothesis before running: at 1000 viewers x 10 events/s the DB, not the network, becomes the
limit; latency grows and an unrelated project slows down.

    ./load/run_in_network.ps1 stress_fanout.py --viewers 1000 --rate 10 --seconds 20
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
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else float("nan")


async def main(a: argparse.Namespace) -> dict:
    async with aiohttp.ClientSession(API, connector=aiohttp.TCPConnector(limit=0),
                                     timeout=aiohttp.ClientTimeout(total=None, sock_read=120)) as s:
        async def project(name: str) -> tuple[dict, str]:
            async with s.post("/api/admin/projects", json={"name": name}, headers=ADMIN) as r:
                h = {"authorization": "Bearer " + (await r.json())["api_key"]}
            async with s.post("/api/runs", json={"name": name}, headers=h) as r:
                return h, (await r.json())["id"]

        (h, run), (hb, run_b) = await project("popular"), await project("bystander")
        e2e: list[float] = []
        seen: list[set[int]] = [set() for _ in range(a.viewers)]
        connected = 0

        async def viewer(i: int) -> None:
            nonlocal connected
            ev = None
            async with s.get(f"/api/runs/{run}/stream", headers=h) as r:
                if r.status != 200:
                    return
                connected += 1
                async for raw in r.content:
                    line = raw.decode().rstrip("\n")
                    if line.startswith("event: "):
                        ev = line[7:]
                    elif line.startswith("data: ") and ev == "metric":
                        d = json.loads(line[6:])
                        e2e.append((time.time() - d["value"]) * 1000)
                        seen[i].add(d["seq"])

        tasks = [asyncio.create_task(viewer(i)) for i in range(a.viewers)]
        t_connect = time.monotonic()
        while connected < a.viewers and time.monotonic() - t_connect < 60:
            await asyncio.sleep(0.2)
        connect_s = time.monotonic() - t_connect

        bystander: list[float] = []

        async def bystand() -> None:
            end = time.monotonic() + a.seconds
            while time.monotonic() < end:
                t0 = time.perf_counter()
                async with s.get(f"/api/runs/{run_b}", headers=hb) as r:
                    await r.read()
                bystander.append((time.perf_counter() - t0) * 1000)
                await asyncio.sleep(0.1)

        async def produce() -> int:
            n = 0
            end = time.monotonic() + a.seconds
            while time.monotonic() < end:
                async with s.post(f"/api/runs/{run}/metrics", headers=h, json={
                        "batch_id": str(uuid.uuid4()), "events": [{"key": "m", "step": n, "value": time.time()}]}) as r:
                    await r.read()
                n += 1
                await asyncio.sleep(1 / a.rate)
            return n

        db = await asyncpg.connect(DB_OWNER)
        x0 = await db.fetchval("SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = 'tracker'")
        t0 = time.monotonic()
        produced, _ = await asyncio.gather(produce(), bystand())
        wall = time.monotonic() - t0
        x1 = await db.fetchval("SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = 'tracker'")
        await db.close()
        await asyncio.sleep(a.drain)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    lost = sum(produced - len(v) for v in seen[:connected])
    return {
        "config": vars(a),
        "viewers_connected": connected,
        "connect_all_s": round(connect_s, 1),
        "events_produced": produced,
        "viewer_events_expected": produced * connected,
        "viewer_events_lost": lost,
        "e2e_ms": {"p50": pct(e2e, .5), "p95": pct(e2e, .95), "p99": pct(e2e, .99), "max": round(max(e2e), 1) if e2e else None},
        "db_transactions_per_s": round((x1 - x0) / wall),
        "bystander_read_ms": {"p50": pct(bystander, .5), "p99": pct(bystander, .99)},
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--viewers", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=10)
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--drain", type=float, default=5)
    print(json.dumps(asyncio.run(main(ap.parse_args())), indent=2))
