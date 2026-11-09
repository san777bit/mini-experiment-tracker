"""Noisy neighbour: can one project's hot run hurt another project?

Project A: W writers hammer ONE run as fast as they can (all serialize on its row lock).
Project B: a quiet client reads and writes its own run every 100 ms.
Hypothesis before running: A's waiters each hold a pooled DB connection while waiting for the
run lock, so the pool (10 per process) is exhausted and B waits for a connection it does not
contend for.

    ./load/run_in_network.ps1 stress_hot_run.py --writers 200 --seconds 15
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid

import aiohttp

API = os.environ.get("API_URL", "http://localhost:8000")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else float("nan")


async def main(a: argparse.Namespace) -> dict:
    async with aiohttp.ClientSession(API, connector=aiohttp.TCPConnector(limit=0),
                                     timeout=aiohttp.ClientTimeout(total=60)) as s:
        async def project(name: str) -> tuple[dict, str]:
            async with s.post("/api/admin/projects", json={"name": name}, headers=ADMIN) as r:
                h = {"authorization": "Bearer " + (await r.json())["api_key"]}
            async with s.post("/api/runs", json={"name": name}, headers=h) as r:
                return h, (await r.json())["id"]

        (ha, run_a), (hb, run_b) = await project("noisy"), await project("victim")

        async def victim(seconds: float) -> dict[str, list[float]]:
            out: dict[str, list[float]] = {"read": [], "write": [], "errors": []}
            end = time.monotonic() + seconds
            step = int(time.time() * 1000)
            while time.monotonic() < end:
                t0 = time.perf_counter()
                async with s.get(f"/api/runs/{run_b}", headers=hb) as r:
                    await r.read()
                    if r.status != 200:
                        out["errors"].append(r.status)
                out["read"].append((time.perf_counter() - t0) * 1000)
                t0 = time.perf_counter()
                step += 1
                async with s.post(f"/api/runs/{run_b}/metrics", headers=hb,
                                  json={"batch_id": str(uuid.uuid4()), "events": [{"key": "v", "step": step, "value": 1.0}]}) as r:
                    await r.read()
                    if r.status != 200:
                        out["errors"].append(r.status)
                out["write"].append((time.perf_counter() - t0) * 1000)
                await asyncio.sleep(0.1)
            return out

        base = await victim(a.baseline)

        stop = asyncio.Event()
        a_done = 0
        a_err: dict[str, int] = {}

        async def writer(w: int) -> None:
            nonlocal a_done
            i = 0
            while not stop.is_set():
                try:
                    async with s.post(f"/api/runs/{run_a}/metrics", headers=ha, json={
                            "batch_id": str(uuid.uuid4()),
                            "events": [{"key": f"w{w}", "step": i, "value": 1.0}]}) as r:
                        await r.read()
                        if r.status == 200:
                            a_done += 1
                        else:
                            a_err[str(r.status)] = a_err.get(str(r.status), 0) + 1
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    a_err[type(e).__name__] = a_err.get(type(e).__name__, 0) + 1
                i += 1

        writers = [asyncio.create_task(writer(w)) for w in range(a.writers)]
        await asyncio.sleep(1.0)
        under = await victim(a.seconds)
        stop.set()
        await asyncio.gather(*writers, return_exceptions=True)

    res = {
        "config": vars(a),
        "noisy_project_events_per_s": round(a_done / (a.seconds + 1.0), 1),
        "noisy_errors": a_err,
        "victim_baseline_ms": {k: {"p50": pct(v, .5), "p99": pct(v, .99)} for k, v in base.items() if k != "errors"},
        "victim_under_attack_ms": {k: {"p50": pct(v, .5), "p99": pct(v, .99), "max": round(max(v), 1)}
                                   for k, v in under.items() if k != "errors"},
        "victim_errors": base["errors"] + under["errors"],
    }
    worst = res["victim_under_attack_ms"]["read"]["p99"]
    res["verdict"] = "ISOLATED" if worst < max(100.0, 5 * res["victim_baseline_ms"]["read"]["p99"]) else "DEGRADED"
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--writers", type=int, default=200)
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--baseline", type=float, default=5)
    print(json.dumps(asyncio.run(main(ap.parse_args())), indent=2))
