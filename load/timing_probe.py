"""Is "foreign run" distinguishable from "missing run" by response time?

Attacker model: has a valid key for project A, wants to learn whether some UUID is a run
of another project. Interleaved requests, many samples, robust statistics.

    ./load/run_in_network.ps1 timing_probe.py --n 3000
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time
import uuid

import aiohttp

API = os.environ.get("API_URL", "http://localhost:8000")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}


def trimmed_mean(xs: list[float], cut: float = 0.1) -> float:
    xs = sorted(xs)
    k = int(len(xs) * cut)
    return statistics.fmean(xs[k:len(xs) - k])


def bootstrap_diff(a: list[float], b: list[float], iters: int = 2000) -> tuple[float, float]:
    diffs = []
    for _ in range(iters):
        ra = random.choices(a, k=len(a))
        rb = random.choices(b, k=len(b))
        diffs.append(statistics.median(ra) - statistics.median(rb))
    diffs.sort()
    return diffs[int(0.025 * iters)], diffs[int(0.975 * iters)]


async def main(a: argparse.Namespace) -> None:
    async with aiohttp.ClientSession(API) as s:
        async def mk(name: str) -> dict:
            async with s.post("/api/admin/projects", json={"name": name}, headers=ADMIN) as r:
                d = await r.json()
            return {"authorization": "Bearer " + d["api_key"]}

        ha, hb = await mk("attacker"), await mk("victim")
        async with s.post("/api/runs", json={"name": "secret"}, headers=hb) as r:
            victim_run = (await r.json())["id"]

        async def t(path: str) -> float:
            t0 = time.perf_counter_ns()
            async with s.get(path, headers=ha) as r:
                await r.read()
                assert r.status == 404
            return (time.perf_counter_ns() - t0) / 1e6

        for _ in range(200):   # warm-up
            await t(f"/api/runs/{victim_run}")
            await t(f"/api/runs/{uuid.uuid4()}")
        foreign, missing = [], []
        for i in range(a.n):
            pair = [("f", f"/api/runs/{victim_run}{a.suffix}"), ("m", f"/api/runs/{uuid.uuid4()}{a.suffix}")]
            random.shuffle(pair)             # random order inside each pair
            for kind, path in pair:
                (foreign if kind == "f" else missing).append(await t(path))

    lo, hi = bootstrap_diff(foreign, missing)
    print(f"endpoint=/api/runs/{{id}}{a.suffix} n={a.n} per class")
    print(f"median   foreign={statistics.median(foreign):.4f} ms  missing={statistics.median(missing):.4f} ms  "
          f"diff={statistics.median(foreign) - statistics.median(missing):+.4f} ms  95% CI [{lo:+.4f}, {hi:+.4f}]")
    print(f"trimmed  foreign={trimmed_mean(foreign):.4f} ms  missing={trimmed_mean(missing):.4f} ms  "
          f"diff={trimmed_mean(foreign) - trimmed_mean(missing):+.4f} ms")
    verdict = "DISTINGUISHABLE" if lo > 0 or hi < 0 else "not distinguishable at this n"
    print(f"verdict: {verdict}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=3000)
    ap.add_argument("--suffix", default="", help="e.g. /metrics to probe another endpoint")
    asyncio.run(main(ap.parse_args()))
