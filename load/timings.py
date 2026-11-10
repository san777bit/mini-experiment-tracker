"""Collect /api/admin/timings from every API worker (each request may land on any worker; new
connection per request, keep asking until WORKERS distinct pids answered)."""
from __future__ import annotations

import os

import aiohttp

API = os.environ.get("API_URL", "http://localhost:8000")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}
WORKERS = int(os.environ.get("API_WORKERS", "4"))


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2) if xs else float("nan")


async def _each_worker(reset: bool) -> list[dict]:
    seen: dict[int, dict] = {}
    async with aiohttp.ClientSession(API, connector=aiohttp.TCPConnector(force_close=True)) as s:
        for _ in range(400):
            async with s.get("/api/admin/timings", params={"reset": "true" if reset else "false"},
                             headers=ADMIN) as r:
                d = await r.json()
            seen.setdefault(d["pid"], d)
            if len(seen) >= WORKERS:
                break
    return list(seen.values())


async def reset() -> int:
    return len(await _each_worker(True))


async def collect() -> dict:
    snaps = await _each_worker(False)
    names = sorted({k for sn in snaps for k in sn["samples"]})
    out: dict = {"workers": len(snaps)}
    for k in names:
        xs = [x for sn in snaps for x in sn["samples"].get(k, [])]
        out[k] = {"n": sum(sn["n"].get(k, 0) for sn in snaps), "p50": pct(xs, .5), "p95": pct(xs, .95),
                  "p99": pct(xs, .99), "max": round(max(xs), 1) if xs else None}
    return out
