"""Where does ingest latency go? Isolates network, app, and DB costs.

    python load/diag.py [base_url]
"""
from __future__ import annotations

import asyncio
import statistics
import sys
import time
import uuid

import httpx

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "tests"))
from helpers import ADMIN  # noqa: E402

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


async def timed(n: int, conc: int, fn) -> tuple[float, float]:
    lat: list[float] = []
    sem = asyncio.Semaphore(conc)

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            await fn(i)
            lat.append((time.perf_counter() - t0) * 1000)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    wall = time.perf_counter() - t0
    return statistics.median(lat), n / wall


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE, timeout=30, limits=httpx.Limits(max_connections=200)) as c:
        r = await c.post("/api/admin/projects", json={"name": "diag"}, headers=ADMIN)
        h = {"authorization": "Bearer " + r.json()["api_key"]}
        runs = [(await c.post("/api/runs", json={"name": f"d{i}"}, headers=h)).json()["id"] for i in range(50)]

        async def health(_i): await c.get("/healthz")
        async def get_run(i): await c.get(f"/api/runs/{runs[i % 50]}", headers=h)
        async def ingest(i): await c.post(f"/api/runs/{runs[i % 50]}/metrics", headers=h,
                                         json={"batch_id": str(uuid.uuid4()), "events": [{"key": "m", "step": i, "value": 1.0}]})

        for name, fn in [("healthz", health), ("get_run", get_run), ("ingest", ingest)]:
            for conc in (1, 10, 50):
                med, rps = await timed(300, conc, fn)
                print(f"{name:8s} conc={conc:3d} median={med:7.1f}ms throughput={rps:7.1f} req/s")


asyncio.run(main())
