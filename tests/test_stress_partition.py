"""Round 2: network partition between API and Postgres (packets vanish, no FIN/RST).

Needs chaos mode: docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build
Expectation before running: queries in flight hang forever (no command timeout), their pooled
connections never come back, and after the partition heals the API stays stuck.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import time

import httpx
import pytest

from helpers import API, batch, db_run, new_project, new_run


def chaos_mode() -> bool:
    out = subprocess.run(["docker", "exec", "taskforwork-api-1", "printenv", "CHAOS_DB_PROXY"],
                         capture_output=True, text=True).stdout.strip()
    return out == "1"


pytestmark = pytest.mark.skipif(not chaos_mode(), reason="API is not routed through toxiproxy (chaos mode)")


async def probe(c: httpx.AsyncClient, p, run_id: str, step: int, timeout: float) -> str:
    try:
        r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, timeout=timeout,
                         json=batch([{"key": "k", "step": step, "value": 1.0}]))
        return str(r.status_code)
    except httpx.TimeoutException:
        return "client-timeout"
    except httpx.HTTPError as e:
        return type(e).__name__


async def test_api_recovers_after_db_partition(toxi):
    p = await new_project("partition")
    run_id = await new_run(p)
    async with httpx.AsyncClient(base_url=API) as c:
        assert await probe(c, p, run_id, 0, 5) == "200"

        # Partition: drop everything in both directions, keep the TCP connections open.
        await toxi.add("pg", "blackhole_up", "timeout", "upstream", {"timeout": 0})
        await toxi.add("pg", "blackhole_down", "timeout", "downstream", {"timeout": 0})
        t0 = time.monotonic()
        during = await asyncio.gather(*(probe(c, p, run_id, 1 + i, 15) for i in range(60)))
        during_s = time.monotonic() - t0
        await toxi.remove("pg", "blackhole_up")
        await toxi.remove("pg", "blackhole_down")

        # Healed. How long until writes and reads work again, and do they work at all?
        healed = time.monotonic()
        recovered_after = None
        after = []
        for i in range(60):
            res = await probe(c, p, run_id, 100 + i, 10)
            after.append(res)
            if res == "200" and recovered_after is None:
                recovered_after = time.monotonic() - healed
            if res == "200" and after[-3:] == ["200"] * 3:
                break
            await asyncio.sleep(1)
        stats = (await c.get("/api/admin/stats", headers={"x-admin-token": "dev-admin-token"})).json()
    counts = {k: during.count(k) for k in set(during)}
    print(f"[partition] during ({during_s:.1f}s): {counts}; after heal: first 200 after "
          f"{recovered_after if recovered_after is None else round(recovered_after, 1)}s, results={after[:10]}...; "
          f"stats={ {k: v for k, v in stats.items() if 'listener' in k or 'timeout' in k or 'db_' in k} }")
    assert recovered_after is not None and recovered_after < 15, "API did not recover after the partition healed"
    # During the partition the API must answer (fast 503), not leave clients hanging.
    assert "client-timeout" not in counts, "requests hung during the partition"
    run = await db_run(run_id)
    assert run["last_seq"] == run["n"]
