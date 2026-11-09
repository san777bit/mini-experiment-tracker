"""Property: any order, any splitting into batches, any repetition of batches
=> the same stored set, contiguous seqs, last_seq == number of distinct points."""
from __future__ import annotations

import asyncio

import httpx
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from helpers import API, batch, db_points, db_run, new_project, new_run

points = st.sets(st.tuples(st.sampled_from(["a", "b", "c"]), st.integers(0, 50)), min_size=1, max_size=60)


def value_of(k: str, s: int) -> float:
    return float(hash((k, s)) % 1000) / 7   # deterministic: no conflicts in this property


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(pts=points, data=st.data())
def test_any_order_any_repetition_same_state(pts, data):
    pts = sorted(pts)
    order = data.draw(st.permutations(pts))
    cuts = sorted(data.draw(st.sets(st.integers(1, max(1, len(order) - 1)), max_size=10)))
    chunks, prev = [], 0
    for c in cuts + [len(order)]:
        if c > prev:
            chunks.append(order[prev:c])
            prev = c
    batches = [batch([{"key": k, "step": s, "value": value_of(k, s)} for k, s in ch]) for ch in chunks]
    repeats = data.draw(st.lists(st.sampled_from(batches), max_size=10))
    sequence = data.draw(st.permutations(batches + repeats))
    concurrent = data.draw(st.booleans())

    async def go():
        p = await new_project("prop")
        run_id = await new_run(p)
        async with httpx.AsyncClient(base_url=API, timeout=20) as c:
            async def send(b):
                r = await c.post(f"/api/runs/{run_id}/metrics", json=b, headers=p.h)
                assert r.status_code == 200, r.text
                return r.json()
            if concurrent:
                res = await asyncio.gather(*(send(b) for b in sequence))
            else:
                res = [await send(b) for b in sequence]
        stored = await db_points(run_id)
        run = await db_run(run_id)
        assert stored == {(k, s): value_of(k, s) for k, s in pts}
        assert run["last_seq"] == run["n"] == run["max_seq"] == len(pts)
        assert sum(r["accepted"] for r in res) == len(pts)
        assert sum(r["conflicts"] for r in res) == 0

    asyncio.run(go())
