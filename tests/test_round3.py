"""Round 3: fair DB admission, cheap rejections, one-round-trip ingest.

Unit tests import the API's limits module directly (pure asyncio, no server).
"""
from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import asyncpg
import httpx
import pytest

from helpers import API, DB_OWNER, batch, db_run, new_project, new_run

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from app import limits  # noqa: E402
from app.errors import ApiError  # noqa: E402


# --- FairGate ---------------------------------------------------------------------------------


async def test_gate_is_work_conserving_for_a_lone_tenant():
    """Round 2's hard cap gave one project 12 of 20 connections even when 8 sat idle."""
    gate = limits.FairGate(20)
    for _ in range(20):
        await asyncio.wait_for(gate.acquire("big"), 0.1)   # all 20, none of them waits
    assert gate.held["big"] == 20


async def test_gate_serves_a_quiet_tenant_before_a_hot_tenants_queue():
    gate = limits.FairGate(4)
    for _ in range(4):
        await gate.acquire("hot")
    order: list[str] = []

    async def take(t: str) -> None:
        await gate.acquire(t)
        order.append(t)

    hot_waiters = [asyncio.create_task(take("hot")) for _ in range(50)]
    await asyncio.sleep(0)
    quiet = asyncio.create_task(take("quiet"))   # arrives AFTER 50 hot waiters
    await asyncio.sleep(0)
    gate.release("hot")
    await asyncio.sleep(0.01)                    # let wait_for() deliver the grant
    assert order == ["quiet"], order             # first release goes to the tenant holding fewer
    for t in hot_waiters:
        t.cancel()
    await asyncio.gather(*hot_waiters, quiet, return_exceptions=True)


async def test_gate_max_min_share_under_contention():
    """3 tenants keep re-requesting; over many releases each ends up holding ~capacity/3."""
    gate = limits.FairGate(6)
    granted = {"a": 0, "b": 0, "c": 0}
    stop = asyncio.Event()

    async def worker(t: str) -> None:
        while not stop.is_set():
            await gate.acquire(t)
            granted[t] += 1
            await asyncio.sleep(0.001)
            gate.release(t)

    tasks = [asyncio.create_task(worker(t)) for t in ("a", "a", "a", "a", "a", "a", "a", "a", "b", "c")]
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.gather(*tasks)
    # "a" has 8 workers, b and c one each; b and c must not be starved (each well above 1/10 share)
    total = sum(granted.values())
    assert granted["b"] / total > 0.1 and granted["c"] / total > 0.1, granted
    assert gate.used == 0 and not gate.waiting and not gate.held


async def test_gate_timeout_and_cancel_do_not_leak_slots(monkeypatch):
    monkeypatch.setattr(limits, "QUEUE_WAIT", 0.05)
    gate = limits.FairGate(1)
    await gate.acquire("a")
    with pytest.raises(ApiError) as e:
        await gate.acquire("b")
    assert e.value.status == 503
    t = asyncio.create_task(gate.acquire("c"))
    await asyncio.sleep(0)
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)
    assert not gate.waiting
    gate.release("a")
    assert gate.used == 0 and not gate.held
    await asyncio.wait_for(gate.acquire("d"), 0.1)   # still usable


# --- token bucket / run queue ownership ----------------------------------------------------------


def test_bucket_allows_burst_then_rate():
    b = limits.TokenBuckets(rate=10, burst=5)
    assert all(b.allow("p", now=0.0) for _ in range(5))
    assert not b.allow("p", now=0.0)
    assert b.allow("p", now=0.1)          # 1 token refilled after 100 ms at 10/s
    assert not b.allow("p", now=0.1)
    assert b.allow("other", now=0.1)      # budgets are per project


def test_byte_bucket_charges_size_and_keeps_debt():
    b = limits.TokenBuckets(rate=100, burst=1000)
    assert b.allow("p", cost=900, now=0.0)        # Content-Length known up front
    assert not b.allow("p", cost=200, now=0.0)    # would exceed the balance
    assert b.allow("p", cost=0, now=0.0)          # chunked: size unknown, balance is positive
    b.charge("p", 600, now=0.0)                   # ... and turned out to be 600 bytes: debt
    assert not b.allow("p", cost=0, now=4.0)      # -500 + 400 refilled: still in debt
    assert b.allow("p", cost=0, now=6.0)


async def test_run_queue_full_is_visible_only_to_the_owner(monkeypatch):
    monkeypatch.setattr(limits, "RUN_QUEUE_MAX", 2)
    q = limits._RunQueues()
    run, owner, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with q.hold(run, owner), q.hold(run, owner):
        assert q.full(run, owner)
        assert not q.full(run, other)     # foreign project keeps getting the normal 404 path
    assert not q.full(run, owner)


# --- e2e ----------------------------------------------------------------------------------------


async def xact_commits() -> int:
    await asyncio.sleep(1.2)   # backends flush pgstat counters at most once per second
    conn = await asyncpg.connect(DB_OWNER)
    try:
        return await conn.fetchval(
            "SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = 'tracker'")
    finally:
        await conn.close()


async def test_requests_with_a_rejected_key_do_not_touch_the_db():
    """Before: every request with a revoked/unknown key was a DB lookup (attacker-controlled load
    on the shared pool). Now the first rejection per process is remembered."""
    p = await new_project()
    async with httpx.AsyncClient(base_url=API, timeout=10,
                                 limits=httpx.Limits(max_connections=20)) as c:
        r = await c.post("/api/keys/rotate", headers=p.h)
        assert r.status_code == 200
        old = p.h
        n = 400
        before = await xact_commits()
        sem = asyncio.Semaphore(20)

        async def hit() -> int:
            async with sem:
                return (await c.get("/api/runs", headers=old)).status_code

        codes = await asyncio.gather(*(hit() for _ in range(n)))
        after = await xact_commits()
    assert set(codes) == {401}
    delta = after - before
    print(f"\n[rejected-key] requests={n} db_transactions={delta}")
    # at most one lookup per worker (4) plus background (listener probes, pool resets)
    assert delta < 40, delta


async def test_ingest_function_classifies_in_batch_repeats_like_the_old_code():
    """Same key/step several times in ONE batch: first occurrence wins, equal repeats are
    duplicates, different values are conflicts counted once per distinct value."""
    p = await new_project()
    run = await new_run(p)
    events = [
        {"key": "a", "step": 1, "value": 1.0},
        {"key": "a", "step": 1, "value": 1.0},
        {"key": "a", "step": 1, "value": 2.0},
        {"key": "a", "step": 1, "value": 2.0},
        {"key": "b", "step": 1, "value": 5.0},
    ]
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r1 = (await c.post(f"/api/runs/{run}/metrics", json=batch(events), headers=p.h)).json()
        r2 = (await c.post(f"/api/runs/{run}/metrics", json=batch(events), headers=p.h)).json()
        hist = (await c.get(f"/api/runs/{run}/metrics", headers=p.h)).json()["events"]
    assert r1 == {"accepted": 2, "duplicates": 1, "conflicts": 2, "last_seq": 2}, r1
    assert r2 == {"accepted": 0, "duplicates": 3, "conflicts": 2, "last_seq": 2}, r2
    assert [(e["seq"], e["key"], e["value"]) for e in hist] == [(1, "a", 1.0), (2, "b", 5.0)]
    assert (await db_run(run))["conflicts"] == 1
