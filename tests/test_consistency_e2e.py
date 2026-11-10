"""Requirement 2: the final page state must equal what is stored.

Producers send metrics out of order, duplicated, late and with conflicting values over a
lossy link (toxiproxy "ingest": latency + responses cut AFTER the server committed).
The browser watches the run through toxiproxy "api", whose connections we cut mid-stream.

What this test does NOT catch (see NOTE.md):
 - more than one API host: compose runs 4 worker processes on one host, so cross-process delivery goes
   through Postgres NOTIFY, but which worker serves the stream and which takes each write is not
   controlled here (test_round4 pins cross-process push and revocation directly);
 - an API or DB crash inside a transaction (covered separately in test_silent_failures);
 - chart pixels (we check the data the chart is drawn from and the table, not the SVG);
 - timing windows narrower than what this run happened to hit; a pass is evidence, not proof;
 - only Chromium, one tab, minutes not hours (no memory growth over a long run);
 - client clock skew: `ts` is stored but nothing orders by it.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
import uuid

import pytest
from playwright.async_api import async_playwright

from helpers import (
    API_PROXY, INGEST_PROXY, ProducerStats, batch, db_points, db_run, fresh_conn_client,
    new_project, new_run, send_until_acked,
)

KEYS = ["loss", "acc", "lr"]


def plan_batches(steps: int, rng: random.Random) -> tuple[list[dict], dict[tuple[str, int], float]]:
    """Builds batches whose union covers every (key, step) once, then messes them up."""
    truth = {(k, s): round(rng.uniform(-10, 10), 6) for k in KEYS for s in range(steps)}
    events = [{"key": k, "step": s, "value": v} for (k, s), v in truth.items()]
    rng.shuffle(events)                                   # out of order
    batches: list[dict] = []
    i = 0
    while i < len(events):
        n = rng.randint(1, 25)
        chunk = events[i:i + n]
        # Overlap: some events also appear in another batch (cross-batch duplicate).
        if batches and rng.random() < 0.2:
            chunk = chunk + rng.sample(batches[-1]["events"], k=min(3, len(batches[-1]["events"])))
        batches.append(batch(chunk))
        i += n
    resend = [b for b in batches if rng.random() < 0.3]   # duplicated batches, same batch_id
    late = [b for b in batches if rng.random() < 0.1]     # sent again at the very end
    ordered = batches + resend
    rng.shuffle(ordered)
    return ordered + late, truth


async def wait_synced(page, run_id: str, timeout_s: float) -> dict:
    deadline = time.monotonic() + timeout_s
    st: dict = {}
    while time.monotonic() < deadline:
        st = await page.evaluate("window.__runState()")
        truth = await db_run(run_id)
        if st["conn"] == "live" and st["applied"] == truth["last_seq"] and st["serverSeq"] == truth["last_seq"]:
            return st
        await asyncio.sleep(0.25)
    return st


async def dom_table(page) -> dict[str, dict[str, str]]:
    rows = await page.query_selector_all('[data-testid="metric-row"]')
    out = {}
    for r in rows:
        key = await r.get_attribute("data-key")
        cells = {}
        for col in ("count", "max-step", "last-value", "sum"):
            cells[col] = await (await r.query_selector(f'[data-col="{col}"]')).inner_text()
        out[key] = cells
    return out


def check_page_equals_db(st: dict, table: dict, stored: dict[tuple[str, int], float], run: dict) -> None:
    page_points = {(k, s): v for k, s, v in st["points"]}
    missing = stored.keys() - page_points.keys()
    extra = page_points.keys() - stored.keys()
    wrong = [k for k in stored.keys() & page_points.keys() if stored[k] != page_points[k]]
    print(f"[compare] db={len(stored)} page={len(page_points)} missing={len(missing)} "
          f"extra={len(extra)} wrong_value={len(wrong)}")
    assert not missing and not extra and not wrong
    assert st["applied"] == run["last_seq"] == run["n"] == run["max_seq"]
    assert st["conflicts"] == run["conflicts"]
    assert st["gaps"] == 0, "client saw a hole in seq: the server skipped an event"

    # The rendered table, not just JS memory.
    for key in {k for k, _ in stored}:
        steps = sorted(s for k, s in stored if k == key)
        row = table[key]
        assert int(row["count"]) == len(steps)
        assert int(row["max-step"]) == steps[-1]
        assert float(row["last-value"]) == stored[(key, steps[-1])]
        assert math.isclose(float(row["sum"]), sum(stored[(key, s)] for s in steps), rel_tol=1e-9, abs_tol=1e-9)
    assert set(table) == {k for k, _ in stored}


async def produce(run_id, project, batches, stats, workers=4):
    queue: asyncio.Queue = asyncio.Queue()
    for b in batches:
        queue.put_nowait(b)
    async with fresh_conn_client(INGEST_PROXY, timeout=3.0) as c:
        async def worker():
            while not queue.empty():
                b = queue.get_nowait()
                await send_until_acked(c, run_id, project, b, stats)
                await asyncio.sleep(random.random() * 0.02)
        await asyncio.gather(*(worker() for _ in range(workers)))


async def test_page_matches_db_under_chaos(toxi):
    seed = int(os.environ.get("SEED") or time.time())
    print(f"[e2e] seed={seed} (rerun the same plan with SEED={seed})")
    rng = random.Random(seed)
    p = await new_project("e2e")
    run_id = await new_run(p, "chaos")
    batches, truth = plan_batches(steps=250, rng=rng)

    # Conflicts: the same (key, step) with a different value. First committed write wins.
    conflict_batches = [batch([{"key": "loss", "step": s, "value": 999.0}]) for s in rng.sample(range(250), 5)]
    batches[len(batches) // 2:len(batches) // 2] = conflict_batches

    # Lossy producer link: jittery latency, and 15% of responses cut after a few bytes.
    # A cut response means "committed, but the client does not know": it must retry.
    await toxi.add("ingest", "lat", "latency", "downstream", {"latency": 30, "jitter": 30})
    await toxi.add("ingest", "cut", "limit_data", "downstream", {"bytes": 20}, toxicity=0.15)

    stats = ProducerStats()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        ctx = await browser.new_context()
        await ctx.add_init_script(f"sessionStorage.setItem('apiKey', {json.dumps(p.key)})")
        page = await ctx.new_page()
        await page.goto(f"{API_PROXY}/runs/{run_id}")

        producer = asyncio.create_task(produce(run_id, p, batches, stats))

        # Connection drops mid-stream, while producers keep writing.
        await asyncio.sleep(1.5)
        await toxi.enable("api", False)        # closes the live stream, refuses reconnects
        await asyncio.sleep(3.0)
        await toxi.enable("api", True)
        await asyncio.sleep(1.5)
        await toxi.add("api", "rst", "reset_peer", "downstream", {"timeout": 0})   # TCP RST
        await asyncio.sleep(1.0)
        await toxi.remove("api", "rst")
        # Half-open-ish: the stream stalls (bandwidth ~0) and then recovers.
        await toxi.add("api", "slow", "bandwidth", "downstream", {"rate": 1})
        await asyncio.sleep(2.0)
        await toxi.remove("api", "slow")

        await producer
        await toxi.reset()
        st = await wait_synced(page, run_id, timeout_s=60)
        table = await dom_table(page)
        await browser.close()

    stored = await db_points(run_id)
    run = await db_run(run_id)
    print(f"[producer] {stats.sent_requests} requests, {stats.failed_attempts} failed attempts, "
          f"{stats.acked_batches} acked batches, accepted={stats.accepted} "
          f"duplicates={stats.duplicates} conflicts={stats.conflicts}")
    print(f"[page] applied={st.get('applied')} reconnects={st.get('reconnects')} "
          f"gaps={st.get('gaps')} repeats_ignored={st.get('dups')} conflicts={st.get('conflicts')}")

    # Ingestion lost nothing: every generated point is stored ...
    assert set(stored) == set(truth)
    # ... with the generated value, except where a conflicting write could have won the race.
    conflicted = {("loss", b["events"][0]["step"]) for b in conflict_batches}
    assert all(stored[k] == v for k, v in truth.items() if k not in conflicted)
    # First run of this test asserted accepted == len(truth) and failed 630 != 750: when a
    # response is cut after commit, the retry reports those points as duplicates. A client
    # cannot count stored points from responses; only the server can. Kept as an observation.
    print(f"[producer] points committed in requests whose response was lost: {len(truth) - stats.accepted}")
    assert stats.accepted <= len(truth)
    assert stats.failed_attempts > 0, "chaos did not happen: test proves nothing"
    assert st["reconnects"] >= 2, "stream was never interrupted: test proves nothing"

    check_page_equals_db(st, table, stored, run)


async def test_client_offline_30s_catches_up(toxi):
    """Live-discussion question: a client was offline for 30 seconds. What does it see?"""
    p = await new_project("offline")
    run_id = await new_run(p, "offline")
    stats = ProducerStats()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        ctx = await browser.new_context()
        await ctx.add_init_script(f"sessionStorage.setItem('apiKey', {json.dumps(p.key)})")
        page = await ctx.new_page()
        await page.goto(f"{API_PROXY}/runs/{run_id}")
        await page.wait_for_function("window.__runState && window.__runState().conn === 'live'")

        async with fresh_conn_client(INGEST_PROXY) as c:
            async def tick(step0: int, n: int):
                for s in range(step0, step0 + n):
                    await send_until_acked(c, run_id, p, batch([{"key": "loss", "step": s, "value": s / 7}]), stats)
                    await asyncio.sleep(0.1)    # 10 events/s

            await tick(0, 20)
            await toxi.enable("api", False)
            t0 = time.monotonic()
            await tick(20, 300)                 # ~30 s of writes while the viewer is offline
            offline_for = time.monotonic() - t0
            during = await page.evaluate("window.__runState()")
            await toxi.enable("api", True)
            t1 = time.monotonic()
            st = await wait_synced(page, run_id, timeout_s=30)
            catch_up = time.monotonic() - t1
        await browser.close()

    print(f"[offline] offline={offline_for:.1f}s, page during outage: conn={during['conn']} "
          f"applied={during['applied']} reconnects={during['reconnects']}; caught up in {catch_up:.1f}s "
          f"(reconnect backoff max 5 s + jitter)")
    assert during["conn"] == "reconnecting" and during["applied"] == 20
    run = await db_run(run_id)
    stored = await db_points(run_id)
    assert st["applied"] == run["last_seq"] == 320
    assert {(k, s): v for k, s, v in st["points"]} == stored
    assert st["gaps"] == 0
