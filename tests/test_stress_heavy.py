"""Round 2, heavy: infrastructure failures and scale on the real page."""
from __future__ import annotations

import asyncio
import json
import subprocess
import time

import httpx
from playwright.async_api import async_playwright

from helpers import API, API_PROXY, ProducerStats, batch, db_points, db_run, new_project, new_run, send_until_acked


async def open_page(pw, key: str, run_id: str):
    browser = await pw.chromium.launch()
    ctx = await browser.new_context()
    await ctx.add_init_script(f"sessionStorage.setItem('apiKey', {json.dumps(key)})")
    page = await ctx.new_page()
    await page.goto(f"{API_PROXY}/runs/{run_id}")
    return browser, page


async def wait_applied(page, target: int, timeout_s: float) -> float:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        st = await page.evaluate("window.__runState()")
        if st["applied"] >= target and st["conn"] == "live":
            return time.monotonic() - t0
        await asyncio.sleep(0.2)
    raise AssertionError(f"page stuck at {st['applied']}/{target} conn={st['conn']}")


async def test_postgres_restart_under_load():
    """docker restart the DB while producers write and a browser watches.
    Expectation before running: some 5xx during the outage (clients retry), the API recovers
    without a restart of its own (pool drops dead connections, listener reconnects), and the page
    converges to the DB with no gaps."""
    p = await new_project("dbrestart")
    run_id = await new_run(p)
    stats = ProducerStats()
    first_ok_after: list[float] = []
    async with async_playwright() as pw:
        browser, page = await open_page(pw, p.key, run_id)
        await page.wait_for_function("window.__runState && window.__runState().conn === 'live'")
        restarted_at = None

        async def produce():
            async with httpx.AsyncClient(base_url=API, timeout=5) as c:
                for s in range(300):
                    await send_until_acked(c, run_id, p, batch([{"key": "k", "step": s, "value": float(s)}]), stats,
                                           max_attempts=200)
                    if restarted_at and not first_ok_after:
                        first_ok_after.append(time.monotonic() - restarted_at)
                    await asyncio.sleep(0.05)

        prod = asyncio.create_task(produce())
        await asyncio.sleep(3)
        restarted_at = time.monotonic()
        subprocess.run(["docker", "restart", "taskforwork-db-1"], check=True, capture_output=True)
        restart_took = time.monotonic() - restarted_at
        first_ok_after.clear()        # measure from the moment the DB is back
        restarted_at = time.monotonic()
        await prod
        synced = await wait_applied(page, 300, 60)
        st = await page.evaluate("window.__runState()")
        await browser.close()
    run = await db_run(run_id)
    stored = await db_points(run_id)
    print(f"[dbrestart] restart took {restart_took:.1f}s; first acked write {first_ok_after[0] if first_ok_after else None:.1f}s "
          f"after DB back; failed attempts={stats.failed_attempts}; page synced {synced:.1f}s after last write; "
          f"reconnects={st['reconnects']} gaps={st['gaps']}")
    assert run["last_seq"] == run["n"] == 300
    assert {(k, s): v for k, s, v in st["points"]} == stored
    assert st["gaps"] == 0


async def test_long_run_page_open_and_stay_live():
    """A long run: 100k points (20 metrics x 5000 steps, ~3 h at 10 events/s).
    Expectation before running: opening the page replays everything over SSE in 500-event pages and
    re-renders all charts per frame; time to first sync is many seconds, and while new events arrive
    the page lags because each frame rebuilds 20 polylines of 5000 points."""
    p = await new_project("long")
    run_id = await new_run(p)
    n_keys, steps = 20, 5000
    async with httpx.AsyncClient(base_url=API, timeout=60) as c:
        t0 = time.monotonic()
        evs = [{"key": f"metric_{k:02d}", "step": s, "value": (s % 97) / 97} for s in range(steps) for k in range(n_keys)]
        for i in range(0, len(evs), 1000):
            r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch(evs[i:i + 1000]))
            assert r.status_code == 200, r.text
        load_s = time.monotonic() - t0
    total = n_keys * steps
    async with async_playwright() as pw:
        browser, page = await open_page(pw, p.key, run_id)
        t_open = time.monotonic()
        sync_s = await wait_applied(page, total, 180)
        heap = await page.evaluate("performance.memory ? performance.memory.usedJSHeapSize : null")

        # Live phase: 10 events/s for 10 s; measure how far the page lags behind the DB.
        lags = []
        async with httpx.AsyncClient(base_url=API, timeout=10) as c:
            for i in range(100):
                await c.post(f"/api/runs/{run_id}/metrics", headers=p.h,
                             json=batch([{"key": "metric_00", "step": steps + i, "value": 0.5}]))
                if i % 10 == 9:
                    st = await page.evaluate("window.__runState()")
                    lags.append(total + i + 1 - st["applied"])
                await asyncio.sleep(0.1)
        live_sync = await wait_applied(page, total + 100, 30)
        long_tasks = await page.evaluate("""() => new Promise(res => {
            let n = 0, worst = 0;
            new PerformanceObserver(l => { for (const e of l.getEntries()) { n++; worst = Math.max(worst, e.duration); } })
              .observe({type: 'longtask', buffered: true});
            setTimeout(() => res({n, worst}), 500);
        })""")
        await browser.close()
    print(f"[long] {total} points loaded via API in {load_s:.1f}s; page synced in {sync_s:.1f}s; "
          f"JS heap={heap and round(heap / 2**20)} MiB; live lag samples (events behind)={lags}; "
          f"final live sync {live_sync:.1f}s; long tasks={long_tasks}")
    assert sync_s < 30, "opening a long run takes too long"
    assert max(lags) <= 20, "page falls behind live updates on a long run"


async def test_one_metric_150k_steps_renders():
    """One metric with 150k steps (a long run logging a single loss).
    Expectation before running: Math.min(...vals) passes every value as a call argument; V8 throws
    RangeError past ~125k arguments, so the chart render throws and the page stops redrawing."""
    p = await new_project("onekey")
    run_id = await new_run(p)
    steps = 150_000
    async with httpx.AsyncClient(base_url=API, timeout=60) as c:
        for i in range(0, steps, 1000):
            evs = [{"key": "loss", "step": s, "value": (s % 89) / 89} for s in range(i, i + 1000)]
            r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch(evs))
            assert r.status_code == 200, r.text
    async with async_playwright() as pw:
        browser, page = await open_page(pw, p.key, run_id)
        errors: list[str] = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        sync_s = await wait_applied(page, steps, 180)
        await page.wait_for_timeout(1000)            # let the last rAF render run
        feet = page.locator(".chart-foot")
        foot = await feet.first.inner_text() if await feet.count() else None
        await browser.close()
    print(f"[onekey] {steps} points of one metric; page synced in {sync_s:.1f}s; chart foot={foot!r}; "
          f"page errors={errors[:3]}")
    assert not errors, errors[:3]
    assert foot and f"{steps - 1}" in foot, "chart did not render the last step"
