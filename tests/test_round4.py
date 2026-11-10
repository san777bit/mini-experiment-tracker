"""Round 4: rows pushed to live tails (no DB read on the hot path) must never create or hide a hole.

Unit tests on RunTail.push / Hub payload handling, in-process (no server, no DB access).
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from app import hub  # noqa: E402
from app.hub import Hub, Row, RunTail  # noqa: E402


def rows(*seqs: int) -> list[Row]:
    return [Row(s, "loss", s, s / 10, 0.0) for s in seqs]


PROJECT = uuid.uuid4()


def tail_at(last_seq: int) -> RunTail:
    return RunTail(uuid.uuid4(), PROJECT, last_seq)


async def test_push_appends_contiguous_rows_and_publishes():
    t = tail_at(10)
    ev = t.changed()
    t.push(PROJECT, rows(11, 12), 12)
    assert [r.seq for r in t.rows] == [11, 12] and t.last_seq == 12
    assert ev.is_set() and t.status[0] == 12
    assert not t.wake.is_set()          # no DB read needed


async def test_push_with_a_hole_is_refused_and_falls_back_to_the_db():
    """Two processes' NOTIFYs for one run can arrive out of order (13 before 11..12)."""
    t = tail_at(10)
    t.push(PROJECT, rows(13), 13)
    assert t.rows == [] and t.last_seq == 10   # nothing appended past the hole
    assert t.wake.is_set()                     # the DB read will fill 11..13 in order


async def test_push_ignores_rows_it_already_has():
    t = tail_at(10)
    t.push(PROJECT, rows(11, 12), 12)
    t.push(PROJECT, rows(11, 12, 13), 13)               # e.g. a DB read raced the push
    assert [r.seq for r in t.rows] == [11, 12, 13]


async def test_push_none_means_read_the_db():
    t = tail_at(10)
    t.push(PROJECT, None, 11)
    assert t.rows == [] and t.wake.is_set()


async def test_notify_payload_round_trip_keeps_values_exact():
    """Rows travel as JSON between processes: floats and unicode keys must survive bit-exact."""
    h = Hub()
    run = uuid.uuid4()
    t = RunTail(run, PROJECT, 0)
    h._tails[run] = [t, 1]
    vals = [-0.0, 5e-324, 1.7976931348623157e308, 0.1 + 0.2]
    sent = [Row(i + 1, "précision/лосс", i, v, 0.0) for i, v in enumerate(vals)]
    payload = '{"p":%d,"i":[%s]}' % (hub.PID + 1, Hub._item(run, PROJECT, len(vals), sent))
    h._on_notify(None, 0, hub.CHANNEL, payload)
    got = [r.value for r in t.rows]
    assert [repr(x) for x in got] == [repr(x) for x in vals]
    assert t.rows[0].key == "précision/лосс"


async def test_own_notify_is_ignored_and_bad_payload_wakes_everyone():
    h = Hub()
    run = uuid.uuid4()
    t = RunTail(run, PROJECT, 0)
    h._tails[run] = [t, 1]
    h._on_notify(None, 0, hub.CHANNEL, '{"p":%d,"i":[%s]}' % (hub.PID, Hub._item(run, PROJECT, 1, rows(1))))
    assert t.rows == []                        # own process already pushed locally
    h._on_notify(None, 0, hub.CHANNEL, "not json")
    assert t.wake.is_set()


async def test_too_big_batch_is_sent_as_a_hint():
    big = [Row(i + 1, "k" * 100, i, 1.0, 0.0) for i in range(200)]
    item = json.loads(Hub._item(uuid.uuid4(), PROJECT, 200, big))
    assert item["rows"] is None


async def test_pushed_rows_claiming_another_project_never_reach_the_tail():
    """Pushed rows skip the DB read and therefore RLS. If routing ever handed run A's tail rows
    that belong to project B (same run id by bug, corrupted payload), the tail must drop them."""
    h = Hub()
    run = uuid.uuid4()
    t = RunTail(run, PROJECT, 0)
    h._tails[run] = [t, 1]
    other = uuid.uuid4()
    h._on_notify(None, 0, hub.CHANNEL, '{"p":%d,"i":[%s]}' % (hub.PID + 1, Hub._item(run, other, 1, rows(1))))
    t.push(other, rows(1), 1)                    # same-process path
    assert t.rows == [] and t.last_seq == 0
    assert not t.wake.is_set()                   # not even a DB read is triggered by foreign data
    t.push(PROJECT, rows(1), 1)
    assert [r.seq for r in t.rows] == [1]


# --- e2e: the push path against the real server ---------------------------------------------------


async def test_pushed_rows_reach_only_their_own_project_stream():
    """Round 4 moved live rows off the DB read (and off RLS). Two projects write concurrently from
    many connections (so several API processes push and NOTIFY); each viewer must see exactly its
    own run's rows, contiguous, and nothing of the other project."""
    import asyncio
    import httpx
    from helpers import API, batch, new_project, new_run, read_sse

    a, b = await new_project("iso-a"), await new_project("iso-b")
    run_a, run_b = await new_run(a), await new_run(b)
    n = 200

    async def write(p, run, tag):
        # 16 keep-alive connections: spread over the API workers, so several processes push/NOTIFY,
        # and under the per-run queue limit (64 per process; 200 writers at once get 429).
        async with httpx.AsyncClient(base_url=API, timeout=30,
                                     limits=httpx.Limits(max_connections=16)) as w:
            async def one(i):
                r = await w.post(f"/api/runs/{run}/metrics",
                                 json=batch([{"key": tag, "step": i, "value": float(i)}]), headers=p.h)
                assert r.status_code == 200
            await asyncio.gather(*(one(i) for i in range(n)))

    async with httpx.AsyncClient(base_url=API, timeout=60) as c:
        va = asyncio.create_task(read_sse(c, run_a, a, 0, stop_at_seq=n, max_seconds=40))
        vb = asyncio.create_task(read_sse(c, run_b, b, 0, stop_at_seq=n, max_seconds=40))
        await asyncio.sleep(0.5)
        await asyncio.gather(write(a, run_a, "only-a"), write(b, run_b, "only-b"))
        got_a, got_b = await va, await vb
    assert [e["seq"] for e in got_a] == list(range(1, n + 1))
    assert [e["seq"] for e in got_b] == list(range(1, n + 1))
    assert {e["key"] for e in got_a} == {"only-a"} and {e["key"] for e in got_b} == {"only-b"}


async def test_revocation_from_another_process_closes_streams_and_drops_cached_auth():
    """Rotation in process X must close streams held by process Y at once (Y gets it via NOTIFY),
    and Y must stop trusting its cached write auth for that key."""
    from app.auth import KEYS
    h = Hub()
    key, digest = uuid.uuid4(), b"d" * 32
    KEYS.mark_good(digest, PROJECT, key)
    t = tail_at(0)
    ev = h.watch_key(key, t)
    changed = t.changed()
    h._on_key_notify(None, 0, hub.KEY_CHANNEL, json.dumps({"p": hub.PID + 1, "k": str(key)}))
    assert ev.is_set() and changed.is_set()      # stream wakes and sees the revocation first
    assert KEYS.known(digest) is None and KEYS.is_bad(digest)
    h.unwatch_key(key, ev)
    assert key not in h._key_watch


# --- loss detection -----------------------------------------------------------------------------------


async def test_points_the_server_never_received_are_visible_and_alertable():
    """The server cannot know about events that never reached it (producer crashed, gave up
    retrying, lost its buffer). With client_points the producer states how many distinct points
    it has generated; the gap shows on the run (API + page) and in the operator's alert list."""
    import json as _json

    import httpx
    from playwright.async_api import async_playwright

    from helpers import ADMIN, API, API_PROXY, batch, new_project, new_run

    p = await new_project("loss")
    run = await new_run(p)
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        produced = 0
        for b in range(3):                      # 3 batches x 10 points; batch 1 is "lost"
            events = [{"key": "loss", "step": b * 10 + i, "value": float(i)} for i in range(10)]
            produced += 10
            body = {**batch(events), "client_points": produced}
            if b != 1:
                assert (await c.post(f"/api/runs/{run}/metrics", json=body, headers=p.h)).status_code == 200
            else:
                lost = body
        r = (await c.get(f"/api/runs/{run}", headers=p.h)).json()
        assert (r["last_seq"], r["client_points"], r["unconfirmed"]) == (20, 30, 10), r
        alerts = (await c.get("/api/admin/missing-points", params={"quiet_seconds": 0}, headers=ADMIN)).json()
        mine = [x for x in alerts["runs"] if x["run_id"] == run]
        assert mine and mine[0]["missing"] == 10 and mine[0]["project_id"] == p.id
        assert set(mine[0]) == {"run_id", "project_id", "stored", "reported", "missing", "quiet_since"}

        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            ctx = await browser.new_context()
            await ctx.add_init_script(f"sessionStorage.setItem('apiKey', {_json.dumps(p.key)})")
            page = await ctx.new_page()
            await page.goto(f"{API_PROXY}/runs/{run}")
            await page.wait_for_function("window.__runState && window.__runState().unconfirmed === 10", timeout=15000)
            # rendering is rAF-throttled: wait for the tile, not just the state
            await page.wait_for_function(
                """document.querySelector('[data-testid="unconfirmed"]').textContent === '10'""", timeout=5000)
            await browser.close()

        # the producer recovers and resends: the gap closes, the alert clears
        assert (await c.post(f"/api/runs/{run}/metrics", json=lost, headers=p.h)).status_code == 200
        r = (await c.get(f"/api/runs/{run}", headers=p.h)).json()
        assert (r["last_seq"], r["unconfirmed"]) == (30, 0)
        alerts = (await c.get("/api/admin/missing-points", params={"quiet_seconds": 0}, headers=ADMIN)).json()
        assert run not in {x["run_id"] for x in alerts["runs"]}
        # admin-only
        assert (await c.get("/api/admin/missing-points", headers=p.h)).status_code == 401
