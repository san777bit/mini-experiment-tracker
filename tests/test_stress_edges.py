"""Round 2: non-obvious failure modes. Each test says what I expected BEFORE running it."""
from __future__ import annotations

import asyncio
import json
import math
import struct
import subprocess
import time
import unicodedata
import uuid

import asyncpg
import httpx
import pytest
from playwright.async_api import async_playwright

from helpers import API, API_PROXY, DB_OWNER, batch, db_points, db_run, new_project, new_run, read_sse


def is_neg_zero(x: float) -> bool:
    return x == 0.0 and math.copysign(1.0, x) < 0


# --- auth races -----------------------------------------------------------------------------


async def test_write_authenticated_before_rotation_cannot_commit_after_it():
    """Race: auth happens at request start, commit happens later. If the run row is locked by
    another writer, the request waits; meanwhile the key is rotated (revoked). Expectation before
    running: the late request still commits with a revoked key (auth checked only once)."""
    p = await new_project("race")
    run_id = await new_run(p)
    owner = await asyncpg.connect(DB_OWNER)
    tx = owner.transaction()
    await tx.start()
    await owner.execute("SELECT 1 FROM runs WHERE id = $1 FOR UPDATE", uuid.UUID(run_id))  # block writers
    async with httpx.AsyncClient(base_url=API, timeout=30) as c:
        late = asyncio.create_task(c.post(f"/api/runs/{run_id}/metrics", headers=p.h,
                                          json=batch([{"key": "late", "step": 0, "value": 1.0}])))
        await asyncio.sleep(1.0)                      # request is authenticated and waiting on the lock
        rot = await c.post("/api/keys/rotate", headers=p.h)
        assert rot.status_code == 200
        await tx.rollback()                           # release the run lock
        await owner.close()
        r = await late
    stored = await db_points(run_id)
    print(f"[race] late write with revoked key -> {r.status_code}, stored={stored}")
    assert r.status_code == 401 and stored == {}, "a write authenticated before revocation committed after it"


async def test_concurrent_rotations_leave_exactly_one_valid_key():
    """Two clients rotate with the same key at once. Expectation before running: both succeed and
    the project ends up with two valid keys, one of which nobody will ever revoke."""
    p = await new_project("rot2")
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        rs = await asyncio.gather(*(c.post("/api/keys/rotate", headers=p.h) for _ in range(10)))
    ok = [r for r in rs if r.status_code == 200]
    owner = await asyncpg.connect(DB_OWNER)
    try:
        active = await owner.fetchval(
            "SELECT count(*) FROM api_keys WHERE project_id = $1 AND revoked_at IS NULL", uuid.UUID(p.id))
    finally:
        await owner.close()
    print(f"[rot2] 10 concurrent rotations: {len(ok)} succeeded, active keys now={active}")
    assert len(ok) == 1 and active == 1


# --- hostile input -------------------------------------------------------------------------------


async def test_huge_body_is_rejected_before_it_is_parsed():
    """Expectation before running: no body limit; a 200 MB body is read into memory and parsed
    (json + pydantic, several copies) before validation says no."""
    p = await new_project("body")
    run_id = await new_run(p)
    size = 200 * 1024 * 1024
    head = b'{"batch_id":"' + str(uuid.uuid4()).encode() + b'","events":[{"key":"'
    tail = b'","step":0,"value":1.0}]}'
    body = head + b"x" * (size - len(head) - len(tail)) + tail

    def mem() -> str:
        return subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "taskforwork-api-1"],
                              capture_output=True, text=True).stdout.strip()

    before = mem()
    peak: list[str] = []
    stop = asyncio.Event()

    async def sampler():
        while not stop.is_set():
            peak.append(await asyncio.to_thread(mem))

    s = asyncio.create_task(sampler())
    t0 = time.monotonic()
    async with httpx.AsyncClient(base_url=API, timeout=120) as c:
        try:
            r = await c.post(f"/api/runs/{run_id}/metrics", content=body,
                             headers={**p.h, "content-type": "application/json"})
            status = r.status_code
        except httpx.HTTPError as e:
            status = type(e).__name__
    elapsed = time.monotonic() - t0
    stop.set()
    await s
    print(f"[body] 200 MB -> {status} in {elapsed:.1f}s; api mem before={before} samples={peak}")
    assert status == 413


async def test_huge_chunked_body_is_cut_off():
    """Same attack without Content-Length (chunked): the size check must count streamed bytes."""
    p = await new_project("chunked")
    run_id = await new_run(p)

    async def gen():
        yield b'{"batch_id":"' + str(uuid.uuid4()).encode() + b'","events":[{"key":"'
        chunk = b"x" * (1024 * 1024)
        for _ in range(100):          # 100 MB, never finishes the JSON
            yield chunk

    t0 = time.monotonic()
    async with httpx.AsyncClient(base_url=API, timeout=60) as c:
        try:
            r = await c.post(f"/api/runs/{run_id}/metrics", content=gen(),
                             headers={**p.h, "content-type": "application/json"})
            status = r.status_code
        except httpx.HTTPError as e:     # server may close while we are still sending
            status = type(e).__name__
    print(f"[chunked] 100 MB chunked -> {status} in {time.monotonic() - t0:.1f}s")
    assert status == 413 or status in ("RemoteProtocolError", "WriteError", "ReadError")
    assert (await db_run(run_id))["n"] == 0


@pytest.mark.parametrize("header,query", [
    ("99999999999999999999999", None),     # > bigint
    ("-1", None), ("1e3", None), ("1_000", None), ("+5", None),   # Python int() accepts the last two
    (None, "99999999999999999999999"),
])
async def test_resume_cursor_out_of_range(header, query):
    """Expectation before running: a cursor above bigint passes Python int parsing and blows up
    in asyncpg as a 500 instead of a 4xx."""
    p = await new_project("cursor")
    run_id = await new_run(p)
    h = dict(p.h)
    if header is not None:
        h["last-event-id"] = header
    url = f"/api/runs/{run_id}/stream" + (f"?after={query}" if query else "")
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        async with c.stream("GET", url, headers=h) as r:
            status = r.status_code
        r2 = await c.get(f"/api/runs/{run_id}/metrics?after={query or header}", headers=p.h)
    print(f"[cursor] header={header!r} query={query!r} stream={status} history={r2.status_code}")
    assert status in (200, 422) and r2.status_code in (200, 422)
    assert status != 500 and r2.status_code != 500


@pytest.mark.parametrize("ts", ["2026-01-01T00:00:00", "2026-01-01T00:00:00+14:00", "0001-01-01T00:00:00Z",
                                "9999-12-31T23:59:59Z", "2026-02-30T00:00:00Z", "1e10"])
async def test_odd_client_timestamps(ts):
    """Expectation before running: a timezone-less ts may 500 (timestamptz needs an aware datetime)."""
    p = await new_project("ts")
    run_id = await new_run(p)
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h,
                         json=batch([{"key": "k", "step": 0, "value": 1.0, "ts": ts}]))
    print(f"[ts] {ts!r} -> {r.status_code} {r.text[:120]}")
    assert r.status_code in (200, 422)


async def test_float_edges_round_trip_exactly():
    """Values that break naive float handling: -0.0, smallest subnormal, max double, 0.1+0.2,
    and 1e400 (overflows to inf when parsed). Expectation: exact round trip DB -> SSE; -0.0 keeps
    its sign; 1e400 rejected."""
    p = await new_project("floats")
    run_id = await new_run(p)
    vals = [-0.0, 5e-324, 1.7976931348623157e308, -1.7976931348623157e308, 0.1 + 0.2, 1e-300]
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h,
                         json=batch([{"key": "f", "step": i, "value": v} for i, v in enumerate(vals)]))
        assert r.status_code == 200, r.text
        raw = b'{"batch_id":"' + str(uuid.uuid4()).encode() + b'","events":[{"key":"f","step":99,"value":1e400}]}'
        r_inf = await c.post(f"/api/runs/{run_id}/metrics", content=raw, headers={**p.h, "content-type": "application/json"})
        got = await read_sse(c, run_id, p, after=0, stop_at_seq=len(vals), max_seconds=5)
    stored = await db_points(run_id)
    print(f"[floats] 1e400 -> {r_inf.status_code}; sse={[e['value'] for e in got]}")
    assert r_inf.status_code == 422
    for i, v in enumerate(vals):
        assert struct.pack("<d", stored[("f", i)]) == struct.pack("<d", v), f"DB changed bits of {v!r}"
        assert struct.pack("<d", got[i]["value"]) == struct.pack("<d", v), f"SSE changed bits of {v!r}"
    assert is_neg_zero(got[0]["value"])


async def test_unicode_keys_that_look_identical_are_one_metric():
    """'é' composed (NFC) and decomposed (NFD) are different strings that render identically.
    Expectation before running: stored as two metrics; the page shows two rows that look the same,
    and their points are split between them."""
    p = await new_project("unicode")
    run_id = await new_run(p)
    nfc, nfd = unicodedata.normalize("NFC", "précision"), unicodedata.normalize("NFD", "précision")
    assert nfc != nfd
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch([{"key": nfc, "step": 0, "value": 1.0}]))
        await c.post(f"/api/runs/{run_id}/metrics", headers=p.h, json=batch([{"key": nfd, "step": 1, "value": 2.0}]))
    keys = {k for k, _ in await db_points(run_id)}
    print(f"[unicode] stored keys={[k.encode('unicode_escape') for k in keys]}")
    assert len(keys) == 1


async def test_page_is_not_injectable_and_shows_edge_values():
    """Metric names are attacker-controlled text rendered by the page of whoever opens the run."""
    p = await new_project("xss")
    run_id = await new_run(p)
    evil = ['<img src=x onerror="window.__xss=1">', "</script><script>window.__xss=2</script>",
            "javascript:window.__xss=3", "‮gnp.exe", "a" * 128]
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_id}/metrics", headers=p.h,
                         json=batch([{"key": k, "step": 0, "value": -0.0} for k in evil]))
        assert r.status_code == 200, r.text
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        ctx = await browser.new_context()
        await ctx.add_init_script(f"sessionStorage.setItem('apiKey', {json.dumps(p.key)})")
        page = await ctx.new_page()
        errors: list[str] = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        await page.goto(f"{API_PROXY}/runs/{run_id}")
        await page.wait_for_function(f"window.__runState && window.__runState().applied === {len(evil)}", timeout=15000)
        xss = await page.evaluate("window.__xss")
        imgs = await page.evaluate("document.querySelectorAll('#metrics img, #charts img').length")
        shown = await page.evaluate("[...document.querySelectorAll('[data-testid=metric-row]')].map(r => r.dataset.key)")
        await browser.close()
    print(f"[xss] window.__xss={xss} injected_imgs={imgs} page_errors={errors}")
    assert xss is None and imgs == 0 and not errors
    assert sorted(shown) == sorted(evil)
