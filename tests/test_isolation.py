"""Requirement 3: project A cannot read, write, subscribe to, or guess anything of project B."""
from __future__ import annotations

import asyncio
import statistics
import subprocess
import time
import uuid

import asyncpg
import httpx
import pytest

from helpers import ADMIN, API, DB_APP, Project, batch, db_points, db_run, new_project, new_run, read_sse

NOT_FOUND = b'{"error":"not_found"}'


@pytest.fixture
async def ab():
    a, b = await new_project("A"), await new_project("B")
    run_a, run_b = await new_run(a, "a-run"), await new_run(b, "b-secret-run")
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_b}/metrics", headers=b.h,
                         json=batch([{"key": "b_secret", "step": s, "value": 42.0 + s} for s in range(10)]))
        assert r.status_code == 200
    return a, b, run_a, run_b


def endpoints(run_id: str) -> list[tuple[str, str, dict | None]]:
    return [
        ("GET", f"/api/runs/{run_id}", None),
        ("GET", f"/api/runs/{run_id}/metrics", None),
        ("GET", f"/api/runs/{run_id}/metrics?after=0&limit=5000", None),
        ("POST", f"/api/runs/{run_id}/metrics", batch([{"key": "b_secret", "step": 0, "value": -1.0},
                                                       {"key": "injected", "step": 0, "value": 1.0}])),
        ("GET", f"/api/runs/{run_id}/stream", None),
    ]


async def test_foreign_run_is_indistinguishable_from_missing(ab):
    a, _b, _run_a, run_b = ab
    missing = str(uuid.uuid4())
    before = await db_points(run_b)
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        for (method, path_b, body), (_, path_missing, _) in zip(endpoints(run_b), endpoints(missing)):
            rb = await c.request(method, path_b, headers={**a.h, "last-event-id": "0"}, json=body)
            rm = await c.request(method, path_missing, headers={**a.h, "last-event-id": "0"}, json=body)
            print(f"{method} {path_b.replace(run_b, '<B>')}: foreign={rb.status_code} missing={rm.status_code}")
            assert rb.status_code == rm.status_code == 404
            assert rb.content == rm.content == NOT_FOUND
            # Same header set (no extra hints such as content-length differences or ETags).
            strip = {"date", "x-request-id"}
            assert {k: v for k, v in rb.headers.items() if k not in strip} == \
                   {k: v for k, v in rm.headers.items() if k not in strip}
    assert await db_points(run_b) == before, "A's write attempt changed B's data"


async def test_list_and_stream_show_only_own_runs(ab):
    a, b, run_a, run_b = ab
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        ids_a = {r["id"] for r in (await c.get("/api/runs", headers=a.h)).json()["runs"]}
        ids_b = {r["id"] for r in (await c.get("/api/runs", headers=b.h)).json()["runs"]}
        assert run_a in ids_a and run_b not in ids_a
        assert run_b in ids_b and run_a not in ids_b
        # B can stream its own run; A's stream of its own run never carries B's events.
        own_b = await read_sse(c, run_b, b, after=0, stop_at_seq=10, max_seconds=5)
        assert [e["seq"] for e in own_b] == list(range(1, 11))
        own_a = await read_sse(c, run_a, a, after=0, max_seconds=2.5)
        assert own_a == []


async def test_bad_keys(ab):
    _a, _b, run_a, _ = ab
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        for h in [{}, {"authorization": "Bearer"},{"authorization": "Bearer sk_nope"},
                  {"authorization": "Basic Zm9vOmJhcg=="}, {"authorization": "bearer " + "x" * 5000}]:
            for method, path, body in endpoints(run_a):
                r = await c.request(method, path, headers=h, json=body)
                assert r.status_code == 401 and r.content == b'{"error":"unauthorized"}'
        # Admin surface is not reachable with a project key.
        r = await c.get("/api/admin/stats", headers=_a_header(ab))
        assert r.status_code == 401
        r = await c.post("/api/admin/projects", json={"name": "x"}, headers=_a_header(ab))
        assert r.status_code == 401


def _a_header(ab) -> dict:
    return {**ab[0].h, "x-admin-token": "wrong"}


async def test_ids_are_not_guessable_and_seq_is_per_run(ab):
    a, b, run_a, run_b = ab
    for rid in (run_a, run_b):
        assert uuid.UUID(rid).version == 4
    # seq is per run: a new run starts at 1 no matter how much other projects wrote,
    # so seq values leak nothing about B's volume.
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_a}/metrics", headers=a.h, json=batch([{"key": "k", "step": 0, "value": 1.0}]))
        assert r.json()["last_seq"] == 1


async def test_rls_is_a_second_wall():
    """Even if a query forgot `project_id = $auth`, Postgres RLS filters rows for the app role."""
    a, b = await new_project("rlsA"), await new_project("rlsB")
    run_b = await new_run(b)
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        await c.post(f"/api/runs/{run_b}/metrics", headers=b.h, json=batch([{"key": "k", "step": 0, "value": 1.0}]))
    conn = await asyncpg.connect(DB_APP)
    try:
        assert await conn.fetchval("SELECT count(*) FROM metrics WHERE run_id = $1", uuid.UUID(run_b)) == 0
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.project_id', $1, true)", a.id)
            assert await conn.fetchval("SELECT count(*) FROM metrics WHERE run_id = $1", uuid.UUID(run_b)) == 0
            assert await conn.fetchval("SELECT count(*) FROM runs WHERE id = $1", uuid.UUID(run_b)) == 0
            with pytest.raises(asyncpg.InsufficientPrivilegeError):   # WITH CHECK blocks cross-project writes
                await conn.execute(
                    "INSERT INTO metrics (run_id, seq, project_id, metric_key, step, value, batch_id) "
                    "VALUES ($1, 99, $2, 'x', 1, 1, $3)", uuid.UUID(run_b), uuid.UUID(b.id), uuid.uuid4())
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.project_id', $1, true)", b.id)
            assert await conn.fetchval("SELECT count(*) FROM metrics WHERE run_id = $1", uuid.UUID(run_b)) == 1
        # set_config(..., true) did not leak past the transaction on this pooled-style connection
        assert await conn.fetchval("SELECT count(*) FROM metrics WHERE run_id = $1", uuid.UUID(run_b)) == 0
    finally:
        await conn.close()


async def test_no_cacheable_responses(ab):
    a, _b, run_a, _ = ab
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        for method, path, body in endpoints(run_a)[:3]:
            r = await c.request(method, path, headers=a.h, json=body)
            assert r.headers.get("cache-control") == "no-store"
            assert "etag" not in r.headers


async def test_errors_do_not_echo_input(ab):
    a, _b, run_a, _ = ab
    marker = "SECRET-MARKER-" + uuid.uuid4().hex
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post(f"/api/runs/{run_a}/metrics", headers=a.h,
                         json={"batch_id": marker, "events": [{"key": marker, "step": -1, "value": marker}]})
        assert r.status_code == 422
        assert marker not in r.text


async def test_timing_foreign_vs_missing(ab):
    """Measure, don't assume. Reported numbers go to results/."""
    a, _b, _run_a, run_b = ab
    n = 300
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        async def t(path: str) -> float:
            t0 = time.perf_counter()
            r = await c.get(path, headers=a.h)
            assert r.status_code == 404
            return (time.perf_counter() - t0) * 1000

        foreign, missing = [], []
        for _ in range(n):   # interleaved so drift hits both equally
            foreign.append(await t(f"/api/runs/{run_b}"))
            missing.append(await t(f"/api/runs/{uuid.uuid4()}"))
    mf, mm = statistics.median(foreign), statistics.median(missing)
    print(f"[timing] n={n} median foreign={mf:.3f}ms missing={mm:.3f}ms diff={mf - mm:+.3f}ms "
          f"p90 foreign={statistics.quantiles(foreign, n=10)[-1]:.3f} missing={statistics.quantiles(missing, n=10)[-1]:.3f}")
    # Same SQL for both (WHERE id = $1 AND project_id = $2): expect no systematic gap.
    # Threshold is loose on purpose; a real attacker averages over many more samples (see NOTE.md).
    assert abs(mf - mm) < 0.5


async def test_logs_do_not_contain_keys_or_values(ab):
    a, b, _run_a, run_b = ab
    await asyncio.sleep(0.5)
    logs = subprocess.run(["docker", "compose", "logs", "api", "--no-color"], capture_output=True, text=True,
                          encoding="utf-8", errors="replace").stdout
    assert logs, "could not read api logs"
    assert a.key not in logs and b.key not in logs
    assert "sk_" not in logs
    assert "b_secret" not in logs   # metric names/values from bodies are never logged
