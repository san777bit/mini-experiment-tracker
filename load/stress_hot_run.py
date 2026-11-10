"""Noisy neighbour: can one project's hot run hurt another project?

Project A: W writers hammer ONE run as fast as they can, ignoring 429 (all serialize on its row lock).
Project B: a quiet client reads and writes its own run every 100 ms.
Hypothesis before running (round 2): A's waiters each hold a pooled DB connection while waiting for
the run lock, so the pool is exhausted and B waits for a connection it does not contend for.

Round 3: attackers run in separate OS processes. Before that, the victim shared one event loop with
200 attackers (2600 req/s, or 1.9 MB JSON bodies serialized 120 times a second), so part of the
victim's "latency" was the load generator's own loop being busy.

    ./load/run_in_network.ps1 stress_hot_run.py --writers 200 --seconds 15 [--pad-kb 1900]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import time
import uuid

import aiohttp

API = os.environ.get("API_URL", "http://localhost:8000")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else float("nan")


def session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(API, connector=aiohttp.TCPConnector(limit=0),
                                 timeout=aiohttp.ClientTimeout(total=60))


async def attack(ha: dict, run_a: str, writers: int, seconds: float, pad_kb: int, base: int) -> dict:
    pad = "x" * (pad_kb * 1024)
    done = 0
    err: dict[str, int] = {}
    end = time.monotonic() + seconds
    async with session() as s:
        async def writer(w: int) -> None:
            nonlocal done
            i = 0
            while time.monotonic() < end:
                body = {"batch_id": str(uuid.uuid4()),
                        "events": [{"key": f"w{base + w}", "step": i, "value": 1.0}]}
                if pad:
                    body["pad"] = pad   # ignored field: the server still has to receive it
                try:
                    async with s.post(f"/api/runs/{run_a}/metrics", headers=ha, json=body) as r:
                        await r.read()
                        if r.status == 200:
                            done += 1
                        else:
                            err[str(r.status)] = err.get(str(r.status), 0) + 1
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    err[type(e).__name__] = err.get(type(e).__name__, 0) + 1
                i += 1

        await asyncio.gather(*(writer(w) for w in range(writers)))
    return {"done": done, "err": err}


def attack_proc(q: mp.Queue, *args: object) -> None:
    q.put(asyncio.run(attack(*args)))  # type: ignore[arg-type]


async def setup() -> tuple[tuple[dict, str], tuple[dict, str]]:
    async with session() as s:
        async def project(name: str) -> tuple[dict, str]:
            async with s.post("/api/admin/projects", json={"name": name}, headers=ADMIN) as r:
                h = {"authorization": "Bearer " + (await r.json())["api_key"]}
            async with s.post("/api/runs", json={"name": name}, headers=h) as r:
                return h, (await r.json())["id"]
        return await project("noisy"), await project("victim")


async def victim(hb: dict, run_b: str, seconds: float) -> dict[str, list]:
    out: dict[str, list] = {"read": [], "write": [], "errors": []}
    end = time.monotonic() + seconds
    step = int(time.time() * 1000)
    async with session() as s:
        while time.monotonic() < end:
            t0 = time.perf_counter()
            async with s.get(f"/api/runs/{run_b}", headers=hb) as r:
                await r.read()
                if r.status != 200:
                    out["errors"].append(r.status)
            out["read"].append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            step += 1
            async with s.post(f"/api/runs/{run_b}/metrics", headers=hb,
                              json={"batch_id": str(uuid.uuid4()), "events": [{"key": "v", "step": step, "value": 1.0}]}) as r:
                await r.read()
                if r.status != 200:
                    out["errors"].append(r.status)
            out["write"].append((time.perf_counter() - t0) * 1000)
            await asyncio.sleep(0.1)
    return out


def main(a: argparse.Namespace) -> dict:
    (ha, run_a), (hb, run_b) = asyncio.run(setup())
    base = asyncio.run(victim(hb, run_b, a.baseline))

    attack_s = a.seconds + 2.0
    q: mp.Queue = mp.Queue()
    per = a.writers // a.procs
    procs = [mp.Process(target=attack_proc, args=(q, ha, run_a, per, attack_s, a.pad_kb, i * per))
             for i in range(a.procs)]
    for p in procs:
        p.start()
    time.sleep(1.0)
    under = asyncio.run(victim(hb, run_b, a.seconds))
    results = [q.get() for _ in procs]
    for p in procs:
        p.join()
    done = sum(r["done"] for r in results)
    err: dict[str, int] = {}
    for r in results:
        for k, v in r["err"].items():
            err[k] = err.get(k, 0) + v

    res = {
        "config": vars(a),
        "noisy_project_events_per_s": round(done / attack_s, 1),
        "noisy_requests_per_s": round((done + sum(err.values())) / attack_s, 1),
        "noisy_errors": err,
        "victim_baseline_ms": {k: {"p50": pct(v, .5), "p99": pct(v, .99)} for k, v in base.items() if k != "errors"},
        "victim_under_attack_ms": {k: {"p50": pct(v, .5), "p99": pct(v, .99), "max": round(max(v), 1)}
                                   for k, v in under.items() if k != "errors"},
        "victim_errors": base["errors"] + under["errors"],
    }
    worst = res["victim_under_attack_ms"]["read"]["p99"]
    res["verdict"] = "ISOLATED" if worst < max(100.0, 5 * res["victim_baseline_ms"]["read"]["p99"]) else "DEGRADED"
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--writers", type=int, default=200)
    ap.add_argument("--procs", type=int, default=2, help="attacker OS processes (victim has its own)")
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--baseline", type=float, default=5)
    ap.add_argument("--pad-kb", type=int, default=0, help="attacker body padding (big-body flood)")
    print(json.dumps(main(ap.parse_args()), indent=2))
