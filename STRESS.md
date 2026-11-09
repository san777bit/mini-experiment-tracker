# Round 2: harsher stress tests

After round 1 everything was green, so I attacked places where the green tests could still be hiding failures.
Every row states my expectation **before** running, what happened, and what changed. Raw output in `results/stress_*`
(`_before` = first run, `_after` = after the fix). Prediction accuracy: of 14 predictions, 10 right, 3 wrong, 1 half right (wrong ones marked ✗).

| # | Attack | Expected | Before | Fix | After |
|---|---|---|---|---|---|
| 1 | Write waits on the run lock while its key is rotated | commits with a revoked key | **200, point stored** | key re-checked inside the write txn with `FOR SHARE` (rotation waits or the write sees revoked) | 401, nothing stored |
| 2 | 10 parallel rotations with one key | several new keys | **5 succeeded, 5 live keys** | revoke first: `UPDATE ... WHERE revoked_at IS NULL RETURNING` | 1 succeeds, 1 live key |
| 3 | 200 MB body (Content-Length) | parsed before rejected | **422 after full read+parse** | body limit 2 MB before the app | 413 in 0.5 s, memory flat |
| 4 | 100 MB chunked body | same | 400 (FastAPI turns any body-read error into 400) | middleware reads up to the limit itself | 413 in 0.8 s |
| 5 | Last-Event-ID / `after` > bigint | 500 | **history 500; stream 200 then dies** | strict ASCII digits, ≤ 2^63-1 (`int()` also accepts `+5`, `1_000`) | 422 |
| 6 | Naive / extreme `ts` | naive → 500 | ✗ 200: naive is silently taken as UTC | none (documented) | — |
| 7 | -0.0, 5e-324, ±max double, 1e400 | exact round trip | ✓ bit-exact DB→SSE, -0.0 keeps sign, 1e400 → 422 | — | — |
| 8 | "précision" NFC vs NFD | two identical-looking metrics | **2 metrics** | keys normalized to NFC (cross-script homoglyphs are NOT merged) | 1 metric |
| 9 | Metric names with `<img onerror>`, `</script>`, RLO | page uses textContent | ✓ nothing executed, names shown verbatim | — | — |
| 10 | 200 writers hammer one run; quiet project B reads/writes its own run | pool exhaustion hurts B | **B read p50 7.4 s** (baseline 7 ms) | writers queue in memory per run (no pooled connection held while waiting), ≤12 of 20 connections per project, queue >64 → 429, wait >5 s → 503 | B read p50 18 ms / p99 185 ms. Still above baseline: the attacker ignores 429 (~1600 rejected req/s, each still authenticated against the DB). Needs an edge rate limiter |
| 11 | 1000 viewers on one run, 10 ev/s | DB load = viewers × events | **~4900 DB txn/s for ~6 ev/s; e2e p99 1.8 s**; 0 lost | one shared tail per run per process (last 5000 rows in memory); viewers behind it read the DB | ~620 txn/s, e2e p99 139 ms, 0 lost of 166 000 |
| 12 | `docker restart` Postgres under load + live page | some 5xx, recovery | ✗ fully transparent: 0 failed writes, 0 page reconnects (clean FIN, asyncpg reconnects) | — | — |
| 13 | Run with 100 000 points: open page, stay live | slow replay, page lags | ✗ synced in 5.3 s, live lag 0, worst long task 96 ms, heap 61 MiB | — | — |
| 14 | Silent API↔DB partition (toxiproxy blackhole, no FIN/RST) | requests hang, API stays dead after heal | half ✗: **60/60 requests hung** (client timeout 15 s) but recovered 0.2 s after heal | `command_timeout` 3 s; own acquire/release with timeouts (asyncpg's `async with pool.acquire()` releases with no timeout and its reset query hangs on a dead socket); DB errors → 503 + Retry-After; listener probed every 5 s | 60/60 fast 503 within 6.4 s, recovery 0.1 s |
| 15 | DB unreachable when the API boots | — (found by accident in #14 setup) | **API exits and stays down** (no restart policy) | retry pool creation with backoff + `restart: unless-stopped` | — |

## Regressions I introduced and caught
- Per-project slots of 6/10 cut a single big project to 470 ev/s and doubled its e2e p50 (65 → 143 ms):
  the load test runs 50 runs in ONE project. Retuned to 12/20 → 499.5 ev/s, e2e p99 253 ms
  (`results/load_50x10_round2.txt`). This is a real trade-off: isolation vs one tenant's throughput.
- The listener heartbeat broke `test_lost_notify_does_not_lose_events`: the test found the listener by its last query
  (`LISTEN`), which is now `SELECT 1`. The listener now has `application_name = tracker-listener`.

## Timing oracle (round 1 concern): withdrawn by measurement
Round 1 reported foreign-vs-missing +0.09/+0.29 ms. That test always measured "foreign" first in each pair.
With random order, warm-up and n=3000 inside the Docker network (`load/timing_probe.py`): difference −0.02 ms,
95% CI of the median difference [−0.07, +0.03] on `/runs/{id}` and [−0.10, +0.05] on `/metrics`. Not distinguishable
at this n. This is not proof of absence: a remote attacker averages more samples, but sees much more network noise.

## How to run
```bash
python -m pytest tests/test_stress_edges.py tests/test_stress_heavy.py -s
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build   # API -> toxiproxy -> Postgres
python -m pytest tests/test_stress_partition.py -s
./load/run_in_network.ps1 stress_hot_run.py --writers 200 --seconds 15
./load/run_in_network.ps1 stress_fanout.py --viewers 1000 --rate 10 --seconds 20
./load/run_in_network.ps1 timing_probe.py --n 3000
```
