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
| 10 | 200 writers hammer one run; quiet project B reads/writes its own run | pool exhaustion hurts B | **B read p50 7.4 s** (baseline 7 ms) | writers queue in memory per run (no pooled connection held while waiting), ≤12 of 20 connections per project, queue >64 → 429, wait >5 s → 503 | B read p50 18 ms / p99 185 ms. Still above baseline: the attacker ignores 429 (~1600 rejected req/s, each still authenticated against the DB). Needs an edge rate limiter. *Round 3: part of this was the load generator itself, see below* |
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

## Round 3: fair DB admission, cheap rejections, one-round-trip ingest
Two open items from round 2: my per-project DB cap (12 of 20) cost a lone big project throughput, and a client that
ignores 429 still hurt neighbours. Design in DESIGN.md "Changed in round 3"; raw output in `results/stress_*_r3_*`,
`results/stress_bigbody_*`, `results/stress_ignore429_*`, `results/load_50x10_round3*`.

**First finding: my noisy-neighbour benchmark measured itself.** The victim shared one asyncio loop with the
attackers. With cheap rejections the attackers did 2600 req/s, and with 1.9 MB bodies the generator serialized
240 MB/s of JSON in that same loop, so the victim's latency included its own client's queue (`*_sameloop.txt`: the
"after" looked no better than "before"). Attackers now run in separate OS processes. Re-measured with round-2 code:
200 writers on one run barely hurt the victim (read p99 11-26 ms vs baseline ~11), because 100 writers per process
leave ~50 waiters per worker, under the 429 threshold of 64. Round 2's "185 ms p99" was partly this artifact.

| # | Attack (attackers in own processes) | Expected | Before (round-2 code) | Fix | After |
|---|---|---|---|---|---|
| 16 | 600 writers on one run, ignore 429 (2 runs each) | rejected requests still cost DB auth + body parse | attacker 1690 req/s, 600-700 × 503, **the hot run itself falls to 19-27 ev/s**; victim read p50 57-322 ms, p99 146-851 ms; write p99 283-1592 ms | 401/429 decided in middleware from the `Authorization` header before the body is read: cached rejected keys, per-project token bucket, run-queue check; early 429 answered after 1 s (= Retry-After) | attacker 685 req/s, 0 × 503, hot run 220 ev/s; victim read p50 7-9 ms, p99 79-112 ms; write p99 48-170 ms |
| 17 | 200 writers send 1.9 MB bodies (under the 2 MB limit), 3 runs | request budget stops it | ✗ request budget alone does not: 126 req/s is under 300 req/s; victim read p99 47 / 62 / **965** ms | byte budget per project (4 MB/s, burst 8 MB per process; chunked bodies charged as they arrive) | victim read p99 30 / 25 / 82 ms; accepted 1.9 MB bodies fall from 57-113/s to ~17/s |
| 18 | Requests with a revoked key (400 in a burst) | — | one DB lookup per request | rejected key hashes cached (they can never become valid) | 15-25 DB transactions for 400 requests (`test_requests_with_a_rejected_key_do_not_touch_the_db`) |
| 19 | 200 writers on one run (round-2 attack, 3 runs) | — | hot run 115-152 ev/s; victim read p99 11-26 ms | ingest is one statement (`ingest_batch`) | hot run 211-228 ev/s (+50%); victim read p99 18-39 ms (same within noise) |

**Regression I introduced and caught:** the byte budget answered 429 to a 200 MB body (burst is 8 MB), before the
body limit could say 413. 429 means "retry later", so a client would retry a request that can never succeed.
Found by round 2's `test_huge_body_is_rejected_before_it_is_parsed`; bodies over the limit now skip the budget.

**Load 50×10 in one project**, alternating builds (`load_50x10_round3_ab.txt`): round-3 code 499.5 / 495.4 ev/s,
e2e p99 286 / 237 ms, 0 lost. Round-2 code in the same series: 490 / 411 ev/s, e2e p99 476 / 546 ms, but that run
had `PROJECT_DB_SLOTS` at its default 6 (I had removed it from compose), i.e. the regressed config. Two earlier
single runs of round-3 code gave 463 and 351 ev/s (`load_50x10_round3.txt`, `_rerun.txt`); Postgres was waiting on
`WALWrite`/`WALSync` (commit fsync), not on the new function. This laptop's run-to-run noise is about ±15%.

**Prediction accuracy (round 3):** 1 of 4 wrong (#17: I expected the request budget to cover big bodies). Not a
prediction but a miss of mine: the benchmark artifact above.

**Still open.** Victim p99 under the 600-writer attack is 79-112 ms against an ~11 ms baseline: API, Postgres and the
attackers share 4 cores, and every early rejection still costs an HTTP parse in Python. An open-loop flood (client
does not wait for responses) is not slowed by the 1 s delay. Both need a limiter in front of the app.

## Round 4 re-check of the noisy-neighbour attacks (after the latency changes in PERF.md)
600 writers ignoring 429: victim read p99 94 / 110 ms (round 3: 79-112), hot run 172-174 ev/s (round 3: 218-224;
not investigated). 1.9 MB bodies: victim read p99 43 / 110 ms (round 3: 25-82). Isolation did not regress beyond
noise, but the target I set (victim p99 ≤ 3× baseline, ~30 ms) is not met (`results/stress_*_round4.txt`).

## How to run
```bash
python -m pytest tests/test_stress_edges.py tests/test_stress_heavy.py -s
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build   # API -> toxiproxy -> Postgres
python -m pytest tests/test_stress_partition.py -s
./load/run_in_network.ps1 stress_hot_run.py --writers 200 --seconds 15            # --pad-kb 1900, --writers 600 --procs 3
./load/r3_series.ps1 after                                                        # both noisy-neighbour attacks x3
./load/run_in_network.ps1 stress_fanout.py --viewers 1000 --rate 10 --seconds 20
./load/run_in_network.ps1 timing_probe.py --n 3000
```
