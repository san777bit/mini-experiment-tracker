# Red runs kept on purpose

These failures happened during development. The lines below are copied from the terminal output of each run
(pytest / scripts), shortened to the relevant lines. Later green runs are in the other files of this folder.

## 1. test_page_matches_db_under_chaos: wrong assertion in the test
```
E       AssertionError: assert 630 == 750
E        +  where 630 = ProducerStats(sent_requests=93, failed_attempts=11, acked_batches=82, accepted=630, duplicates=399, conflicts=5, errors=[]).accepted
```
The DB had all 750 points. 120 were committed in requests whose response toxiproxy cut, and the retries got
`duplicates`. A client cannot count stored points from responses. The test now prints this number instead.

## 2. test_partial_write_is_impossible_bad_event_mid_batch: real bug
```
[partial] status=500 body={"error":"internal","request_id":"af2da8f50ade"}
>       assert r.status_code == 422
E       assert 500 == 422
```
A NUL byte in a metric key passed pydantic and failed inside the Postgres transaction. Nothing was stored (atomic),
but 5xx is retryable, so this batch would block a producer forever. Fixed with a key validator (422).

## 3. test_api_crash_mid_ingest_leaves_no_partial_batch: wrong test data
```
[crash] sent=28 acked=24 stored_batches=6 partial=0 stored_without_ack=0
E       AssertionError: a batch was acknowledged but is not stored
```
Four writer tasks reused the same keys, so later batches were deduplicated into earlier ones and stored under another
batch_id. Keys are now unique per batch; the green run shows `partial=0 stored_without_ack=0` and every acked batch stored.

## 4. test_bad_keys: test client limitation
`httpx.LocalProtocolError: Illegal header value b'Bearer '` (httpx refuses a trailing space). Case changed to `Bearer`.

## 5. Load test: measured the client, then a real server bottleneck
results/load_50x10_httpx_INVALID.txt: 53 events/s, every request behind schedule. Cause: httpx client
(load/client_check.py vs load/client_check_aiohttp.py: 43 vs 1099 req/s, same server).
With aiohttp: ~320-410 events/s, API at 92% CPU of one core. With 4 workers: 282 events/s (worse);
pg_stat_activity showed 19 sessions in `Lock/object` and one in `IO/WALSync`: NOTIFY inside the transaction serialized
commits. After moving NOTIFY after commit: 499.5 events/s (results/load_50x10_4workers_notify_after_commit.txt).

## Round 2 (details and before/after numbers in STRESS.md)
- `stress_edges_before.txt`: 7 red. Real bugs: write with a key revoked while it waited (200, stored); 10 parallel
  rotations → 5 live keys; 200 MB body parsed before rejection; cursor > bigint → 500; NFC/NFD split one metric in two.
  One red was the test client again (httpx refuses the header value `" 5"`).
- First chunked-body fix returned 400, not 413: FastAPI turns any exception raised while reading the body into 400.
- `stress_hot_run_before.txt`: quiet project's read took 7.4 s while another project hammered one run.
- `stress_fanout_before.txt`: ~4900 DB txn/s for ~6 events/s with 1000 viewers; e2e p99 1.8 s.
- `stress_partition_before.txt`: first attempt skipped (toxiproxy had not reloaded its config) and exposed a real bug:
  the API exits for good if the DB is unreachable at boot. Second attempt: 60/60 requests hung. After `command_timeout`
  alone: still 52/60 hung (asyncpg releases without timeout; reset hangs on the dead socket).
- `full_suite` after the listener heartbeat: `test_lost_notify_does_not_lose_events` red because it located the
  listener by its last query (`LISTEN`), now `SELECT 1`. Test fixed via `application_name`.
- Load regression from my per-project slots (470 ev/s, e2e p50 143 ms) → retuned (499.5 ev/s).

## Round 3 (details in STRESS.md "Round 3")
- `stress_*_sameloop.txt`: the noisy-neighbour benchmark ran victim and attackers in one asyncio loop. After the fix the
  attacker made 2600 req/s and the victim looked as bad as before (read p50 200 ms). Measurement artifact: attackers
  now run in separate processes; all before/after numbers were re-taken that way.
- `stress_bigbody_after_sameloop.txt` / `stress_bigbody_before.txt`: the request budget did not stop 1.9 MB bodies
  (126 req/s < 300 req/s budget). Added a byte budget.
- `test_gate_serves_a_quiet_tenant_before_a_hot_tenants_queue`: first version checked one `sleep(0)` after the release;
  `wait_for` needs more loop turns to deliver the grant (test bug, `order == []`).
- `full_suite` after the byte budget: `test_huge_body_is_rejected_before_it_is_parsed` got `assert 429 == 413`. Real
  bug: an oversized body was charged to the 8 MB byte budget and answered "retry later". Now it skips the budget (413).
- `load_50x10_round3.txt` (463 ev/s) and `_rerun.txt` (351 ev/s): same code later gave 499.5 / 495.4 in an alternating
  series (`_ab.txt`). Postgres waited on WAL fsync, not on the new function. Kept as evidence of run-to-run noise.

## Round 4 (details in PERF.md)
- `perf_o1_no_reset.txt`, `perf_o2_*`: **133-655 early 429s on a legitimate 500 req/s single-project load.** The
  per-process budget (300/s) was exceeded on the busiest worker because connections spread unevenly. Visible only as
  "first run slow" until the summary printed request errors. Budget raised to 1000/s; 0 × 429 since.
- `perf_o6_sse_diag.txt`: server "txn→SSE" p99 130 ms vs client 250-300 ms. My timer used the newest row of each
  batch; the oldest waits longest. Fixed (`perf_o6_oldest_row.txt`: server and client agree).
- `perf_o5_commit_delay_1ms.txt`, `perf_o8_pool_20_2.txt`, `perf_o9_no_access_log.txt`: no measurable gain, reverted.
- `perf_diag_sync_commit_off.txt`: diagnostic only (setting reverted): p99 unchanged, so the tail is CPU, not fsync.
- Target e2e p99 ≤ 150 ms: not met (190-224 ms worst case).

## Round 5 (review against the task; CHANGES.md "Round 5")
- Found by reading, not by a test: round 4's pushed rows bypass RLS and nothing checked their project. Added the
  check, a unit test, an e2e test with two projects writing concurrently, and mutation M10.
- My first version of that e2e test sent 200 writes to one run at once and got `429 run_busy` (the per-run queue limit
  working as designed); the second opened 400 new TCP connections and hit `ConnectTimeout` on Windows port forwarding.
  Test bugs; now 16 keep-alive connections.
- Loss-detection page test read the tile one animation frame too early (`'-' == '10'`; state was already 10). Test bug.
- `DESIGN.md` measured 2 pages (the task says one) since round 1, by `scripts/page_count.py`; my estimate that the
  note was ~3 pages was wrong (it was 2). Design tightened to 1 page, round-by-round changes moved to CHANGES.md.

## 6. Mutation check: script bug
First run reported `SURPRISE green M5`: the expected string was "green but slower", compared to "green".
Expectation wording fixed; rerun in results/mutations.txt.
