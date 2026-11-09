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

## 6. Mutation check: script bug
First run reported `SURPRISE green M5`: the expected string was "green but slower", compared to "green".
Expectation wording fixed; rerun in results/mutations.txt.
