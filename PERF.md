# Round 4: latency, measured phase by phase

After round 3 the 50×10 numbers were correct (0 lost) but slow: send→screen p99 ~400 ms. Goal of this round:
find where the time goes before changing anything, then change one thing at a time.

**Method.** Admin-only per-phase timers in the API (`/api/admin/timings`, one reservoir per worker), an event-loop lag
probe in every API worker and in the load generator, and a per-event decomposition of e2e from the DB
(`received_at` = ingest txn start; the event's value = its send time; the generator records ack and receive times).
All runs: 50 runs × 10 ev/s, 1 event/request, one live viewer per run, 40 s, two runs per step
(`load/perf_step.ps1`, raw JSON in `results/perf_*.txt`). Run-to-run noise on this laptop is large; I only claim
what both runs agree on.

## What the breakdown showed (round-3 code)
- The API's event loop was not saturated (loop lag p99 8-11 ms); capping the generator to 1 CPU changed nothing.
- Requests waited for a **pooled connection**: wait p95 63 ms / p99 97 ms, while `ingest_batch` itself was p99 28 ms.
- **~4.3 pooled connections per ingested event**: auth lookup, ingest, `pg_notify`, the tail's read (a 5-statement
  transaction), plus asyncpg's reset query on every release.

## Steps (in order; each row = both runs)
| Step | Change | Effect |
|---|---|---|
| baseline | round-3 code | ingest p99 375-385 ms, e2e p50 141-149 / p99 400-407 ms |
| O1 | no reset query on release (pool connections hold no session state; asyncpg still rolls back open txns) | conn wait p95 58 → 45 ms; e2e within noise |
| O2 | tail read = one SQL function `tail_fetch` (was BEGIN, set_config, 2 SELECTs, COMMIT); status and rows from one snapshot | tail read p50 10-37 → 4.7 ms |
| — | whole pool opened at boot (min 2 → 20) | removes connection setup from the first burst after a restart |
| **bug** | per-process request budget 300/s gave a legitimate 500 req/s project **133-655 early 429s** on its busiest worker (uneven connection spread) | budget → 1000/s, burst 2000 per process; 0 × 429 since. Round-3 published runs had 0 × 429 (checked) |
| O3 | ingest/create-run skip the separate auth lookup for a recently verified key (they re-check it inside the txn anyway) | conn per event 4.4 → 2.6-2.8 |
| O4 | NOTIFY off the request path, one sender per process, batched while one is in flight | ingest p99 240-300 → ~165 ms |
| ✗ | `commit_delay=1ms` | within noise, reverted |
| O6 | separate 4-connection lane for tail reads/NOTIFY | no clear effect alone; kept (live path does not queue behind writes) |
| **O7** | **rows pushed to live tails**: the committing process hands them to its own tail and to other processes inside the NOTIFY payload; a tail appends only `seq == last+1`, anything else (hole, conflict, too big) falls back to the DB read | commit→SSE p50 4 / p99 54-61 ms; txn→viewer p99 240-430 → 110 ms; e2e p99 204-242 ms; conn per event 1.4 |
| ✗ | pool 22 main / 2 tail lanes | no gain, reverted |
| ✗ | access log off | within noise; kept on |

## Result
| 50×10/s, 1 event/request | before (round 3) | after |
|---|---|---|
| all producers send in the same ms (worst case, default) | ingest p99 375-385, e2e p50 141-149 / p99 400-407 ms | ingest p99 142-163, e2e p50 37-39 / p99 190-224 ms |
| producers with random phase (`--jitter`) | — | ingest p99 120-147, e2e p50 20-27 / p99 172-191 ms |
| capacity, 1 event/request (100×10 and 150×10 offered) | 712 ev/s | 844-849 ev/s, 0 lost |

0 lost, 0 duplicates, 0 request errors in every run.

**Targets I set:** e2e p99 ≤ 150 ms: **not met** (190-224 ms worst case, 172-191 ms with random phase).
Capacity > 712 ev/s: met (+19%).

## Why p99 stops at ~150-200 ms here
A diagnostic run with `synchronous_commit=off` (not kept: it can lose acknowledged writes on a Postgres crash)
cut e2e **p50** from ~20 to ~9 ms but left **p99** at 160-193 ms. So the tail is not the commit fsync
(3 ms per fdatasync on this Docker Desktop disk, `pg_test_fsync`); it is CPU: API, Postgres and the generator share 4
vCPUs at ~360% busy, the API loop lag p99 is 23-32 ms and `ingest_batch` p99 is 36-62 ms even without fsync.
On separate machines (clients elsewhere, Postgres on its own cores and NVMe) I would expect the tail to shrink, but I
have not measured that, so the verdict uses the laptop numbers.

## Measurement mistakes in this round
- My first server-side "txn → SSE" timer used the newest row of each batch; the oldest row waits longest. It read
  p99 ~130 ms while clients saw ~300 ms; fixed to the oldest row, then server and client agreed.
- The load test counted 429s as errors without retrying, and I only printed latency at first, so the legitimate-429 bug
  above was visible only as a slow first run. The summary line now prints request errors.
