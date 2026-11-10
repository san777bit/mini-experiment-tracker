# Note: mini experiment tracker

FastAPI + Postgres 16 + SSE, vanilla JS page; `docker compose up`. Design before code: [DESIGN.md](DESIGN.md); what
changed after and why: [CHANGES.md](CHANGES.md); attacks with predictions: [STRESS.md](STRESS.md); latency work:
[PERF.md](PERF.md). Raw output in `results/` (every number below re-measured on the final code: `results/round5_*`,
`*_round5.txt`); failed runs kept in `results/00_red_runs.md`.

## 1. Consistency: does the page always match the DB?
A point is `(run, key, step)`; the UI orders by `step`. Every accepted point gets a per-run `seq` under a row lock,
so committed seqs are a gap-free prefix. Live rows reach the page from an in-memory tail per run per process, fed by
the committing process (directly or in the NOTIFY payload) or by a DB read; a tail and the page both apply only
`seq == last+1` and read the DB on any hole. The DB is the source of truth; NOTIFY is never needed for correctness.

**Evidence.** Playwright + toxiproxy: 750 points shuffled, overlapping batches, 30% resent, 10% late, conflicting
values, 15% of responses cut *after* commit, browser stream disabled/reset/throttled → `db=750 page=750 missing=0
extra=0 wrong_value=0` (102 of them committed in requests whose response was lost); the rendered table equals SQL.
Also: offline 30+ s then caught up in 1.9 s; property test; 16 writers on
one run with a reconnecting reader; Postgres restart under load; a 100k-point run. **Not caught:** chart pixels (I
check the data the SVG is drawn from), races narrower than the runs hit (the mutation check is my answer), multi-host
API, DB failover, non-Chromium browsers, client clock skew (a naive `ts` is silently taken as UTC).

## 2. Isolation
Every query filters by the key's project; RLS (`app.project_id`, non-owner role) is the second wall. Foreign and
missing IDs give byte-identical 404s on all surfaces, SSE included; keys are sha256 in the DB and only ever in a header
(the page streams with `fetch`, not `EventSource`); `no-store`; errors echo nothing; logs hold no keys or data
(grepped). Timing: −0.03 ms, 95% CI [−0.07, +0.02] at n=3000 (not proof of absence). **What RLS does not cover,
checked:** the `app` role can set `app.project_id` itself, so RLS stops query bugs, not a stolen DB password. Live
rows pushed between processes skip RLS, so each carries its project and a tail drops foreign ones (test + mutation).
**Noisy neighbour:** one project hammering one run made another's reads take 7.4 s; now a fair DB gate and
header-only 401/429 before the body is read: victim p99 94-110 ms under 600 writers ignoring 429 (baseline ~11 ms).

## 3. Silent failures: how each was checked
| Failure | Check | Result |
|---|---|---|
| Lost on reconnect | 16 writers × 1 run + reconnecting reader; listener killed | contiguous 1..1920; 50/50 after kill |
| Duplicates | same batch ×20 concurrently; retries after cut responses | accepted=100, dup=1900 |
| Slow client | reader stops during 20k writes; 1000 viewers on one run | 20000/20000, memory flat; 0 lost of 170 000 |
| Partial writes | bad event mid-batch; `kill -9` API under 4×1000-event writers | 0 partial batches |
| Old key after rotation | old key everywhere; open stream; write in flight; parallel rotations | 401 everywhere; stream closed in <1 s (was 5.5 s); in-flight write 401; 1 live key |
| Never arrived | producer reports `client_points`; one batch withheld | run shows 10 unconfirmed (API, page), admin alert lists it; clears on resend |
| Hidden hangs | silent API↔DB partition | 60/60 fast 503 in 6.5 s, recovery 0.1 s |

**Mutation check** (`results/mutations.txt`): 13 injected bugs in Python and SQL. Global-sequence seq, off-by-one
resume, filter + RLS off, rotation not revoking, no in-txn key re-check, rejected-key cache off, FIFO gate, pushed
rows without contiguity or project check, revocation notice ignored, `client_points` ignored: all red. Filter removed
with RLS on, and no push/NOTIFY at all: green, as designed (second wall; pushes are hints, the 2 s poll delivers).

## 4. Verdict: SHIP for 50 runs × 10 events/s (4 API workers)
| Load (1 event/request, one viewer per run) | Achieved | Lost | Ingest p50/p99 | Send→screen p50/p99 |
|---|---|---|---|---|
| 50×10/s, all producers in the same ms (worst case) | 499.4 ev/s | 0 | 32-34 / 151-185 ms | 37-40 / 174-220 ms |
| 50×10/s, random phase | 498-499 ev/s | 0 | 16-21 / 120-147 ms | 20-27 / 172-191 ms |
| 100×10/s (capacity) | 849 ev/s | 0 | 106 / 304 ms | 158 / 415 ms |

One 4-vCPU Docker Desktop VM runs API, Postgres and the generator at ~90% CPU; ±15% run to run. The p99 tail is CPU
there, not fsync (`synchronous_commit=off` left p99 unchanged). **Change first:** (1) a rate limiter in front of the
app: early rejections still cost an HTTP parse in Python and an open-loop flood is not slowed by the delayed 429;
(2) ship a producer client that buffers to disk and sends `client_points`, and alert on `/api/admin/missing-points`;
(3) measure on separate machines before tuning further.

## What surprised me, what still concerns me
The first load test said DON'T SHIP and was wrong twice: httpx was the bottleneck (43 vs 1100 req/s), then
`pg_notify` inside the transaction serialized every commit. Later my own benchmarks misled me twice more (a victim
sharing an event loop with 200 attackers; a timer that measured the newest row instead of the oldest), and a request
budget I added throttled a legitimate load until the generator printed errors, not just latency. asyncpg's
`pool.acquire()` releases without a timeout and hung on dead sockets. Still concerning: open-loop floods; homoglyph
metric names stay distinct; first-write-wins hides a buggy producer unless someone reads `/conflicts`; the key sits
in sessionStorage; no backup or replica; every number comes from one laptop.
