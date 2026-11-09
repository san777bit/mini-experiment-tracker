# Note: mini experiment tracker

FastAPI + Postgres 16 + SSE, vanilla JS page. `docker compose up`, tests in `tests/`, raw output in `results/`
(red runs kept: `results/00_red_runs.md`). Design before code: [DESIGN.md](DESIGN.md). Second, harsher round of
attacks with predictions written before each run: [STRESS.md](STRESS.md).

## 1. Consistency: does the page always match the DB?
**Mechanism.** A point is `(run, key, step)`; the UI orders by `step`. Each accepted point gets a per-run `seq`
assigned under a row lock on the run, so committed seqs are a gap-free prefix. Streams read `seq > cursor` from the DB
(through one shared in-memory tail per run per process; NOTIFY only wakes it; 2 s poll as fallback). The page applies
only `seq == applied+1`, ignores repeats and treats a hole as a bug: count it, reconnect from `applied`.

**Evidence.** Playwright + toxiproxy: 750 points shuffled, overlapping batches, 30% resent, 10% late, conflicting
values, 15% of responses cut *after* commit, browser stream disabled / reset / throttled mid-run →
`db=750 page=750 missing=0 extra=0 wrong_value=0`, 0 gaps; the rendered table equals SQL. Also: 30 s offline then
catch-up; hypothesis property test (any order/split/repetition → same state); 16 writers on one run with a reader
reconnecting every ~0.2 s; Postgres restart under load with a live page; a 100k-point run (synced in 5.3 s).

**Not caught by these tests.** Chart pixels (I check the data the SVG is drawn from). Races narrower than what the
runs hit; the mutation check (§3) is my answer to "would it even notice". Multi-host API, DB failover, replicas.
Browsers other than Chromium. Client clock skew (`ts` stored, unused; a naive `ts` is silently taken as UTC).

## 2. Isolation
Every query filters by the key's project, and Postgres RLS (`SET LOCAL app.project_id`, non-owner role) is a second wall.
Foreign and missing IDs give byte-identical `404` with identical headers on all 6 surfaces, SSE included. UUIDv4 IDs;
per-run `seq` leaks nothing about other projects' volume. Keys stored as sha256, sent only in a header (the page streams
with `fetch`, not `EventSource`, to keep keys out of URLs). `no-store`, no cache. Errors don't echo input; logs hold no
keys or metric data (grepped). Global counters are admin-only. **Timing:** round 1 showed +0.09/+0.29 ms; that test
always measured "foreign" first. Randomized, n=3000, in-network: −0.02 ms, 95% CI [−0.07, +0.03]: not distinguishable
at this n (not proof of absence). **Noisy neighbour** is also an isolation leak: one project hammering one run made
another project's reads take 7.4 s (pool exhaustion); now 18 ms p50 / 185 ms p99 (see §5).

## 3. Silent failures: how each was checked
| Failure | Check | Result |
|---|---|---|
| Lost on reconnect | 16 writers × 1 run + reconnecting reader; listener backend killed | contiguous 1..1920; 50/50 after kill |
| Duplicates | same batch ×20 concurrently; retries after cut responses | accepted=100, dup=1900 |
| Slow client overflow | reader stops reading during 20k writes; 1000 viewers on one run | 20000/20000, memory flat; 0 lost of 166 000 |
| Partial writes | bad event mid-batch; `kill -9` API under 4×1000-event writers | 0 partial batches, every acked batch stored |
| Old key after rotation | old key everywhere; open stream; **write in flight during rotation; parallel rotations** | 401 everywhere; stream closed in 5.5 s; in-flight write 401 (was 200); 1 live key (was 5) |
| Hidden hangs | silent API↔DB partition | 60/60 fast 503 in 6.4 s (were 60/60 hung), recovers in 0.1 s |

**Mutation check** (`results/mutations.txt`): 6 injected bugs. Global-sequence-style seq, off-by-one resume, missing
project filter + RLS off, rotation not revoking: all red. Missing filter with RLS on, and no NOTIFY at all: green, as
designed (second wall; NOTIFY is only a hint). The round-2 fixes are not yet covered by mutations.

## 4. Verdict: SHIP for 50 runs × 10 events/s (4 API workers).
Load generator inside the compose network, one live viewer per run, every viewer checked for loss:

| Load | Achieved | Stored / delivered | Ingest p50/p99 | Send→screen p50/p99 |
|---|---|---|---|---|
| 50×10/s, 1 event/request | 499.5 ev/s | 30000 / 30000, 0 lost, 0 dup | 57 / 198 ms | 72 / 253 ms |
| 50×10/s, 10 events/request | 499.4 ev/s | 20000 / 20000 | 55 / 193 ms | 62 / 206 ms |
| 200×10/s, 10 events/request | 1993 ev/s | 80000 / 80000 | 241 / 607 ms | 279 / 632 ms |
| 100×10/s, 1 event/request | **712 of 1000** | 28699 / 28699 | 116 / 575 ms | 163 / 648 ms |

(rows 2-4 are round-1 measurements.) One 4-core Docker Desktop VM shared by API, Postgres and the load generator:
numbers are pessimistic and not a production benchmark. **What to change first:** (1) an edge rate limiter per project,
because a client that ignores 429 still costs a DB auth lookup per request and degrades neighbours (185 ms p99);
(2) ingest in one round trip (6-7 statements now; the measured limit for one-event requests is ~1.4× target);
(3) a downsampled history endpoint for runs far past 100k points.

## What surprised me, what still concerns me
The first load test said DON'T SHIP, and it was wrong twice. First I was measuring my own tool: httpx 0.28.1 did 43
req/s where aiohttp did 1100 against the same `/healthz`. Then Postgres showed 19 sessions waiting on `Lock/object`:
`pg_notify` inside the ingest transaction holds a global lock until the commit is fsynced, so every ingest committed
one by one, and 4 workers made it worse. Round 2 surprised me more. asyncpg's `async with pool.acquire()` releases
without a timeout, so even with `command_timeout` a request hung on a dead socket in the connection reset. A write
authenticated before a key rotation still committed after it. And my own isolation fix (per-project DB slots) cut one
big tenant's throughput by 6% until I retuned it. Wrong predictions are listed in STRESS.md (3 of 14).
Still concerning: neighbours under a client that ignores 429; cross-script homoglyph metric names (Latin "o" vs
Cyrillic "о") stay distinct; first-write-wins hides a buggy client unless someone reads `/conflicts`; the key sits in
the tab's sessionStorage; no backup, retention or replica; every number here comes from one laptop.
