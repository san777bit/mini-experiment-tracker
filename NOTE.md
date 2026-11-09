# Note: mini experiment tracker

FastAPI + Postgres 16 + SSE, vanilla JS page. `docker compose up`, tests in `tests/`, raw output in `results/`.
Design written before code: [DESIGN.md](DESIGN.md) (plus one change after measurement, marked there).

## 1. Consistency: does the page always match the DB?
**Mechanism.** A point is identified by `(run, key, step)`; the UI orders by `step`. Each accepted point gets a per-run
`seq` assigned under a row lock on the run, so committed seqs are always a gap-free prefix. The stream reads
`seq > cursor` from the DB (NOTIFY only wakes it up; a 2 s poll is the fallback), the page applies only `seq == applied+1`,
ignores repeats and treats a hole as a bug: count it, reconnect from `applied`.

**Evidence.** `test_page_matches_db_under_chaos` (Playwright + toxiproxy): 750 points sent shuffled, in overlapping
batches, 30% resent, 10% resent late, 5 conflicting values, over a link that adds jitter and cuts 15% of responses
*after* commit. The browser stream is disabled for 3 s, TCP-reset and throttled to ~0 mid-run.
Result (results/full_suite.txt): `db=750 page=750 missing=0 extra=0 wrong_value=0`, 5 reconnects, 0 gaps; the rendered
table (count, max step, value, sum per metric) equals SQL. Also: 30 s offline then catch-up (4.5-9.5 s, backoff-bound),
a hypothesis property test (any order/split/repetition, sequential or concurrent → same state), 16 concurrent writers
on one run with a reader reconnecting every ~0.2 s (1920/1920 contiguous).

**What these tests do NOT catch.** Chart pixels (I compare the data the SVG is drawn from, not the drawing). Races
narrower than what the runs happened to hit (a pass is evidence, not proof; the mutation check below is my answer to
"would it even notice"). Multi-host API and DB failover (one host, one DB, no replica). Hours-long runs (memory growth of
the tab, replay size). Other browsers. Client clock skew (`ts` is stored but nothing uses it).

## 2. Isolation
Every query filters by the key's `project_id`, and Postgres RLS (`SET LOCAL app.project_id`, non-owner role) is a second
wall. Foreign and missing IDs return byte-identical `404 {"error":"not_found"}` with identical headers on all 5 surfaces
(read, list, write, history, SSE). UUIDv4 IDs; `seq` is per run, so it leaks nothing about other projects' volume.
Keys are stored as sha256 and travel only in a header (the page uses `fetch` streaming, not `EventSource`, to avoid
keys in URLs). No HTTP cache; `Cache-Control: no-store`. Validation errors don't echo input; logs contain no keys or
metric data (checked by grepping the container log). Admin counters are behind the admin token because global counters
leak other projects' activity. **Timing, honestly:** median foreign-vs-missing difference was +0.09 ms and +0.29 ms in
two runs of n=300 (test threshold 0.5 ms). Same SQL, but a foreign ID finds an index entry and is filtered while a
missing one does not. With enough samples this may be an oracle for "this UUID exists somewhere". Not exploitable for
guessing (UUIDv4), but not proven absent either.

## 3. Silent failures: how each was checked (results/silent_failures.txt, results/full_suite.txt)
| Failure | Check | Result |
|---|---|---|
| Lost on reconnect | 16 writers × 1 run, reader reconnecting with Last-Event-ID; plus listener backend killed with `pg_terminate_backend` | contiguous 1..1920; 50/50 delivered after listener kill |
| Duplicates | same batch ×20 concurrently; retries after cut responses | accepted=100, dup=1900; page repeats ignored=0 |
| Slow client overflow | reader stops reading while 20k events are written | 20000/20000 in order, API memory flat (no per-client queue by design) |
| Partial writes | event that fails in the DB mid-batch; `kill -9` API under 4×1000-event writers | 0 partial batches, every acked batch stored, seq contiguous |
| Old key after rotation | old key on every endpoint; stream opened before rotation | 401 everywhere; open stream gets `revoked` and closes in 5.5 s |

**Mutation check** (results/mutations.txt): I injected 6 bugs and reran the targeted tests. Global-sequence-style seq,
off-by-one resume, missing project filter + RLS off, rotation not revoking: all red. Missing project filter with RLS on,
and no NOTIFY at all: green, as intended (second wall; NOTIFY is only a hint).

## 4. Verdict: SHIP for 50 runs × 10 events/s, with 4 API workers.
Load generator inside the compose network, 1 viewer per run, every viewer checked for loss (results/load_*.txt):

| Load | Achieved | Stored / delivered | Ingest p50 / p99 | Send→screen p50 / p99 |
|---|---|---|---|---|
| 50×10/s, 1 event/request | 499.5 ev/s | 30000 / 30000, 0 lost, 0 dup | 54 / 253 ms | 65 / 294 ms |
| 50×10/s, 10 events/request | 499.4 ev/s | 20000 / 20000 | 55 / 193 ms | 62 / 206 ms |
| 200×10/s, 10 events/request | 1993 ev/s | 80000 / 80000 | 241 / 607 ms | 279 / 632 ms |
| 100×10/s, 1 event/request | **712 of 1000** | 28699 / 28699 | 116 / 575 ms | 163 / 648 ms |

Headroom is real for batching clients (4×) and thin for one-event-per-request clients (saturates at ~1.4×).
All of this is one 4-core Docker Desktop VM shared by API, Postgres and the load generator, so absolute numbers are
pessimistic and not a production benchmark. **What to change first:** (1) make ingest one round trip (a single CTE or
SQL function instead of 6 statements); this is the measured limit for small requests; (2) initial page load replays the
whole run, so a 10-hour run at 10 ev/s replays 360k events: add a snapshot or downsampled history endpoint;
(3) add a per-project rate limit, since one noisy project can take the whole ingest budget.

## What surprised me, what still concerns me
The first load test said DON'T SHIP (53 events/s, p50 636 ms), and it was wrong twice. First, the client: httpx 0.28.1
in my generator managed ~43 req/s where aiohttp did ~1100 req/s against the same `/healthz`, so I was measuring the tool.
Second, the server: with a correct client, Postgres showed 19 sessions waiting on `Lock/object`. `pg_notify` inside
the ingest transaction takes a global lock held until the commit is fsynced, so all ingests committed one by one and
4 workers made it *worse*. Moving NOTIFY after commit (safe because it is only a hint) took it from ~300 to 500 ev/s.
The tests also caught a real bug: a NUL byte in a metric name passed validation and failed inside the transaction as a
500. The write was atomic, but clients retry 5xx, so one bad event would block a producer forever. Still concerning:
the timing difference above; the `conflicts` counter counts rejected attempts, not distinct points, so retries inflate
it; first-write-wins hides a buggy client unless someone watches `conflicts`; the key sits in the tab's sessionStorage;
there is no backup, retention or replica; and every number here comes from one laptop.
