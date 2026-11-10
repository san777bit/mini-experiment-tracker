# Design (written before code)

The design as decided before coding, tightened to one page (original wording: commit 82f48bc). Changes forced
later by tests and measurements: [CHANGES.md](CHANGES.md).

## Model and event format
`projects` 1-n `api_keys` (sha256, `revoked_at`); `projects` 1-n `runs` (`last_seq`); `runs` 1-n
`metrics(run_id, seq, project_id, metric_key, step, value, client_ts, batch_id)`, `PK(run_id, seq)`,
`UNIQUE(run_id, metric_key, step)`. `POST /api/runs/{id}/metrics` takes
`{"batch_id": uuid, "events": [{"key": "loss", "step": 12, "value": 0.31, "ts": "..."}]}` (≤1000, finite values).

## Ordering
A point is `(run, key, step)`; the UI orders by `step`, never by arrival. For streaming, every *accepted* point
gets a per-run `seq` (1, 2, 3, no gaps). Not a global `BIGSERIAL`: sequence values commit out of order, so a reader
that saw 11 could later miss 10. The batch transaction locks the run row (`FOR UPDATE`), reads `last_seq`, inserts,
bumps it: committed seqs of a run are always a contiguous prefix. Cost: one row lock per batch per run.

## Same batch sent again
One transaction per batch: all or nothing. Under the run lock each event is **new** (insert), **duplicate** (same
key/step/value, ignored) or **conflict** (same key/step, other value: first write wins, counted, returned, logged).
Response `{accepted, duplicates, conflicts, last_seq}`; a retry returns `accepted=0`. A client that lost the response
cannot know whether it committed, so it resends the same events; idempotency makes that safe.

## Live updates and catch-up after a disconnect
SSE `GET /api/runs/{id}/stream`, each message `id: <seq>`. The page streams with `fetch` (key in the
`Authorization` header, never in a URL) and reconnects with `Last-Event-ID: <last applied seq>`; the server sends
`seq > cursor ORDER BY seq` from the DB. No per-client queue: the cursor is the queue, the DB is the buffer, so a slow
client lags (TCP backpressure) but cannot overflow. After commit a Postgres `NOTIFY` wakes streams; it is a hint
(*changed after measurement: first inside the transaction, where it serialized all commits; now after commit*), and
streams also poll every 2 s, so a lost NOTIFY costs latency, not data. The page applies only `seq == last+1`, counts
repeats and gaps, and reconnects on a gap. First load streams from 0: one code path for snapshot and catch-up.

## Project isolation
Key → project. Every query filters `project_id = $auth`, and RLS on `runs`/`metrics` (`app.project_id` set per
transaction, non-owner role) is a second wall. Foreign, missing and malformed IDs return the same 404 from the same
query; IDs are UUIDv4. SSE uses the same auth; open streams re-check the key every 5 s. `Cache-Control: no-store`, no
response cache; logs never contain keys or bodies. Rotation issues a new key and revokes the caller's at once.

## Known weak spots (to attack in tests)
Per-run lock throughput; one process holds the stream hub; full replay on first load grows with the run;
first-write-wins hides a client bug unless someone reads `conflicts`; key in browser memory.
