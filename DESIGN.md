# Design (written before code)

## Model
`projects` 1-n `api_keys` (sha256 of key, `revoked_at`), `projects` 1-n `runs` (`last_seq` counter),
`runs` 1-n `metrics(run_id, seq, project_id, metric_key, step, value, client_ts, batch_id)`
with `PK(run_id, seq)` and `UNIQUE(run_id, metric_key, step)`.

## Event format and ordering
Client sends `POST /api/runs/{id}/metrics` with
`{"batch_id": uuid, "events": [{"key": "loss", "step": 12, "value": 0.31, "ts": "..."}]}` (max 1000 events, finite values).
The identity of a point is `(run, key, step)`. The UI orders points by `step`, never by arrival time.
For streaming, the server gives every *accepted* point a per-run `seq` (1, 2, 3, ... no gaps).

`seq` is NOT a global `BIGSERIAL`: sequence values commit out of order, so a reader that saw seq 11
can later miss seq 10 committed after it. Instead the batch transaction takes `SELECT ... FOR UPDATE`
on the run row, reads `last_seq`, inserts, bumps `last_seq`. Writes to one run are serialized,
so committed seqs of a run are always a contiguous prefix. Cost: one row lock per batch per run.

## Same batch sent again
The whole batch is one transaction: all of it is stored or none of it. Under the run lock each event is
classified against what is stored: **new** (insert), **duplicate** (same key/step/value, ignored),
**conflict** (same key/step, different value: first write wins, counted, returned, logged).
Response: `{accepted, duplicates, conflicts, last_seq}`. A retried batch returns `accepted=0`.
A client that lost the response cannot tell committed from not committed, so it must retry the same
events; idempotency makes that safe. `batch_id` is stored for audit only.

## Live updates and catch-up
SSE `GET /api/runs/{id}/stream`, each message `id: <seq>`. The browser client uses `fetch` streaming
(so the API key goes in the `Authorization` header, never in a URL) and reconnects with
`Last-Event-ID: <last seq it applied>`. The server streams from the DB: `seq > cursor ORDER BY seq`.
There is no per-client in-memory event queue: the cursor is the queue and the DB is the buffer.
A slow client just lags (TCP backpressure blocks the generator); it cannot overflow or drop.
Wake-up: after commit, Postgres `NOTIFY run_events` (payload `run_id:seq`) is a *hint*.
*(Changed after measurement: originally NOTIFY ran inside the ingest transaction; Postgres holds a global
notify lock until the commit is flushed, which serialized all ingests. It now runs right after commit.)* Every stream
also polls every 2 s, so a lost NOTIFY (listener reconnect) costs latency, not data.
Client checks `seq == last+1`; a gap or duplicate is counted and shown on the page, gap forces reconnect.
First page load = stream from `seq 0`, so there is one code path for snapshot and catch-up.

## Project isolation
- Key -> project. Every query has `project_id = $auth` and Postgres RLS on `runs`/`metrics`
  (`SET LOCAL app.project_id`) as a second wall; the app connects as a non-owner role.
- Foreign run and non-existent run return the same `404 {"error":"not_found"}` from the same query
  (no timing or message difference). IDs are UUIDv4. Malformed IDs are also 404.
- SSE uses the same auth dependency; open streams re-check the key every 5 s and close if revoked.
- No response cache; API sets `Cache-Control: no-store`. Logs never contain keys or request bodies.
- Rotation `POST /api/keys/rotate` issues a new key and revokes the calling key immediately (no grace period).

## Changed in round 2 (after attacks, see STRESS.md)
- Streams share one in-memory tail (last 5000 rows) per run per process; a viewer behind it reads the DB.
- Writers of one run queue in memory before taking a DB connection; a project gets at most 12 of 20 connections per
  process; queue > 64 → 429, wait > 5 s → 503.
- Writes re-check the key inside the transaction (`FOR SHARE`); rotation revokes first and only if still active.
- Every DB call is bounded (3 s command timeout, bounded acquire/release); DB trouble → 503 + Retry-After.
- Bodies over 2 MB → 413 before parsing; metric keys normalized to NFC; cursors limited to bigint.
- Rejected conflicting values are stored once each in `metric_conflicts`; `runs.conflicts` counts distinct values.

## Known weak spots (to attack in tests)
Per-run lock throughput; single API process holds the stream hub; full replay on first load grows with run size;
first-write-wins hides a client bug unless someone reads `conflicts`; key in browser memory.
