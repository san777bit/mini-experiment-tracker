# Design changes after the first version

[DESIGN.md](DESIGN.md) is the one-page design written before code. Each change below was forced by a test,
an attack or a measurement; the evidence is in STRESS.md (rounds 2-3), PERF.md (round 4) and `results/`.

## Round 2 (after attacks, see STRESS.md)
- Streams share one in-memory tail (last 5000 rows) per run per process; a viewer behind it reads the DB.
- Writers of one run queue in memory before taking a DB connection; a project gets at most 12 of 20 connections per
  process; queue > 64 → 429, wait > 5 s → 503.
- Writes re-check the key inside the transaction (`FOR SHARE`); rotation revokes first and only if still active.
- Every DB call is bounded (3 s command timeout, bounded acquire/release); DB trouble → 503 + Retry-After.
- Bodies over 2 MB → 413 before parsing; metric keys normalized to NFC; cursors limited to bigint.
- Rejected conflicting values are stored once each in `metric_conflicts`; `runs.conflicts` counts distinct values.

## Round 3 (fairness and cheap rejections, see STRESS.md)
- The hard per-project cap (12 of 20) is gone. Every pooled connection goes through a work-conserving, max-min
  fair gate: a lone project can use all 20; when tenants wait, a released connection goes to the waiting tenant
  holding the fewest. Writers of one run still queue in memory first (one per run per process reaches the DB).
- Rejections happen before the body is read and without the DB, from the `Authorization` header alone: a key the DB
  already rejected (cached; unknown and revoked keys never become valid) → 401; a project over its token-bucket
  budget (300 req/s, burst 600, per process) → 429; a write to a run whose queue is full → 429 (only for the
  owning project, so a foreign run id still looks missing). The key→project cache is used only to charge budgets,
  never to grant access: the DB check and the in-transaction key re-check are unchanged.
- Ingest is one statement: plpgsql `ingest_batch` (row lock, key re-check, classify, insert, bump `last_seq`).
  Statements inside a VOLATILE plpgsql function take fresh snapshots, so reads after the lock see the previous
  writer's commit; a single CTE statement would not (its snapshot predates the lock wait).

## Round 4 (latency, see PERF.md)
- Live rows are pushed, not re-read: after commit the ingesting process gives the new rows to its own tail and puts them
  in the NOTIFY payload for the others (hint only if they do not fit or a conflict changed the counter). A tail appends
  only `seq == last_seq + 1`; a hole (e.g. two processes' NOTIFYs out of order) falls back to a DB read, and tails still
  poll every 2 s. The DB stays the source of truth; the page's contiguity check is unchanged.
- NOTIFY is sent off the request path by one background sender per process, batched while one is in flight.
- Ingest and run creation skip the separate key lookup for a key this process verified recently; their transaction
  re-checks the key `FOR SHARE` anyway (a stale cache entry ends in 401 there and is dropped). Reads always hit the DB.
- Tail reads are one SQL function (`tail_fetch`); tails and NOTIFY use their own small pool lane.
- No reset query on connection release (pool connections hold no session state); whole pool opened at boot.
- Per-process request budget raised to 1000/s (300/s throttled a legitimate single-project load on its busiest worker).

## Round 5 (review against the task: isolation of the push path, revocation, loss detection)
- Pushed rows skip the DB read and therefore RLS. Each NOTIFY item now names its project, and a tail drops rows whose
  project is not its own (counted, logged). Threat model, checked: the `app` role sees nothing without
  `app.project_id`, but can set it to any project itself, so RLS protects against query bugs in this app, not against
  someone holding the app's DB credentials. Such a holder could also `LISTEN` to the channel; the payload gives
  them nothing they could not already read.
- Rotation announces the revoked key on a `key_events` channel: every process closes that key's open streams at once
  (was: next 5 s re-check; measured 5.5 s) and drops its cached write auth. The 5 s re-check stays as the fallback
  for a missed notification.
- Loss detection: a batch may carry `client_points` (distinct points the producer has generated for the run). The run
  shows `unconfirmed = client_points - last_seq` (API and page), and `GET /api/admin/missing-points` lists runs
  where the producer reported more than is stored and has been quiet for N seconds: an alerting hook for events
  that never reached the server, which the server cannot otherwise see.
- Pool/gate sampling every 50 ms is off unless `DIAG=1`.
