-- Runs once on an empty data volume (docker-entrypoint-initdb.d), as the owner role.
-- The API connects as role "app": not owner, no BYPASSRLS, so RLS policies apply to it.

CREATE ROLE app LOGIN PASSWORD 'app';

CREATE TABLE projects (
    id          uuid PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    project_id  uuid NOT NULL REFERENCES projects(id),
    key_hash    bytea NOT NULL UNIQUE,
    prefix      text NOT NULL,              -- first chars, safe to log/show
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE TABLE runs (
    id          uuid PRIMARY KEY,
    project_id  uuid NOT NULL REFERENCES projects(id),
    name        text NOT NULL,
    last_seq    bigint NOT NULL DEFAULT 0,  -- per-run gapless counter, bumped under row lock
    conflicts   bigint NOT NULL DEFAULT 0,  -- events rejected: same key/step, different value
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX runs_project_idx ON runs (project_id, created_at DESC);

CREATE TABLE metrics (
    run_id      uuid NOT NULL REFERENCES runs(id),
    seq         bigint NOT NULL,
    project_id  uuid NOT NULL,              -- denormalized for the RLS policy
    metric_key  text NOT NULL,
    step        bigint NOT NULL,
    value       double precision NOT NULL,
    client_ts   timestamptz,
    batch_id    uuid NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, metric_key, step)
);

-- Values rejected by first-write-wins, kept for audit. One row per distinct rejected value,
-- so client retries do not inflate runs.conflicts.
CREATE TABLE metric_conflicts (
    run_id      uuid NOT NULL REFERENCES runs(id),
    project_id  uuid NOT NULL,
    metric_key  text NOT NULL,
    step        bigint NOT NULL,
    value       double precision NOT NULL,
    batch_id    uuid NOT NULL,              -- first batch that carried it
    seen_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, metric_key, step, value)
);

ALTER TABLE runs    ENABLE ROW LEVEL SECURITY;
ALTER TABLE metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE metric_conflicts ENABLE ROW LEVEL SECURITY;

-- NULLIF: after a SET LOCAL transaction ends the GUC becomes '' (not NULL) on the same connection.
CREATE POLICY runs_isolation ON runs
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid);
CREATE POLICY metrics_isolation ON metrics
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid);

GRANT SELECT, INSERT ON projects TO app;
GRANT SELECT, INSERT, UPDATE ON api_keys TO app;
GRANT SELECT, INSERT, UPDATE ON runs TO app;
CREATE POLICY conflicts_isolation ON metric_conflicts
    USING (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid)
    WITH CHECK (project_id = NULLIF(current_setting('app.project_id', true), '')::uuid);

GRANT SELECT, INSERT ON metrics TO app;
GRANT SELECT, INSERT ON metric_conflicts TO app;

-- One ingest batch in one round trip (round 3; round 2 sent 6-7 statements per batch).
-- SECURITY INVOKER (default): runs as the calling `app` role, so RLS still applies.
-- VOLATILE plpgsql under READ COMMITTED: every statement below takes a FRESH snapshot, so the
-- reads after the row lock see everything committed by the writer we waited for. A single
-- CTE statement would not: its snapshot is taken before it waits for the lock.
CREATE FUNCTION ingest_batch(
    p_run uuid, p_project uuid, p_key uuid, p_batch uuid,
    p_keys text[], p_steps bigint[], p_values float8[], p_ts timestamptz[],
    OUT o_status text, OUT o_accepted int, OUT o_duplicates int, OUT o_conflicts int,
    OUT o_new_conflicts int, OUT o_last_seq bigint,
    OUT o_new_ords int[])           -- 1-based positions of the accepted events, in seq order
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    v_last bigint;
BEGIN
    o_accepted := 0; o_duplicates := 0; o_conflicts := 0; o_new_conflicts := 0;
    PERFORM set_config('app.project_id', p_project::text, true);   -- reset at end of statement

    -- Serializes all writers of this run: seqs are assigned and committed strictly in order.
    SELECT r.last_seq INTO v_last FROM runs r WHERE r.id = p_run AND r.project_id = p_project FOR UPDATE;
    IF NOT FOUND THEN o_status := 'not_found'; RETURN; END IF;
    -- A key revoked while this write waited: rotation either committed first (we see it) or
    -- waits on this share lock until we commit.
    PERFORM 1 FROM api_keys k WHERE k.id = p_key AND k.revoked_at IS NULL FOR SHARE;
    IF NOT FOUND THEN o_status := 'revoked'; RETURN; END IF;

    WITH ev AS (
        SELECT u.k, u.s, u.v, u.t, u.ord
          FROM unnest(p_keys, p_steps, p_values, p_ts) WITH ORDINALITY AS u(k, s, v, t, ord)
    ), firsts AS (          -- first occurrence of each point in this batch
        SELECT DISTINCT ON (k, s) k, s, v, ord FROM ev ORDER BY k, s, ord
    ), ref AS (             -- the value that wins: stored if any, else the batch's first
        SELECT f.k, f.s, f.ord AS first_ord, m.value IS NULL AS is_new, COALESCE(m.value, f.v) AS v
          FROM firsts f
          LEFT JOIN metrics m ON m.run_id = p_run AND m.metric_key = f.k AND m.step = f.s
    ), cls AS (
        SELECT e.k, e.s, e.v, e.t, e.ord,
               CASE WHEN r.is_new AND e.ord = r.first_ord THEN 'new'
                    WHEN e.v = r.v THEN 'dup'
                    ELSE 'rej' END AS c
          FROM ev e JOIN ref r ON r.k = e.k AND r.s = e.s
    ), ins AS (
        INSERT INTO metrics (run_id, seq, project_id, metric_key, step, value, client_ts, batch_id)
        SELECT p_run, v_last + row_number() OVER (ORDER BY ord), p_project, k, s, v, t, p_batch
          FROM cls WHERE c = 'new'
        RETURNING 1
    ), rej AS (             -- one row per distinct rejected value (retries do not inflate it)
        INSERT INTO metric_conflicts (run_id, project_id, metric_key, step, value, batch_id)
        SELECT p_run, p_project, k, s, v, p_batch FROM cls WHERE c = 'rej'
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT (SELECT count(*) FROM ins), (SELECT count(*) FROM cls WHERE c = 'dup'),
           (SELECT count(*) FROM cls WHERE c = 'rej'), (SELECT count(*) FROM rej),
           (SELECT array_agg(ord ORDER BY ord) FROM cls WHERE c = 'new')
      INTO o_accepted, o_duplicates, o_conflicts, o_new_conflicts, o_new_ords;

    o_last_seq := v_last + o_accepted;
    IF o_accepted > 0 OR o_new_conflicts > 0 THEN
        UPDATE runs SET last_seq = o_last_seq, conflicts = runs.conflicts + o_new_conflicts
         WHERE id = p_run;
    END IF;
    o_status := 'ok';
END
$$;
REVOKE ALL ON FUNCTION ingest_batch FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ingest_batch TO app;

-- A tail fetch in one round trip (round 4; it was BEGIN, set_config, 2 SELECTs, COMMIT). One statement
-- also means the run's status and its rows come from ONE snapshot. Status row is returned even when
-- there are no new rows (t_seq is NULL then); no row at all = run not visible to this project.
CREATE FUNCTION tail_fetch(p_run uuid, p_project uuid, p_after bigint, p_limit int)
RETURNS TABLE (t_last_seq bigint, t_conflicts bigint, t_seq bigint, t_key text, t_step bigint,
               t_value float8, t_received_at timestamptz)
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM set_config('app.project_id', p_project::text, true);
    RETURN QUERY
        SELECT r.last_seq, r.conflicts, m.seq, m.metric_key, m.step, m.value, m.received_at
          FROM runs r
          LEFT JOIN LATERAL (
                SELECT mm.seq, mm.metric_key, mm.step, mm.value, mm.received_at
                  FROM metrics mm
                 WHERE mm.run_id = r.id AND mm.project_id = p_project AND mm.seq > p_after
                 ORDER BY mm.seq LIMIT p_limit) m ON true
         WHERE r.id = p_run AND r.project_id = p_project
         ORDER BY m.seq;
END
$$;
REVOKE ALL ON FUNCTION tail_fetch FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tail_fetch TO app;
