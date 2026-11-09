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
