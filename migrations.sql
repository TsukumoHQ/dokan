-- dokan schema: scripts, runs, logs. Hand-applied at boot (idempotent).

CREATE TABLE IF NOT EXISTS scripts (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT        NOT NULL,
    runtime      TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    description  TEXT,
    version      INT         NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scripts_name ON scripts (name);

-- Semantic search (P3): local BGE-small embeddings, 384-dim. pgvector cosine.
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE scripts ADD COLUMN IF NOT EXISTS embedding vector(384);

CREATE TABLE IF NOT EXISTS runs (
    id           BIGSERIAL PRIMARY KEY,
    script_id    BIGINT      NOT NULL REFERENCES scripts (id),
    status       TEXT        NOT NULL DEFAULT 'pending',  -- pending|running|succeeded|failed|canceled
    input        JSONB,
    exit_code    INT,
    error        TEXT,
    attempts     INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_script ON runs (script_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs (status);

-- One row per log line. seq is monotonic per run; serves as the opaque cursor.
CREATE TABLE IF NOT EXISTS logs (
    run_id  BIGINT      NOT NULL REFERENCES runs (id),
    seq     BIGINT      NOT NULL,
    stream  TEXT        NOT NULL,  -- stdout|stderr
    line    TEXT        NOT NULL,
    ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_logs_err ON logs (run_id, seq) WHERE stream = 'stderr';

-- Backfill for DBs created before `attempts` existed.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;

-- Cron schedules: each tick inserts a pending run for script_id.
CREATE TABLE IF NOT EXISTS schedules (
    id         BIGSERIAL PRIMARY KEY,
    script_id  BIGINT      NOT NULL REFERENCES scripts (id),
    cron       TEXT        NOT NULL,   -- 6-field (leading seconds) per tokio-cron-scheduler
    input      JSONB,
    enabled    BOOLEAN     NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pending-run claim path; partial index keeps the queue scan tight.
CREATE INDEX IF NOT EXISTS idx_runs_pending ON runs (id) WHERE status = 'pending';

-- ── Flows (P2): declarative DAG of steps, each step = one container run. ──

CREATE TABLE IF NOT EXISTS flows (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    spec       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS flow_runs (
    id          BIGSERIAL PRIMARY KEY,
    flow_id     BIGINT      NOT NULL REFERENCES flows (id),
    status      TEXT        NOT NULL DEFAULT 'pending',  -- pending|running|succeeded|failed
    input       JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_flow_runs_pending ON flow_runs (id) WHERE status = 'pending';

-- One row per step per flow_run. Status here IS the durability checkpoint:
-- on resume, 'succeeded' steps are skipped and the DAG continues at the boundary.
CREATE TABLE IF NOT EXISTS flow_steps (
    id          BIGSERIAL PRIMARY KEY,
    flow_run_id BIGINT      NOT NULL REFERENCES flow_runs (id),
    step_id     TEXT        NOT NULL,
    script_id   BIGINT      NOT NULL REFERENCES scripts (id),
    input       JSONB,
    depends_on  TEXT[]      NOT NULL DEFAULT '{}',
    status      TEXT        NOT NULL DEFAULT 'pending',  -- pending|running|succeeded|failed
    run_id      BIGINT,
    output      TEXT,
    UNIQUE (flow_run_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_flow_steps_run ON flow_steps (flow_run_id);

-- Secrets (P3): injected as env vars into every job. Values masked in the UI/API.
CREATE TABLE IF NOT EXISTS secrets (
    name       TEXT PRIMARY KEY,
    value      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
