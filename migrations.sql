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
