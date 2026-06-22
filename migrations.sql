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

-- Provenance: free-text creator/owner tag, surfaced in the operator UI.
ALTER TABLE scripts ADD COLUMN IF NOT EXISTS created_by TEXT;

-- Multi-worker reclaim: lease/heartbeat for flow_runs so a dead engine's in-flight
-- flows can be reclaimed by a healthy one WITHOUT yanking live work (replaces the old
-- blunt "reset every running flow_run" boot reset). `runs` already has started_at.
ALTER TABLE flow_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;

-- Typo-tolerant script search: pg_trgm powers similarity() so search_script catches
-- near-misses (the substring-only fallback returned 0 on fuzzy queries). GIN trigram
-- index keeps it cheap as the registry grows.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_scripts_name_trgm ON scripts USING gin (name gin_trgm_ops);

-- Structured result channel: a job emits `::dokan:result:: {json}` on stdout; dokan
-- captures the last one here so monitors return findings without callers parsing stdout,
-- and the relay egress carries it for event-driven alerting.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS result JSONB;

-- Run-or-recall: content-addressed cache. cache_key = hash(runtime+source+input+secrets
-- generation). A cache:true run recalls a prior succeeded run with the same key instead of
-- spawning a container — exploits dokan's determinism. Bump secrets_generation on any secret
-- change so env-dependent results invalidate.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cache_key TEXT;
CREATE INDEX IF NOT EXISTS idx_runs_cache_key ON runs (cache_key) WHERE cache_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v BIGINT NOT NULL);
INSERT INTO meta (k, v) VALUES ('secrets_generation', 0) ON CONFLICT (k) DO NOTHING;

-- Agent identity (multi-agent fleet on one runtime). Runs carry the triggering agent for
-- provenance, secret scoping, and quota. Secrets can be global (in `secrets`) or scoped to
-- one agent (here); a job sees global + its agent's scoped secrets (scoped overrides).
ALTER TABLE runs ADD COLUMN IF NOT EXISTS agent_id TEXT;
CREATE INDEX IF NOT EXISTS idx_runs_agent ON runs (agent_id) WHERE agent_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS agent_secrets (
    agent_id   TEXT        NOT NULL,
    name       TEXT        NOT NULL,
    value      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (agent_id, name)
);

-- Agent-defined triggers: reactive composition without an external orchestrator. When a
-- run of source_script emits a structured result that CONTAINS `predicate` (JSONB @>),
-- the executor enqueues target_script. Fires server-side in one query at result time.
CREATE TABLE IF NOT EXISTS triggers (
    id               BIGSERIAL PRIMARY KEY,
    source_script_id BIGINT      NOT NULL REFERENCES scripts (id),
    predicate        JSONB       NOT NULL DEFAULT '{}',
    target_script_id BIGINT      NOT NULL REFERENCES scripts (id),
    agent_id         TEXT,
    enabled          BOOLEAN     NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_triggers_source ON triggers (source_script_id) WHERE enabled;
