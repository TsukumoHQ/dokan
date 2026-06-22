//! Postgres-backed state: scripts, runs, logs. Plain runtime queries (no compile-time
//! macros) so the build needs no live DB or offline `.sqlx` cache.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

#[derive(Clone)]
pub struct Db {
    pub pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct Script {
    pub id: i64,
    pub name: String,
    pub runtime: String,
    pub source: String,
    pub description: Option<String>,
    pub created_by: Option<String>,
    pub version: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ScriptSummary {
    pub id: i64,
    pub name: String,
    pub runtime: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Run {
    pub id: i64,
    pub script_id: i64,
    pub status: String,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    // Joined script provenance — for the operator UI (name > bare id).
    pub script_name: String,
    pub script_description: Option<String>,
    pub script_created_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub run_id: i64,
    #[allow(dead_code)]
    pub script_id: i64,
    pub runtime: String,
    pub source: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct FlowStep {
    pub step_id: String,
    pub script_id: i64,
    pub input: serde_json::Value,
    pub depends_on: Vec<String>,
    pub status: String,
    pub output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Schedule {
    pub id: i64,
    pub script_id: i64,
    pub script_name: String,
    pub cron: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct LogLine {
    pub seq: i64,
    pub stream: String,
    pub line: String,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        // raw_sql runs the whole multi-statement script in one go (simple query
        // protocol) — no fragile semicolon splitting.
        sqlx::raw_sql(include_str!("../migrations.sql"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---- scripts ----

    pub async fn insert_script(
        &self,
        name: &str,
        runtime: &str,
        source: &str,
        description: Option<&str>,
        created_by: Option<&str>,
        embedding: Option<Vec<f32>>,
    ) -> Result<(i64, i32)> {
        let vec = embedding.map(pgvector::Vector::from);
        let row = sqlx::query(
            "INSERT INTO scripts (name, runtime, source, description, created_by, embedding) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, version",
        )
        .bind(name)
        .bind(runtime)
        .bind(source)
        .bind(description)
        .bind(created_by)
        .bind(vec)
        .fetch_one(&self.pool)
        .await?;
        Ok((row.get("id"), row.get("version")))
    }

    /// Semantic ranking by cosine distance over embeddings. Returns (summaries, total
    /// embedded). Caller falls back to `search_scripts` when no embedder is available.
    pub async fn semantic_search(
        &self,
        query_vec: Vec<f32>,
        limit: i64,
    ) -> Result<(Vec<ScriptSummary>, i64)> {
        let v = pgvector::Vector::from(query_vec);
        let total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM scripts WHERE embedding IS NOT NULL")
                .fetch_one(&self.pool)
                .await?;
        let rows = sqlx::query(
            "SELECT id, name, runtime, description FROM scripts \
             WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2",
        )
        .bind(&v)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|r| ScriptSummary {
                id: r.get("id"),
                name: r.get("name"),
                runtime: r.get("runtime"),
                description: r.get("description"),
            })
            .collect();
        Ok((out, total))
    }

    pub async fn get_script(&self, id: i64) -> Result<Option<Script>> {
        let row = sqlx::query(
            "SELECT id, name, runtime, source, description, created_by, version, created_at \
             FROM scripts WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| Script {
            id: r.get("id"),
            name: r.get("name"),
            runtime: r.get("runtime"),
            source: r.get("source"),
            description: r.get("description"),
            created_by: r.get("created_by"),
            version: r.get("version"),
            created_at: r.get("created_at"),
        }))
    }

    /// Substring OR trigram-similarity ranking over name+description — the fallback when
    /// no embedder is loaded. pg_trgm makes it typo-tolerant (the old substring-only path
    /// returned nothing on a fuzzy query). similarity() is in [0,1]; 0.2 is a permissive
    /// floor so near-misses surface without flooding on noise.
    pub async fn search_scripts(&self, query: &str, limit: i64) -> Result<(Vec<ScriptSummary>, i64)> {
        let pattern = format!("%{}%", query);
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM scripts \
             WHERE name ILIKE $1 OR description ILIKE $1 \
                OR similarity(name, $2) > 0.2 OR similarity(coalesce(description, ''), $2) > 0.2",
        )
        .bind(&pattern)
        .bind(query)
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT id, name, runtime, description, \
                GREATEST(similarity(name, $2), similarity(coalesce(description, ''), $2)) AS sim \
             FROM scripts \
             WHERE name ILIKE $1 OR description ILIKE $1 \
                OR similarity(name, $2) > 0.2 OR similarity(coalesce(description, ''), $2) > 0.2 \
             ORDER BY sim DESC, id DESC LIMIT $3",
        )
        .bind(&pattern)
        .bind(query)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|r| ScriptSummary {
                id: r.get("id"),
                name: r.get("name"),
                runtime: r.get("runtime"),
                description: r.get("description"),
            })
            .collect();
        Ok((out, total))
    }

    /// Look up a script by exact name (newest first) for idempotent re-provisioning.
    /// Returns (id, source, version) so upload can no-op when nothing changed.
    pub async fn find_script_by_name(&self, name: &str) -> Result<Option<(i64, String, i32)>> {
        let row = sqlx::query(
            "SELECT id, source, version FROM scripts WHERE name = $1 ORDER BY id DESC LIMIT 1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.get("id"), r.get("source"), r.get("version"))))
    }

    /// Update an existing script in place and bump its version. Returns the new version.
    /// Used by upsert-by-name so a respawned agent re-provisions without duplicating rows.
    pub async fn update_script(
        &self,
        id: i64,
        runtime: &str,
        source: &str,
        description: Option<&str>,
        created_by: Option<&str>,
        embedding: Option<Vec<f32>>,
    ) -> Result<i32> {
        let vec = embedding.map(pgvector::Vector::from);
        let version: i32 = sqlx::query_scalar(
            "UPDATE scripts SET runtime = $2, source = $3, description = $4, created_by = $5, \
                 embedding = $6, version = version + 1 WHERE id = $1 RETURNING version",
        )
        .bind(id)
        .bind(runtime)
        .bind(source)
        .bind(description)
        .bind(created_by)
        .bind(vec)
        .fetch_one(&self.pool)
        .await?;
        Ok(version)
    }

    // ---- runs ----

    pub async fn insert_run(&self, script_id: i64, input: &serde_json::Value) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO runs (script_id, input, status) VALUES ($1, $2, 'pending') RETURNING id",
        )
        .bind(script_id)
        .bind(input)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn finish_run(
        &self,
        run_id: i64,
        status: &str,
        exit_code: Option<i32>,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE runs SET status = $2, exit_code = $3, error = $4, finished_at = now() \
             WHERE id = $1",
        )
        .bind(run_id)
        .bind(status)
        .bind(exit_code)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn run_status(&self, id: i64) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT status FROM runs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// (status, exit_code) — the retry decision input. A present exit_code means the
    /// script ran to completion, so a `failed` status is its own deterministic verdict
    /// (exit≠0), NOT a transient infra failure: retrying it just burns compute and
    /// reprints findings. Only a NULL exit_code (couldn't execute / timeout) is retryable.
    pub async fn run_outcome(&self, id: i64) -> Result<Option<(String, Option<i32>)>> {
        let row = sqlx::query("SELECT status, exit_code FROM runs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get("status"), r.get("exit_code"))))
    }

    /// Recent runs, newest first, optionally filtered by status.
    pub async fn list_runs(&self, status: Option<&str>, limit: i64) -> Result<Vec<Run>> {
        let rows = if let Some(st) = status {
            sqlx::query(
                "SELECT r.id, r.script_id, r.status, r.exit_code, r.error, r.created_at, \
                     s.name AS script_name, s.description AS script_description, \
                     s.created_by AS script_created_by \
                 FROM runs r JOIN scripts s ON s.id = r.script_id \
                 WHERE r.status = $1 ORDER BY r.id DESC LIMIT $2",
            )
            .bind(st)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT r.id, r.script_id, r.status, r.exit_code, r.error, r.created_at, \
                     s.name AS script_name, s.description AS script_description, \
                     s.created_by AS script_created_by \
                 FROM runs r JOIN scripts s ON s.id = r.script_id \
                 ORDER BY r.id DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| Run {
                id: r.get("id"),
                script_id: r.get("script_id"),
                status: r.get("status"),
                exit_code: r.get("exit_code"),
                error: r.get("error"),
                created_at: r.get("created_at"),
                script_name: r.get("script_name"),
                script_description: r.get("script_description"),
                script_created_by: r.get("script_created_by"),
            })
            .collect())
    }

    /// Atomically claim the oldest pending run whose runtime a worker can serve.
    /// `FOR UPDATE SKIP LOCKED` makes this safe across N concurrent workers — the
    /// hand-rolled queue (PRD §9: <100 LOC, no apalis).
    pub async fn claim_run(&self, caps: &[String]) -> Result<Option<ClaimedJob>> {
        let row = sqlx::query(
            "WITH c AS ( \
                 SELECT r.id FROM runs r JOIN scripts s ON s.id = r.script_id \
                 WHERE r.status = 'pending' AND s.runtime = ANY($1) \
                 ORDER BY r.id FOR UPDATE OF r SKIP LOCKED LIMIT 1 \
             ) \
             UPDATE runs SET status = 'running', started_at = now() \
             FROM c WHERE runs.id = c.id \
             RETURNING runs.id, runs.script_id, runs.input, \
                 (SELECT runtime FROM scripts WHERE id = runs.script_id) AS runtime, \
                 (SELECT source  FROM scripts WHERE id = runs.script_id) AS source",
        )
        .bind(caps)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| ClaimedJob {
            run_id: r.get("id"),
            script_id: r.get("script_id"),
            runtime: r.get("runtime"),
            source: r.get("source"),
            input: r.get::<Option<serde_json::Value>, _>("input").unwrap_or(serde_json::json!({})),
        }))
    }

    pub async fn mark_attempt(&self, run_id: i64) -> Result<i32> {
        let n: i32 = sqlx::query_scalar(
            "UPDATE runs SET attempts = attempts + 1 WHERE id = $1 RETURNING attempts",
        )
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    /// Requeue a failed run for retry (back to pending).
    pub async fn requeue(&self, run_id: i64) -> Result<()> {
        sqlx::query("UPDATE runs SET status = 'pending', started_at = NULL WHERE id = $1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Reclaim runs orphaned by a dead worker: still `running` past `lease_secs`, which is
    /// set well beyond the hard job timeout — a live worker always kills + finalizes by
    /// then, so anything still running is owner-less. Lease-based, so concurrent healthy
    /// workers are never disturbed. `mark_attempt` already counted the attempt, so the
    /// worker's `MAX_ATTEMPTS` cap still bounds the requeue→fail loop. Returns reclaimed n.
    pub async fn reap_orphan_runs(&self, lease_secs: f64) -> Result<u64> {
        let r = sqlx::query(
            "UPDATE runs SET status = 'pending', started_at = NULL \
             WHERE status = 'running' AND started_at < now() - make_interval(secs => $1)",
        )
        .bind(lease_secs)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected())
    }

    // ---- schedules (cron) ----

    pub async fn insert_schedule(
        &self,
        script_id: i64,
        cron: &str,
        input: &serde_json::Value,
    ) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO schedules (script_id, cron, input) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(script_id)
        .bind(cron)
        .bind(input)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn enabled_schedules(&self) -> Result<Vec<Schedule>> {
        let rows = sqlx::query(
            "SELECT sc.id, sc.script_id, s.name AS script_name, sc.cron, sc.input \
             FROM schedules sc JOIN scripts s ON s.id = sc.script_id \
             WHERE sc.enabled = true ORDER BY sc.id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Schedule {
                id: r.get("id"),
                script_id: r.get("script_id"),
                script_name: r.get("script_name"),
                cron: r.get("cron"),
                input: r.get::<Option<serde_json::Value>, _>("input")
                    .unwrap_or(serde_json::json!({})),
            })
            .collect())
    }

    /// Enable/disable a schedule. Returns rows affected (0 = not found).
    pub async fn set_schedule_enabled(&self, id: i64, enabled: bool) -> Result<u64> {
        let r = sqlx::query("UPDATE schedules SET enabled = $2 WHERE id = $1")
            .bind(id)
            .bind(enabled)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected())
    }

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let rows = sqlx::query(
            "SELECT sc.id, sc.script_id, s.name AS script_name, sc.cron, sc.input \
             FROM schedules sc JOIN scripts s ON s.id = sc.script_id \
             WHERE sc.enabled = true ORDER BY sc.id DESC LIMIT 50",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Schedule {
                id: r.get("id"),
                script_id: r.get("script_id"),
                script_name: r.get("script_name"),
                cron: r.get("cron"),
                input: r.get::<Option<serde_json::Value>, _>("input")
                    .unwrap_or(serde_json::json!({})),
            })
            .collect())
    }

    /// status -> count, for token-frugal `list_runs` aggregation.
    pub async fn run_status_counts(&self) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query("SELECT status, count(*) AS n FROM runs GROUP BY status")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("status"), r.get::<i64, _>("n")))
            .collect())
    }

    // ---- secrets (P3) ----

    pub async fn upsert_secret(&self, name: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO secrets (name, value) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(name)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn secret_names(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT name FROM secrets ORDER BY name")
            .fetch_all(&self.pool)
            .await?)
    }

    /// All secrets as (name, value) — injected into job env at execution time.
    pub async fn all_secrets(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query("SELECT name, value FROM secrets")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("name"), r.get::<String, _>("value")))
            .collect())
    }

    // ---- flows (P2) ----

    pub async fn insert_flow(&self, name: &str, spec: &serde_json::Value) -> Result<i64> {
        Ok(
            sqlx::query_scalar("INSERT INTO flows (name, spec) VALUES ($1, $2) RETURNING id")
                .bind(name)
                .bind(spec)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    pub async fn get_flow_spec(&self, flow_id: i64) -> Result<Option<serde_json::Value>> {
        Ok(
            sqlx::query_scalar("SELECT spec FROM flows WHERE id = $1")
                .bind(flow_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Create a flow_run plus its flow_steps (the durability ledger), from the spec.
    pub async fn insert_flow_run(
        &self,
        flow_id: i64,
        spec: &serde_json::Value,
        input: &serde_json::Value,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let flow_run_id: i64 = sqlx::query_scalar(
            "INSERT INTO flow_runs (flow_id, input) VALUES ($1, $2) RETURNING id",
        )
        .bind(flow_id)
        .bind(input)
        .fetch_one(&mut *tx)
        .await?;

        let steps = spec.get("steps").and_then(|s| s.as_array()).cloned().unwrap_or_default();
        for st in steps {
            let step_id = st.get("id").and_then(|v| v.as_str()).unwrap_or_default();
            let script_id = st.get("script_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let step_input = st.get("input").cloned().unwrap_or(serde_json::json!({}));
            let deps: Vec<String> = st
                .get("depends_on")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|d| d.as_str().map(String::from)).collect())
                .unwrap_or_default();
            sqlx::query(
                "INSERT INTO flow_steps (flow_run_id, step_id, script_id, input, depends_on) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(flow_run_id)
            .bind(step_id)
            .bind(script_id)
            .bind(step_input)
            .bind(&deps)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(flow_run_id)
    }

    /// Claim a pending flow_run for driving (SKIP LOCKED, multi-engine safe).
    pub async fn claim_flow_run(&self) -> Result<Option<(i64, serde_json::Value)>> {
        let row = sqlx::query(
            "WITH c AS ( \
                 SELECT id FROM flow_runs WHERE status = 'pending' \
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1 \
             ) \
             UPDATE flow_runs SET status = 'running', started_at = now() FROM c \
             WHERE flow_runs.id = c.id \
             RETURNING flow_runs.id, flow_runs.input",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.get("id"),
                r.get::<Option<serde_json::Value>, _>("input")
                    .unwrap_or(serde_json::json!({})),
            )
        }))
    }

    pub async fn flow_steps(&self, flow_run_id: i64) -> Result<Vec<FlowStep>> {
        let rows = sqlx::query(
            "SELECT step_id, script_id, input, depends_on, status, output \
             FROM flow_steps WHERE flow_run_id = $1 ORDER BY id",
        )
        .bind(flow_run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| FlowStep {
                step_id: r.get("step_id"),
                script_id: r.get("script_id"),
                input: r.get::<Option<serde_json::Value>, _>("input")
                    .unwrap_or(serde_json::json!({})),
                depends_on: r.get("depends_on"),
                status: r.get("status"),
                output: r.get("output"),
            })
            .collect())
    }

    pub async fn set_step_running(&self, flow_run_id: i64, step_id: &str, run_id: i64) -> Result<()> {
        sqlx::query(
            "UPDATE flow_steps SET status = 'running', run_id = $3 \
             WHERE flow_run_id = $1 AND step_id = $2",
        )
        .bind(flow_run_id)
        .bind(step_id)
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finish_step(
        &self,
        flow_run_id: i64,
        step_id: &str,
        status: &str,
        output: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE flow_steps SET status = $3, output = $4 \
             WHERE flow_run_id = $1 AND step_id = $2",
        )
        .bind(flow_run_id)
        .bind(step_id)
        .bind(status)
        .bind(output)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn finish_flow_run(&self, flow_run_id: i64, status: &str) -> Result<()> {
        sqlx::query("UPDATE flow_runs SET status = $2, finished_at = now() WHERE id = $1")
            .bind(flow_run_id)
            .bind(status)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn flow_run_status(&self, flow_run_id: i64) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT status FROM flow_runs WHERE id = $1")
            .bind(flow_run_id)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// Heartbeat a driving flow_run — the engine bumps this between step batches so the
    /// reaper can tell "alive but long-running" from "owner died". Cheap single-row update.
    pub async fn touch_flow_run(&self, id: i64) -> Result<()> {
        sqlx::query("UPDATE flow_runs SET started_at = now() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Reclaim flow_runs whose driving engine died: `running` with a stale (or absent)
    /// heartbeat past `lease_secs`. Completed steps persist, so a reclaimed flow resumes at
    /// the step boundary. Unlike the old blunt "reset every running flow_run", this is
    /// lease-based — a healthy engine heartbeating its flows is never reclaimed out from
    /// under itself, which is what makes multi-engine safe. Returns reclaimed n.
    pub async fn reap_orphan_flow_runs(&self, lease_secs: f64) -> Result<u64> {
        let r = sqlx::query(
            "UPDATE flow_runs SET status = 'pending' \
             WHERE status = 'running' \
               AND (started_at IS NULL OR started_at < now() - make_interval(secs => $1))",
        )
        .bind(lease_secs)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected())
    }

    // ---- logs ----

    pub async fn append_log(&self, run_id: i64, seq: i64, stream: &str, line: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO logs (run_id, seq, stream, line) VALUES ($1, $2, $3, $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(run_id)
        .bind(seq)
        .bind(stream)
        .bind(line)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lines with seq > after, oldest-first, capped at `limit`.
    pub async fn read_logs_after(
        &self,
        run_id: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<LogLine>> {
        let rows = sqlx::query(
            "SELECT seq, stream, line FROM logs WHERE run_id = $1 AND seq > $2 \
             ORDER BY seq ASC LIMIT $3",
        )
        .bind(run_id)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| LogLine {
                seq: r.get("seq"),
                stream: r.get("stream"),
                line: r.get("line"),
            })
            .collect())
    }

    /// Last stdout line of a run — used as a step's "output" passed to dependents.
    pub async fn last_stdout(&self, run_id: i64) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT line FROM logs WHERE run_id = $1 AND stream = 'stdout' \
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn max_log_seq(&self, run_id: i64) -> Result<i64> {
        let v: Option<i64> =
            sqlx::query_scalar("SELECT max(seq) FROM logs WHERE run_id = $1")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(v.unwrap_or(0))
    }
}
