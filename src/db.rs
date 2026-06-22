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
    ) -> Result<(i64, i32)> {
        let row = sqlx::query(
            "INSERT INTO scripts (name, runtime, source, description) \
             VALUES ($1, $2, $3, $4) RETURNING id, version",
        )
        .bind(name)
        .bind(runtime)
        .bind(source)
        .bind(description)
        .fetch_one(&self.pool)
        .await?;
        Ok((row.get("id"), row.get("version")))
    }

    pub async fn get_script(&self, id: i64) -> Result<Option<Script>> {
        let row = sqlx::query(
            "SELECT id, name, runtime, source, description, version, created_at \
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
            version: r.get("version"),
            created_at: r.get("created_at"),
        }))
    }

    /// Substring ranking over name+description. Real semantic search (fastembed+pgvector)
    /// is a later phase; this keeps the tool contract stable in the meantime.
    pub async fn search_scripts(&self, query: &str, limit: i64) -> Result<(Vec<ScriptSummary>, i64)> {
        let pattern = format!("%{}%", query);
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM scripts WHERE name ILIKE $1 OR description ILIKE $1",
        )
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT id, name, runtime, description FROM scripts \
             WHERE name ILIKE $1 OR description ILIKE $1 ORDER BY id DESC LIMIT $2",
        )
        .bind(&pattern)
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

    pub async fn mark_running(&self, run_id: i64) -> Result<()> {
        sqlx::query("UPDATE runs SET status = 'running', started_at = now() WHERE id = $1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
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

    /// Recent runs, newest first, optionally filtered by status.
    pub async fn list_runs(&self, status: Option<&str>, limit: i64) -> Result<Vec<Run>> {
        let rows = if let Some(st) = status {
            sqlx::query(
                "SELECT id, script_id, status, exit_code, error, created_at FROM runs \
                 WHERE status = $1 ORDER BY id DESC LIMIT $2",
            )
            .bind(st)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, script_id, status, exit_code, error, created_at FROM runs \
                 ORDER BY id DESC LIMIT $1",
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

    pub async fn max_log_seq(&self, run_id: i64) -> Result<i64> {
        let v: Option<i64> =
            sqlx::query_scalar("SELECT max(seq) FROM logs WHERE run_id = $1")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(v.unwrap_or(0))
    }
}
