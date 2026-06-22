//! The MCP control plane — dokan's primary API. Every response is engineered for
//! low token usage: IDs over payloads, field projection, cursor pagination,
//! tail/error-first log truncation, and "showing X of Y" budget notes.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::cron::Cron;
use crate::db::Db;
use crate::embed::Embedder;
use crate::exec::Executor;

#[derive(Clone)]
pub struct Dokan {
    db: Db,
    exec: Arc<Executor>,
    cron: Option<Arc<Cron>>,
    embedder: Option<Embedder>,
    // Populated by #[tool_router]; read by the generated ServerHandler glue.
    #[allow(dead_code)]
    tool_router: rmcp::handler::server::router::tool::ToolRouter<Self>,
}

/// Default per-agent in-flight (pending+running) cap — backpressure so one agent can't
/// swamp the shared runtime. Generous; tighten per deployment if needed.
const AGENT_MAX_CONCURRENT: i64 = 25;

/// Runtimes dokan can execute (reported by whoami so an agent self-configures).
const SUPPORTED_RUNTIMES: [&str; 3] = ["python", "node", "bash"];

/// Canonicalize a JSON value to a deterministic string (object keys sorted recursively),
/// so the cache key is stable regardless of input key order.
fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{:?}:{}", k, canonical_json(&m[k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(a) => {
            let inner: Vec<String> = a.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => other.to_string(),
    }
}

/// Content-address a run: hash(runtime + source + canonical(input) + secrets generation).
/// Same inputs ⇒ same key ⇒ recallable (dokan jobs are deterministic).
fn run_cache_key(
    runtime: &str,
    image_digest: &str,
    source: &str,
    input: &serde_json::Value,
    secrets_gen: i64,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(runtime.as_bytes());
    h.update([0x1f]);
    h.update(image_digest.as_bytes()); // an image update invalidates the cache
    h.update([0x1f]);
    h.update(source.as_bytes());
    h.update([0x1f]);
    h.update(canonical_json(input).as_bytes());
    h.update([0x1f]);
    h.update(secrets_gen.to_le_bytes());
    format!("{:x}", h.finalize())
}

fn ok<T: serde::Serialize>(v: T) -> Result<CallToolResult, McpError> {
    let s = serde_json::to_string(&v)
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![Content::text(s)]))
}

// ---- tool parameter structs ----

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchArgs {
    /// Search query, matched against script name + description.
    pub query: String,
    /// Max results (default 10).
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetScriptArgs {
    pub id: i64,
    /// When true, include the full source body (costly). Default false.
    pub include_source: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadArgs {
    /// Human-readable script name.
    pub name: String,
    /// One of: python, node, bash.
    pub runtime: String,
    /// Script source code.
    pub source: String,
    /// One-line description (used by search). Optional but recommended.
    pub description: Option<String>,
    /// Free-text creator/owner tag (e.g. agent name or human). Shown in the operator UI.
    pub created_by: Option<String>,
    /// Idempotent re-provision: if a script of this name exists, update it (version bump,
    /// or no-op when the source is identical) and return its id instead of creating a
    /// duplicate. Default false. Use it so a respawned agent can safely re-upload.
    pub upsert: Option<bool>,
    /// Network access for the job. Default true (monitors that hit APIs need it). Set FALSE
    /// for a pure-compute script: it runs network-disabled, making its output a deterministic
    /// function of its inputs — soundly cacheable (cache:true) and provable via its receipt.
    pub network: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunArgs {
    /// Script id to run.
    pub script_id: i64,
    /// Arbitrary JSON passed to the job as the DOKAN_INPUT env var. Optional.
    pub input: Option<serde_json::Value>,
    /// Run-or-recall: if true and an identical run (same script source + input + secrets
    /// generation) already succeeded, return its result WITHOUT spawning a container
    /// (status "recalled"). Opt-in — leave false for monitors/time-sensitive jobs that must
    /// re-execute. Exploits dokan's determinism.
    pub cache: Option<bool>,
    /// Your agent id. Tags the run for provenance, selects which scoped secrets the job
    /// sees (global + this agent's), and counts against this agent's concurrency quota.
    pub agent_id: Option<String>,
    /// Exactly-once key: if a run with this key already exists, return it instead of
    /// enqueuing a duplicate. Use for safe retries of the enqueue call itself.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadLogsArgs {
    pub run_id: i64,
    /// Return only lines after this cursor (use next_cursor from a prior call). Default 0.
    pub after_cursor: Option<i64>,
    /// Max lines (default 200).
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WaitForArgs {
    pub run_id: i64,
    /// Max seconds to block (default 30, max 120).
    pub timeout: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListRunsArgs {
    /// Filter by status: pending|running|succeeded|failed|canceled. Optional.
    pub status: Option<String>,
    /// Max rows (default 20).
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CancelArgs {
    pub run_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ComposeFlowArgs {
    /// Flow name.
    pub name: String,
    /// DAG spec: { "steps": [ { "id", "script_id", "input"?, "depends_on"? [step ids] } ] }.
    pub spec: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunFlowArgs {
    pub flow_id: i64,
    /// JSON passed to every step as deps.flow_input. Optional.
    pub input: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FlowRunArgs {
    pub flow_run_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UnscheduleArgs {
    pub schedule_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ScheduleArgs {
    pub script_id: i64,
    /// 6-field cron with leading seconds, e.g. "0 */5 * * * *" = every 5 min.
    pub cron: String,
    /// JSON input passed to each scheduled run. Optional.
    pub input: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteScriptArgs {
    pub script_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OnResultArgs {
    /// Watch runs of this script.
    pub source_script_id: i64,
    /// Fire when the run's structured result CONTAINS this object (JSONB superset match),
    /// e.g. {"alert": true}. Empty {} fires on any result.
    pub predicate: Option<serde_json::Value>,
    /// Script to enqueue when the predicate matches. It receives
    /// {trigger_result, source_run_id} as DOKAN_INPUT.
    pub target_script_id: i64,
    /// Your agent id — the enqueued run inherits it (provenance + scoped secrets + quota).
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteTriggerArgs {
    pub trigger_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetSecretArgs {
    /// Env var name injected into a job container, e.g. "OPENAI_API_KEY".
    pub name: String,
    /// Secret value. Write-only — never returned by any tool, never logged.
    pub value: String,
    /// Scope: omit for a GLOBAL secret (all jobs see it); set to your agent id for a secret
    /// only this agent's runs see (overrides a global of the same name).
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetReceiptArgs {
    pub run_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WhoamiArgs {
    /// Your agent id — to report your scoped secrets + quota usage. Optional.
    pub agent_id: Option<String>,
}

/// Validate a tokio-cron-scheduler expression: 6 whitespace-separated fields (the leading
/// column is SECONDS, unlike standard 5-field crontab). The scheduler silently rejects a
/// 5-field string by simply never firing, so catch the common mistake loudly up front.
fn validate_cron(expr: &str) -> std::result::Result<(), String> {
    let n = expr.split_whitespace().count();
    if n != 6 {
        return Err(format!(
            "cron must be 6 fields (leading SECONDS): `sec min hour day month weekday` — \
             got {n}. A standard 5-field crontab needs a leading `0 ` (e.g. `0 {expr}`)."
        ));
    }
    Ok(())
}

#[tool_router]
impl Dokan {
    pub fn new(
        db: Db,
        exec: Arc<Executor>,
        cron: Option<Arc<Cron>>,
        embedder: Option<Embedder>,
    ) -> Self {
        Self {
            db,
            exec,
            cron,
            embedder,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Search the script registry. Returns ranked IDs + 1-line descriptions only, never bodies.")]
    async fn search_script(
        &self,
        Parameters(a): Parameters<SearchArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = a.limit.unwrap_or(10).clamp(1, 100);
        // Semantic ranking when an embedder is loaded; substring fallback otherwise.
        let (rows, total, mode) = match &self.embedder {
            Some(emb) => match emb.embed(a.query.clone()).await {
                Ok(qv) => {
                    let (r, t) = self.db.semantic_search(qv, limit).await.map_err(internal)?;
                    (r, t, "semantic")
                }
                Err(_) => {
                    let (r, t) = self.db.search_scripts(&a.query, limit).await.map_err(internal)?;
                    (r, t, "fuzzy")
                }
            },
            None => {
                let (r, t) = self.db.search_scripts(&a.query, limit).await.map_err(internal)?;
                (r, t, "fuzzy")
            }
        };
        let items: Vec<_> = rows
            .iter()
            .map(|s| json!({"id": s.id, "name": s.name, "runtime": s.runtime, "desc": s.description}))
            .collect();
        ok(json!({
            "results": items,
            "mode": mode,
            "note": format!("showing {} of {}", rows.len(), total),
        }))
    }

    #[tool(description = "Fetch a script's metadata. Source body included only when include_source=true.")]
    async fn get_script(
        &self,
        Parameters(a): Parameters<GetScriptArgs>,
    ) -> Result<CallToolResult, McpError> {
        let s = self.db.get_script(a.id).await.map_err(internal)?;
        let Some(s) = s else {
            return ok(json!({"error": "not_found", "id": a.id}));
        };
        let mut v = json!({
            "id": s.id, "name": s.name, "runtime": s.runtime,
            "desc": s.description, "created_by": s.created_by, "version": s.version,
            "network": s.network, "created_at": s.created_at.to_rfc3339(),
        });
        if a.include_source.unwrap_or(false) {
            v["source"] = json!(s.source);
        } else {
            v["source_bytes"] = json!(s.source.len());
        }
        ok(v)
    }

    #[tool(description = "Upload a script. Returns script_id + version. Runtime: python|node|bash. INPUT CONTRACT: the script reads its input from the DOKAN_INPUT env var (a JSON string) — NOT stdin or argv. Secrets set via set_secret arrive as their own env vars (e.g. $OPENAI_API_KEY). A nonzero exit is treated as the script's own deterministic verdict (e.g. a monitor finding) and is NOT retried; only a container/infra failure retries. Pass upsert=true to re-provision by name idempotently (no duplicate rows on respawn). STRUCTURED RESULT: print a line `::dokan:result:: {json}` on stdout to attach a structured result to the run — it is captured (not logged), returned by wait_for/read_logs, and POSTed to the relay, so a monitor's finding reaches the agent event-driven.")]
    async fn upload_script(
        &self,
        Parameters(a): Parameters<UploadArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Idempotent re-provision: with upsert, reuse the script of the same name. No-op
        // when the source is unchanged (a respawned agent re-uploading the same thing),
        // update + version bump when it changed — never a duplicate row.
        if a.upsert.unwrap_or(false) {
            if let Some((id, source, version)) =
                self.db.find_script_by_name(&a.name).await.map_err(internal)?
            {
                if source == a.source {
                    return ok(json!({"script_id": id, "version": version, "status": "unchanged"}));
                }
                let embedding = self.embed_script(&a.name, &a.description).await;
                let version = self
                    .db
                    .update_script(
                        id,
                        &a.runtime,
                        &a.source,
                        a.description.as_deref(),
                        a.created_by.as_deref(),
                        a.network.unwrap_or(true),
                        embedding,
                    )
                    .await
                    .map_err(internal)?;
                return ok(json!({"script_id": id, "version": version, "status": "updated"}));
            }
        }
        // Captured before insert so the duplicate-name warning can compare against it.
        let prior = self.db.find_script_by_name(&a.name).await.ok().flatten();
        let embedding = self.embed_script(&a.name, &a.description).await;
        let (id, version) = self
            .db
            .insert_script(
                &a.name,
                &a.runtime,
                &a.source,
                a.description.as_deref(),
                a.created_by.as_deref(),
                a.network.unwrap_or(true),
                embedding,
            )
            .await
            .map_err(internal)?;
        let mut out = json!({"script_id": id, "version": version, "status": "uploaded"});
        // Footgun guard: a plain upload of a name that already exists silently spawns a
        // duplicate script_id (orphan accumulation). `prior` was captured before insert.
        if let Some((existing, _, _)) = prior {
            out["warning"] = json!(format!(
                "another script named '{}' already exists (id {}); this created a NEW id {}. \
                 Pass upsert=true to update in place instead of accumulating duplicates.",
                a.name, existing, id
            ));
        }
        ok(out)
    }

    #[tool(description = "Register a reactive trigger: when a run of source_script emits a result CONTAINING predicate (e.g. {\"alert\":true}), enqueue target_script with {trigger_result, source_run_id} as input. Event-driven composition with no external orchestrator. Returns trigger_id.")]
    async fn on_result(
        &self,
        Parameters(a): Parameters<OnResultArgs>,
    ) -> Result<CallToolResult, McpError> {
        for id in [a.source_script_id, a.target_script_id] {
            if self.db.get_script(id).await.map_err(internal)?.is_none() {
                return ok(json!({"error": "script_not_found", "id": id}));
            }
        }
        let predicate = a.predicate.unwrap_or(json!({}));
        if !predicate.is_object() {
            return ok(json!({"error": "predicate_must_be_object"}));
        }
        let id = self
            .db
            .insert_trigger(
                a.source_script_id,
                &predicate,
                a.target_script_id,
                a.agent_id.as_deref(),
            )
            .await
            .map_err(internal)?;
        ok(json!({"trigger_id": id, "status": "armed"}))
    }

    #[tool(description = "List active reactive triggers (on/when/run + agent).")]
    async fn list_triggers(&self) -> Result<CallToolResult, McpError> {
        let items = self.db.list_triggers().await.map_err(internal)?;
        ok(json!({"triggers": items}))
    }

    #[tool(description = "Delete a reactive trigger by id.")]
    async fn delete_trigger(
        &self,
        Parameters(a): Parameters<DeleteTriggerArgs>,
    ) -> Result<CallToolResult, McpError> {
        let removed = self.db.delete_trigger(a.trigger_id).await.map_err(internal)?;
        ok(json!({"trigger_id": a.trigger_id, "status": if removed {"deleted"} else {"not_found"}}))
    }

    /// Embed name + description for semantic search (best-effort; None when no embedder).
    async fn embed_script(&self, name: &str, description: &Option<String>) -> Option<Vec<f32>> {
        match &self.embedder {
            Some(emb) => {
                let text = format!("{} {}", name, description.clone().unwrap_or_default());
                emb.embed(text).await.ok()
            }
            None => None,
        }
    }

    #[tool(description = "Delete a script and cascade its runs, logs, and schedules (live cron jobs stopped). Refused if a flow references it. Use to clean up orphan scripts from re-uploads.")]
    async fn delete_script(
        &self,
        Parameters(a): Parameters<DeleteScriptArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.db.delete_script(a.script_id).await.map_err(internal)? {
            crate::db::DeleteResult::NotFound => {
                ok(json!({"error": "script_not_found", "id": a.script_id}))
            }
            crate::db::DeleteResult::BlockedByFlow(n) => ok(json!({
                "error": "referenced_by_flow", "id": a.script_id, "flow_steps": n,
                "hint": "a flow depends on this script; delete/retire the flow first"
            })),
            crate::db::DeleteResult::Deleted { runs, schedules } => {
                // Stop the live cron jobs for the schedules we just removed.
                if let Some(cron) = &self.cron {
                    for sid in &schedules {
                        let _ = cron.remove(*sid).await;
                    }
                }
                ok(json!({
                    "script_id": a.script_id, "status": "deleted",
                    "runs_removed": runs, "schedules_removed": schedules.len()
                }))
            }
        }
    }

    #[tool(description = "Trigger a script run. Returns run_id immediately; never blocks. Poll with read_logs or wait_for.")]
    async fn run_script(
        &self,
        Parameters(a): Parameters<RunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let script = self.db.get_script(a.script_id).await.map_err(internal)?;
        let Some(script) = script else {
            return ok(json!({"error": "script_not_found", "id": a.script_id}));
        };
        // Idempotency: a repeated enqueue with the same key returns the existing run.
        if let Some(key) = a.idempotency_key.as_deref() {
            if let Some((run_id, status)) =
                self.db.find_run_by_idempotency(key).await.map_err(internal)?
            {
                return ok(json!({"run_id": run_id, "status": status, "idempotent": true}));
            }
        }
        let input = a.input.unwrap_or(json!({}));
        // Run-or-recall: if opted in and an identical run already succeeded, return its
        // result instead of spawning a container. The key folds in the secrets generation,
        // so a secret change invalidates env-dependent recalls.
        let cache_key = if a.cache.unwrap_or(false) {
            let secrets_gen = self.db.secrets_generation().await.map_err(internal)?;
            let digest = self.exec.image_digest(&script.runtime).unwrap_or_default();
            let key = run_cache_key(&script.runtime, &digest, &script.source, &input, secrets_gen);
            if let Some((run_id, exit, result)) =
                self.db.find_cached_run(&key).await.map_err(internal)?
            {
                let mut hit = json!({
                    "run_id": run_id, "status": "recalled", "exit": exit, "cache_key": key,
                });
                if let Some(r) = result {
                    hit["result"] = r;
                }
                return ok(hit);
            }
            Some(key)
        } else {
            None
        };
        // Per-agent concurrency quota: a runaway agent can't swamp the shared runtime.
        if let Some(aid) = a.agent_id.as_deref() {
            let n = self.db.agent_running_count(aid).await.map_err(internal)?;
            if n >= AGENT_MAX_CONCURRENT {
                return ok(json!({
                    "error": "quota_exceeded", "agent_id": aid,
                    "in_flight": n, "limit": AGENT_MAX_CONCURRENT
                }));
            }
        }
        // Enqueue only — a worker claims it from the queue (FOR UPDATE SKIP LOCKED).
        let run_id = self
            .db
            .insert_run(a.script_id, &input, a.agent_id.as_deref())
            .await
            .map_err(internal)?;
        if let Some(key) = &cache_key {
            let _ = self.db.set_run_cache_key(run_id, key).await;
        }
        if let Some(key) = a.idempotency_key.as_deref() {
            let _ = self.db.set_run_idempotency(run_id, key).await;
        }
        let mut out = json!({"run_id": run_id, "status": "pending"});
        if let Some(key) = cache_key {
            out["cache_key"] = json!(key);
        }
        ok(out)
    }

    #[tool(description = "Read logs for a run. Cursor-paginated, error-first. Returns new lines since after_cursor + next_cursor + status.")]
    async fn read_logs(
        &self,
        Parameters(a): Parameters<ReadLogsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let after = a.after_cursor.unwrap_or(0);
        let limit = a.limit.unwrap_or(200).clamp(1, 1000);
        let status = self
            .db
            .run_status(a.run_id)
            .await
            .map_err(internal)?
            .unwrap_or_else(|| "unknown".into());
        let lines = self
            .db
            .read_logs_after(a.run_id, after, limit)
            .await
            .map_err(internal)?;
        let max_seq = self.db.max_log_seq(a.run_id).await.map_err(internal)?;
        let next_cursor = lines.last().map(|l| l.seq).unwrap_or(after);
        // CSV-ish "seq|stream|text" — avoids repeated JSON keys per line.
        let rendered: Vec<String> = lines
            .iter()
            .map(|l| format!("{}|{}|{}", l.seq, l.stream, l.line))
            .collect();
        let remaining = (max_seq - next_cursor).max(0);
        let mut out = json!({
            "status": status,
            "lines": rendered,
            "next_cursor": next_cursor,
            "note": format!("{} more after cursor", remaining),
        });
        if let Some(r) = self.db.run_result(a.run_id).await.ok().flatten() {
            out["result"] = r;
        }
        ok(out)
    }

    #[tool(description = "Long-poll a run until it reaches a terminal status or timeout. Returns final status + tail logs. Fewer round-trips than polling.")]
    async fn wait_for(
        &self,
        Parameters(a): Parameters<WaitForArgs>,
    ) -> Result<CallToolResult, McpError> {
        let timeout = a.timeout.unwrap_or(30).min(120);
        let deadline = timeout * 2; // 500ms ticks
        let mut status = String::from("unknown");
        for _ in 0..deadline {
            status = self
                .db
                .run_status(a.run_id)
                .await
                .map_err(internal)?
                .unwrap_or_else(|| "unknown".into());
            if matches!(status.as_str(), "succeeded" | "failed" | "canceled") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        // Tail: last ~40 lines.
        let max_seq = self.db.max_log_seq(a.run_id).await.map_err(internal)?;
        let from = (max_seq - 40).max(0);
        let lines = self
            .db
            .read_logs_after(a.run_id, from, 40)
            .await
            .map_err(internal)?;
        let rendered: Vec<String> = lines
            .iter()
            .map(|l| format!("{}|{}|{}", l.seq, l.stream, l.line))
            .collect();
        let mut out = json!({
            "status": status,
            "tail": rendered,
            "next_cursor": max_seq,
        });
        if let Some(r) = self.db.run_result(a.run_id).await.ok().flatten() {
            out["result"] = r;
        }
        ok(out)
    }

    #[tool(description = "List recent runs with server-side status counts. Optional status filter. Cursor-light summary, not every row.")]
    async fn list_runs(
        &self,
        Parameters(a): Parameters<ListRunsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = a.limit.unwrap_or(20).clamp(1, 200);
        let counts = self.db.run_status_counts().await.map_err(internal)?;
        let counts_obj: serde_json::Map<String, serde_json::Value> = counts
            .into_iter()
            .map(|(k, v)| (k, json!(v)))
            .collect();
        let rows = self
            .db
            .list_runs(a.status.as_deref(), limit)
            .await
            .map_err(internal)?;
        let items: Vec<_> = rows
            .iter()
            .map(|r| {
                // error only when present, to stay token-frugal on the happy path.
                let mut o = json!({"run_id": r.id, "script_id": r.script_id, "script": r.script_name, "status": r.status, "exit": r.exit_code, "at": r.created_at.to_rfc3339()});
                if let Some(e) = &r.error {
                    o["error"] = json!(e);
                }
                o
            })
            .collect();
        ok(json!({"counts": counts_obj, "recent": items}))
    }

    #[tool(description = "Compose a flow: a declarative DAG of steps wired over MCP. Validates acyclicity. Returns flow_id. The wire-over-MCP differentiator.")]
    async fn compose_flow(
        &self,
        Parameters(a): Parameters<ComposeFlowArgs>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(e) = crate::flow::validate_spec(&a.spec) {
            return ok(json!({"error": "invalid_spec", "detail": e}));
        }
        let id = self
            .db
            .insert_flow(&a.name, &a.spec)
            .await
            .map_err(internal)?;
        ok(json!({"flow_id": id, "status": "composed"}))
    }

    #[tool(description = "Run a flow. Returns flow_run_id immediately; the DAG executes step-by-step with checkpointing. Poll get_flow_run.")]
    async fn run_flow(
        &self,
        Parameters(a): Parameters<RunFlowArgs>,
    ) -> Result<CallToolResult, McpError> {
        let Some(spec) = self.db.get_flow_spec(a.flow_id).await.map_err(internal)? else {
            return ok(json!({"error": "flow_not_found", "id": a.flow_id}));
        };
        let input = a.input.unwrap_or(json!({}));
        let id = self
            .db
            .insert_flow_run(a.flow_id, &spec, &input)
            .await
            .map_err(internal)?;
        ok(json!({"flow_run_id": id, "status": "pending"}))
    }

    #[tool(description = "Status of a flow run: overall status + per-step status/output. Token-frugal step ledger.")]
    async fn get_flow_run(
        &self,
        Parameters(a): Parameters<FlowRunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let status = self
            .db
            .flow_run_status(a.flow_run_id)
            .await
            .map_err(internal)?
            .unwrap_or_else(|| "unknown".into());
        let steps = self.db.flow_steps(a.flow_run_id).await.map_err(internal)?;
        let items: Vec<_> = steps
            .iter()
            .map(|s| json!({"id": s.step_id, "status": s.status, "out": s.output}))
            .collect();
        ok(json!({"status": status, "steps": items}))
    }

    #[tool(description = "Schedule a script on a cron (6-field, leading seconds). Each tick enqueues a run. Returns schedule_id.")]
    async fn schedule(
        &self,
        Parameters(a): Parameters<ScheduleArgs>,
    ) -> Result<CallToolResult, McpError> {
        let Some(cron) = &self.cron else {
            return ok(json!({"error": "scheduler_disabled"}));
        };
        if let Err(e) = validate_cron(&a.cron) {
            return ok(json!({"error": "invalid_cron", "detail": e}));
        }
        if self.db.get_script(a.script_id).await.map_err(internal)?.is_none() {
            return ok(json!({"error": "script_not_found", "id": a.script_id}));
        }
        let input = a.input.unwrap_or(json!({}));
        let id = self
            .db
            .insert_schedule(a.script_id, &a.cron, &input)
            .await
            .map_err(internal)?;
        cron.add_job(id, a.script_id, &a.cron, input)
            .await
            .map_err(internal)?;
        ok(json!({"schedule_id": id, "cron": a.cron, "status": "scheduled"}))
    }

    #[tool(description = "List active cron schedules: id, script_id, script name, cron expression.")]
    async fn list_schedules(&self) -> Result<CallToolResult, McpError> {
        let rows = self.db.list_schedules().await.map_err(internal)?;
        let items: Vec<_> = rows
            .iter()
            .map(|s| json!({"schedule_id": s.id, "script_id": s.script_id, "script": s.script_name, "cron": s.cron}))
            .collect();
        ok(json!({"schedules": items}))
    }

    #[tool(description = "Set a secret, injected as an env var into every job container (e.g. OPENAI_API_KEY). Write-only: values are never returned or logged. Upsert by name.")]
    async fn set_secret(
        &self,
        Parameters(a): Parameters<SetSecretArgs>,
    ) -> Result<CallToolResult, McpError> {
        if a.name.trim().is_empty() {
            return ok(json!({"error": "empty_name"}));
        }
        self.db
            .upsert_secret(&a.name, &a.value, a.agent_id.as_deref())
            .await
            .map_err(internal)?;
        let scope = a.agent_id.as_deref().unwrap_or("global");
        ok(json!({"name": a.name, "scope": scope, "status": "set"}))
    }

    #[tool(description = "Fetch a run's signed reproducibility receipt: the image digest, source/input/output hashes, secrets generation, exit, and an HMAC signature. Proves what produced the result; for a network=false (deterministic) run it certifies a recall is sound.")]
    async fn get_receipt(
        &self,
        Parameters(a): Parameters<GetReceiptArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.db.run_receipt(a.run_id).await.map_err(internal)? {
            Some(r) => ok(r),
            None => ok(json!({"error": "no_receipt", "run_id": a.run_id})),
        }
    }

    #[tool(description = "List secret names (values are write-only and never returned). Use to check which keys a job will see in its env.")]
    async fn list_secrets(
        &self,
        Parameters(a): Parameters<WhoamiArgs>,
    ) -> Result<CallToolResult, McpError> {
        let names = self
            .db
            .secret_names(a.agent_id.as_deref())
            .await
            .map_err(internal)?;
        ok(json!({"secrets": names}))
    }

    #[tool(description = "Self-describe the runtime for the calling agent: supported runtimes, per-job mem/cpu caps, secret names you can see (global + your scoped), and your concurrency quota + current in-flight usage. Use to self-configure instead of guessing.")]
    async fn whoami(
        &self,
        Parameters(a): Parameters<WhoamiArgs>,
    ) -> Result<CallToolResult, McpError> {
        let (mem_bytes, nano_cpus) = self.exec.limits();
        let secrets = self
            .db
            .secret_names(a.agent_id.as_deref())
            .await
            .unwrap_or_default();
        let in_flight = match a.agent_id.as_deref() {
            Some(aid) => self.db.agent_running_count(aid).await.unwrap_or(0),
            None => 0,
        };
        ok(json!({
            "agent_id": a.agent_id,
            "runtimes": SUPPORTED_RUNTIMES,
            "limits": { "mem_mb": mem_bytes / (1024 * 1024), "cpus": nano_cpus as f64 / 1e9 },
            "secrets": secrets,
            "quota": { "max_concurrent": AGENT_MAX_CONCURRENT, "in_flight": in_flight },
            "input_contract": "job reads DOKAN_INPUT (JSON env), secrets as env vars; emit `::dokan:result:: {json}` for a structured result",
        }))
    }

    #[tool(description = "Stop a cron schedule: removes the live job and disables it so it won't reload. Always unschedule test/temporary crons.")]
    async fn unschedule(
        &self,
        Parameters(a): Parameters<UnscheduleArgs>,
    ) -> Result<CallToolResult, McpError> {
        let Some(cron) = &self.cron else {
            return ok(json!({"error": "scheduler_disabled"}));
        };
        let removed = cron.remove(a.schedule_id).await.map_err(internal)?;
        ok(json!({"schedule_id": a.schedule_id, "status": if removed {"unscheduled"} else {"not_found"}}))
    }

    #[tool(description = "Cancel a run: kill its container and mark it canceled. Compact ack.")]
    async fn cancel(
        &self,
        Parameters(a): Parameters<CancelArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.exec.cancel(a.run_id).await;
        self.db
            .finish_run(a.run_id, "canceled", None, Some("canceled by operator"))
            .await
            .map_err(internal)?;
        ok(json!({"run_id": a.run_id, "status": "canceled"}))
    }
}

fn internal(e: anyhow::Error) -> McpError {
    McpError::internal_error(e.to_string(), None)
}

#[tool_handler]
impl ServerHandler for Dokan {
    fn get_info(&self) -> ServerInfo {
        // ServerInfo (InitializeResult) and Implementation are #[non_exhaustive];
        // build from Default then assign fields.
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        let mut imp = Implementation::default();
        imp.name = "dokan".into();
        imp.version = env!("CARGO_PKG_VERSION").into();
        imp.title = Some("dokan — agent-operated script runtime".into());
        info.server_info = imp;
        info.instructions = Some(
            "dokan runs deterministic scripts in Docker. You are the operator. \
             Workflow: upload_script -> run_script (returns run_id immediately, never blocks) \
             -> read_logs(after_cursor) to poll, or wait_for for fewer round-trips. \
             Token rules: always request only the fields you need; always paginate logs with \
             next_cursor; never fetch a script body unless you must (use include_source=true \
             explicitly). No LLM runs inside dokan — intelligence is yours, applied at the edge."
                .into(),
        );
        info
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_json, run_cache_key, validate_cron};
    use serde_json::json;

    #[test]
    fn canonical_json_is_key_order_stable() {
        let a = json!({"b": 1, "a": [3, {"y": 1, "x": 2}], "c": "z"});
        let b = json!({"c": "z", "a": [3, {"x": 2, "y": 1}], "b": 1});
        assert_eq!(canonical_json(&a), canonical_json(&b), "key order must not matter");
        // arrays stay ordered (semantically significant)
        assert_ne!(canonical_json(&json!([1, 2])), canonical_json(&json!([2, 1])));
    }

    #[test]
    fn cache_key_stable_and_input_sensitive() {
        let i1 = json!({"a": 1, "b": 2});
        let i2 = json!({"b": 2, "a": 1}); // same content, different order
        let k = |i: &serde_json::Value, g: i64| run_cache_key("bash", "sha-d", "src", i, g);
        assert_eq!(k(&i1, 0), k(&i2, 0), "input key-order doesn't change the cache key");
        assert_ne!(k(&i1, 0), k(&json!({"a": 9}), 0), "different input -> different key");
        assert_ne!(k(&i1, 0), k(&i1, 1), "secrets generation invalidates");
        assert_ne!(
            run_cache_key("bash", "digestA", "s", &i1, 0),
            run_cache_key("bash", "digestB", "s", &i1, 0),
            "image digest invalidates"
        );
    }

    #[test]
    fn cron_requires_six_fields() {
        assert!(validate_cron("0 */5 * * * *").is_ok(), "6-field ok");
        assert!(validate_cron("*/5 * * * *").is_err(), "5-field rejected");
        assert!(validate_cron("0 0 0 0 0 0 0").is_err(), "7-field rejected");
        assert!(validate_cron("  0   0 * * * *  ").is_ok(), "extra whitespace tolerated");
    }
}
