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
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunArgs {
    /// Script id to run.
    pub script_id: i64,
    /// Arbitrary JSON passed to the job as the DOKAN_INPUT env var. Optional.
    pub input: Option<serde_json::Value>,
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
pub struct SetSecretArgs {
    /// Env var name injected into every job container, e.g. "OPENAI_API_KEY".
    pub name: String,
    /// Secret value. Write-only — never returned by any tool, never logged.
    pub value: String,
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
                    (r, t, "substring")
                }
            },
            None => {
                let (r, t) = self.db.search_scripts(&a.query, limit).await.map_err(internal)?;
                (r, t, "substring")
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
            "created_at": s.created_at.to_rfc3339(),
        });
        if a.include_source.unwrap_or(false) {
            v["source"] = json!(s.source);
        } else {
            v["source_bytes"] = json!(s.source.len());
        }
        ok(v)
    }

    #[tool(description = "Upload a script. Returns script_id + version. Runtime: python|node|bash. INPUT CONTRACT: the script reads its input from the DOKAN_INPUT env var (a JSON string) — NOT stdin or argv. Secrets set via set_secret arrive as their own env vars (e.g. $OPENAI_API_KEY). A nonzero exit is treated as the script's own deterministic verdict (e.g. a monitor finding) and is NOT retried; only a container/infra failure retries.")]
    async fn upload_script(
        &self,
        Parameters(a): Parameters<UploadArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Embed name + description for semantic search (best-effort).
        let embedding = match &self.embedder {
            Some(emb) => {
                let text = format!("{} {}", a.name, a.description.clone().unwrap_or_default());
                emb.embed(text).await.ok()
            }
            None => None,
        };
        let (id, version) = self
            .db
            .insert_script(
                &a.name,
                &a.runtime,
                &a.source,
                a.description.as_deref(),
                a.created_by.as_deref(),
                embedding,
            )
            .await
            .map_err(internal)?;
        ok(json!({"script_id": id, "version": version, "status": "uploaded"}))
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
        let _ = &script; // existence checked above; worker reloads it on claim.
        let input = a.input.unwrap_or(json!({}));
        // Enqueue only — a worker claims it from the queue (FOR UPDATE SKIP LOCKED).
        let run_id = self
            .db
            .insert_run(a.script_id, &input)
            .await
            .map_err(internal)?;
        ok(json!({"run_id": run_id, "status": "pending"}))
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
        ok(json!({
            "status": status,
            "lines": rendered,
            "next_cursor": next_cursor,
            "note": format!("{} more after cursor", remaining),
        }))
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
        ok(json!({
            "status": status,
            "tail": rendered,
            "next_cursor": max_seq,
        }))
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
        self.db.upsert_secret(&a.name, &a.value).await.map_err(internal)?;
        ok(json!({"name": a.name, "status": "set"}))
    }

    #[tool(description = "List secret names (values are write-only and never returned). Use to check which keys a job will see in its env.")]
    async fn list_secrets(&self) -> Result<CallToolResult, McpError> {
        let names = self.db.secret_names().await.map_err(internal)?;
        ok(json!({"secrets": names}))
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
