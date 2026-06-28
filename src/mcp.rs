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

/// Per-agent compute budget: max container service-seconds over the rolling window. Cheap
/// runaway guard the agent can see (whoami) and reason about. Generous default.
const AGENT_COMPUTE_BUDGET_SECS: f64 = 3600.0;
const AGENT_BUDGET_WINDOW_SECS: i64 = 86400;

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

/// Some MCP clients stringify object/array params instead of sending them inline.
/// If a JSON value arrives as a string that itself parses to an object or array,
/// decode it once — otherwise a `{...}` input reaches the job double-encoded
/// (DOKAN_INPUT = a quoted JSON string, so one JSON.parse yields a string, not the
/// object, and the job silently reads its fields as undefined). A scalar string
/// (or anything that doesn't parse to a container) passes through untouched, so a
/// legitimately-stringy input is never mangled.
fn destringify(v: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::String(s) = &v
        && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s)
            && (parsed.is_object() || parsed.is_array()) {
                return parsed;
            }
    v
}

/// Canonical, order-stable "name:sha,name:sha" rendering of a run's input-blob map
/// ({ "<dest-name>": "<sha>" }), sorted by name. Folded into the cache key + the receipt so
/// identical (source+input+image+files) recall, and a changed file misses. None/empty → "".
pub(crate) fn canonical_input_blobs(input_blobs: Option<&serde_json::Value>) -> String {
    let Some(map) = input_blobs.and_then(|v| v.as_object()) else {
        return String::new();
    };
    let mut pairs: Vec<String> = map
        .iter()
        .map(|(name, sha)| format!("{name}:{}", sha.as_str().unwrap_or_default()))
        .collect();
    pairs.sort();
    pairs.join(",")
}

/// Max bytes for a single uploaded blob (32 MiB, spec §5) — reject oversize at ingest rather
/// than bloat the Postgres bytea store.
pub(crate) const MAX_BLOB_BYTES: usize = 32 * 1024 * 1024;

/// Ingest validation for an uploaded blob's decoded byte length: reject empty (garbage) and
/// oversize uploads before they touch the store. Pure — unit-tested.
pub(crate) fn validate_blob_bytes(len: usize) -> Result<(), String> {
    if len == 0 {
        return Err("blob is empty (0 bytes)".to_string());
    }
    if len > MAX_BLOB_BYTES {
        return Err(format!(
            "blob too large: {len} bytes (cap {MAX_BLOB_BYTES} bytes / 32 MiB)"
        ));
    }
    Ok(())
}

/// Content-address a run: hash(runtime + source + canonical(input) + secrets generation +
/// input blobs). Same inputs ⇒ same key ⇒ recallable (dokan jobs are deterministic). A
/// changed input file (different sha) shifts the key, so the cache stays correct.
pub(crate) fn run_cache_key(
    runtime: &str,
    image_digest: &str,
    source: &str,
    input: &serde_json::Value,
    secrets_gen: i64,
    input_blobs: Option<&serde_json::Value>,
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
    h.update([0x1f]);
    h.update(canonical_input_blobs(input_blobs).as_bytes());
    format!("{:x}", h.finalize())
}

fn ok<T: serde::Serialize>(v: T) -> Result<CallToolResult, McpError> {
    let s = serde_json::to_string(&v)
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![Content::text(s)]))
}

/// Uniform error envelope for a tool result: a stable machine-readable `error` code, a human
/// `message`, an optional actionable `hint`, plus any extra context fields (id, run_id, …).
/// Every tool's expected-error path returns this shape so a caller parses one structure — the
/// `error` code stays a top-level string for back-compat. (Unexpected/internal failures go
/// through `internal()` → a JSON-RPC McpError; this is for the handled, returned errors.)
fn err_ctx(
    code: &str,
    message: impl Into<String>,
    hint: Option<&str>,
    extra: serde_json::Value,
) -> Result<CallToolResult, McpError> {
    ok(error_envelope(code, &message.into(), hint, extra))
}

/// Pure builder for the uniform error envelope (so the shape is unit-testable).
fn error_envelope(code: &str, message: &str, hint: Option<&str>, extra: serde_json::Value) -> serde_json::Value {
    let mut o = json!({"error": code, "message": message});
    if let Some(h) = hint {
        o["hint"] = json!(h);
    }
    if let serde_json::Value::Object(m) = extra {
        for (k, v) in m {
            o[k] = v;
        }
    }
    o
}

/// Uniform error envelope, no extra context fields.
fn err(code: &str, message: impl Into<String>, hint: Option<&str>) -> Result<CallToolResult, McpError> {
    err_ctx(code, message, hint, json!({}))
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
pub struct ListScriptsArgs {
    /// Max rows (default 50, max 500).
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListBlobsArgs {
    /// Max rows (default 50, max 500).
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
    /// Idempotent re-provision: if a script of this name exists, update it in place and return
    /// its id instead of creating a duplicate. A changed source bumps the version
    /// (status "updated"); an identical source with changed limits/flags (mem_limit_mb, cpu_limit,
    /// network, feed_prev_result, description) applies them WITHOUT a version bump
    /// (status "metadata_updated") — so a cap-only re-provision actually takes; a fully identical
    /// re-upload is a cheap no-op (status "unchanged"). Default false. Use it so a respawned agent
    /// can safely re-upload and so you can tune a script's caps without perturbing its source.
    pub upsert: Option<bool>,
    /// Network access for the job. Default true (monitors that hit APIs need it). Set FALSE
    /// for a pure-compute script: it runs network-disabled, making its output a deterministic
    /// function of its inputs — soundly cacheable (cache:true) and provable via its receipt.
    pub network: Option<bool>,
    /// Optional per-job memory cap in MiB; null = the executor's global default. Raise it for a
    /// heavier job that OOMs (exit 137) under the default cap. A script with any override runs
    /// outside the warm pool on a fresh one-off container, so the common path is unaffected.
    pub mem_limit_mb: Option<i64>,
    /// Optional per-job CPU cap in cores (e.g. 4.0); null = the executor's global default.
    pub cpu_limit: Option<f64>,
    /// Opt-in: feed the previous run's structured result into the next run as DOKAN_INPUT.prev_result (for stateful monitors).
    pub feed_prev_result: Option<bool>,
    /// Optional secret ALLOWLIST: the subset of secret names this script's jobs may see (least
    /// privilege). null/omitted = back-compat (all of your + global secrets). When set, only these
    /// names are injected — as env vars AND files under tmpfs /run/secrets/<name>.
    pub secrets: Option<Vec<String>>,
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
    /// REQUIRED. Your agent id — provenance for the run; selects which scoped secrets the job
    /// sees (global + this agent's) and meters this agent's concurrency + compute quota.
    /// NOTE: this is an UNAUTHENTICATED provenance tag, NOT an isolation/auth boundary. dokan's
    /// trust model is single-tenant (all jobs trusted), so per-agent secret scoping is
    /// defense-in-depth, not a guarantee against a caller that passes another agent's id. True
    /// non-spoofable isolation is the held per-agent-token upgrade (see SECURITY.md).
    pub agent_id: String,
    /// Exactly-once key: if a run with this key already exists, return it instead of
    /// enqueuing a duplicate. Use for safe retries of the enqueue call itself.
    pub idempotency_key: Option<String>,
    /// Run artifacts (input files): a map { "<dest-name>": "<handle>" } where each handle
    /// comes from upload_blob. Each file is materialized READ-ONLY at /input/<dest-name>
    /// in the container before exec. Unknown handle → loud error, no run created. The blob
    /// shas enter the cache key + receipt, so the run stays a pure function of its inputs.
    /// Dest names must be plain filenames (no "/" or "..").
    pub files: Option<std::collections::HashMap<String, String>>,
    /// Output files (opt-in): when true, a writable /output is mounted in the container; after
    /// exec dokan captures every file the job wrote there as a content-addressed blob and
    /// records output_blobs = { "<relative-name>": "<sha>" } on the run (surfaced by wait_for /
    /// read_logs, downloadable via download_blob, and folded into the receipt). Default false —
    /// leaving it off keeps the run on the fast warm path. true forces the one-off container
    /// path (like input files), so use it only when a job emits artifacts.
    pub capture_output: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadBlobArgs {
    /// File bytes, base64-encoded (MCP is JSON, so binary arrives base64). Cap: 32 MiB decoded.
    pub data: String,
    /// Optional original filename — advisory only (the content address is the bytes' sha).
    pub filename: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DownloadBlobArgs {
    /// The blob handle (sha) returned by upload_blob.
    pub handle: String,
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
    /// Filter by raw status: pending|running|succeeded|failed|canceled. Optional.
    pub status: Option<String>,
    /// Max rows (default 20).
    pub limit: Option<i64>,
    /// Filter to a single script's runs (e.g. scope one noisy monitor). Optional.
    pub script_id: Option<i64>,
    /// Filter by classified OUTCOME: ok | verdict | error | canceled | running | pending.
    /// `error` = real execution failures only (skips intentional monitor verdicts — the way to
    /// find actual bugs); `verdict` = deterministic findings (a monitor that ran and exit≠0).
    pub outcome: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CancelArgs {
    pub run_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct VerifyArgs {
    pub run_id: i64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ComposeFlowArgs {
    /// Flow name.
    pub name: String,
    /// DAG spec: { "steps": [ { "id", "script_id", "input"?, "depends_on"? [ids],
    /// "when"? {ref,op,value}, "map"? "<ref>", "compensate"? <script_id>, "retries"? <n>,
    /// "cache"? <bool> } ] }.
    /// See compose_flow's description for when/map/compensate/retries/cache semantics.
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
    /// Max top-level steps to return (default 200, max 1000). Map children are always collapsed
    /// into a per-parent count, so this bounds the distinct-step list, not the fan-out.
    pub limit: Option<i64>,
    /// Return only steps after this 0-based offset cursor (use next_cursor from a prior call).
    pub after: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateWebhookArgs {
    /// What to trigger: "script" or "flow".
    pub target: String,
    /// The script_id or flow_id to run when the webhook fires.
    pub target_id: i64,
    /// Optional agent tag — provenance + scoped secrets for the triggered run.
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteWebhookArgs {
    pub webhook_id: i64,
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
pub struct ReproduceArgs {
    /// The prior run to reproduce. Its receipt is the binding the new run is diffed against.
    pub run_id: i64,
    /// Max seconds to wait for the re-run to finish (default 120, max 300). On timeout the
    /// verdict is INCONCLUSIVE and `repro_run_id` is still running.
    pub timeout: Option<u64>,
    /// Provenance/quota id for the re-run (defaults to the original run's agent, else "reproduce").
    pub agent_id: Option<String>,
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

    #[tool(description = "List all scripts (newest first): id + name + runtime + 1-line desc, no bodies. The catalog of input-driven scripts — search needs a query, list_schedules is crons only. Use to spot duplicates/orphans. Cursor-light: returns up to limit with a \"showing X of Y\" note.")]
    async fn list_scripts(
        &self,
        Parameters(a): Parameters<ListScriptsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = a.limit.unwrap_or(50).clamp(1, 500);
        let (rows, total) = self.db.list_scripts(limit).await.map_err(internal)?;
        let items: Vec<_> = rows
            .iter()
            .map(|s| json!({"id": s.id, "name": s.name, "runtime": s.runtime, "desc": s.description}))
            .collect();
        ok(json!({"scripts": items, "note": format!("showing {} of {}", rows.len(), total)}))
    }

    #[tool(description = "Fetch a script's metadata. Source body included only when include_source=true.")]
    async fn get_script(
        &self,
        Parameters(a): Parameters<GetScriptArgs>,
    ) -> Result<CallToolResult, McpError> {
        let s = self.db.get_script(a.id).await.map_err(internal)?;
        let Some(s) = s else {
            return err_ctx("not_found", "no script with that id", Some("check list_scripts for valid ids"), json!({"id": a.id}));
        };
        let mut v = json!({
            "id": s.id, "name": s.name, "runtime": s.runtime,
            "desc": s.description, "created_by": s.created_by, "version": s.version,
            "network": s.network, "created_at": s.created_at.to_rfc3339(),
            "mem_limit_mb": s.mem_limit_mb, "cpu_limit": s.cpu_limit,
            "feed_prev_result": s.feed_prev_result,
        });
        if a.include_source.unwrap_or(false) {
            v["source"] = json!(s.source);
        } else {
            v["source_bytes"] = json!(s.source.len());
        }
        ok(v)
    }

    #[tool(description = "Upload a script. Returns script_id + version. Runtime: python|node|bash. INPUT CONTRACT: the script reads its input from the DOKAN_INPUT env var (a JSON string) — NOT stdin or argv. Secrets set via set_secret arrive as their own env vars (e.g. $OPENAI_API_KEY). A nonzero exit is treated as the script's own deterministic verdict (e.g. a monitor finding) and is NOT retried; only a container/infra failure retries. Pass upsert=true to re-provision by name idempotently (no duplicate rows on respawn). STRUCTURED RESULT: print a line `::dokan:result:: {json}` on stdout to attach a structured result to the run — it is captured (not logged), returned by wait_for/read_logs, and POSTed to the relay, so a monitor's finding reaches the agent event-driven. PROGRESS: print `::dokan:progress:: <text>` to set the run's live status line (latest wins, overwritten each emit) — surfaced by list_runs/read_logs/wait_for and the UI, NOT logged. Use it in a long loop (e.g. `meeting 3/6`) so the operator sees current state without paging logs; flush stdout (Python: print(..., flush=True)) so it lands live. STATEFUL MONITORS: set feed_prev_result=true and this script's previous structured result is injected as DOKAN_INPUT.prev_result on the next run (null on the first) — for week-over-week diffs without external state. Per-job caps: mem_limit_mb / cpu_limit / network (see args).")]
    async fn upload_script(
        &self,
        Parameters(a): Parameters<UploadArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Idempotent re-provision: with upsert, reuse the script of the same name. No-op
        // when the source is unchanged (a respawned agent re-uploading the same thing),
        // update + version bump when it changed — never a duplicate row.
        if a.upsert.unwrap_or(false)
            && let Some((id, source, version)) =
                self.db.find_script_by_name(&a.name).await.map_err(internal)?
            {
                if source == a.source {
                    // Source unchanged: still apply a metadata-only update (limits/flags/desc)
                    // WITHOUT a version bump — so a cap-only re-provision (e.g. setting
                    // mem_limit_mb) actually takes, instead of being a silent no-op. Re-asserts
                    // the passed definition; the IS DISTINCT FROM guard keeps an identical
                    // re-upload a cheap no-op.
                    let changed = self
                        .db
                        .update_script_meta(
                            id,
                            &a.runtime,
                            a.description.as_deref(),
                            a.created_by.as_deref(),
                            a.network.unwrap_or(true),
                            a.mem_limit_mb,
                            a.cpu_limit,
                            a.feed_prev_result.unwrap_or(false),
                        )
                        .await
                        .map_err(internal)?;
                    if let Some(ref names) = a.secrets {
                        self.db.set_script_secrets(id, Some(names)).await.map_err(internal)?;
                    }
                    let status = if changed { "metadata_updated" } else { "unchanged" };
                    return ok(json!({"script_id": id, "version": version, "status": status}));
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
                        a.mem_limit_mb,
                        a.cpu_limit,
                        a.feed_prev_result.unwrap_or(false),
                        embedding,
                    )
                    .await
                    .map_err(internal)?;
                if let Some(ref names) = a.secrets {
                    self.db.set_script_secrets(id, Some(names)).await.map_err(internal)?;
                }
                return ok(json!({"script_id": id, "version": version, "status": "updated"}));
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
                a.mem_limit_mb,
                a.cpu_limit,
                a.feed_prev_result.unwrap_or(false),
                embedding,
            )
            .await
            .map_err(internal)?;
        if let Some(ref names) = a.secrets {
            self.db.set_script_secrets(id, Some(names)).await.map_err(internal)?;
        }
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
                return err_ctx("script_not_found", "no script with that id", Some("check list_scripts for valid ids"), json!({"id": id}));
            }
        }
        let predicate = a.predicate.unwrap_or(json!({}));
        if !predicate.is_object() {
            return err("predicate_must_be_object", "predicate must be a JSON object", Some("e.g. {\"verdict\":\"FAIL\"}"));
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

    #[tool(description = "List registered executors and whether each is live (heartbeat within 30s). Shows the fleet that runs jobs; a dead executor's runs are reclaimed by the lease reaper.")]
    async fn list_executors(&self) -> Result<CallToolResult, McpError> {
        let items = self.db.list_executors(30).await.map_err(internal)?;
        ok(json!({"executors": items}))
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
                err_ctx("script_not_found", "no script with that id", Some("check list_scripts for valid ids"), json!({"id": a.script_id}))
            }
            crate::db::DeleteResult::BlockedByFlow(n) => ok(json!({
                "error": "referenced_by_flow", "message": "a flow still references this script", "id": a.script_id, "flow_steps": n,
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

    #[tool(description = "Upload a file's bytes into dokan's content-addressed store and get a reusable handle. Bytes arrive base64 in `data` (MCP is JSON). The handle = the bytes' sha; re-uploading identical bytes returns the same handle and stores nothing new (dedup). Pass the handle in run_script files={\"<name>\": \"<handle>\"} to materialize it read-only at /input/<name>. Cap: 32 MiB per blob. Returns {handle, sha, size}.")]
    async fn upload_blob(
        &self,
        Parameters(a): Parameters<UploadBlobArgs>,
    ) -> Result<CallToolResult, McpError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(a.data.as_bytes())
            .map_err(|e| McpError::invalid_params(format!("data is not valid base64: {e}"), None))?;
        // Ingest limits: reject an empty (garbage) or oversize upload loudly, before it bloats
        // the Postgres bytea store.
        validate_blob_bytes(bytes.len()).map_err(|e| McpError::invalid_params(e, None))?;
        let (sha, size) = self.db.put_blob(&bytes).await.map_err(internal)?;
        let _ = &a.filename; // advisory only; the content address is the bytes' sha
        ok(json!({ "handle": sha, "sha": sha, "size": size }))
    }

    #[tool(description = "Inventory the content-addressed blob store: handle (sha) + size + created/last-used timestamps, no bytes. The catalog of uploaded input artifacts — pair a handle with run_script files={\"<name>\": \"<handle>\"}. Most-recently-used first. Cursor-light: returns up to limit with a \"showing X of Y\" note.")]
    async fn list_blobs(
        &self,
        Parameters(a): Parameters<ListBlobsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = a.limit.unwrap_or(50).clamp(1, 500);
        let (rows, total) = self.db.list_blobs(limit).await.map_err(internal)?;
        let items: Vec<_> = rows
            .iter()
            .map(|b| json!({
                "handle": b.sha,
                "size": b.size,
                "created_at": b.created_at.to_rfc3339(),
                "last_used_at": b.last_used_at.to_rfc3339(),
            }))
            .collect();
        ok(json!({"blobs": items, "note": format!("showing {} of {}", rows.len(), total)}))
    }

    #[tool(description = "Fetch a blob's bytes by handle (the sha from upload_blob). Returns {data (base64), size}, or an error if the handle is unknown.")]
    async fn download_blob(
        &self,
        Parameters(a): Parameters<DownloadBlobArgs>,
    ) -> Result<CallToolResult, McpError> {
        use base64::Engine;
        match self.db.get_blob(&a.handle).await.map_err(internal)? {
            Some(bytes) => {
                let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
                ok(json!({ "data": data, "size": bytes.len() }))
            }
            None => ok(json!({ "error": "unknown_blob_handle", "message": "no blob with that handle", "hint": "the handle is the sha from upload_blob", "handle": a.handle })),
        }
    }

    #[tool(description = "Trigger a script run. Returns run_id immediately; never blocks. Poll with read_logs or wait_for. agent_id is REQUIRED: it tags provenance, selects the job's scoped secrets (global + this agent's), and meters your concurrency/compute quota — it is unauthenticated provenance, NOT an isolation/auth boundary (see SECURITY.md). INPUT FILES: pass files={\"<name>\": \"<handle>\"} (handles from upload_blob) to materialize each file READ-ONLY at /input/<name> in the container — the way to feed a job a real document (a PDF, dataset, .md). The blob shas enter the cache key + receipt, so the run stays deterministic. Unknown handle → loud error, no run created. OUTPUT FILES: set capture_output=true to mount a writable /output — dokan captures every file the job writes there as a content-addressed blob and returns output_blobs={\"<name>\": \"<sha>\"} (on wait_for/read_logs, downloadable via download_blob, folded into the receipt). Opt-in (default off keeps the fast warm path). CACHE: set cache=true for run-or-recall — an identical prior run (same source+input+secrets-gen) returns its result with status \"recalled\", no container spawned (deterministic jobs only; leave false for monitors/time-sensitive runs).")]
    async fn run_script(
        &self,
        Parameters(a): Parameters<RunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let script = self.db.get_script(a.script_id).await.map_err(internal)?;
        let Some(script) = script else {
            return err_ctx("script_not_found", "no script with that id", Some("check list_scripts for valid ids"), json!({"id": a.script_id}));
        };
        let input = destringify(a.input.unwrap_or(json!({})));
        // Run artifacts: validate every file handle exists BEFORE a run is created (unknown
        // handle → loud error, no run). Build the content-addressed input_blobs map
        // { "<dest-name>": "<sha>" } stored on the run and folded into the cache key + receipt.
        let input_blobs: Option<serde_json::Value> = match &a.files {
            Some(files) if !files.is_empty() => {
                let mut map = serde_json::Map::with_capacity(files.len());
                for (name, handle) in files {
                    if name.is_empty() || name.contains('/') || name.contains("..") {
                        return ok(json!({
                            "error": "invalid_file_name", "message": "a file dest name is invalid", "name": name,
                            "hint": "dest names must be plain filenames (no '/' or '..')"
                        }));
                    }
                    if !self.db.blob_exists(handle).await.map_err(internal)? {
                        return ok(json!({
                            "error": "unknown_blob_handle", "message": "an input file handle is unknown", "name": name, "handle": handle,
                            "hint": "upload the file with upload_blob first; no run was created"
                        }));
                    }
                    map.insert(name.clone(), json!(handle));
                }
                Some(serde_json::Value::Object(map))
            }
            _ => None,
        };
        // Run-or-recall: if opted in and an identical run already succeeded, return its
        // result instead of spawning a container. The key folds in the secrets generation +
        // input blobs, so a secret change or a changed input file invalidates env-dependent
        // recalls.
        let cache_key = if a.cache.unwrap_or(false) {
            let secrets_gen = self.db.secrets_generation().await.map_err(internal)?;
            let digest = self.exec.image_digest(&script.runtime).unwrap_or_default();
            let key = run_cache_key(&script.runtime, &digest, &script.source, &input, secrets_gen, input_blobs.as_ref());
            if let Some((run_id, exit, result)) =
                self.db.find_cached_run(&key).await.map_err(internal)?
            {
                // Cache hit-rate observability: a recall served without spawning a container.
                metrics::counter!("dokan_run_cache_total", "result" => "hit").increment(1);
                let mut hit = json!({
                    "run_id": run_id, "status": "recalled", "exit": exit, "cache_key": key,
                });
                if let Some(r) = result {
                    hit["result"] = r;
                }
                return ok(hit);
            }
            metrics::counter!("dokan_run_cache_total", "result" => "miss").increment(1);
            Some(key)
        } else {
            None
        };
        // agent_id is REQUIRED — closes the quota-omit bypass (omitting it used to skip quota
        // entirely). It is unauthenticated provenance, not an auth boundary (single-tenant trust
        // model; see SECURITY.md) — a non-empty id is all we enforce here.
        let aid = a.agent_id.trim();
        if aid.is_empty() {
            return ok(json!({
                "error": "agent_id_required", "message": "agent_id is required",
                "hint": "pass your agent id — it tags provenance, scopes secrets, and meters quota"
            }));
        }
        // Per-agent backpressure: concurrency quota + rolling compute budget. A runaway
        // agent can't swamp the shared runtime or burn unbounded compute.
        let n = self.db.agent_running_count(aid).await.map_err(internal)?;
        if n >= AGENT_MAX_CONCURRENT {
            return ok(json!({
                "error": "quota_exceeded", "message": "this agent has too many runs in flight", "hint": "wait for in-flight runs to finish or back off", "agent_id": aid,
                "in_flight": n, "limit": AGENT_MAX_CONCURRENT
            }));
        }
        let spent = self
            .db
            .agent_compute_seconds(aid, AGENT_BUDGET_WINDOW_SECS)
            .await
            .map_err(internal)?;
        if spent >= AGENT_COMPUTE_BUDGET_SECS {
            return ok(json!({
                "error": "budget_exceeded", "message": "this agent exhausted its rolling compute budget", "hint": "retry after the budget window rolls over", "agent_id": aid,
                "compute_seconds_24h": spent, "budget": AGENT_COMPUTE_BUDGET_SECS
            }));
        }
        // Enqueue only — a worker claims it from the queue (FOR UPDATE SKIP LOCKED). When an
        // idempotency_key is supplied, insert-or-return atomically: two racing identical
        // enqueues collapse to ONE run via the partial UNIQUE index (exactly-once). created=false
        // recalls the existing run and returns the `idempotent: true` shape.
        let run_id = if let Some(key) = a.idempotency_key.as_deref() {
            let (id, created) = self
                .db
                .insert_run_idempotent(a.script_id, &input, Some(aid), input_blobs.as_ref(), a.capture_output.unwrap_or(false), key)
                .await
                .map_err(internal)?;
            if !created {
                let status = self
                    .db
                    .find_run_by_idempotency(key)
                    .await
                    .map_err(internal)?
                    .map(|(_, st)| st)
                    .unwrap_or_else(|| "pending".to_string());
                return ok(json!({"run_id": id, "status": status, "idempotent": true}));
            }
            id
        } else {
            self.db
                .insert_run_with_blobs(a.script_id, &input, Some(aid), input_blobs.as_ref(), a.capture_output.unwrap_or(false))
                .await
                .map_err(internal)?
        };
        if let Some(key) = &cache_key {
            let _ = self.db.set_run_cache_key(run_id, key).await;
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
        // Captured output files ({name: sha}), if the run opted into capture_output — each sha is
        // downloadable via download_blob.
        if let Some(ob) = self.db.run_output_blobs(a.run_id).await.ok().flatten() {
            out["output_blobs"] = ob;
        }
        // Latest progress line (live status of a long run), if the job emitted one.
        if let Some(p) = self.db.run_progress(a.run_id).await.ok().flatten() {
            out["progress"] = json!(p);
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
        // Captured output files ({name: sha}) — each sha is downloadable via download_blob.
        if let Some(ob) = self.db.run_output_blobs(a.run_id).await.ok().flatten() {
            out["output_blobs"] = ob;
        }
        if let Some(p) = self.db.run_progress(a.run_id).await.ok().flatten() {
            out["progress"] = json!(p);
        }
        ok(out)
    }

    #[tool(description = "List recent runs with server-side status counts. Filters: status, script_id, and outcome. Each run carries an OUTCOME that separates a deterministic VERDICT (a monitor/gate that ran and chose exit≠0 — a finding, NOT a failure) from a real ERROR (timeout/vanished/setup/OOM). Use outcome=error to surface ONLY genuine failures (don't chase monitor verdicts as bugs); outcome=verdict for findings. The response carries `outcomes` (counts by class) and `all_green` (true iff zero errors AND zero verdicts in the window) — the quick 'is everything passing?' check. Cursor-light summary, not every row.")]
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
            .list_runs(a.status.as_deref(), a.script_id, limit)
            .await
            .map_err(internal)?;
        // Classify every row, optionally keep only one outcome class, and tally a per-class
        // summary so an operator sees verdict-vs-error at a glance instead of a wall of "failed".
        let want = a.outcome.as_deref();
        let mut outcomes: std::collections::BTreeMap<&'static str, i64> = std::collections::BTreeMap::new();
        let mut items: Vec<serde_json::Value> = Vec::new();
        for r in &rows {
            // Tally over the FULL window so `outcomes` + `all_green` reflect reality regardless
            // of the display filter; emit only rows matching the requested outcome.
            let outcome = r.outcome();
            *outcomes.entry(outcome).or_insert(0) += 1;
            if want.is_some_and(|w| w != outcome) {
                continue;
            }
            // error only when present, to stay token-frugal on the happy path.
            let mut o = json!({"run_id": r.id, "script_id": r.script_id, "script": r.script_name, "status": r.status, "outcome": outcome, "exit": r.exit_code, "at": r.created_at.to_rfc3339()});
            if let Some(e) = &r.error {
                o["error"] = json!(e);
            }
            // Latest progress line — the cheap "what is this long run doing now" signal.
            if let Some(p) = &r.progress {
                o["progress"] = json!(p);
            }
            items.push(o);
        }
        let outcomes_obj: serde_json::Map<String, serde_json::Value> =
            outcomes.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
        // all_green = nothing in the window is an error OR an unresolved verdict: every check passed.
        let all_green = outcomes.get("error").copied().unwrap_or(0) == 0
            && outcomes.get("verdict").copied().unwrap_or(0) == 0;
        ok(json!({"counts": counts_obj, "outcomes": outcomes_obj, "all_green": all_green, "recent": items}))
    }

    #[tool(description = "Compose a flow: a declarative DAG of steps wired over MCP. Validates acyclicity. Returns flow_id. \
        Step fields: id, script_id, input?, depends_on? (ids). \
        Rich control flow (all optional): \
        when {ref,op,value} gates a step — ref is `deps.<id>` or `flow_input.<f>`, op eq|ne|contains|truthy|falsy; false → step skipped and the skip propagates to its dependents (branching). \
        map \"<ref>\" fans the step out over an array (one child run per element, element passed as `step`); parent succeeds with the array of outputs or fails if any child fails. \
        compensate <script_id> runs that script (saga rollback) if the flow later fails, in reverse order, for each succeeded step. \
        retries <n> overrides per-step retries (default 1; set 0 for non-idempotent steps). \
        cache true opts the step into the content-addressed run cache: since the key folds in upstream deps, re-running a flow recalls unchanged steps and only re-executes the changed subgraph (partial flow recall — best for deterministic/network:false steps). Each step sees {flow_input, deps, step}.")]
    async fn compose_flow(
        &self,
        Parameters(a): Parameters<ComposeFlowArgs>,
    ) -> Result<CallToolResult, McpError> {
        let spec = destringify(a.spec);
        if let Err(e) = crate::flow::validate_spec(&spec) {
            return err_ctx("invalid_spec", "the flow spec is invalid", None, json!({"detail": e}));
        }
        let id = self
            .db
            .insert_flow(&a.name, &spec)
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
            return err_ctx("flow_not_found", "no flow with that id", Some("check the flow_id from compose_flow"), json!({"id": a.flow_id}));
        };
        let input = destringify(a.input.unwrap_or(json!({})));
        let id = self
            .db
            .insert_flow_run(a.flow_id, &spec, &input)
            .await
            .map_err(internal)?;
        ok(json!({"flow_run_id": id, "status": "pending"}))
    }

    #[tool(description = "Status of a flow run: overall status + per-step status/output. Token-frugal step ledger — map children collapse into a per-parent {n,ok,failed} (+ failed_children indices on partial failure). Top-level steps are paginated: pass limit (default 200, max 1000) + after (cursor); the reply carries total_steps and next_cursor when more remain.")]
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
        // SQL-level pagination (TSU-177): page the TOP-LEVEL steps with LIMIT/OFFSET and
        // aggregate the map children with a GROUP BY — so a flow with thousands of fan-out
        // children never loads every step row into memory. Cursor shape matches list_runs.
        const FAILED_IDX_CAP: i64 = 20;
        let after = a.after.unwrap_or(0).max(0);
        let limit = a.limit.unwrap_or(200).clamp(1, 1000);
        let total_steps = self.db.flow_top_step_count(a.flow_run_id).await.map_err(internal)?;
        let page = self.db.flow_top_steps(a.flow_run_id, limit, after).await.map_err(internal)?;

        // Per-map-parent {n, ok, failed} from a GROUP BY (children never fetched row-by-row).
        let mut counts: std::collections::HashMap<String, (i64, i64, i64)> = std::collections::HashMap::new();
        for (parent, st, n) in self.db.flow_map_counts(a.flow_run_id).await.map_err(internal)? {
            let e = counts.entry(parent).or_default();
            e.0 += n;
            match st.as_str() {
                "succeeded" => e.1 += n,
                "failed" => e.2 += n,
                _ => {}
            }
        }
        // Failed child indices (capped per parent) for the partial-failure detail.
        let mut failed_idx: std::collections::HashMap<String, Vec<i64>> = std::collections::HashMap::new();
        for (parent, idx) in self.db.flow_failed_child_idx(a.flow_run_id, FAILED_IDX_CAP).await.map_err(internal)? {
            failed_idx.entry(parent).or_default().push(idx);
        }

        let items: Vec<_> = page
            .iter()
            .map(|s| {
                let mut o = json!({"id": s.step_id, "status": s.status});
                match counts.get(&s.step_id) {
                    Some((n, ok, failed)) => {
                        let mut m = json!({"n": n, "ok": ok, "failed": failed});
                        if *failed > 0 {
                            let idx = failed_idx.get(&s.step_id).cloned().unwrap_or_default();
                            m["failed_children"] = json!(idx);
                            if *failed > idx.len() as i64 {
                                m["failed_children_truncated"] = json!(*failed - idx.len() as i64);
                            }
                        }
                        o["map"] = m;
                    }
                    None => o["out"] = json!(s.output),
                }
                if s.compensated {
                    o["comp"] = json!(true);
                }
                o
            })
            .collect();
        let next = after + items.len() as i64;
        let mut out = json!({"status": status, "steps": items, "total_steps": total_steps});
        if next < total_steps {
            out["next_cursor"] = json!(next);
        }
        ok(out)
    }

    #[tool(description = "Schedule a script on a cron (6-field, leading seconds). Each tick enqueues a run. Returns schedule_id.")]
    async fn schedule(
        &self,
        Parameters(a): Parameters<ScheduleArgs>,
    ) -> Result<CallToolResult, McpError> {
        let Some(cron) = &self.cron else {
            return err("scheduler_disabled", "the cron scheduler is not enabled on this daemon", None);
        };
        if let Err(e) = validate_cron(&a.cron) {
            return err_ctx("invalid_cron", "the cron expression is invalid", Some("6 fields with leading seconds, e.g. 0 */5 * * * *"), json!({"detail": e}));
        }
        if self.db.get_script(a.script_id).await.map_err(internal)?.is_none() {
            return err_ctx("script_not_found", "no script with that id", Some("check list_scripts for valid ids"), json!({"id": a.script_id}));
        }
        let input = destringify(a.input.unwrap_or(json!({})));
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

    #[tool(description = "Register an inbound webhook so an external service can trigger a script/flow over HTTP. An external POST to /hook/<token> enqueues the target with the request body as input (non-blocking). target is \"script\" or \"flow\". Returns the token + path. The unguessable URL token is the auth (the endpoint is outside the bearer gate). dokan owns only the endpoint — making a local daemon publicly reachable (tunnel/relay) is the operator's concern.")]
    async fn create_webhook(
        &self,
        Parameters(a): Parameters<CreateWebhookArgs>,
    ) -> Result<CallToolResult, McpError> {
        if a.target != "script" && a.target != "flow" {
            return err("bad_target", "target must be \"script\" or \"flow\"", None);
        }
        let exists = if a.target == "flow" {
            self.db.get_flow_spec(a.target_id).await.map_err(internal)?.is_some()
        } else {
            self.db.get_script(a.target_id).await.map_err(internal)?.is_some()
        };
        if !exists {
            return err_ctx(&format!("{}_not_found", a.target), "the target does not exist", Some("create the script/flow before the webhook"), json!({"id": a.target_id}));
        }
        let token = crate::crypto::random_token();
        let id = self
            .db
            .insert_webhook(&token, &a.target, a.target_id, a.agent_id.as_deref())
            .await
            .map_err(internal)?;
        ok(json!({"webhook_id": id, "token": token, "path": format!("/hook/{token}"), "status": "created"}))
    }

    #[tool(description = "List inbound webhooks: id, token (the URL secret), target kind + id, agent. Operator-only surface.")]
    async fn list_webhooks(&self) -> Result<CallToolResult, McpError> {
        let rows = self.db.list_webhooks().await.map_err(internal)?;
        ok(json!({"webhooks": rows}))
    }

    #[tool(description = "Revoke an inbound webhook by id (its /hook/<token> URL stops working).")]
    async fn delete_webhook(
        &self,
        Parameters(a): Parameters<DeleteWebhookArgs>,
    ) -> Result<CallToolResult, McpError> {
        let deleted = self.db.delete_webhook(a.webhook_id).await.map_err(internal)?;
        ok(json!({"webhook_id": a.webhook_id, "deleted": deleted}))
    }

    #[tool(description = "Set a secret, injected as an env var into every job container (e.g. OPENAI_API_KEY). Write-only: values are never returned or logged. Upsert by name.")]
    async fn set_secret(
        &self,
        Parameters(a): Parameters<SetSecretArgs>,
    ) -> Result<CallToolResult, McpError> {
        if a.name.trim().is_empty() {
            return err("empty_name", "name must be non-empty", None);
        }
        self.db
            .upsert_secret(&a.name, &a.value, a.agent_id.as_deref())
            .await
            .map_err(internal)?;
        let scope = a.agent_id.as_deref().unwrap_or("global");
        ok(json!({"name": a.name, "scope": scope, "status": "set"}))
    }

    #[tool(description = "Fetch a run's tamper-evident reproducibility receipt: the image digest, source/input/output hashes, secrets generation, exit, a keyed HMAC tag (alg+sig), AND an Ed25519/in-toto DSSE envelope (public-key, third-party-verifiable). The HMAC is the DOKAN_RECEIPT_KEY-holder tamper check; for a key-free third-party check use `verify` (offline) or `reproduce` (re-execution). For a network=false (deterministic) run it attests a recall is sound.")]
    async fn get_receipt(
        &self,
        Parameters(a): Parameters<GetReceiptArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.db.run_receipt(a.run_id).await.map_err(internal)? {
            Some(r) => ok(r),
            None => err_ctx("no_receipt", "this run has no receipt yet", Some("receipts attach once a run reaches a terminal state"), json!({"run_id": a.run_id})),
        }
    }

    #[tool(description = "Reproduce a prior run by RE-EXECUTION: re-run its EXACT recorded invocation (cache DISABLED → a real container, never a recall) and byte-compare the new output against the receipt — verify by re-execution, not by trust. The differentiator no provenance-signing tool offers, because they don't own execution. Blocks up to `timeout`s for the re-run, then returns {verdict, code, repro_run_id, ...}: REPRODUCED(0) byte-identical; DIVERGED(6) authentic receipt but a different output (the workload isn't deterministic — unseeded RNG / wall-clock / map order); TAMPERED(5) the original receipt fails verification; INCONCLUSIVE(7) can't soundly reproduce (no receipt, network was on, source drifted, or the re-run didn't finish in time). Sound only for network-disabled runs: the runtime is deterministic, your output is reproducible iff your code is — this proves which.")]
    async fn reproduce(
        &self,
        Parameters(a): Parameters<ReproduceArgs>,
    ) -> Result<CallToolResult, McpError> {
        let v = reproduce_run(&self.db, &self.exec, a.run_id, a.timeout.unwrap_or(120).min(300), a.agent_id)
            .await
            .map_err(internal)?;
        ok(v)
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
        let (in_flight, spent) = match a.agent_id.as_deref() {
            Some(aid) => (
                self.db.agent_running_count(aid).await.unwrap_or(0),
                self.db
                    .agent_compute_seconds(aid, AGENT_BUDGET_WINDOW_SECS)
                    .await
                    .unwrap_or(0.0),
            ),
            None => (0, 0.0),
        };
        ok(json!({
            "agent_id": a.agent_id,
            "runtimes": SUPPORTED_RUNTIMES,
            "limits": { "mem_mb": mem_bytes / (1024 * 1024), "cpus": nano_cpus as f64 / 1e9 },
            "secrets": secrets,
            "quota": { "max_concurrent": AGENT_MAX_CONCURRENT, "in_flight": in_flight },
            "budget": {
                "compute_seconds_24h": (spent * 10.0).round() / 10.0,
                "compute_budget": AGENT_COMPUTE_BUDGET_SECS
            },
            "input_contract": "job reads DOKAN_INPUT (JSON env), secrets as env vars; emit `::dokan:result:: {json}` for a structured result",
        }))
    }

    #[tool(description = "Stop a cron schedule: removes the live job and disables it so it won't reload. Always unschedule test/temporary crons.")]
    async fn unschedule(
        &self,
        Parameters(a): Parameters<UnscheduleArgs>,
    ) -> Result<CallToolResult, McpError> {
        let Some(cron) = &self.cron else {
            return err("scheduler_disabled", "the cron scheduler is not enabled on this daemon", None);
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
        // Authoritative cancel write — wins over the killed container's racing failed-finish.
        let canceled = self.db.cancel_run(a.run_id, "canceled by operator").await.map_err(internal)?;
        ok(json!({"run_id": a.run_id, "status": if canceled { "canceled" } else { "already_succeeded" }}))
    }

    #[tool(description = "Verify a run's receipt WITHOUT re-executing — offline, instant. Checks the Ed25519/DSSE signature against the receipt's embedded public key (third-party-verifiable, NO shared secret needed), the HMAC binding with the daemon key (key-holder check), and that the signed in-toto Statement attests THIS run's output. Returns {ok, ed25519_valid, hmac_valid, binding_consistent, hermetic, deterministic, keyid}. hermetic=true means the run was network-disabled (its output is a pure function of inputs). For verify-by-RE-EXECUTION (re-run + byte-compare), use the reproduce primitive.")]
    async fn verify(&self, Parameters(a): Parameters<VerifyArgs>) -> Result<CallToolResult, McpError> {
        let Some(receipt) = self.db.run_receipt(a.run_id).await.map_err(internal)? else {
            return err_ctx("no_receipt", "this run has no receipt yet", Some("receipts attach once a run reaches a terminal state"), json!({"run_id": a.run_id}));
        };
        let rep = crate::receipt::verify_receipt(&receipt);
        let hmac_valid = self.exec.verify_receipt_hmac(&receipt);
        ok(json!({
            "run_id": a.run_id,
            "ok": rep.ok() && hmac_valid,
            "ed25519_valid": rep.ed25519_valid,
            "hmac_valid": hmac_valid,
            "binding_consistent": rep.binding_consistent,
            "hermetic": rep.hermetic,
            "deterministic": receipt.get("deterministic").and_then(|v| v.as_bool()).unwrap_or(false),
            "keyid": rep.keyid,
        }))
    }
}

/// Reproduce a recorded run: re-execute the exact recorded invocation and byte-compare the new
/// output against the original receipt. Shared by the MCP `reproduce` tool and the HTTP endpoint.
/// Returns a verdict object — REPRODUCED(0) / DIVERGED(6) / TAMPERED(5) / INCONCLUSIVE(7) — rather
/// than throwing, so every terminal state is a structured answer the operator can act on.
pub(crate) async fn reproduce_run(
    db: &crate::db::Db,
    exec: &crate::exec::Executor,
    run_id: i64,
    timeout_secs: u64,
    agent_id: Option<String>,
) -> anyhow::Result<serde_json::Value> {
    let verdict = |name: &str, code: i64, mut extra: serde_json::Value| {
        extra["verdict"] = json!(name);
        extra["code"] = json!(code);
        extra["run_id"] = json!(run_id);
        extra
    };
    // 1. The original receipt is the thing we reproduce against.
    let Some(orig) = db.run_receipt(run_id).await? else {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"reason": "no_receipt", "detail": "original run has no receipt to compare against"})));
    };
    let orig_output = orig.get("output_sha256").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let orig_source = orig.get("source_sha256").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let orig_out_blobs = canonical_input_blobs(orig.get("output_blobs"));
    let network = orig.get("network").and_then(|v| v.as_bool()).unwrap_or(true);

    // 2. A networked run's output isn't a pure function of its inputs — not soundly reproducible.
    if network {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"reason": "non_deterministic", "detail": "original run had network enabled; output is not a pure function of inputs"})));
    }
    // 3. If the recorded receipt itself doesn't verify, the record was altered — re-running is moot.
    let rep = crate::receipt::verify_receipt(&orig);
    let hmac_ok = exec.verify_receipt_hmac(&orig);
    if !(rep.ok() && hmac_ok) {
        return Ok(verdict("TAMPERED", 5, json!({"reason": "receipt_failed_verification", "ed25519_valid": rep.ed25519_valid, "hmac_valid": hmac_ok, "binding_consistent": rep.binding_consistent})));
    }
    // 4. Re-execute the recorded invocation (same script, input, input-blobs, capture flag).
    let Some((script_id, input, input_blobs, orig_aid, capture_output)) =
        db.run_reproduce_inputs(run_id).await?
    else {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"reason": "run_not_found"})));
    };
    // Source is read live from `scripts` at claim time, not snapshotted per run — so if the
    // script changed since the original, a re-run would execute DIFFERENT bytes. Catch that
    // upfront (no wasted container): the exact code is gone, so the result is INCONCLUSIVE.
    let Some(script) = db.get_script(script_id).await? else {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"reason": "script_not_found", "script_id": script_id})));
    };
    let current_source = crate::receipt::sha256_hex(script.source.as_bytes());
    if current_source != orig_source {
        return Ok(verdict("INCONCLUSIVE", 7, json!({
            "reason": "source_changed", "script_id": script_id,
            "original_source_sha256": orig_source, "current_source_sha256": current_source,
            "detail": "script source changed since the original run; the exact bytes can't be reproduced — no run created"
        })));
    }
    let aid = agent_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or(orig_aid)
        .unwrap_or_else(|| "reproduce".to_string());
    let repro_id = db
        .insert_run_with_blobs(script_id, &input, Some(&aid), input_blobs.as_ref(), capture_output)
        .await?;
    // 5. Long-poll the re-run to terminal (a worker claims it from the queue).
    let mut status = String::from("pending");
    for _ in 0..(timeout_secs * 2) {
        status = db.run_status(repro_id).await?.unwrap_or_else(|| "unknown".into());
        if matches!(status.as_str(), "succeeded" | "failed" | "canceled") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    if !matches!(status.as_str(), "succeeded" | "failed") {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"repro_run_id": repro_id, "reason": "timeout_or_unrunnable", "status": status})));
    }
    // 6. Compare the re-run's receipt to the original.
    let Some(repro) = db.run_receipt(repro_id).await? else {
        return Ok(verdict("INCONCLUSIVE", 7, json!({"repro_run_id": repro_id, "reason": "no_repro_receipt"})));
    };
    let repro_output = repro.get("output_sha256").and_then(|v| v.as_str()).unwrap_or_default();
    let repro_out_blobs = canonical_input_blobs(repro.get("output_blobs"));
    if repro_output == orig_output && repro_out_blobs == orig_out_blobs {
        Ok(verdict("REPRODUCED", 0, json!({"repro_run_id": repro_id, "output_sha256": orig_output})))
    } else {
        Ok(verdict("DIVERGED", 6, json!({
            "repro_run_id": repro_id,
            "expected_output_sha256": orig_output,
            "actual_output_sha256": repro_output,
            "detail": "re-execution produced a different output; the workload is not byte-reproducible (unseeded RNG / wall-clock / map iteration order)"
        })))
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
             explicitly). No LLM runs inside dokan — intelligence is yours, applied at the edge. \
             Flows: compose_flow wires a DAG; steps support when (branch), map (fan-out), \
             compensate (saga rollback), retries. Poll get_flow_run — map children are collapsed \
             into a {n,ok,failed} count (+ failed_children indices on partial failure), and \
             top-level steps paginate (limit/after -> next_cursor). \
             Provenance: every run gets a tamper-evident receipt (get_receipt); verify checks it \
             OFFLINE (Ed25519/DSSE, no shared secret); reproduce re-executes + byte-compares \
             (REPRODUCED/DIVERGED/TAMPERED/INCONCLUSIVE) — sound for network=false runs. \
             Recurring: schedule (6-field cron) / create_webhook (inbound POST /hook/<token>). \
             Secrets: set_secret (write-only, env-injected); list_secrets = names only. \
             Errors are a uniform {error: code, message, hint?} envelope. \
             Discover the rest with whoami (caps, your secrets, quota) and list_runs (outcome= \
             error|verdict, all_green)."
                .into(),
        );
        info
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_input_blobs, canonical_json, destringify, run_cache_key, validate_cron};
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
        let k = |i: &serde_json::Value, g: i64| run_cache_key("bash", "sha-d", "src", i, g, None);
        assert_eq!(k(&i1, 0), k(&i2, 0), "input key-order doesn't change the cache key");
        assert_ne!(k(&i1, 0), k(&json!({"a": 9}), 0), "different input -> different key");
        assert_ne!(k(&i1, 0), k(&i1, 1), "secrets generation invalidates");
        assert_ne!(
            run_cache_key("bash", "digestA", "s", &i1, 0, None),
            run_cache_key("bash", "digestB", "s", &i1, 0, None),
            "image digest invalidates"
        );
    }

    #[test]
    fn input_blobs_participate_in_cache_key() {
        // Same canonicalization regardless of map key order; sorted "name:sha".
        let a = json!({ "note.txt": "sha1", "data.csv": "sha2" });
        let b = json!({ "data.csv": "sha2", "note.txt": "sha1" });
        assert_eq!(canonical_input_blobs(Some(&a)), canonical_input_blobs(Some(&b)));
        assert_eq!(canonical_input_blobs(None), "");
        let i = json!({});
        let base = run_cache_key("bash", "d", "src", &i, 0, None);
        let with_file = run_cache_key("bash", "d", "src", &i, 0, Some(&a));
        assert_ne!(base, with_file, "declaring an input file shifts the key");
        // A changed file content (different sha) misses; identical files hit.
        let changed = json!({ "note.txt": "sha9", "data.csv": "sha2" });
        assert_ne!(
            run_cache_key("bash", "d", "src", &i, 0, Some(&a)),
            run_cache_key("bash", "d", "src", &i, 0, Some(&changed)),
            "a changed input file invalidates the cache"
        );
        assert_eq!(
            run_cache_key("bash", "d", "src", &i, 0, Some(&a)),
            run_cache_key("bash", "d", "src", &i, 0, Some(&b)),
            "same files (any order) recall"
        );
    }

    #[test]
    fn destringify_decodes_client_stringified_objects() {
        // The bug: an MCP client sends the input object as a JSON *string*. Left as-is it
        // reaches the job double-encoded (DOKAN_INPUT = a quoted JSON string).
        assert_eq!(destringify(json!(r#"{"write":true}"#)), json!({"write": true}));
        assert_eq!(destringify(json!("[1,2,3]")), json!([1, 2, 3]));
        // A real object passes through unchanged (idempotent — applying it twice is safe).
        let obj = json!({"write": true});
        assert_eq!(destringify(obj.clone()), obj);
        assert_eq!(destringify(destringify(json!(r#"{"write":true}"#))), obj);
        // A scalar string is NOT mangled, even when it happens to parse as a JSON scalar.
        assert_eq!(destringify(json!("hello")), json!("hello"));
        assert_eq!(destringify(json!("123")), json!("123"));
        assert_eq!(destringify(json!("true")), json!("true"));
        // Non-string values are untouched.
        assert_eq!(destringify(json!(42)), json!(42));
    }

    #[test]
    fn cron_requires_six_fields() {
        assert!(validate_cron("0 */5 * * * *").is_ok(), "6-field ok");
        assert!(validate_cron("*/5 * * * *").is_err(), "5-field rejected");
        assert!(validate_cron("0 0 0 0 0 0 0").is_err(), "7-field rejected");
        assert!(validate_cron("  0   0 * * * *  ").is_ok(), "extra whitespace tolerated");
    }

    #[test]
    fn error_envelope_is_uniform() {
        // Code + message always present; hint + extra context fields when supplied.
        let e = super::error_envelope("not_found", "no script with that id", Some("check list_scripts"), serde_json::json!({"id": 7}));
        assert_eq!(e["error"], "not_found", "stable machine-readable code");
        assert_eq!(e["message"], "no script with that id");
        assert_eq!(e["hint"], "check list_scripts");
        assert_eq!(e["id"], 7, "extra context merged in");
        // No hint / no extra → just code + message, no hint key.
        let e2 = super::error_envelope("empty_name", "name must be non-empty", None, serde_json::json!({}));
        assert_eq!(e2["error"], "empty_name");
        assert!(e2.get("hint").is_none(), "hint omitted when not supplied");
        assert!(e2["message"].is_string());
    }

    #[test]
    fn blob_ingest_rejects_empty_and_oversize() {
        assert!(super::validate_blob_bytes(0).is_err(), "empty blob rejected as garbage");
        assert!(super::validate_blob_bytes(1).is_ok(), "1 byte ok");
        assert!(super::validate_blob_bytes(super::MAX_BLOB_BYTES).is_ok(), "exactly the cap ok");
        assert!(
            super::validate_blob_bytes(super::MAX_BLOB_BYTES + 1).is_err(),
            "one over the cap rejected"
        );
    }
}
