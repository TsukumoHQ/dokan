//! Flow engine (P2): drives a declarative DAG of steps. Each step is one container run.
//! Durability is at the STEP boundary — completed steps are checkpointed in Postgres
//! (`flow_steps.status`), so a crashed engine resumes the DAG where it left off. Inside a
//! step there is no magic: a step that dies is re-run, so steps must be idempotent.
//! This is the deliberate escape from the Temporal replay trap (PRD §6).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::db::Db;
use crate::exec::{runtime_spec, Executor};
use crate::scale::Concurrency;

const IDLE_POLL: Duration = Duration::from_millis(400);
/// Flow-run lease: the driver heartbeats periodically (see `spawn_heartbeat`), so the lease
/// stays fresh even across a long step batch. Set above the job timeout so a healthy long
/// step is never mistaken for a dead engine. 2× the job timeout gives comfortable margin.
const FLOW_LEASE_SECS: f64 = (crate::exec::DEFAULT_TIMEOUT_SECS * 2) as f64;

/// Hard cap on a single `map` fan-out's child count. Container execution is already throttled
/// to the worker concurrency cap (each child holds a `slots` permit), but a pathological array
/// would still create that many run rows + tokio tasks up front — bound it. A map over a larger
/// array fails fast with a clear verdict rather than degrading the whole runtime.
const MAX_MAP_FANOUT: usize = 1000;

/// Whether a map fan-out of `n` children is within the cap. Pure — unit-tested.
fn fanout_within_cap(n: usize) -> bool {
    n <= MAX_MAP_FANOUT
}

#[derive(Clone)]
pub struct FlowEngine {
    db: Db,
    exec: Arc<Executor>,
    /// Shared with the worker: bounds total in-flight containers so a large `map` fan-out
    /// can't spawn one container per element at once and swamp the host.
    slots: Arc<Concurrency>,
}

impl FlowEngine {
    pub fn new(db: Db, exec: Arc<Executor>, slots: Arc<Concurrency>) -> Self {
        Self { db, exec, slots }
    }

    /// Resume orphaned flows, then loop claiming and driving pending flow_runs.
    pub async fn start(self) -> anyhow::Result<()> {
        let n = self.db.reap_orphan_flow_runs(FLOW_LEASE_SECS).await?;
        if n > 0 {
            tracing::info!(flow_runs = n, "resuming orphaned flows at step boundary");
        }
        tokio::spawn(async move {
            loop {
                match self.db.claim_flow_run().await {
                    Ok(Some((flow_run_id, input))) => {
                        let engine = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = engine.drive(flow_run_id, input).await {
                                tracing::error!(flow_run_id, "flow driver error: {e}");
                                let _ = engine.db.finish_flow_run(flow_run_id, "failed").await;
                                metrics::counter!("dokan_flow_runs_finished_total", "status" => "failed").increment(1);
                            }
                        });
                    }
                    Ok(None) => tokio::time::sleep(IDLE_POLL).await,
                    Err(e) => {
                        tracing::error!("claim_flow_run: {e}");
                        tokio::time::sleep(IDLE_POLL).await;
                    }
                }
            }
        });
        Ok(())
    }

    /// Background lease heartbeat: bumps `started_at` on an interval well inside the lease,
    /// so even a long-running step batch (e.g. a large map fan-out) never lets the reaper
    /// reclaim this live driver. Aborted when `drive` returns.
    fn spawn_heartbeat(&self, flow_run_id: i64) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let beat = Duration::from_secs((FLOW_LEASE_SECS / 4.0) as u64);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(beat).await;
                let _ = db.touch_flow_run(flow_run_id).await;
            }
        })
    }

    /// Drive one flow_run to terminal status, with a background lease heartbeat for its
    /// whole lifetime.
    async fn drive(&self, flow_run_id: i64, input: serde_json::Value) -> anyhow::Result<()> {
        let hb = self.spawn_heartbeat(flow_run_id);
        let res = self.drive_inner(flow_run_id, input).await;
        hb.abort();
        res
    }

    /// Each pass: aggregate finished map fan-outs, resolve pending steps (skip dead branches,
    /// evaluate `when` gates, expand `map` steps, or run plain steps in parallel), checkpoint,
    /// repeat until the DAG completes or fails. On failure, succeeded steps with a
    /// `compensate` script are rolled back (saga).
    async fn drive_inner(&self, flow_run_id: i64, input: serde_json::Value) -> anyhow::Result<()> {
        loop {
            // Belt-and-suspenders beat at the top of each pass (the background heartbeat is
            // the real guarantee for long batches).
            let _ = self.db.touch_flow_run(flow_run_id).await;
            let steps = self.db.flow_steps(flow_run_id).await?;

            if steps.iter().any(|s| s.status == "failed") {
                let comp_failed = self.compensate(flow_run_id, &input, &steps).await;
                // If a rollback step itself failed, the saga didn't fully unwind — surface it as
                // a distinct terminal status (needs-attention) rather than a plain "failed". (TSU-190)
                let status = if comp_failed > 0 { "compensation_failed" } else { "failed" };
                self.db.finish_flow_run(flow_run_id, status).await?;
                metrics::counter!("dokan_flow_runs_finished_total", "status" => status).increment(1);
                return Ok(());
            }
            // Terminal when no step is still pending/running/expanded. Succeeded if we got
            // here without any failure (remaining steps are succeeded and/or skipped).
            let active = steps
                .iter()
                .any(|s| matches!(s.status.as_str(), "pending" | "running" | "expanded"));
            if !active {
                self.db.finish_flow_run(flow_run_id, "succeeded").await?;
                metrics::counter!("dokan_flow_runs_finished_total", "status" => "succeeded").increment(1);
                return Ok(());
            }

            // Outputs of succeeded steps (deps + ref resolution); set of skipped steps.
            let outputs: HashMap<String, String> = steps
                .iter()
                .filter(|s| s.status == "succeeded")
                .map(|s| (s.step_id.clone(), s.output.clone().unwrap_or_default()))
                .collect();
            let skipped: HashSet<String> = steps
                .iter()
                .filter(|s| s.status == "skipped")
                .map(|s| s.step_id.clone())
                .collect();

            // 1) Aggregate any map parents whose children have all finished.
            let mut progressed = false;
            for parent in steps.iter().filter(|s| s.status == "expanded") {
                match aggregate_children(&steps, &parent.step_id) {
                    Some(Agg::Failed) => {
                        self.db
                            .finish_step(flow_run_id, &parent.step_id, "failed", Some("map_child_failed"))
                            .await
                            .ok();
                        progressed = true;
                    }
                    Some(Agg::Succeeded(out)) => {
                        self.db
                            .finish_step(flow_run_id, &parent.step_id, "succeeded", Some(&out))
                            .await
                            .ok();
                        metrics::counter!("dokan_flow_steps_finished_total", "status" => "succeeded").increment(1);
                        progressed = true;
                    }
                    None => {}
                }
            }
            if progressed {
                continue;
            }

            // 2) Resolve pending steps: skip dead branches, gate on `when`, expand `map`,
            //    else queue for a normal container run.
            let mut to_run = Vec::new();
            for step in steps.iter().filter(|s| s.status == "pending").cloned() {
                // A step is actionable only once every dep is terminal (succeeded or skipped).
                let dep_ready = step
                    .depends_on
                    .iter()
                    .all(|d| outputs.contains_key(d) || skipped.contains(d));
                if !dep_ready {
                    continue;
                }
                // Dead-branch propagation, AND-semantics: if ANY dependency was skipped this
                // step is skipped too (a step needs all its deps to have actually produced
                // output). On a diamond merge that means one skipped upstream skips the join —
                // model an OR-merge by not depending on the optional branch.
                if step.depends_on.iter().any(|d| skipped.contains(d)) {
                    self.db.mark_step_skipped(flow_run_id, &step.step_id).await.ok();
                    metrics::counter!("dokan_flow_steps_finished_total", "status" => "skipped").increment(1);
                    progressed = true;
                    continue;
                }
                let deps = build_deps(&step.depends_on, &outputs);
                // `when` gate: false → skip.
                if let Some(cond) = &step.when_cond
                    && !eval_when(cond, &input, &deps) {
                        self.db.mark_step_skipped(flow_run_id, &step.step_id).await.ok();
                        metrics::counter!("dokan_flow_steps_finished_total", "status" => "skipped").increment(1);
                        progressed = true;
                        continue;
                    }
                // `map` fan-out: expand into children, parent becomes `expanded`.
                if let Some(mref) = step.map_ref.clone() {
                    self.expand(flow_run_id, &step, &input, &deps, &mref).await;
                    progressed = true;
                    continue;
                }
                to_run.push((step, deps));
            }

            if !to_run.is_empty() {
                let mut handles = Vec::new();
                for (step, deps) in to_run {
                    let me = self.clone();
                    let input = input.clone();
                    handles.push(tokio::spawn(async move {
                        me.run_step(flow_run_id, step, input, deps).await
                    }));
                }
                for h in handles {
                    let _ = h.await;
                }
                continue;
            }
            if progressed {
                continue;
            }
            // Nothing actionable but work remains (a step running elsewhere) → poll.
            tokio::time::sleep(IDLE_POLL).await;
        }
    }

    /// Expand a `map` step: resolve its ref to an array and create one child run per element
    /// (`<id>#<i>`, each carrying the element as its `step` input and the parent's deps). An
    /// empty array completes the parent immediately; a non-array fails it.
    async fn expand(
        &self,
        flow_run_id: i64,
        step: &crate::db::FlowStep,
        flow_input: &serde_json::Value,
        deps: &serde_json::Value,
        map_ref: &str,
    ) {
        let items = match resolve_ref(map_ref, flow_input, deps) {
            Some(serde_json::Value::Array(a)) => a,
            _ => {
                self.db
                    .finish_step(flow_run_id, &step.step_id, "failed", Some("map_ref_not_array"))
                    .await
                    .ok();
                metrics::counter!("dokan_flow_steps_finished_total", "status" => "failed").increment(1);
                return;
            }
        };
        if items.is_empty() {
            self.db
                .finish_step(flow_run_id, &step.step_id, "succeeded", Some("[]"))
                .await
                .ok();
            metrics::counter!("dokan_flow_steps_finished_total", "status" => "succeeded").increment(1);
            return;
        }
        // Backpressure guard: refuse a fan-out wider than the cap rather than create thousands
        // of run rows + tokio tasks at once. Fail the parent with a clear, terminal verdict.
        if !fanout_within_cap(items.len()) {
            self.db
                .finish_step(flow_run_id, &step.step_id, "failed", Some("map_fanout_too_large"))
                .await
                .ok();
            metrics::counter!("dokan_flow_steps_finished_total", "status" => "failed").increment(1);
            tracing::warn!(flow_run_id, step = %step.step_id, count = items.len(), cap = MAX_MAP_FANOUT, "map fan-out exceeds cap");
            return;
        }
        let children: Vec<_> = items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                (
                    format!("{}#{}", step.step_id, i),
                    step.script_id,
                    item.clone(),
                    step.depends_on.clone(),
                )
            })
            .collect();
        if let Err(e) = self
            .db
            .expand_map_step(flow_run_id, &step.step_id, &children, step.cache)
            .await
        {
            tracing::error!(flow_run_id, step = %step.step_id, "map expand failed: {e}");
        }
    }

    /// Saga rollback: for each succeeded step with a `compensate` script, in reverse
    /// completion order, run that script with the step's input/output. Best-effort — a
    /// compensation whose own script fails is logged + counted but does not stop the rest,
    /// so a partial flow is unwound as far as possible. Returns the number of compensations
    /// whose own script FAILED (so the caller can flag the saga as needs-attention). (TSU-190)
    async fn compensate(
        &self,
        flow_run_id: i64,
        flow_input: &serde_json::Value,
        steps: &[crate::db::FlowStep],
    ) -> usize {
        let mut comp_failed = 0usize;
        // Reverse *completion* order (latest finished first), not declaration order — correct
        // for DAGs with parallel branches. Steps without finished_at sort last.
        let mut to_comp: Vec<&crate::db::FlowStep> = steps
            .iter()
            .filter(|s| s.status == "succeeded" && !s.compensated && s.compensate.is_some())
            .collect();
        to_comp.sort_by_key(|s| std::cmp::Reverse(s.finished_at));

        for step in to_comp {
            let comp_id = step.compensate.unwrap();
            let Some(script) = self.db.get_script(comp_id).await.ok().flatten() else {
                tracing::warn!(flow_run_id, step = %step.step_id, comp_id, "compensate script missing");
                continue;
            };
            if runtime_spec(&script.runtime).is_none() {
                continue;
            }
            let comp_input = serde_json::json!({
                "flow_input": flow_input,
                "step": step.input,
                "output": step.output,
            });
            let Ok(run_id) = self.db.insert_run(comp_id, &comp_input, None).await else {
                continue;
            };
            // Stateful monitors: feed the compensation script's most-recent prior structured
            // result into its input as `prev_result` (null on first run).
            let comp_run_input = if script.feed_prev_result {
                let prev = self.db.last_result_for_script(script.id, run_id).await.ok().flatten();
                let mut v = comp_input.clone();
                match v.as_object_mut() {
                    Some(obj) => { obj.insert("prev_result".into(), prev.unwrap_or(serde_json::Value::Null)); }
                    None => { v = serde_json::json!({ "input": comp_input, "prev_result": prev }); }
                }
                v
            } else {
                comp_input.clone()
            };
            {
                let _permit = self.slots.acquire().await;
                self.exec
                    .run(&self.db, run_id, &script.runtime, &script.source, &comp_run_input, None, script.network, script.mem_limit_mb, script.cpu_limit, None, false)
                    .await;
            }
            // Surface a compensation whose own script failed instead of silently marking it done.
            let ok = self
                .db
                .run_status(run_id)
                .await
                .ok()
                .flatten()
                .as_deref()
                == Some("succeeded");
            self.db.mark_step_compensated(flow_run_id, &step.step_id).await.ok();
            let result = if ok { "ok" } else { "failed" };
            metrics::counter!("dokan_flow_compensations_total", "result" => result).increment(1);
            if ok {
                tracing::info!(flow_run_id, step = %step.step_id, "compensated");
            } else {
                comp_failed += 1;
                tracing::warn!(flow_run_id, step = %step.step_id, run_id, "compensation script FAILED");
            }
        }
        comp_failed
    }

    /// Execute a single step as a container run, with per-step retry. Records the
    /// step's status + output (last stdout line) as the durability checkpoint.
    async fn run_step(
        &self,
        flow_run_id: i64,
        step: crate::db::FlowStep,
        flow_input: serde_json::Value,
        deps: serde_json::Value,
    ) {
        let Some(script) = self.db.get_script(step.script_id).await.ok().flatten() else {
            let _ = self
                .db
                .finish_step(flow_run_id, &step.step_id, "failed", Some("script_not_found"))
                .await;
            return;
        };
        if runtime_spec(&script.runtime).is_none() {
            let _ = self
                .db
                .finish_step(flow_run_id, &step.step_id, "failed", Some("unknown_runtime"))
                .await;
            return;
        }

        // Each step sees the flow input, upstream outputs, and its own configured input.
        let step_input = serde_json::json!({
            "flow_input": flow_input,
            "deps": deps,
            "step": step.input,
        });

        // Partial flow recall: when the step opts into the cache, content-address it on
        // (runtime, image, source, step_input, secrets) — step_input folds in upstream
        // `deps`, so an unchanged upstream subgraph recalls and only the dirty part re-runs.
        let cache_key = if step.cache {
            let secrets_gen = self.db.secrets_generation().await.unwrap_or(0);
            let digest = self.exec.image_digest(&script.runtime).unwrap_or_default();
            let key = crate::mcp::run_cache_key(&script.runtime, &digest, &script.source, &step_input, secrets_gen, None);
            if let Ok(Some((cached_run_id, _exit, _result))) = self.db.find_cached_run(&key).await {
                let out = self.db.last_stdout(cached_run_id).await.ok().flatten();
                let _ = self
                    .db
                    .finish_step(flow_run_id, &step.step_id, "succeeded", out.as_deref())
                    .await;
                metrics::counter!("dokan_flow_steps_recalled_total").increment(1);
                metrics::counter!("dokan_flow_steps_finished_total", "status" => "succeeded").increment(1);
                return;
            }
            Some(key)
        } else {
            None
        };

        // attempts = retries + 1; a step may override the default (e.g. 0 retries for a
        // non-idempotent step that must never re-run).
        let max_attempts = step.retries.unwrap_or(1).max(0) as u32 + 1;
        for attempt in 1..=max_attempts {
            let run_id = match self.db.insert_run(step.script_id, &step_input, None).await {
                Ok(id) => id,
                Err(e) => {
                    let _ = self
                        .db
                        .finish_step(flow_run_id, &step.step_id, "failed", Some(&e.to_string()))
                        .await;
                    return;
                }
            };
            // Tag the produced run so future flows recall it.
            if let Some(key) = &cache_key {
                let _ = self.db.set_run_cache_key(run_id, key).await;
            }
            let _ = self.db.set_step_running(flow_run_id, &step.step_id, run_id).await;
            // Stateful monitors: feed this script's most-recent prior structured result into
            // the step input as `prev_result` (null on first run).
            let run_input = if script.feed_prev_result {
                let prev = self.db.last_result_for_script(script.id, run_id).await.ok().flatten();
                let mut v = step_input.clone();
                match v.as_object_mut() {
                    Some(obj) => { obj.insert("prev_result".into(), prev.unwrap_or(serde_json::Value::Null)); }
                    None => { v = serde_json::json!({ "input": step_input, "prev_result": prev }); }
                }
                v
            } else {
                step_input.clone()
            };
            // Drive the container to completion (this finishes the underlying run). Hold a
            // shared concurrency permit only while the container runs, so a wide map fan-out
            // is throttled to the same cap as the worker rather than spawning all at once.
            {
                let _permit = self.slots.acquire().await;
                self.exec
                    .run(
                        &self.db,
                        run_id,
                        &script.runtime,
                        &script.source,
                        &run_input,
                        None,
                        script.network,
                        script.mem_limit_mb,
                        script.cpu_limit,
                        // TODO(v0.2.x): run_flow files — flow-level input artifacts visible to every step.
                        None,
                        // Flow steps don't opt into output capture (no per-step capture_output yet).
                        false,
                    )
                    .await;
            }

            let status = self
                .db
                .run_status(run_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| "failed".into());
            if status == "succeeded" {
                let out = self.db.last_stdout(run_id).await.ok().flatten();
                let _ = self
                    .db
                    .finish_step(flow_run_id, &step.step_id, "succeeded", out.as_deref())
                    .await;
                metrics::counter!("dokan_flow_steps_finished_total", "status" => "succeeded").increment(1);
                return;
            }
            tracing::warn!(flow_run_id, step = %step.step_id, attempt, "step failed, maybe retry");
        }
        let _ = self
            .db
            .finish_step(flow_run_id, &step.step_id, "failed", Some("exhausted retries"))
            .await;
        metrics::counter!("dokan_flow_steps_finished_total", "status" => "failed").increment(1);
    }
}

/// Result of aggregating a map step's children.
#[derive(Debug)]
enum Agg {
    Succeeded(String),
    Failed,
}

/// Aggregate a map parent's children (`<parent>#<i>`). Returns `None` while any child is
/// still pending/running, `Failed` if any child failed, else `Succeeded` with a JSON array
/// of child outputs in index order. A skipped child contributes a null slot.
fn aggregate_children(steps: &[crate::db::FlowStep], parent: &str) -> Option<Agg> {
    let prefix = format!("{parent}#");
    let mut children: Vec<&crate::db::FlowStep> =
        steps.iter().filter(|s| s.step_id.starts_with(&prefix)).collect();
    if children.is_empty() {
        return None;
    }
    if children.iter().any(|c| matches!(c.status.as_str(), "pending" | "running" | "expanded")) {
        return None;
    }
    if children.iter().any(|c| c.status == "failed") {
        return Some(Agg::Failed);
    }
    // Order by the numeric index suffix so the aggregated array matches input order.
    children.sort_by_key(|c| c.step_id.rsplit('#').next().and_then(|n| n.parse::<u64>().ok()).unwrap_or(0));
    let arr: Vec<serde_json::Value> = children
        .iter()
        .map(|c| match (c.status.as_str(), &c.output) {
            ("succeeded", Some(o)) => serde_json::Value::String(o.clone()),
            _ => serde_json::Value::Null,
        })
        .collect();
    Some(Agg::Succeeded(serde_json::Value::Array(arr).to_string()))
}

/// Resolve a dotted ref against `{flow_input, deps}`. Roots are `flow_input` or `deps`.
/// Step outputs are stored as strings, so a string that parses as JSON is decoded at each
/// hop — letting `map: "deps.fetch"` see an array a step printed as JSON.
fn resolve_ref(r: &str, flow_input: &serde_json::Value, deps: &serde_json::Value) -> Option<serde_json::Value> {
    let mut parts = r.split('.');
    let mut cur = match parts.next()? {
        "flow_input" => flow_input.clone(),
        "deps" => deps.clone(),
        _ => return None,
    };
    for p in parts {
        if let serde_json::Value::String(s) = &cur
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                cur = v;
            }
        cur = cur.get(p).cloned()?;
    }
    if let serde_json::Value::String(s) = &cur
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            return Some(v);
        }
    Some(cur)
}

/// Evaluate a `when` gate `{ref, op, value}`. ops: eq, ne, contains, truthy, falsy.
fn eval_when(cond: &serde_json::Value, flow_input: &serde_json::Value, deps: &serde_json::Value) -> bool {
    let r = cond.get("ref").and_then(|v| v.as_str()).unwrap_or("");
    let op = cond.get("op").and_then(|v| v.as_str()).unwrap_or("truthy");
    let actual = resolve_ref(r, flow_input, deps).unwrap_or(serde_json::Value::Null);
    let expected = cond.get("value").cloned().unwrap_or(serde_json::Value::Null);
    match op {
        "eq" => coerce_str(&actual) == coerce_str(&expected),
        "ne" => coerce_str(&actual) != coerce_str(&expected),
        "contains" => coerce_str(&actual).contains(&coerce_str(&expected)),
        "falsy" => !is_truthy(&actual),
        _ => is_truthy(&actual), // "truthy" and any unknown op
    }
}

/// Compare-friendly string form: strings as-is, everything else via JSON repr.
fn coerce_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn is_truthy(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        serde_json::Value::String(s) => !s.is_empty() && s != "false" && s != "0",
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
    }
}

/// Build the `{dep_id: output}` object for a step's satisfied dependencies.
fn build_deps(depends_on: &[String], outputs: &HashMap<String, String>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for d in depends_on {
        if let Some(o) = outputs.get(d) {
            m.insert(d.clone(), serde_json::Value::String(o.clone()));
        }
    }
    serde_json::Value::Object(m)
}

/// Validate a flow spec: steps have unique ids, deps reference real steps, and the
/// DAG is acyclic. Returns an error string on failure.
pub fn validate_spec(spec: &serde_json::Value) -> Result<(), String> {
    let steps = spec
        .get("steps")
        .and_then(|s| s.as_array())
        .ok_or("spec.steps must be an array")?;
    if steps.is_empty() {
        return Err("spec.steps is empty".into());
    }
    let mut ids = Vec::new();
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    for st in steps {
        let id = st
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or("each step needs a string id")?
            .to_string();
        if ids.contains(&id) {
            return Err(format!("duplicate step id: {id}"));
        }
        // '#' is reserved for map-fan-out child ids (`<id>#<i>`).
        if id.contains('#') {
            return Err(format!("step id may not contain '#': {id}"));
        }
        if st.get("script_id").and_then(|v| v.as_i64()).is_none() {
            return Err(format!("step {id} missing script_id"));
        }
        if let Some(c) = st.get("compensate")
            && !c.is_i64() {
                return Err(format!("step {id} compensate must be a script_id (int)"));
            }
        if let Some(m) = st.get("map")
            && !m.is_string() {
                return Err(format!("step {id} map must be a string ref"));
            }
        if let Some(r) = st.get("retries")
            && r.as_u64().is_none() {
                return Err(format!("step {id} retries must be a non-negative int"));
            }
        if let Some(c) = st.get("cache")
            && !c.is_boolean() {
                return Err(format!("step {id} cache must be a boolean"));
            }
        if let Some(w) = st.get("when") {
            if w.get("ref").and_then(|v| v.as_str()).is_none() {
                return Err(format!("step {id} when needs a string ref"));
            }
            let op = w.get("op").and_then(|v| v.as_str()).unwrap_or("truthy");
            if !["eq", "ne", "contains", "truthy", "falsy"].contains(&op) {
                return Err(format!("step {id} when.op invalid: {op}"));
            }
        }
        let deps: Vec<String> = st
            .get("depends_on")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|d| d.as_str().map(String::from)).collect())
            .unwrap_or_default();
        edges.insert(id.clone(), deps);
        ids.push(id);
    }
    for (id, deps) in &edges {
        for d in deps {
            if !ids.contains(d) {
                return Err(format!("step {id} depends on unknown step {d}"));
            }
        }
    }
    if has_cycle(&edges) {
        return Err("flow spec has a cycle".into());
    }
    Ok(())
}

fn has_cycle(edges: &HashMap<String, Vec<String>>) -> bool {
    // DFS with three-color marking.
    let mut state: HashMap<&str, u8> = HashMap::new(); // 0=unseen,1=in-stack,2=done
    fn dfs<'a>(
        n: &'a str,
        edges: &'a HashMap<String, Vec<String>>,
        state: &mut HashMap<&'a str, u8>,
    ) -> bool {
        state.insert(n, 1);
        if let Some(deps) = edges.get(n) {
            for d in deps {
                match state.get(d.as_str()).copied().unwrap_or(0) {
                    1 => return true,
                    0
                        if dfs(d, edges, state) => {
                            return true;
                        }
                    _ => {}
                }
            }
        }
        state.insert(n, 2);
        false
    }
    for n in edges.keys() {
        if state.get(n.as_str()).copied().unwrap_or(0) == 0 && dfs(n, edges, &mut state) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::FlowStep;
    use serde_json::json;

    fn child(step_id: &str, status: &str, output: Option<&str>) -> FlowStep {
        FlowStep {
            step_id: step_id.to_string(),
            script_id: 1,
            input: json!({}),
            depends_on: vec![],
            status: status.to_string(),
            output: output.map(String::from),
            when_cond: None,
            map_ref: None,
            compensate: None,
            compensated: false,
            retries: None,
            finished_at: None,
            cache: false,
        }
    }

    // ---- aggregate_children: map fan-out completion + partial-failure (saga trigger) ----

    #[test]
    fn aggregate_none_while_a_child_is_pending() {
        let steps = vec![child("p#0", "succeeded", Some("a")), child("p#1", "running", None)];
        assert!(aggregate_children(&steps, "p").is_none(), "not terminal while a child runs");
    }

    #[test]
    fn aggregate_none_when_no_children() {
        let steps = vec![child("other#0", "succeeded", Some("x"))];
        assert!(aggregate_children(&steps, "p").is_none(), "no children for this parent");
    }

    #[test]
    fn aggregate_failed_if_any_child_failed() {
        // Partial failure: one of three children failed → the whole map parent fails,
        // which is what triggers saga compensation of upstream steps.
        let steps = vec![
            child("p#0", "succeeded", Some("a")),
            child("p#1", "failed", None),
            child("p#2", "succeeded", Some("c")),
        ];
        assert!(matches!(aggregate_children(&steps, "p"), Some(Agg::Failed)), "any child failed → Failed");
    }

    #[test]
    fn aggregate_succeeded_orders_outputs_by_index() {
        // Children deliberately out of insertion order; aggregation must sort by the #index.
        let steps = vec![
            child("p#2", "succeeded", Some("c")),
            child("p#0", "succeeded", Some("a")),
            child("p#1", "succeeded", Some("b")),
        ];
        match aggregate_children(&steps, "p") {
            Some(Agg::Succeeded(arr)) => assert_eq!(arr, json!(["a", "b", "c"]).to_string()),
            other => panic!("expected ordered Succeeded, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_skipped_child_is_null_slot() {
        let steps = vec![
            child("p#0", "succeeded", Some("a")),
            child("p#1", "skipped", None),
        ];
        match aggregate_children(&steps, "p") {
            Some(Agg::Succeeded(arr)) => assert_eq!(arr, json!(["a", null]).to_string()),
            other => panic!("expected Succeeded with null slot, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_does_not_confuse_a_prefixed_sibling_parent() {
        // "p" children must not match " p10"-style other parents: prefix is "p#".
        let steps = vec![child("p#0", "succeeded", Some("a")), child("p2#0", "failed", None)];
        match aggregate_children(&steps, "p") {
            Some(Agg::Succeeded(arr)) => assert_eq!(arr, json!(["a"]).to_string()),
            other => panic!("sibling parent leaked into aggregation: {other:?}"),
        }
    }

    // ---- eval_when: branch gate + skip decision ----

    fn deps(pairs: &[(&str, &str)]) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), json!(v));
        }
        serde_json::Value::Object(m)
    }

    #[test]
    fn when_eq_and_ne() {
        let d = deps(&[("a", "ok")]);
        assert!(eval_when(&json!({"ref":"deps.a","op":"eq","value":"ok"}), &json!({}), &d));
        assert!(!eval_when(&json!({"ref":"deps.a","op":"eq","value":"nope"}), &json!({}), &d));
        assert!(eval_when(&json!({"ref":"deps.a","op":"ne","value":"nope"}), &json!({}), &d));
    }

    #[test]
    fn when_contains_truthy_falsy() {
        let d = deps(&[("a", "flagged: spam")]);
        assert!(eval_when(&json!({"ref":"deps.a","op":"contains","value":"spam"}), &json!({}), &d));
        assert!(eval_when(&json!({"ref":"deps.a","op":"truthy"}), &json!({}), &d));
        assert!(!eval_when(&json!({"ref":"deps.missing","op":"truthy"}), &json!({}), &d));
        assert!(eval_when(&json!({"ref":"deps.missing","op":"falsy"}), &json!({}), &d));
    }

    #[test]
    fn when_unknown_op_defaults_to_truthy() {
        let d = deps(&[("a", "x")]);
        assert!(eval_when(&json!({"ref":"deps.a","op":"bogus"}), &json!({}), &d));
    }

    #[test]
    fn when_reads_flow_input_root() {
        assert!(eval_when(
            &json!({"ref":"flow_input.mode","op":"eq","value":"live"}),
            &json!({"mode":"live"}),
            &json!({})
        ));
    }

    // ---- resolve_ref: dep hop + JSON-string decode ----

    #[test]
    fn resolve_decodes_json_string_array_for_map() {
        // A step prints a JSON array as a string; map:"deps.emit" must see the array.
        let d = deps(&[("emit", "[1,2,3]")]);
        assert_eq!(resolve_ref("deps.emit", &json!({}), &d), Some(json!([1, 2, 3])));
    }

    #[test]
    fn resolve_nested_and_bad_root() {
        let d = json!({"obj": "{\"k\":\"v\"}"});
        assert_eq!(resolve_ref("deps.obj.k", &json!({}), &d), Some(json!("v")));
        assert_eq!(resolve_ref("nope.x", &json!({}), &d), None);
    }

    // ---- is_truthy edges ----

    #[test]
    fn is_truthy_edges() {
        assert!(!is_truthy(&json!(null)));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(0)));
        assert!(!is_truthy(&json!("")));
        assert!(!is_truthy(&json!("false")));
        assert!(!is_truthy(&json!("0")));
        assert!(!is_truthy(&json!([])));
        assert!(is_truthy(&json!("ok")));
        assert!(is_truthy(&json!(1)));
        assert!(is_truthy(&json!(["x"])));
    }

    // ---- validate_spec: saga/DAG spec gate ----

    fn spec(steps: serde_json::Value) -> serde_json::Value {
        json!({ "steps": steps })
    }

    #[test]
    fn validate_accepts_a_well_formed_saga() {
        let s = spec(json!([
            {"id":"a","script_id":1,"compensate":9},
            {"id":"b","script_id":2,"depends_on":["a"],"when":{"ref":"deps.a","op":"eq","value":"ok"}},
            {"id":"c","script_id":3,"depends_on":["a"],"map":"deps.a","retries":2,"cache":true},
        ]));
        assert!(validate_spec(&s).is_ok());
    }

    #[test]
    fn validate_rejects_cycle() {
        let s = spec(json!([
            {"id":"a","script_id":1,"depends_on":["b"]},
            {"id":"b","script_id":2,"depends_on":["a"]},
        ]));
        assert_eq!(validate_spec(&s), Err("flow spec has a cycle".into()));
    }

    #[test]
    fn validate_rejects_duplicate_id_and_hash_id() {
        let dup = spec(json!([{"id":"a","script_id":1},{"id":"a","script_id":2}]));
        assert!(validate_spec(&dup).unwrap_err().contains("duplicate"));
        let hash = spec(json!([{"id":"a#0","script_id":1}]));
        assert!(validate_spec(&hash).unwrap_err().contains("'#'"));
    }

    #[test]
    fn validate_rejects_unknown_dep_and_bad_field_types() {
        let dep = spec(json!([{"id":"a","script_id":1,"depends_on":["ghost"]}]));
        assert!(validate_spec(&dep).unwrap_err().contains("unknown step ghost"));
        let comp = spec(json!([{"id":"a","script_id":1,"compensate":"not-int"}]));
        assert!(validate_spec(&comp).unwrap_err().contains("compensate"));
        let map = spec(json!([{"id":"a","script_id":1,"map":123}]));
        assert!(validate_spec(&map).unwrap_err().contains("map must be a string"));
        let op = spec(json!([{"id":"a","script_id":1,"when":{"ref":"deps.x","op":"gt","value":1}}]));
        assert!(validate_spec(&op).unwrap_err().contains("when.op invalid"));
    }

    #[test]
    fn validate_rejects_empty_steps() {
        assert!(validate_spec(&spec(json!([]))).unwrap_err().contains("empty"));
    }

    #[test]
    fn map_fanout_cap_bounds_child_count() {
        assert!(fanout_within_cap(1));
        assert!(fanout_within_cap(MAX_MAP_FANOUT));
        assert!(!fanout_within_cap(MAX_MAP_FANOUT + 1), "one over the cap is refused");
    }
}
