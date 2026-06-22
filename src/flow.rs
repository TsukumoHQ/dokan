//! Flow engine (P2): drives a declarative DAG of steps. Each step is one container run.
//! Durability is at the STEP boundary — completed steps are checkpointed in Postgres
//! (`flow_steps.status`), so a crashed engine resumes the DAG where it left off. Inside a
//! step there is no magic: a step that dies is re-run, so steps must be idempotent.
//! This is the deliberate escape from the Temporal replay trap (PRD §6).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::db::Db;
use crate::exec::{runtime_spec, Executor};

const STEP_MAX_ATTEMPTS: u32 = 2;
const IDLE_POLL: Duration = Duration::from_millis(400);
/// Flow-run lease: a driver heartbeats between step batches, but a single in-flight step
/// can run silent up to the job timeout. Set above that bound so a healthy long step is
/// never mistaken for a dead engine. Tunable; 2× the job timeout gives comfortable margin.
const FLOW_LEASE_SECS: f64 = (crate::exec::DEFAULT_TIMEOUT_SECS * 2) as f64;

#[derive(Clone)]
pub struct FlowEngine {
    db: Db,
    exec: Arc<Executor>,
}

impl FlowEngine {
    pub fn new(db: Db, exec: Arc<Executor>) -> Self {
        Self { db, exec }
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

    /// Drive one flow_run to terminal status. Runs all currently-ready steps in parallel,
    /// checkpoints each, and repeats until the DAG completes or a step fails.
    async fn drive(&self, flow_run_id: i64, input: serde_json::Value) -> anyhow::Result<()> {
        loop {
            // Heartbeat: keeps this flow's lease fresh between step batches so the reaper
            // never reclaims a live driver. A single step can run up to the job timeout
            // with no beat, hence FLOW_LEASE_SECS sits above that bound.
            let _ = self.db.touch_flow_run(flow_run_id).await;
            let steps = self.db.flow_steps(flow_run_id).await?;

            if steps.iter().any(|s| s.status == "failed") {
                self.db.finish_flow_run(flow_run_id, "failed").await?;
                metrics::counter!("dokan_flow_runs_finished_total", "status" => "failed").increment(1);
                return Ok(());
            }
            if steps.iter().all(|s| s.status == "succeeded") {
                self.db.finish_flow_run(flow_run_id, "succeeded").await?;
                metrics::counter!("dokan_flow_runs_finished_total", "status" => "succeeded").increment(1);
                return Ok(());
            }

            // Map of completed step outputs, for dependents.
            let outputs: HashMap<String, String> = steps
                .iter()
                .filter(|s| s.status == "succeeded")
                .map(|s| (s.step_id.clone(), s.output.clone().unwrap_or_default()))
                .collect();

            // Ready = pending with all deps succeeded.
            let ready: Vec<_> = steps
                .iter()
                .filter(|s| s.status == "pending")
                .filter(|s| s.depends_on.iter().all(|d| outputs.contains_key(d)))
                .cloned()
                .collect();

            if ready.is_empty() {
                // Nothing ready and not all done → either a running step elsewhere or a
                // stuck/unsatisfiable dep. Poll briefly; if truly stuck the next pass with
                // no progress will be caught by the failed/succeeded checks above.
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            }

            let mut handles = Vec::new();
            for step in ready {
                let me = self.clone();
                let input = input.clone();
                let deps = build_deps(&step.depends_on, &outputs);
                handles.push(tokio::spawn(async move {
                    me.run_step(flow_run_id, step, input, deps).await
                }));
            }
            for h in handles {
                let _ = h.await;
            }
        }
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

        for attempt in 1..=STEP_MAX_ATTEMPTS {
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
            let _ = self.db.set_step_running(flow_run_id, &step.step_id, run_id).await;
            // Drive the container to completion (this finishes the underlying run).
            self.exec
                .run(&self.db, run_id, &script.runtime, &script.source, &step_input, None)
                .await;

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
        if st.get("script_id").and_then(|v| v.as_i64()).is_none() {
            return Err(format!("step {id} missing script_id"));
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
                    0 => {
                        if dfs(d, edges, state) {
                            return true;
                        }
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
