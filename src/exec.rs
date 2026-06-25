//! Docker execution: one job = one clean container, then discard.
//! Jobs run by `docker exec` into a warm pooled container; code is trusted, so raw
//! containers suffice (no gVisor/Firecracker). Resource caps live on the container.

use anyhow::{anyhow, Result};
use base64::Engine;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::Docker;
use futures_util::Stream;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db::Db;
use crate::pool::WarmPool;

/// Hard wall-clock ceiling per job. A live worker kills + finalizes at this bound, so a
/// run still `running` well past it can only mean the worker died — see the lease reaper.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

#[derive(Clone)]
pub struct Executor {
    docker: Docker,
    pool: Arc<WarmPool>,
    /// run_id -> container_id for in-flight jobs, so `cancel` can kill the right one.
    active: Arc<Mutex<HashMap<i64, String>>>,
    /// Optional relay endpoint: job results POSTed here for the mesh (PRD §5).
    relay: Option<String>,
    http: reqwest::Client,
    signer: crate::receipt::Signer,
}

/// Maps a declared runtime to its base image and in-container interpreter.
pub fn runtime_spec(runtime: &str) -> Option<(&'static str, &'static str)> {
    match runtime {
        "python" | "python3" | "python3.12" => Some(("python:3.12-slim", "python")),
        "node" | "nodejs" | "javascript" => Some(("node:22-slim", "node")),
        "bash" | "sh" | "shell" => Some(("alpine:3.20", "sh")),
        _ => None,
    }
}

impl Executor {
    pub fn connect(
        warm_idle: usize,
        mem_bytes: i64,
        nano_cpus: i64,
        relay: Option<String>,
    ) -> Result<Self> {
        // Honor DOCKER_HOST (Colima/Docker Desktop sockets live outside /var/run).
        let docker = if std::env::var("DOCKER_HOST").is_ok() {
            Docker::connect_with_defaults()?
        } else {
            Docker::connect_with_local_defaults()?
        };
        let pool = WarmPool::new(docker.clone(), warm_idle, mem_bytes, nano_cpus);
        Ok(Self {
            docker,
            pool,
            active: Arc::new(Mutex::new(HashMap::new())),
            relay,
            http: reqwest::Client::new(),
            signer: crate::receipt::Signer::from_env(),
        })
    }

    /// Record terminal metrics and POST the outcome to the relay (mesh egress). The
    /// structured `result` (if the job emitted one) rides along so a monitor's finding
    /// reaches the agent event-driven — no polling.
    async fn finalize(
        &self,
        run_id: i64,
        status: &str,
        exit_code: Option<i32>,
        result: Option<serde_json::Value>,
    ) {
        metrics::counter!("dokan_runs_finished_total", "status" => status.to_string())
            .increment(1);
        if let Some(url) = &self.relay {
            let body = serde_json::json!({
                "run_id": run_id, "status": status, "exit_code": exit_code, "result": result,
            });
            let _ = self.http.post(url).json(&body).send().await;
        }
    }

    /// Arm the warm pool's background filler — only the executor process should call this.
    pub fn arm_pool(&self) {
        self.pool.arm();
    }

    /// Per-job (mem_bytes, nano_cpus) caps — reported by whoami.
    pub fn limits(&self) -> (i64, i64) {
        self.pool.limits()
    }

    /// Retune warm-pool depth per image (autoscaler). Returns the value set.
    pub fn set_warm_target(&self, n: usize) -> usize {
        self.pool.set_target_idle(n)
    }

    /// Resolved content digest for a runtime's image (for the cache key / receipt). None
    /// until the pool has created at least one container of that image.
    pub fn image_digest(&self, runtime: &str) -> Option<String> {
        let (image, _) = runtime_spec(runtime)?;
        self.pool.digest(image)
    }

    /// Pre-pull + pre-warm the known runtime images at startup so the first job of each
    /// runtime doesn't eat the image-pull / cold-create tail. (Perf #3.)
    pub fn prewarm(&self) {
        self.pool.prewarm(&["python:3.12-slim", "node:22-slim", "alpine:3.20"]);
    }

    /// Eagerly resolve image digests at boot so the run-cache key is stable from the first
    /// run (T1 — a lazily-resolved digest would shift the key between calls).
    pub async fn resolve_digests(&self) {
        self.pool
            .resolve_all(&["python:3.12-slim", "node:22-slim", "alpine:3.20"])
            .await;
    }

    /// Reclaim warm containers orphaned by a crashed dokan. Run at executor startup.
    pub async fn sweep_orphans(&self) -> usize {
        self.pool.sweep_orphans().await
    }

    /// Build the signed receipt for a finished run.
    async fn build_receipt(
        &self,
        db: &Db,
        image: &str,
        source: &str,
        input: &serde_json::Value,
        result: &Option<serde_json::Value>,
        exit_code: i64,
        network: bool,
    ) -> serde_json::Value {
        use crate::receipt::sha256_hex;
        let digest = self.pool.digest(image).unwrap_or_else(|| image.to_string());
        let secrets_gen = db.secrets_generation().await.unwrap_or(0);
        let output_hash = sha256_hex(
            result
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
        // The signed payload is a canonical, order-stable string of the binding.
        let payload = format!(
            "v1|{digest}|{}|{}|{secrets_gen}|{output_hash}|{exit_code}|{network}",
            sha256_hex(source.as_bytes()),
            sha256_hex(input.to_string().as_bytes()),
        );
        let sig = self.signer.sign(&payload);
        serde_json::json!({
            "v": 1,
            "image_digest": digest,
            "source_sha256": sha256_hex(source.as_bytes()),
            "input_sha256": sha256_hex(input.to_string().as_bytes()),
            "secrets_generation": secrets_gen,
            "output_sha256": output_hash,
            "exit": exit_code,
            "network": network,
            "deterministic": !network,
            "alg": "hmac-sha256",
            "sig": sig,
        })
    }

    /// Kill a running job's container (best-effort).
    pub async fn cancel(&self, run_id: i64) {
        let cid = self.active.lock().unwrap().get(&run_id).cloned();
        if let Some(cid) = cid {
            let _ = self.docker.kill_container(&cid, None).await;
        }
    }

    /// Full lifecycle: acquire warm container → exec → stream logs → exit code → discard.
    /// Drives to completion; the worker spawns this. Failures are recorded against the run.
    pub async fn run(
        &self,
        db: &Db,
        run_id: i64,
        runtime: &str,
        source: &str,
        input: &serde_json::Value,
        agent_id: Option<&str>,
        network: bool,
        mem_limit_mb: Option<i64>,
        cpu_limit: Option<f64>,
    ) {
        let t0 = std::time::Instant::now();
        metrics::gauge!("dokan_runs_active").increment(1.0);
        let mut terminal = "succeeded";
        if let Err(e) = self
            .run_inner(db, run_id, runtime, source, input, agent_id, network, mem_limit_mb, cpu_limit)
            .await
        {
            // Err here = dokan-side failure (could not execute) — distinct from a script
            // that ran and exited nonzero, which finishes inside exec_job.
            metrics::counter!("dokan_run_internal_errors_total").increment(1);
            let msg = e.to_string();
            let seq = db.max_log_seq(run_id).await.unwrap_or(0) + 1;
            let _ = db.append_log(run_id, seq, "stderr", &format!("dokan: {msg}")).await;
            let _ = db.finish_run(run_id, "failed", None, Some(&msg)).await;
            self.finalize(run_id, "failed", None, None).await;
            terminal = "failed";
        }
        self.active.lock().unwrap().remove(&run_id);
        metrics::gauge!("dokan_runs_active").decrement(1.0);
        // Terminal status for the happy path lives in the DB (exec_job set it); read it so
        // the duration series is labeled correctly even on script-nonzero exits.
        let status = db
            .run_status(run_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| terminal.to_string());
        metrics::histogram!("dokan_run_duration_seconds",
            "runtime" => runtime.to_string(), "status" => status)
            .record(t0.elapsed().as_secs_f64());
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inner(
        &self,
        db: &Db,
        run_id: i64,
        runtime: &str,
        source: &str,
        input: &serde_json::Value,
        agent_id: Option<&str>,
        network: bool,
        mem_limit_mb: Option<i64>,
        cpu_limit: Option<f64>,
    ) -> Result<()> {
        let (image, interp) =
            runtime_spec(runtime).ok_or_else(|| anyhow!("unknown runtime: {runtime}"))?;

        // No override → the common path is 100% unchanged: a warm container (network), or an
        // isolated network-disabled one (deterministic). Any per-script cap override skips the
        // global-only warm pool and cold-creates a fresh one-off container with the override
        // caps (a missing dimension falls back to the executor's global default).
        let cid = if mem_limit_mb.is_none() && cpu_limit.is_none() {
            if network {
                self.pool.acquire(image).await?
            } else {
                self.pool.acquire_isolated(image).await?
            }
        } else {
            let (def_mem, def_cpu) = self.pool.limits();
            let mem_bytes = mem_limit_mb
                .map(|mb| mb.saturating_mul(1024 * 1024))
                .unwrap_or(def_mem);
            let nano_cpus = cpu_limit
                .map(|c| (c * 1_000_000_000.0) as i64)
                .unwrap_or(def_cpu);
            self.pool
                .acquire_with_caps(image, mem_bytes, nano_cpus, /*isolated=*/ !network)
                .await?
        };
        self.active.lock().unwrap().insert(run_id, cid.clone());

        let result = self
            .exec_job(db, run_id, &cid, image, interp, source, input, agent_id, network)
            .await;

        // Run clean, discard — never reuse a dirty container.
        self.pool.discard(&cid).await;
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_job(
        &self,
        db: &Db,
        run_id: i64,
        cid: &str,
        image: &str,
        interp: &str,
        source: &str,
        input: &serde_json::Value,
        agent_id: Option<&str>,
        network: bool,
    ) -> Result<()> {
        let src_b64 = base64::engine::general_purpose::STANDARD.encode(source);
        let bootstrap = format!(
            "printf '%s' \"$DOKAN_SRC\" | base64 -d > /tmp/dokan_script && exec {interp} /tmp/dokan_script"
        );

        // Inject configured secrets as env vars (best-effort; never logged).
        let mut env = vec![
            format!("DOKAN_SRC={src_b64}"),
            format!("DOKAN_INPUT={input}"),
            format!("DOKAN_RUN_ID={run_id}"),
        ];
        if let Ok(secrets) = db.all_secrets_for(agent_id).await {
            for (k, v) in secrets {
                env.push(format!("{k}={v}"));
            }
        }

        let exec = self
            .docker
            .create_exec(
                cid,
                CreateExecOptions {
                    cmd: Some(vec!["sh".into(), "-c".into(), bootstrap]),
                    env: Some(env),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await
            .map_err(stale_container)?;
        let exec_id = exec.id;

        let started = self
            .docker
            .start_exec(&exec_id, None::<StartExecOptions>)
            .await
            .map_err(stale_container)?;

        let output = match started {
            StartExecResults::Attached { output, .. } => output,
            StartExecResults::Detached => return Err(anyhow!("exec detached unexpectedly")),
        };

        let last_seq = match tokio::time::timeout(
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            pump_logs(db, run_id, output),
        )
        .await
        {
            Ok(r) => r?,
            Err(_) => {
                metrics::counter!("dokan_run_timeouts_total").increment(1);
                let _ = self.docker.kill_container(cid, None).await;
                let s = db.max_log_seq(run_id).await.unwrap_or(0) + 1;
                db.append_log(run_id, s, "stderr", "dokan: timeout, container killed")
                    .await?;
                db.finish_run(run_id, "failed", None, Some("timeout")).await?;
                self.finalize(run_id, "failed", None, None).await;
                return Ok(());
            }
        };
        let (last_seq, result_raw) = last_seq;
        let _ = last_seq;
        // Parse the structured result (best-effort: non-JSON is wrapped as a string so the
        // channel still works for a plain status token).
        let result = result_raw.map(|s| {
            serde_json::from_str::<serde_json::Value>(&s).unwrap_or(serde_json::Value::String(s))
        });

        let exit_code = match self.docker.inspect_exec(&exec_id).await {
            Ok(inspect) => inspect.exit_code.unwrap_or(0),
            // Container vanished before we could read the exit code (teardown race / host
            // resource pressure under many concurrent jobs). Don't dump a raw Docker 404
            // into the run's stderr tail — record a clean line and finish as a NULL-exit
            // failure, which the worker treats as a transient infra error worth retrying.
            Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }) => {
                let s = db.max_log_seq(run_id).await.unwrap_or(0) + 1;
                db.append_log(run_id, s, "stderr", "dokan: container vanished before exit code; marking failed")
                    .await?;
                db.finish_run(run_id, "failed", None, Some("container vanished")).await?;
                self.finalize(run_id, "failed", None, result).await;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let status = if exit_code == 0 { "succeeded" } else { "failed" };
        db.finish_run(run_id, status, Some(exit_code as i32), None)
            .await?;
        // Signed reproducibility receipt: binds (image digest, source, input, secrets gen) to
        // (output, exit). Sound proof for network=false runs; advisory for networked ones.
        let receipt = self
            .build_receipt(db, image, source, input, &result, exit_code, network)
            .await;
        let _ = db.set_run_receipt(run_id, &receipt).await;
        if let Some(r) = &result {
            let _ = db.set_run_result(run_id, r).await;
            // Reactive composition: fire agent-defined triggers whose predicate the result
            // matches, enqueuing their target scripts. No external orchestrator.
            match db.fire_triggers(run_id, r).await {
                Ok(fired) if !fired.is_empty() => {
                    metrics::counter!("dokan_triggers_fired_total").increment(fired.len() as u64);
                    tracing::info!(run_id, fired = ?fired, "result matched triggers");
                }
                Err(e) => tracing::error!(run_id, "fire_triggers: {e}"),
                _ => {}
            }
        }
        self.finalize(run_id, status, Some(exit_code as i32), result).await;
        Ok(())
    }
}

/// Map a "container vanished" (404) Docker error into a clean, retryable infra error: the
/// acquired warm container was torn down (multi-daemon / teardown race) before we could
/// exec into it. The run then finishes with a NULL exit code, so the worker retries it onto
/// a fresh container instead of dumping a raw Docker 404 into the job's stderr tail.
fn stale_container(e: bollard::errors::Error) -> anyhow::Error {
    match e {
        bollard::errors::Error::DockerResponseServerError { status_code: 404, .. } => {
            anyhow!("warm container unavailable (stale); retrying on a fresh one")
        }
        other => other.into(),
    }
}

/// A stdout line of the form `::dokan:result:: {json}` is the job's structured result —
/// captured (last one wins) rather than logged, so monitors return findings without the
/// caller parsing stdout, and the relay egress can carry it.
const RESULT_MARKER: &str = "::dokan:result::";

/// A stdout line of the form `::dokan:progress:: <text>` is a transient status update —
/// written to the run's `progress` field (latest wins, live) rather than logged, so the
/// operator sees a long job's current step ("meeting 3/6") without paging the whole log.
const PROGRESS_MARKER: &str = "::dokan:progress::";

/// Stream container output line-by-line into the DB. Returns (last seq written, captured
/// structured result if the job emitted a RESULT_MARKER line).
async fn pump_logs(
    db: &Db,
    run_id: i64,
    mut stream: impl Stream<Item = Result<LogOutput, bollard::errors::Error>> + Unpin,
) -> Result<(i64, Option<String>)> {
    let mut seq = db.max_log_seq(run_id).await.unwrap_or(0);
    let mut buf_out = String::new();
    let mut buf_err = String::new();
    let mut result: Option<String> = None;
    // Accumulate lines and write them in batches (one INSERT per chunk, not per line).
    let mut batch: Vec<(i64, &'static str, String)> = Vec::new();

    async fn flush(
        db: &Db,
        run_id: i64,
        batch: &mut Vec<(i64, &'static str, String)>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let rows: Vec<(i64, &str, &str)> =
            batch.iter().map(|(s, st, l)| (*s, *st, l.as_str())).collect();
        db.append_logs_batch(run_id, &rows).await?;
        batch.clear();
        Ok(())
    }

    while let Some(item) = stream.next().await {
        let out = match item {
            Ok(o) => o,
            // Container torn down mid-stream (warm-pool discard / teardown race): Docker
            // answers the in-flight read with 404 "no such container". The job already
            // produced its output and exit code, so this is an expected end-of-stream, not
            // a failure — stop reading cleanly instead of surfacing a scary `dokan:` line.
            Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }) => {
                break
            }
            Err(e) => return Err(anyhow!("log stream: {e}")),
        };
        let (stream_name, bytes): (&'static str, _) = match out {
            LogOutput::StdOut { message } => ("stdout", message),
            LogOutput::StdErr { message } => ("stderr", message),
            LogOutput::Console { message } => ("stdout", message),
            LogOutput::StdIn { message } => ("stdout", message),
        };
        let text = String::from_utf8_lossy(&bytes);
        let buf = if stream_name == "stderr" { &mut buf_err } else { &mut buf_out };
        buf.push_str(&text);
        while let Some(nl) = buf.find('\n') {
            let line: String = buf.drain(..=nl).collect();
            let line = line.trim_end_matches('\n').trim_end_matches('\r');
            if stream_name == "stdout" {
                if let Some(rest) = line.trim_start().strip_prefix(RESULT_MARKER) {
                    result = Some(rest.trim().to_string()); // control line: result, not a log
                    continue;
                }
                if let Some(rest) = line.trim_start().strip_prefix(PROGRESS_MARKER) {
                    // Live status, latest-wins; not a log line. Best-effort — a failed
                    // progress write must never break the run's log pump.
                    let _ = db.update_run_progress(run_id, rest.trim()).await;
                    continue;
                }
            }
            seq += 1;
            batch.push((seq, stream_name, line.to_string()));
            metrics::counter!("dokan_log_lines_total", "stream" => stream_name).increment(1);
            if batch.len() >= 256 {
                flush(db, run_id, &mut batch).await?;
            }
        }
        // Flush at chunk boundaries so the live tail stays fresh.
        flush(db, run_id, &mut batch).await?;
    }
    for (stream_name, buf) in [("stdout", &buf_out), ("stderr", &buf_err)] {
        if buf.is_empty() {
            continue;
        }
        if stream_name == "stdout" {
            if let Some(rest) = buf.trim_start().strip_prefix(RESULT_MARKER) {
                result = Some(rest.trim().to_string());
                continue;
            }
            if let Some(rest) = buf.trim_start().strip_prefix(PROGRESS_MARKER) {
                let _ = db.update_run_progress(run_id, rest.trim()).await;
                continue;
            }
        }
        seq += 1;
        batch.push((seq, stream_name, buf.clone()));
        metrics::counter!("dokan_log_lines_total", "stream" => stream_name).increment(1);
    }
    flush(db, run_id, &mut batch).await?;
    Ok((seq, result))
}
