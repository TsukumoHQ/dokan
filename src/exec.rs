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

/// Non-root uid:gid the JOB runs as (defense-in-depth: a job needs no privilege — it reads
/// /input, writes /tmp (tmpfs) + /output (a 0777 bind), and hits the network if allowed).
/// `65534:65534` = nobody:nogroup, present numerically in every image without name lookup.
/// Combined with cap_drop ALL + no-new-privileges + a read-only rootfs, the blast radius of
/// untrusted code is minimized.
const RUN_USER: &str = "65534:65534";

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

    /// Build the tamper-evident (HMAC) receipt for a finished run.
    #[allow(clippy::too_many_arguments)]
    async fn build_receipt(
        &self,
        db: &Db,
        run_id: i64,
        image: &str,
        source: &str,
        input: &serde_json::Value,
        result: &Option<serde_json::Value>,
        exit_code: i64,
        network: bool,
        input_blobs: Option<&serde_json::Value>,
        output_blobs: Option<&serde_json::Value>,
    ) -> serde_json::Value {
        use crate::receipt::{dsse_pae, sha256_hex, DSSE_PAYLOAD_TYPE, PREDICATE_TYPE};
        let digest = self.pool.digest(image).unwrap_or_else(|| image.to_string());
        let secrets_gen = db.secrets_generation().await.unwrap_or(0);
        let source_sha = sha256_hex(source.as_bytes());
        let input_sha = sha256_hex(input.to_string().as_bytes());
        let output_hash = sha256_hex(
            result
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
        // Content-addressed inputs: a canonical, order-stable "name:sha,name:sha" string of
        // the run's declared input files, so the receipt proves the output is a function of
        // (source, input, image, secrets, input-blobs) — portable to any executor that can
        // fetch those blobs by handle.
        let blobs_canon = canonical_input_blobs(input_blobs);
        // Content-addressed OUTPUTS: the same canonical "name:sha,name:sha" rendering of the
        // files the job wrote to /output, folded into the binding so the captured output set is
        // tamper-evident too (symmetric with input blobs). None/empty → "".
        let output_blobs_canon = canonical_input_blobs(output_blobs);
        // The HMAC'd payload is a canonical, order-stable string of the binding (key-holder
        // tamper-evidence — back-compat).
        let payload = format!(
            "v1|{digest}|{source_sha}|{input_sha}|{secrets_gen}|{output_hash}|{exit_code}|{network}|{blobs_canon}|{output_blobs_canon}",
        );
        let sig = self.signer.sign(&payload);

        // in-toto Statement (v1): the SAME binding, expressed as a portable attestation whose
        // subject is the run's output. hermetic = !network is the signed SLSA-L4-style claim.
        // Subjects: the result hash plus every captured /output blob (content-addressed).
        let mut subject = vec![serde_json::json!({
            "name": "result", "digest": { "sha256": output_hash }
        })];
        if let Some(serde_json::Value::Object(ob)) = output_blobs {
            for (name, sha) in ob {
                if let Some(s) = sha.as_str() {
                    subject.push(serde_json::json!({ "name": name, "digest": { "sha256": s } }));
                }
            }
        }
        let statement = serde_json::json!({
            "_type": "https://in-toto.io/Statement/v1",
            "subject": subject,
            "predicateType": PREDICATE_TYPE,
            "predicate": {
                "builder": { "id": "dokan", "version": env!("CARGO_PKG_VERSION") },
                "invocation": { "invocationId": run_id.to_string(), "image": image },
                "image_digest": digest,
                "source_sha256": source_sha,
                "input_sha256": input_sha,
                "secrets_generation": secrets_gen,
                "output_sha256": output_hash,
                "exit": exit_code,
                "network": network,
                "hermetic": !network,
                "input_blobs": input_blobs.cloned().unwrap_or(serde_json::Value::Null),
                "output_blobs": output_blobs.cloned().unwrap_or(serde_json::Value::Null),
            }
        });
        // DSSE-wrap the statement and Ed25519-sign its pre-authentication encoding. A third
        // party verifies with the PUBLIC key alone (no shared secret) — the real public-verify.
        let payload_bytes = serde_json::to_vec(&statement).unwrap_or_default();
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload_bytes);
        let ed_sig = self.signer.ed_sign(&dsse_pae(DSSE_PAYLOAD_TYPE, &payload_bytes));
        let keyid = self.signer.ed_keyid();
        let dsse = serde_json::json!({
            "payloadType": DSSE_PAYLOAD_TYPE,
            "payload": payload_b64,
            "signatures": [ { "keyid": keyid, "sig": ed_sig } ],
        });

        serde_json::json!({
            "v": 1,
            "image_digest": digest,
            "source_sha256": source_sha,
            "input_sha256": input_sha,
            "secrets_generation": secrets_gen,
            "output_sha256": output_hash,
            "exit": exit_code,
            "network": network,
            "deterministic": !network,
            "hermetic": !network,
            "input_blobs": input_blobs.cloned().unwrap_or(serde_json::Value::Null),
            "output_blobs": output_blobs.cloned().unwrap_or(serde_json::Value::Null),
            "alg": "hmac-sha256",
            "sig": sig,
            "statement": statement,
            "dsse": dsse,
            "ed25519": { "keyid": keyid, "public_key": self.signer.ed_public_b64() },
        })
    }

    /// Recompute + check a receipt's HMAC binding with the daemon's key (the key-holder
    /// tamper-evidence check). Complements the key-free Ed25519/DSSE verification.
    pub fn verify_receipt_hmac(&self, receipt: &serde_json::Value) -> bool {
        self.signer.verify_hmac(receipt)
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
    #[allow(clippy::too_many_arguments)]
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
        input_blobs: Option<&serde_json::Value>,
        capture_output: bool,
    ) {
        let t0 = std::time::Instant::now();
        metrics::gauge!("dokan_runs_active").increment(1.0);
        let mut terminal = "succeeded";
        if let Err(e) = self
            .run_inner(db, run_id, runtime, source, input, agent_id, network, mem_limit_mb, cpu_limit, input_blobs, capture_output)
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
        input_blobs: Option<&serde_json::Value>,
        capture_output: bool,
    ) -> Result<()> {
        let (image, interp) =
            runtime_spec(runtime).ok_or_else(|| anyhow!("unknown runtime: {runtime}"))?;

        // Run artifacts: if the run carries input files, materialize their content-addressed
        // bytes into a per-run, dokan-owned host dir (~/.dokan/runs/<id>/input/) and bind it
        // read-only at /input. A run WITH files (like one with cap overrides) must bypass the
        // warm pool — a warm container has no /input mount — so it goes through a one-off
        // create below. Empty/absent map → no mount, the common path is untouched.
        let input_dir = materialize_input(db, run_id, input_blobs).await?;
        // Output artifacts (opt-in): when the run requests capture_output, create a writable
        // per-run host dir (~/.dokan/runs/<id>/output/) bound at /output. Like input files, this
        // forces the one-off container path — the warm pool has no /output mount. Default
        // (false) leaves the warm path 100% unchanged.
        let output_dir = if capture_output {
            Some(prepare_output_dir(run_id).await?)
        } else {
            None
        };
        let has_artifacts = input_dir.is_some() || output_dir.is_some();

        // No override AND no artifacts → the common path is 100% unchanged: a warm container
        // (network), or an isolated network-disabled one (deterministic). Otherwise skip the
        // global-only warm pool and cold-create a fresh one-off container with the override
        // caps and/or the /input + /output binds (a missing cap dimension falls back to the
        // global default).
        let cid = if mem_limit_mb.is_none() && cpu_limit.is_none() && !has_artifacts {
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
                .acquire_with_caps(image, mem_bytes, nano_cpus, /*isolated=*/ !network, input_dir.as_deref(), output_dir.as_deref())
                .await?
        };
        self.active.lock().unwrap().insert(run_id, cid.clone());

        // The job runs, then (if capture_output) dokan reads /output's host dir directly — a
        // plain bind dir, readable without a docker archive call — and folds the captured map
        // into the receipt before the dir is cleaned up below.
        let result = self
            .exec_job(db, run_id, &cid, image, interp, source, input, agent_id, network, input_blobs, output_dir.as_deref())
            .await;

        // Run clean, discard — never reuse a dirty container.
        self.pool.discard(&cid).await;
        // Best-effort cleanup of the per-run materialization dir (both /input and /output live
        // under ~/.dokan/runs/<id>/). The bytes live durably in the CAS `blobs` table; this is
        // just the ephemeral on-host copy. Either dir shares the same run-root parent.
        if let Some(dir) = input_dir.as_deref().or(output_dir.as_deref())
            && let Some(run_root) = std::path::Path::new(dir).parent() {
                let _ = tokio::fs::remove_dir_all(run_root).await;
            }
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
        input_blobs: Option<&serde_json::Value>,
        output_dir: Option<&str>,
    ) -> Result<()> {
        let src_b64 = base64::engine::general_purpose::STANDARD.encode(source);
        let bootstrap = format!(
            "printf '%s' \"$DOKAN_SRC\" | base64 -d > /tmp/dokan_script && exec {interp} /tmp/dokan_script"
        );

        // Inject configured secrets as env vars. Their VALUES are also collected so the log
        // pump can redact any that leak into the job's stdout/stderr (GAP-2 leak-safety: a job
        // that echoes $SECRET, or a stack trace that prints it, must not persist it in the run
        // log). Short values (<8 chars) are skipped — too collision-prone to mask safely.
        let mut env = vec![
            format!("DOKAN_SRC={src_b64}"),
            format!("DOKAN_INPUT={input}"),
            format!("DOKAN_RUN_ID={run_id}"),
            // The job runs as a non-root uid on a read-only rootfs; point HOME at the /tmp
            // tmpfs so runtime tools that write to ~ (pip ~/.cache, npm ~/.npm) don't hit a
            // non-writable or nonexistent home. /tmp is the one writable, job-private surface.
            "HOME=/tmp".to_string(),
        ];
        let mut secret_vals: Vec<String> = Vec::new();
        if let Ok(secrets) = db.all_secrets_for(agent_id).await {
            for (k, v) in secrets {
                if v.len() >= 8 {
                    secret_vals.push(v.clone());
                }
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
                    // Drop to a non-root uid for the job itself (the idle `sleep` keeps the
                    // image default; only the untrusted job is de-privileged).
                    user: Some(RUN_USER.to_string()),
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
            pump_logs(db, run_id, output, &secret_vals),
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
        // Output artifacts: capture whatever the job wrote to /output (the writable bind dir) as
        // content-addressed blobs and record the { "<relative-name>": "<sha>" } map on the run.
        // Done BEFORE build_receipt so the output set is folded into the receipt's HMAC. The host
        // bind dir is readable directly (no docker archive needed). Empty/no /output → None.
        let output_blobs = match output_dir {
            Some(dir) => match capture_output(db, dir).await {
                Ok(map) => map,
                Err(e) => {
                    // Capture failure is non-fatal to the run (the script already succeeded) —
                    // log and proceed with no output_blobs rather than fail a good run.
                    tracing::error!(run_id, "capturing /output failed: {e}");
                    None
                }
            },
            None => None,
        };
        if let Some(ob) = &output_blobs {
            let _ = db.set_run_output_blobs(run_id, ob).await;
        }
        // Tamper-evident reproducibility receipt: binds (image digest, source, input, secrets gen,
        // input/output blobs) to (output, exit) under a keyed HMAC. Sound check for network=false
        // runs; advisory for networked ones.
        let receipt = self
            .build_receipt(db, run_id, image, source, input, &result, exit_code, network, input_blobs, output_blobs.as_ref())
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

/// Canonical, order-stable "name:sha,name:sha" rendering of a run's input-blob map — folded
/// into the receipt's HMAC'd payload. Same canonicalization the cache key uses, so the two
/// agree on what set of files a run declared. None/empty → "".
fn canonical_input_blobs(input_blobs: Option<&serde_json::Value>) -> String {
    crate::mcp::canonical_input_blobs(input_blobs)
}

/// Materialize a run's content-addressed input files into a per-run, dokan-owned host dir
/// (`$HOME/.dokan/runs/<run_id>/input/`) and return that dir for a read-only `/input` bind.
/// The bytes live durably in the CAS `blobs` table; this is the ephemeral on-host copy the
/// container reads. None when the run declares no files (the common, no-mount path).
async fn materialize_input(
    db: &Db,
    run_id: i64,
    input_blobs: Option<&serde_json::Value>,
) -> Result<Option<String>> {
    let Some(map) = input_blobs.and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    if map.is_empty() {
        return Ok(None);
    }
    let home = std::env::var("HOME").map_err(|_| anyhow!("HOME unset; cannot materialize /input"))?;
    let dir = format!("{home}/.dokan/runs/{run_id}/input");
    tokio::fs::create_dir_all(&dir).await?;
    for (name, sha_v) in map {
        // The dest name lands as /input/<name>; keep it a plain filename (no traversal).
        if name.is_empty() || name.contains('/') || name.contains("..") {
            return Err(anyhow!("invalid input file name: {name:?}"));
        }
        let sha = sha_v
            .as_str()
            .ok_or_else(|| anyhow!("input_blobs[{name}] is not a string handle"))?;
        let bytes = db
            .get_blob(sha)
            .await?
            .ok_or_else(|| anyhow!("input blob not found for handle {sha}"))?;
        tokio::fs::write(format!("{dir}/{name}"), &bytes).await?;
    }
    Ok(Some(dir))
}

/// Per-run cap on a single captured output file. Generous (input blobs cap at 32 MiB, but a job
/// can legitimately emit a larger artifact); anything over this is SKIPPED with a log line —
/// never silently truncated.
const MAX_OUTPUT_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// Create the writable per-run host dir (`$HOME/.dokan/runs/<run_id>/output/`) bound at
/// `/output`. Lives under the same run-root as the /input materialization dir, so the run-root
/// cleanup in `run_inner` removes both.
async fn prepare_output_dir(run_id: i64) -> Result<String> {
    let home = std::env::var("HOME").map_err(|_| anyhow!("HOME unset; cannot prepare /output"))?;
    let dir = format!("{home}/.dokan/runs/{run_id}/output");
    tokio::fs::create_dir_all(&dir).await?;
    // The job writes here as the container's own uid (often root, or a userns-remapped uid),
    // while this host dir is owned by the dokan user. A Linux bind mount keeps host perms, so a
    // default 0755 dir blocks the container from creating files ("Permission denied"). Make it
    // world-writable so any uid can write; the files the job creates land 0644 by default, so
    // dokan reads them back fine. (macOS Docker Desktop is permissive, hence Linux-only bite.)
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).await?;
    Ok(dir)
}

/// Scan the /output host dir recursively after exec; store every regular file's bytes in the CAS
/// blob store (identical bytes dedupe automatically) and build the content-addressed map
/// `{ "<relative-name>": "<sha>" }`. Nested files keep their relative path as the name (e.g.
/// "sub/report.csv"). Files over `MAX_OUTPUT_FILE_BYTES` are skipped + logged. None when nothing
/// was written (empty /output → no-op).
async fn capture_output(db: &Db, dir: &str) -> Result<Option<serde_json::Value>> {
    let root = std::path::Path::new(dir);
    let mut map = serde_json::Map::new();
    // Iterative DFS — no recursion in async fn.
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&d).await {
            Ok(rd) => rd,
            Err(e) => {
                tracing::warn!("capture /output: read_dir {d:?} failed: {e}");
                continue;
            }
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                stack.push(path);
                continue;
            }
            // Only capture regular files — skip symlinks/sockets/fifos (a container shouldn't be
            // able to exfiltrate a host path via a symlink in its own writable dir).
            if !ft.is_file() {
                tracing::warn!("capture /output: skipping non-regular file {path:?}");
                continue;
            }
            let size = entry.metadata().await?.len();
            if size > MAX_OUTPUT_FILE_BYTES {
                tracing::warn!(
                    "capture /output: skipping {path:?} ({size} bytes > cap {MAX_OUTPUT_FILE_BYTES})"
                );
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            let bytes = tokio::fs::read(&path).await?;
            let (sha, _size) = db.put_blob(&bytes).await?;
            map.insert(rel, serde_json::Value::String(sha));
        }
    }
    if map.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::Value::Object(map)))
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
/// Redact any configured secret value that leaked into a log line, replacing each occurrence
/// with `***`. No-op (single empty loop) on the common path where no secrets are configured.
/// Scope: run LOGS only — the structured `::dokan:result::` payload is the job's intentional
/// output and is left intact (masking it could corrupt the JSON the caller parses).
fn redact_secrets(line: &str, secrets: &[String]) -> String {
    let mut out = line.to_string();
    for s in secrets {
        if out.contains(s.as_str()) {
            out = out.replace(s.as_str(), "***");
        }
    }
    out
}

async fn pump_logs(
    db: &Db,
    run_id: i64,
    mut stream: impl Stream<Item = Result<LogOutput, bollard::errors::Error>> + Unpin,
    secrets: &[String],
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
            batch.push((seq, stream_name, redact_secrets(line, secrets)));
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
        batch.push((seq, stream_name, redact_secrets(buf, secrets)));
        metrics::counter!("dokan_log_lines_total", "stream" => stream_name).increment(1);
    }
    flush(db, run_id, &mut batch).await?;
    Ok((seq, result))
}

#[cfg(test)]
mod tests {
    use super::redact_secrets;

    #[test]
    fn redacts_each_secret_occurrence() {
        let secrets = vec!["sk-supersecret-1234".to_string()];
        let line = "calling api with token sk-supersecret-1234 then sk-supersecret-1234 again";
        assert_eq!(
            redact_secrets(line, &secrets),
            "calling api with token *** then *** again"
        );
    }

    #[test]
    fn redacts_multiple_distinct_secrets() {
        let secrets = vec!["alpha-token-9999".to_string(), "beta-key-8888".to_string()];
        let line = "alpha-token-9999 / beta-key-8888";
        assert_eq!(redact_secrets(line, &secrets), "*** / ***");
    }

    #[test]
    fn no_secrets_is_identity() {
        let line = "nothing to hide here";
        assert_eq!(redact_secrets(line, &[]), line);
    }

    #[test]
    fn leaves_non_matching_lines_untouched() {
        let secrets = vec!["sk-supersecret-1234".to_string()];
        let line = "ordinary log line with no secret";
        assert_eq!(redact_secrets(line, &secrets), line);
    }
}
