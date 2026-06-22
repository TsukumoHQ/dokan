//! Docker execution: one job = one clean container run, then discard.
//! "Pool warm, run clean, discard" — the warm pool is a later phase; v1 creates
//! a fresh container per run. Code is trusted, so raw containers suffice.

use anyhow::{anyhow, Result};
use base64::Engine;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, LogsOptionsBuilder,
    RemoveContainerOptionsBuilder, StartContainerOptions, WaitContainerOptions,
};
use bollard::Docker;
use futures_util::StreamExt;
use std::time::Duration;

use crate::db::Db;

const MEM_LIMIT_BYTES: i64 = 512 * 1024 * 1024; // 512 MiB
const NANO_CPUS: i64 = 1_000_000_000; // 1.0 CPU
const DEFAULT_TIMEOUT_SECS: u64 = 300;

#[derive(Clone)]
pub struct Executor {
    docker: Docker,
}

/// Maps a declared runtime to its base image and in-container interpreter.
fn runtime_spec(runtime: &str) -> Option<(&'static str, &'static str)> {
    match runtime {
        "python" | "python3" | "python3.12" => Some(("python:3.12-slim", "python")),
        "node" | "nodejs" | "javascript" => Some(("node:22-slim", "node")),
        "bash" | "sh" | "shell" => Some(("alpine:3.20", "sh")),
        _ => None,
    }
}

impl Executor {
    pub fn connect() -> Result<Self> {
        // Honor DOCKER_HOST (Colima/Docker Desktop sockets live outside /var/run);
        // fall back to the local default socket otherwise.
        let docker = if std::env::var("DOCKER_HOST").is_ok() {
            Docker::connect_with_defaults()?
        } else {
            Docker::connect_with_local_defaults()?
        };
        Ok(Self { docker })
    }

    /// Kill a running job's container (best-effort).
    pub async fn cancel(&self, run_id: i64) {
        let name = format!("dokan-run-{run_id}");
        let _ = self.docker.kill_container(&name, None).await;
    }

    /// Pull the base image if it is not already present locally.
    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        let opts = CreateImageOptionsBuilder::default()
            .from_image(image)
            .build();
        let mut stream = self.docker.create_image(Some(opts), None, None);
        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow!("pull {image}: {e}"))?;
        }
        Ok(())
    }

    /// Full lifecycle: pull → create → start → stream logs to DB → wait → finish → remove.
    /// Runs to completion; the caller spawns this so `run_script` can return immediately.
    pub async fn run(
        &self,
        db: &Db,
        run_id: i64,
        runtime: &str,
        source: &str,
        input: &serde_json::Value,
    ) {
        if let Err(e) = self
            .run_inner(db, run_id, runtime, source, input)
            .await
        {
            let msg = e.to_string();
            let _ = db.append_log(run_id, db.max_log_seq(run_id).await.unwrap_or(0) + 1, "stderr", &format!("dokan: {msg}")).await;
            let _ = db.finish_run(run_id, "failed", None, Some(&msg)).await;
        }
    }

    async fn run_inner(
        &self,
        db: &Db,
        run_id: i64,
        runtime: &str,
        source: &str,
        input: &serde_json::Value,
    ) -> Result<()> {
        let (image, interp) =
            runtime_spec(runtime).ok_or_else(|| anyhow!("unknown runtime: {runtime}"))?;

        db.mark_running(run_id).await?;
        self.ensure_image(image).await?;

        let src_b64 = base64::engine::general_purpose::STANDARD.encode(source);
        // Decode the source inside the container, then exec the interpreter on it.
        let bootstrap = format!(
            "printf '%s' \"$DOKAN_SRC\" | base64 -d > /tmp/dokan_script && exec {interp} /tmp/dokan_script"
        );

        let body = ContainerCreateBody {
            image: Some(image.to_string()),
            cmd: Some(vec!["sh".into(), "-c".into(), bootstrap]),
            env: Some(vec![
                format!("DOKAN_SRC={src_b64}"),
                format!("DOKAN_INPUT={}", input),
                format!("DOKAN_RUN_ID={run_id}"),
            ]),
            host_config: Some(HostConfig {
                memory: Some(MEM_LIMIT_BYTES),
                nano_cpus: Some(NANO_CPUS),
                ..Default::default()
            }),
            ..Default::default()
        };

        let name = format!("dokan-run-{run_id}");
        let opts = CreateContainerOptionsBuilder::default().name(&name).build();
        let created = self.docker.create_container(Some(opts), body).await?;
        let cid = created.id;

        // Ensure cleanup even on early return.
        let result = self.run_streaming(db, run_id, &cid).await;

        let _ = self
            .docker
            .remove_container(
                &cid,
                Some(RemoveContainerOptionsBuilder::default().force(true).build()),
            )
            .await;

        result
    }

    async fn run_streaming(&self, db: &Db, run_id: i64, cid: &str) -> Result<()> {
        self.docker
            .start_container(cid, None::<StartContainerOptions>)
            .await?;

        // Stream logs concurrently with the wait, capping total runtime.
        let log_opts = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .follow(true)
            .build();
        let mut logs = self.docker.logs(cid, Some(log_opts));

        let mut seq: i64 = 0;
        let mut buf_out = String::new();
        let mut buf_err = String::new();

        let pump = async {
            while let Some(item) = logs.next().await {
                let out = item.map_err(|e| anyhow!("log stream: {e}"))?;
                let (stream, bytes) = match out {
                    bollard::container::LogOutput::StdOut { message } => ("stdout", message),
                    bollard::container::LogOutput::StdErr { message } => ("stderr", message),
                    bollard::container::LogOutput::Console { message } => ("stdout", message),
                    bollard::container::LogOutput::StdIn { message } => ("stdout", message),
                };
                let text = String::from_utf8_lossy(&bytes);
                let buf = if stream == "stderr" { &mut buf_err } else { &mut buf_out };
                buf.push_str(&text);
                while let Some(nl) = buf.find('\n') {
                    let line: String = buf.drain(..=nl).collect();
                    let line = line.trim_end_matches('\n').trim_end_matches('\r');
                    seq += 1;
                    db.append_log(run_id, seq, stream, line).await?;
                }
            }
            // Flush any trailing partial lines.
            for (stream, buf) in [("stdout", &buf_out), ("stderr", &buf_err)] {
                if !buf.is_empty() {
                    seq += 1;
                    db.append_log(run_id, seq, stream, buf).await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        match tokio::time::timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), pump).await {
            Ok(r) => r?,
            Err(_) => {
                let _ = self.docker.kill_container(cid, None).await;
                let s = db.max_log_seq(run_id).await.unwrap_or(seq) + 1;
                db.append_log(run_id, s, "stderr", "dokan: timeout, container killed")
                    .await?;
                db.finish_run(run_id, "failed", None, Some("timeout")).await?;
                return Ok(());
            }
        }

        // Container has produced all logs; collect exit code.
        let mut wait = self
            .docker
            .wait_container(cid, None::<WaitContainerOptions>);
        let mut exit_code: i64 = 0;
        while let Some(item) = wait.next().await {
            match item {
                Ok(resp) => exit_code = resp.status_code,
                // wait errors with the non-zero code as part of the message on some daemons.
                Err(bollard::errors::Error::DockerContainerWaitError { code, .. }) => {
                    exit_code = code;
                }
                Err(e) => return Err(anyhow!("wait: {e}")),
            }
        }

        let status = if exit_code == 0 { "succeeded" } else { "failed" };
        db.finish_run(run_id, status, Some(exit_code as i32), None)
            .await?;
        Ok(())
    }
}
