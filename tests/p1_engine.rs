//! P1 engine proofs: SKIP LOCKED queue + bounded-concurrency worker, capability
//! routing, and cron scheduling. Each test spawns its own dokan over stdio.

use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use serde_json::{json, Value};
use tokio::process::Command;

fn obj(v: Value) -> serde_json::Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn parse(r: &CallToolResult) -> Value {
    let text = r
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    serde_json::from_str(&text).unwrap_or(json!({ "raw": text }))
}

async fn spawn() -> anyhow::Result<RunningService<RoleClient, ()>> {
    Ok(()
        .serve(TokioChildProcess::new(
            Command::new(env!("CARGO_BIN_EXE_dokan")).configure(|cmd| {
                cmd.arg("--transport").arg("stdio");
            }),
        )?)
        .await?)
}

async fn call(
    c: &RunningService<RoleClient, ()>,
    name: &'static str,
    args: Value,
) -> anyhow::Result<Value> {
    Ok(parse(
        &c.call_tool(CallToolRequestParams::new(name).with_arguments(obj(args)))
            .await?,
    ))
}

async fn upload(c: &RunningService<RoleClient, ()>, runtime: &str, source: &str) -> i64 {
    call(
        c,
        "upload_script",
        json!({"name": "t", "runtime": runtime, "source": source, "description": "p1"}),
    )
    .await
    .unwrap()["script_id"]
        .as_i64()
        .unwrap()
}

async fn wait_status(c: &RunningService<RoleClient, ()>, run_id: i64, timeout: u64) -> String {
    call(c, "wait_for", json!({"run_id": run_id, "timeout": timeout}))
        .await
        .unwrap()["status"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

/// Many runs enqueued at once all execute (bounded concurrency, SKIP LOCKED claims).
#[tokio::test]
async fn queue_runs_many_concurrently() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo done $DOKAN_RUN_ID\n").await;

    let mut ids = vec![];
    for _ in 0..6 {
        let r = call(&c, "run_script", json!({"script_id": sid})).await?;
        ids.push(r["run_id"].as_i64().unwrap());
    }
    for id in &ids {
        assert_eq!(wait_status(&c, *id, 90).await, "succeeded", "run {id}");
    }
    c.cancel().await?;
    Ok(())
}

/// A runtime no worker advertises is never claimed — it stays pending (routing).
#[tokio::test]
async fn unroutable_runtime_stays_pending() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "ruby", "puts 'hi'\n").await;
    let run = call(&c, "run_script", json!({"script_id": sid})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    // No python/node/bash worker will touch a ruby job.
    let status = wait_status(&c, run_id, 4).await;
    assert_eq!(status, "pending", "ruby unroutable -> stays queued");
    c.cancel().await?;
    Ok(())
}

/// A cron schedule ticking every second enqueues runs that the worker executes.
#[tokio::test]
async fn cron_enqueues_runs() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo tick\n").await;
    let s = call(
        &c,
        "schedule",
        json!({"script_id": sid, "cron": "* * * * * *"}),
    )
    .await?;
    let schedule_id = s["schedule_id"].as_i64().expect(&format!("scheduled: {s}"));

    // Let it tick a few times, then confirm a run for this script materialized.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    let runs = call(&c, "list_runs", json!({"limit": 50})).await?;
    let found = runs["recent"]
        .as_array()
        .map(|a| a.iter().any(|r| r["script_id"].as_i64() == Some(sid)))
        .unwrap_or(false);
    assert!(found, "cron should have enqueued a run for script {sid}: {runs}");

    let sched_list = call(&c, "list_schedules", json!({})).await?;
    assert!(
        sched_list["schedules"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false),
        "list_schedules non-empty"
    );

    // Clean up: stop the per-second cron so it doesn't flood the shared DB forever.
    let un = call(&c, "unschedule", json!({"schedule_id": schedule_id})).await?;
    assert_eq!(un["status"], "unscheduled", "cron stopped: {un}");
    let after = call(&c, "list_schedules", json!({})).await?;
    let still = after["schedules"]
        .as_array()
        .map(|a| a.iter().any(|s| s["schedule_id"].as_i64() == Some(schedule_id)))
        .unwrap_or(false);
    assert!(!still, "unscheduled cron gone from list");

    c.cancel().await?;
    Ok(())
}
