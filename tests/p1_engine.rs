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

async fn logs_text(c: &RunningService<RoleClient, ()>, run_id: i64) -> String {
    let r = call(c, "read_logs", json!({"run_id": run_id, "after_cursor": 0, "limit": 500}))
        .await
        .unwrap();
    r["lines"]
        .as_array()
        .map(|a| a.iter().filter_map(|l| l.as_str()).collect::<Vec<_>>().join("\n"))
        .unwrap_or_default()
}

/// A script that runs to completion and exits nonzero is a deterministic verdict (a
/// monitor/gate finding), NOT a transient crash. It must execute exactly once — no 3x
/// auto-retry that reprints the verdict and burns compute. (Terrain P0, 2 leads.)
#[tokio::test]
async fn nonzero_exit_is_not_retried() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo VERDICT-LINE\nexit 2\n").await;
    let run = call(&c, "run_script", json!({"script_id": sid})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "failed", "nonzero exit -> failed");
    // Give any (wrongly-scheduled) retry time to fire before counting the verdict.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let text = logs_text(&c, run_id).await;
    assert_eq!(
        text.matches("VERDICT-LINE").count(),
        1,
        "verdict printed exactly once, not retried: {text}"
    );
    c.cancel().await?;
    Ok(())
}

/// Secrets set over MCP are injected as env vars into the job container. (Terrain P0:
/// leads had no way to provision API keys for monitors.)
#[tokio::test]
async fn secret_injected_into_job_env() -> anyhow::Result<()> {
    let c = spawn().await?;
    // Unique name so concurrent/other runs against the shared DB can't collide.
    let set = call(
        &c,
        "set_secret",
        json!({"name": "DOKAN_TEST_KEY", "value": "sekret-42"}),
    )
    .await?;
    assert_eq!(set["status"], "set", "secret set: {set}");
    let names = call(&c, "list_secrets", json!({})).await?;
    assert!(
        names["secrets"].as_array().map(|a| a.iter().any(|n| n == "DOKAN_TEST_KEY")).unwrap_or(false),
        "secret name listed (value never returned): {names}"
    );
    let sid = upload(&c, "bash", "echo KEY=$DOKAN_TEST_KEY\n").await;
    let run = call(&c, "run_script", json!({"script_id": sid})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "succeeded", "ran");
    let text = logs_text(&c, run_id).await;
    assert!(text.contains("KEY=sekret-42"), "secret reached job env: {text}");
    c.cancel().await?;
    Ok(())
}

/// upsert=true re-provisions a script by name: same id back, no duplicate rows, no-op
/// when the source is unchanged, version bump when it changes. (Terrain P2.)
#[tokio::test]
async fn upsert_dedups_by_name() -> anyhow::Result<()> {
    let c = spawn().await?;
    let name = "p2-upsert-probe";
    let up = |src: &'static str| {
        call(
            &c,
            "upload_script",
            json!({"name": name, "runtime": "bash", "source": src, "upsert": true}),
        )
    };
    let a = up("echo v1\n").await?;
    let id = a["script_id"].as_i64().unwrap();
    // Same source -> unchanged, same id.
    let b = up("echo v1\n").await?;
    assert_eq!(b["script_id"].as_i64().unwrap(), id, "same id on re-upload");
    assert_eq!(b["status"], "unchanged", "no-op when source identical: {b}");
    // Changed source -> updated, same id, bumped version.
    let d = up("echo v2\n").await?;
    assert_eq!(d["script_id"].as_i64().unwrap(), id, "still same id after change");
    assert_eq!(d["status"], "updated", "updated when source differs: {d}");
    assert!(
        d["version"].as_i64().unwrap() > a["version"].as_i64().unwrap(),
        "version bumped: {d}"
    );
    c.cancel().await?;
    Ok(())
}

/// search_script tolerates typos via pg_trgm (the substring-only fallback returned 0 on
/// fuzzy queries). (Terrain P2.)
#[tokio::test]
async fn fuzzy_search_finds_typo() -> anyhow::Result<()> {
    let c = spawn().await?;
    let name = "citation-monitor-fuzztest";
    call(
        &c,
        "upload_script",
        json!({"name": name, "runtime": "bash", "source": "echo hi\n", "upsert": true}),
    )
    .await?;
    // Misspelled query — substring ILIKE would miss this; trigram similarity catches it.
    let r = call(&c, "search_script", json!({"query": "citaton-moniter-fuzztst"})).await?;
    let hit = r["results"]
        .as_array()
        .map(|a| a.iter().any(|s| s["name"].as_str() == Some(name)))
        .unwrap_or(false);
    assert!(hit, "fuzzy query found the script (mode {}): {r}", r["mode"]);
    c.cancel().await?;
    Ok(())
}

/// A job emits a structured result via the `::dokan:result::` stdout channel; dokan
/// captures it (not as a log line) and returns it from wait_for. (Terrain #1+#5.)
#[tokio::test]
async fn structured_result_is_captured() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(
        &c,
        "bash",
        "echo working\necho '::dokan:result:: {\"alert\":true,\"n\":3}'\n",
    )
    .await;
    let run = call(&c, "run_script", json!({"script_id": sid})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    let w = call(&c, "wait_for", json!({"run_id": run_id, "timeout": 60})).await?;
    assert_eq!(w["status"], "succeeded", "ran: {w}");
    assert_eq!(w["result"]["alert"], true, "result captured: {w}");
    assert_eq!(w["result"]["n"], 3, "result payload intact: {w}");
    // The sentinel line is a control channel, not log output.
    let text = logs_text(&c, run_id).await;
    assert!(text.contains("working"), "normal stdout logged: {text}");
    assert!(!text.contains("::dokan:result::"), "sentinel not logged: {text}");
    c.cancel().await?;
    Ok(())
}

/// delete_script removes the script and cascades its runs; afterwards get_script 404s.
/// (Terrain #2 — orphan cleanup.)
#[tokio::test]
async fn delete_script_cascades() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo bye\n").await;
    let run = call(&c, "run_script", json!({"script_id": sid})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "succeeded", "ran");
    let d = call(&c, "delete_script", json!({"script_id": sid})).await?;
    assert_eq!(d["status"], "deleted", "deleted: {d}");
    assert!(d["runs_removed"].as_i64().unwrap() >= 1, "cascaded a run: {d}");
    let g = call(&c, "get_script", json!({"id": sid})).await?;
    assert_eq!(g["error"], "not_found", "script gone: {g}");
    c.cancel().await?;
    Ok(())
}

/// A 5-field crontab (missing the leading SECONDS column) is rejected loudly instead of
/// being silently accepted and never firing. (Terrain P2.)
#[tokio::test]
async fn invalid_cron_is_rejected() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo hi\n").await;
    let r = call(&c, "schedule", json!({"script_id": sid, "cron": "*/5 * * * *"})).await?;
    assert_eq!(r["error"], "invalid_cron", "5-field cron rejected: {r}");
    assert!(r.get("schedule_id").is_none(), "no schedule created: {r}");
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
