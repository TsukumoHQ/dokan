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
                // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
                cmd.arg("--transport").arg("stdio").env("DOKAN_DEV_INSECURE", "1");
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
        let r = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
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
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
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
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
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
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "succeeded", "ran");
    let text = logs_text(&c, run_id).await;
    assert!(text.contains("KEY=sekret-42"), "secret reached job env: {text}");
    c.cancel().await?;
    Ok(())
}

/// Some MCP clients stringify an object param. When `input` arrives as a JSON *string*
/// of an object, the job must still see it single-encoded in DOKAN_INPUT (so one
/// JSON.parse yields the object) — not double-encoded (a quoted JSON string that parses
/// to a string and silently reads its fields as undefined). (Field bug: a write-flag job
/// ran in dry mode because input{write:true} arrived stringified.)
#[tokio::test]
async fn stringified_object_input_is_not_double_encoded() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo \"IN=$DOKAN_INPUT\"\n").await;
    // Simulate a client that sends the object as a JSON string instead of inline.
    let run = call(
        &c,
        "run_script",
        json!({"script_id": sid, "input": "{\"write\":true}", "agent_id": "test"}),
    )
    .await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "succeeded", "ran");
    let text = logs_text(&c, run_id).await;
    assert!(
        text.contains("IN={\"write\":true}"),
        "input reaches job single-encoded (a real object): {text}"
    );
    assert!(
        !text.contains("\\\""),
        "NOT double-encoded (no escaped quotes in DOKAN_INPUT): {text}"
    );
    c.cancel().await?;
    Ok(())
}

/// The `::dokan:progress::` channel sets a live, latest-wins status line on the run —
/// surfaced by read_logs/list_runs but NOT written to the log stream (so an operator sees
/// "step 3/3" without paging the whole log). Regular stdout still logs normally.
#[tokio::test]
async fn progress_channel_sets_latest_and_is_not_logged() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(
        &c,
        "bash",
        "echo '::dokan:progress:: step 1/3'\n\
         echo regular-log-line\n\
         echo '::dokan:progress:: step 3/3'\n\
         echo '::dokan:result:: {\"done\":true}'\n",
    )
    .await;
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
    let run_id = run["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run_id, 60).await, "succeeded", "ran");
    let r = call(&c, "read_logs", json!({"run_id": run_id, "after_cursor": 0, "limit": 500})).await?;
    assert_eq!(r["progress"], json!("step 3/3"), "latest progress wins: {r}");
    assert_eq!(r["result"], json!({"done": true}), "result still captured: {r}");
    let text = r["lines"]
        .as_array()
        .map(|a| a.iter().filter_map(|l| l.as_str()).collect::<Vec<_>>().join("\n"))
        .unwrap_or_default();
    assert!(text.contains("regular-log-line"), "normal stdout is logged: {text}");
    assert!(!text.contains("step 1/3"), "progress not in logs: {text}");
    assert!(!text.contains("step 3/3"), "progress not in logs: {text}");
    assert!(!text.contains("::dokan:progress::"), "progress sentinel not logged: {text}");
    c.cancel().await?;
    Ok(())
}

/// upsert=true re-provisions a script by name: same id back, no duplicate rows, no-op when
/// source + metadata are unchanged, version bump when the source changes, and a metadata-only
/// update (e.g. a new mem cap on identical source) applies WITHOUT a version bump. (Terrain P2.)
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
    // Same source but a NEW mem cap -> metadata_updated, same id, NO version bump (the cap-only
    // re-provision that used to be a silent no-op).
    let v_after_d = d["version"].as_i64().unwrap();
    let e = call(
        &c,
        "upload_script",
        json!({"name": name, "runtime": "bash", "source": "echo v2\n", "upsert": true, "mem_limit_mb": 256}),
    )
    .await?;
    assert_eq!(e["script_id"].as_i64().unwrap(), id, "same id on metadata update");
    assert_eq!(e["status"], "metadata_updated", "metadata applied on identical source: {e}");
    assert_eq!(e["version"].as_i64().unwrap(), v_after_d, "version NOT bumped on metadata-only: {e}");
    let g = call(&c, "get_script", json!({"id": id})).await?;
    assert_eq!(g["mem_limit_mb"].as_i64(), Some(256), "mem cap took: {g}");
    // Re-applying the identical source + metadata -> unchanged (IS DISTINCT FROM guard).
    let h = call(
        &c,
        "upload_script",
        json!({"name": name, "runtime": "bash", "source": "echo v2\n", "upsert": true, "mem_limit_mb": 256}),
    )
    .await?;
    assert_eq!(h["status"], "unchanged", "no-op when source + metadata identical: {h}");
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
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
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
    let run = call(&c, "run_script", json!({"script_id": sid, "agent_id": "test"})).await?;
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

/// Run-or-recall: a cache:true run of identical (source+input) returns the prior result
/// without executing again; a different input is NOT recalled. (Moat #1.)
#[tokio::test]
async fn run_or_recall_recalls_identical() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo '::dokan:result:: {\"v\":42}'\n").await;
    // First cached run executes.
    let r1 = call(&c, "run_script", json!({"script_id": sid, "cache": true, "agent_id": "test"})).await?;
    let run1 = r1["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, run1, 60).await, "succeeded", "first ran");
    // Identical cached run is recalled — same run_id, no new execution.
    let r2 = call(&c, "run_script", json!({"script_id": sid, "cache": true, "agent_id": "test"})).await?;
    assert_eq!(r2["status"], "recalled", "second recalled: {r2}");
    assert_eq!(r2["run_id"].as_i64().unwrap(), run1, "recalled the same run");
    assert_eq!(r2["result"]["v"], 42, "recalled result intact: {r2}");
    // A different input is a different key -> not recalled.
    let r3 = call(&c, "run_script", json!({"script_id": sid, "cache": true, "input": {"x": 1}, "agent_id": "test"})).await?;
    assert_eq!(r3["status"], "pending", "different input executes fresh: {r3}");
    c.cancel().await?;
    Ok(())
}

/// Secrets scope per agent: a job sees global secrets + ITS agent's scoped secrets, not
/// another agent's. whoami reports the agent's view. (Moat #2.)
#[tokio::test]
async fn agent_scoped_secrets_and_whoami() -> anyhow::Result<()> {
    let c = spawn().await?;
    // Global secret + a secret scoped to agent "alpha".
    call(&c, "set_secret", json!({"name": "DK_GLOB", "value": "g-val"})).await?;
    call(&c, "set_secret", json!({"name": "DK_SCOPED", "value": "a-val", "agent_id": "alpha"})).await?;
    let sid = upload(&c, "bash", "echo \"G=${DK_GLOB:-} S=${DK_SCOPED:-}\"\n").await;

    // Run as alpha -> sees both.
    let ra = call(&c, "run_script", json!({"script_id": sid, "agent_id": "alpha"})).await?;
    let ida = ra["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, ida, 60).await, "succeeded", "alpha ran");
    assert!(logs_text(&c, ida).await.contains("G=g-val S=a-val"), "alpha sees both");

    // Run as beta -> sees global only, NOT alpha's scoped secret.
    let rb = call(&c, "run_script", json!({"script_id": sid, "agent_id": "beta"})).await?;
    let idb = rb["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, idb, 60).await, "succeeded", "beta ran");
    let tb = logs_text(&c, idb).await;
    assert!(tb.contains("G=g-val S="), "beta sees global, not alpha scoped: {tb}");
    assert!(!tb.contains("S=a-val"), "beta must NOT see alpha's secret: {tb}");

    // whoami reflects scope + quota.
    let w = call(&c, "whoami", json!({"agent_id": "alpha"})).await?;
    assert!(w["secrets"].as_array().unwrap().iter().any(|s| s == "DK_SCOPED"), "whoami lists scoped: {w}");
    assert_eq!(w["limits"]["mem_mb"], 1024, "whoami limits: {w}");
    assert_eq!(w["quota"]["max_concurrent"], 25, "whoami quota: {w}");
    c.cancel().await?;
    Ok(())
}

/// on_result reactive trigger: a result containing the predicate enqueues the target with
/// {trigger_result, source_run_id} as input. No external orchestrator. (Moat #3.)
#[tokio::test]
async fn on_result_fires_target() -> anyhow::Result<()> {
    let c = spawn().await?;
    let src = upload(&c, "bash", "echo '::dokan:result:: {\"alert\":true,\"sev\":9}'\n").await;
    let tgt = upload(&c, "bash", "echo \"GOT $DOKAN_INPUT\"\n").await;
    let t = call(&c, "on_result", json!({
        "source_script_id": src, "predicate": {"alert": true}, "target_script_id": tgt
    })).await?;
    assert_eq!(t["status"], "armed", "trigger armed: {t}");

    let r = call(&c, "run_script", json!({"script_id": src, "agent_id": "test"})).await?;
    assert_eq!(wait_status(&c, r["run_id"].as_i64().unwrap(), 60).await, "succeeded", "source ran");

    // The trigger should enqueue a run of the target script.
    let mut target_run = None;
    for _ in 0..24 {
        let runs = call(&c, "list_runs", json!({"limit": 50})).await?;
        if let Some(run) = runs["recent"].as_array().unwrap()
            .iter().find(|x| x["script_id"].as_i64() == Some(tgt))
        {
            target_run = run["run_id"].as_i64();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let tid = target_run.expect("trigger enqueued a target run");
    assert_eq!(wait_status(&c, tid, 60).await, "succeeded", "target ran");
    let logs = logs_text(&c, tid).await;
    assert!(logs.contains("trigger_result"), "target got the result as input: {logs}");
    assert!(logs.contains("\"sev\":9"), "result payload passed through: {logs}");
    c.cancel().await?;
    Ok(())
}

/// network=false runs network-disabled (deterministic), gets a signed receipt, and is
/// soundly recallable. (T1.)
#[tokio::test]
async fn deterministic_network_off_receipt_and_recall() -> anyhow::Result<()> {
    let c = spawn().await?;
    let src = "wget -q -T 2 -O /dev/null http://1.1.1.1 2>/dev/null && echo NET || echo NO_NET\n\
               echo '::dokan:result:: {\"x\":1}'\n";
    let sid = call(&c, "upload_script",
        json!({"name": "det-probe", "runtime": "bash", "source": src, "network": false}))
        .await?["script_id"].as_i64().unwrap();
    let r1 = call(&c, "run_script", json!({"script_id": sid, "cache": true, "agent_id": "test"})).await?;
    let id1 = r1["run_id"].as_i64().unwrap();
    assert_eq!(wait_status(&c, id1, 60).await, "succeeded", "ran");
    // Network was disabled.
    assert!(logs_text(&c, id1).await.contains("NO_NET"), "no network in deterministic run");
    // Signed receipt, marked deterministic.
    let rec = call(&c, "get_receipt", json!({"run_id": id1})).await?;
    assert_eq!(rec["deterministic"], true, "receipt deterministic: {rec}");
    assert!(rec["sig"].as_str().map(|s| !s.is_empty()).unwrap_or(false), "signed: {rec}");
    assert!(rec["output_sha256"].as_str().is_some(), "binds output: {rec}");
    // Recall: identical inputs → recalled without re-running.
    let r2 = call(&c, "run_script", json!({"script_id": sid, "cache": true, "agent_id": "test"})).await?;
    assert_eq!(r2["status"], "recalled", "recalled: {r2}");
    assert_eq!(r2["run_id"].as_i64().unwrap(), id1, "same run recalled");
    c.cancel().await?;
    Ok(())
}

/// An idempotency key makes a repeated enqueue return the same run, not a duplicate. (T5.)
#[tokio::test]
async fn idempotency_key_dedups() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = upload(&c, "bash", "echo hi\n").await;
    let a = call(&c, "run_script", json!({"script_id": sid, "idempotency_key": "job-42", "agent_id": "test"})).await?;
    let id = a["run_id"].as_i64().unwrap();
    let b = call(&c, "run_script", json!({"script_id": sid, "idempotency_key": "job-42", "agent_id": "test"})).await?;
    assert_eq!(b["idempotent"], true, "second is idempotent: {b}");
    assert_eq!(b["run_id"].as_i64().unwrap(), id, "same run returned");
    // A different key enqueues a fresh run.
    let d = call(&c, "run_script", json!({"script_id": sid, "idempotency_key": "job-99", "agent_id": "test"})).await?;
    assert_ne!(d["run_id"].as_i64().unwrap(), id, "different key -> new run");
    c.cancel().await?;
    Ok(())
}

/// The executor heartbeats into the registry; list_executors shows it live. (T4.)
#[tokio::test]
async fn list_executors_shows_live() -> anyhow::Result<()> {
    let c = spawn().await?;
    // First heartbeat fires on the immediate interval tick; give it a moment.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let r = call(&c, "list_executors", json!({})).await?;
    let execs = r["executors"].as_array().expect("executors array");
    assert!(!execs.is_empty(), "at least one executor registered: {r}");
    assert!(execs.iter().any(|e| e["live"] == true), "an executor is live: {r}");
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
