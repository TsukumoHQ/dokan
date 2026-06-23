//! P2 proofs: declarative compose_flow DAG, ordered step execution with dependency
//! output passing, step-boundary checkpointing, and spec validation (cycle rejection).

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
async fn call(c: &RunningService<RoleClient, ()>, name: &'static str, args: Value) -> Value {
    parse(
        &c.call_tool(CallToolRequestParams::new(name).with_arguments(obj(args)))
            .await
            .unwrap(),
    )
}

/// fetch → transform → ship, each one container run; transform sees fetch's output.
#[tokio::test]
async fn flow_dag_runs_in_order_with_deps() -> anyhow::Result<()> {
    let c = spawn().await?;
    // Script echoes its full input (which carries deps) as the last stdout line = output.
    let sid = call(
        &c,
        "upload_script",
        json!({"name":"echo","runtime":"bash","source":"echo \"$DOKAN_INPUT\"\n","description":"flow node"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();

    let flow = call(
        &c,
        "compose_flow",
        json!({
            "name": "etl",
            "spec": { "steps": [
                {"id":"fetch","script_id": sid},
                {"id":"transform","script_id": sid, "depends_on":["fetch"]},
                {"id":"ship","script_id": sid, "depends_on":["transform"]}
            ]}
        }),
    )
    .await;
    let flow_id = flow["flow_id"].as_i64().expect(&flow.to_string());

    let fr = call(&c, "run_flow", json!({"flow_id": flow_id, "input": {"src":"x"}})).await;
    let flow_run_id = fr["flow_run_id"].as_i64().unwrap();

    // Poll to terminal.
    let mut last = json!({});
    for _ in 0..60 {
        last = call(&c, "get_flow_run", json!({"flow_run_id": flow_run_id})).await;
        let st = last["status"].as_str().unwrap_or("");
        if st == "succeeded" || st == "failed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    eprintln!("flow_run -> {last}");
    assert_eq!(last["status"], "succeeded", "flow should succeed: {last}");

    let steps = last["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 3);
    assert!(steps.iter().all(|s| s["status"] == "succeeded"), "all steps ok");

    // transform's output must carry fetch's result under deps.
    let transform = steps.iter().find(|s| s["id"] == "transform").unwrap();
    let out = transform["out"].as_str().unwrap_or("");
    assert!(out.contains("fetch"), "transform saw upstream dep: {out}");

    c.cancel().await?;
    Ok(())
}

/// Poll a flow_run to a terminal status, returning the final get_flow_run payload.
async fn poll_flow(c: &RunningService<RoleClient, ()>, flow_run_id: i64) -> Value {
    let mut last = json!({});
    for _ in 0..80 {
        last = call(c, "get_flow_run", json!({"flow_run_id": flow_run_id})).await;
        match last["status"].as_str().unwrap_or("") {
            "succeeded" | "failed" => break,
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    last
}
fn step<'a>(steps: &'a [Value], id: &str) -> &'a Value {
    steps.iter().find(|s| s["id"] == id).unwrap_or_else(|| panic!("missing step {id}"))
}

/// `when` gates a step; a skip propagates to dependents of the skipped step.
#[tokio::test]
async fn flow_when_branch_and_skip_propagation() -> anyhow::Result<()> {
    let c = spawn().await?;
    let sid = call(
        &c,
        "upload_script",
        json!({"name":"ok","runtime":"bash","source":"echo ok\n","description":"emits ok"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();

    let flow = call(
        &c,
        "compose_flow",
        json!({
            "name": "branch",
            "spec": { "steps": [
                {"id":"a","script_id": sid},
                {"id":"b","script_id": sid, "depends_on":["a"], "when":{"ref":"deps.a","op":"eq","value":"ok"}},
                {"id":"c","script_id": sid, "depends_on":["a"], "when":{"ref":"deps.a","op":"eq","value":"spam"}},
                {"id":"d","script_id": sid, "depends_on":["c"]}
            ]}
        }),
    )
    .await;
    let flow_id = flow["flow_id"].as_i64().expect(&flow.to_string());
    let fr = call(&c, "run_flow", json!({"flow_id": flow_id})).await;
    let last = poll_flow(&c, fr["flow_run_id"].as_i64().unwrap()).await;
    eprintln!("branch -> {last}");

    assert_eq!(last["status"], "succeeded", "flow ok: {last}");
    let steps = last["steps"].as_array().unwrap();
    assert_eq!(step(steps, "a")["status"], "succeeded");
    assert_eq!(step(steps, "b")["status"], "succeeded", "when true → runs");
    assert_eq!(step(steps, "c")["status"], "skipped", "when false → skipped");
    assert_eq!(step(steps, "d")["status"], "skipped", "skip propagates to dependents");

    c.cancel().await?;
    Ok(())
}

/// `map` fans a step out over an upstream array into one child run per element.
#[tokio::test]
async fn flow_map_fanout() -> anyhow::Result<()> {
    let c = spawn().await?;
    let emit = call(
        &c,
        "upload_script",
        json!({"name":"emit","runtime":"bash","source":"echo '[10,20,30]'\n","description":"emits a list"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();
    let proc = call(
        &c,
        "upload_script",
        json!({"name":"proc","runtime":"bash","source":"echo \"$DOKAN_INPUT\"\n","description":"echo element"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();

    let flow = call(
        &c,
        "compose_flow",
        json!({
            "name": "fanout",
            "spec": { "steps": [
                {"id":"emit","script_id": emit},
                {"id":"proc","script_id": proc, "depends_on":["emit"], "map":"deps.emit"}
            ]}
        }),
    )
    .await;
    let flow_id = flow["flow_id"].as_i64().expect(&flow.to_string());
    let fr = call(&c, "run_flow", json!({"flow_id": flow_id})).await;
    let last = poll_flow(&c, fr["flow_run_id"].as_i64().unwrap()).await;
    eprintln!("fanout -> {last}");

    assert_eq!(last["status"], "succeeded", "flow ok: {last}");
    let steps = last["steps"].as_array().unwrap();
    let proc = step(steps, "proc");
    assert_eq!(proc["status"], "succeeded", "map parent succeeds");
    // Token-frugal: children are collapsed into a {n, ok, failed} count, not listed.
    assert_eq!(proc["map"], json!({"n": 3, "ok": 3, "failed": 0}), "fan-out of 3: {last}");
    let listed_children = steps
        .iter()
        .filter(|s| s["id"].as_str().unwrap_or("").contains('#'))
        .count();
    assert_eq!(listed_children, 0, "children collapsed, not listed: {last}");

    c.cancel().await?;
    Ok(())
}

/// A failing step triggers saga compensation of upstream succeeded steps.
#[tokio::test]
async fn flow_saga_compensation() -> anyhow::Result<()> {
    let c = spawn().await?;
    let good = call(
        &c,
        "upload_script",
        json!({"name":"good","runtime":"bash","source":"echo ok\n","description":"succeeds"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();
    let comp = call(
        &c,
        "upload_script",
        json!({"name":"comp","runtime":"bash","source":"echo compensated\n","description":"rollback"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();
    let bad = call(
        &c,
        "upload_script",
        json!({"name":"bad","runtime":"bash","source":"exit 1\n","description":"fails"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();

    let flow = call(
        &c,
        "compose_flow",
        json!({
            "name": "saga",
            "spec": { "steps": [
                {"id":"s0","script_id": good, "compensate": comp},
                {"id":"s1","script_id": good, "depends_on":["s0"], "compensate": comp},
                {"id":"s2","script_id": bad, "depends_on":["s1"], "retries": 0}
            ]}
        }),
    )
    .await;
    let flow_id = flow["flow_id"].as_i64().expect(&flow.to_string());
    let fr = call(&c, "run_flow", json!({"flow_id": flow_id})).await;
    let last = poll_flow(&c, fr["flow_run_id"].as_i64().unwrap()).await;
    eprintln!("saga -> {last}");

    assert_eq!(last["status"], "failed", "flow fails: {last}");
    let steps = last["steps"].as_array().unwrap();
    assert_eq!(step(steps, "s1")["status"], "succeeded");
    // Both upstream succeeded steps with a compensate are rolled back (reverse order).
    assert_eq!(step(steps, "s0")["comp"], json!(true), "s0 compensated: {last}");
    assert_eq!(step(steps, "s1")["comp"], json!(true), "s1 compensated: {last}");

    c.cancel().await?;
    Ok(())
}

/// A `cache:true` step recalls a prior identical run instead of re-executing (partial
/// flow recall). The script echoes its own run id, which differs on every real execution —
/// so an identical output across two flow runs proves the second was recalled.
#[tokio::test]
async fn flow_step_cache_recall() -> anyhow::Result<()> {
    let c = spawn().await?;
    let rid = call(
        &c,
        "upload_script",
        json!({"name":"rid","runtime":"bash","source":"echo \"$DOKAN_RUN_ID\"\n","description":"echoes run id"}),
    )
    .await["script_id"]
        .as_i64()
        .unwrap();

    let flow = call(
        &c,
        "compose_flow",
        json!({
            "name": "cacheflow",
            "spec": { "steps": [ {"id":"x","script_id": rid, "cache": true} ]}
        }),
    )
    .await;
    let flow_id = flow["flow_id"].as_i64().expect(&flow.to_string());

    // First run: cache miss → executes, tags the run with its cache key.
    let fr1 = call(&c, "run_flow", json!({"flow_id": flow_id})).await;
    let r1 = poll_flow(&c, fr1["flow_run_id"].as_i64().unwrap()).await;
    assert_eq!(r1["status"], "succeeded", "run1: {r1}");
    let out1 = step(r1["steps"].as_array().unwrap(), "x")["out"].as_str().unwrap_or("").to_string();
    assert!(!out1.is_empty(), "run1 produced an output: {r1}");

    // Second run: cache hit → recalled, same output despite a fresh run id would differ.
    let fr2 = call(&c, "run_flow", json!({"flow_id": flow_id})).await;
    let r2 = poll_flow(&c, fr2["flow_run_id"].as_i64().unwrap()).await;
    assert_eq!(r2["status"], "succeeded", "run2: {r2}");
    let out2 = step(r2["steps"].as_array().unwrap(), "x")["out"].as_str().unwrap_or("").to_string();

    assert_eq!(out1, out2, "step recalled (identical output) — not re-executed: {out1} vs {out2}");
    c.cancel().await?;
    Ok(())
}

/// A cyclic spec is rejected at compose time, not at run time.
#[tokio::test]
async fn cyclic_flow_rejected() -> anyhow::Result<()> {
    let c = spawn().await?;
    let res = call(
        &c,
        "compose_flow",
        json!({
            "name":"loop",
            "spec": { "steps": [
                {"id":"a","script_id":1,"depends_on":["b"]},
                {"id":"b","script_id":1,"depends_on":["a"]}
            ]}
        }),
    )
    .await;
    assert_eq!(res["error"], "invalid_spec", "cycle rejected: {res}");
    c.cancel().await?;
    Ok(())
}
