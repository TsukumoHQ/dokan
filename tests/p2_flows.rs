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
