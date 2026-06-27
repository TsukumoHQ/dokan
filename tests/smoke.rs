//! End-to-end proof of the wedge (PRD §11 step 4): an MCP client spawns dokan over
//! stdio, uploads a Python script, runs it, and reads logs back — zero human clicks.
//!
//! Requires the dokan Postgres (port 5499) and a running Docker daemon.
//!   cargo test --test smoke -- --nocapture

use rmcp::model::CallToolRequestParams;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::ServiceExt;
use serde_json::{json, Value};
use tokio::process::Command;

fn obj(v: Value) -> serde_json::Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn parse(result: &rmcp::model::CallToolResult) -> Value {
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default();
    serde_json::from_str(&text).unwrap_or(json!({ "raw": text }))
}

#[tokio::test]
async fn wedge_upload_run_read() -> anyhow::Result<()> {
    let client = ()
        .serve(TokioChildProcess::new(
            Command::new(env!("CARGO_BIN_EXE_dokan")).configure(|cmd| {
                // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
                cmd.arg("--transport").arg("stdio").env("DOKAN_DEV_INSECURE", "1");
            }),
        )?)
        .await?;

    eprintln!("server: {:?}\n", client.peer_info());

    let tools = client.list_all_tools().await?;
    eprintln!("tools exposed: {}", tools.len());
    for t in &tools {
        eprintln!("  - {}", t.name);
    }
    assert!(tools.iter().any(|t| t.name == "upload_script"));

    // 1. upload
    let up = parse(
        &client
            .call_tool(CallToolRequestParams::new("upload_script").with_arguments(obj(json!({
                "name": "hello",
                "runtime": "python",
                "source": "import os\nprint('hello from dokan')\nprint('input was', os.environ.get('DOKAN_INPUT'))\nimport sys; print('to stderr', file=sys.stderr)\n",
                "description": "smoke test greeter"
            }))))
            .await?,
    );
    eprintln!("\nupload_script -> {up}");
    let script_id = up["script_id"].as_i64().expect("script_id");

    // 2. run (returns immediately)
    let run = parse(
        &client
            .call_tool(
                CallToolRequestParams::new("run_script")
                    .with_arguments(obj(json!({ "script_id": script_id, "input": {"n": 42} }))),
            )
            .await?,
    );
    eprintln!("run_script -> {run}");
    let run_id = run["run_id"].as_i64().expect("run_id");

    // 3. wait_for terminal status
    let waited = parse(
        &client
            .call_tool(
                CallToolRequestParams::new("wait_for")
                    .with_arguments(obj(json!({ "run_id": run_id, "timeout": 90 }))),
            )
            .await?,
    );
    let status = waited["status"].as_str().unwrap_or("").to_string();
    eprintln!("wait_for -> status={status}");

    // 4. read all logs from the start
    let logs = parse(
        &client
            .call_tool(
                CallToolRequestParams::new("read_logs")
                    .with_arguments(obj(json!({ "run_id": run_id, "after_cursor": 0 }))),
            )
            .await?,
    );
    eprintln!(
        "read_logs -> status={} next_cursor={}",
        logs["status"], logs["next_cursor"]
    );
    if let Some(lines) = logs["lines"].as_array() {
        for l in lines {
            eprintln!("    {}", l.as_str().unwrap_or_default());
        }
    }

    // 5. list_runs summary
    let list = parse(
        &client
            .call_tool(
                CallToolRequestParams::new("list_runs").with_arguments(obj(json!({ "limit": 5 }))),
            )
            .await?,
    );
    eprintln!("\nlist_runs -> {list}");

    client.cancel().await?;

    assert_eq!(status, "succeeded", "run should succeed");
    let body = logs["lines"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| l.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    assert!(body.contains("hello from dokan"), "stdout captured");
    assert!(body.contains("to stderr"), "stderr captured");
    eprintln!("\n✅ WEDGE PROVEN: agent uploaded + ran + read logs over MCP, zero UI.");
    Ok(())
}
