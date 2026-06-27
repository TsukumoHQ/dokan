//! P3 proof: semantic search_script via local fastembed + pgvector. A query with no
//! lexical overlap with the target description still ranks it first — substring match
//! cannot do this. Requires --embed (loads the BGE model from .fastembed_cache).

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
async fn call(c: &RunningService<RoleClient, ()>, name: &'static str, args: Value) -> Value {
    parse(
        &c.call_tool(CallToolRequestParams::new(name).with_arguments(obj(args)))
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn semantic_ranks_without_lexical_overlap() -> anyhow::Result<()> {
    let c = ()
        .serve(TokioChildProcess::new(
            Command::new(env!("CARGO_BIN_EXE_dokan")).configure(|cmd| {
                // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
                cmd.arg("--transport").arg("stdio").arg("--embed").env("DOKAN_DEV_INSECURE", "1");
            }),
        )?)
        .await?;

    // Distinct intents, each as a description. (DB is shared across runs, so assert on
    // the matched *description*, not a per-run id — duplicates may exist.)
    for (name, desc) in [
        ("a", "back up the production database and store the dump in s3"),
        ("b", "post a chat message to the team messaging channel"),
        ("c", "generate small preview thumbnails from uploaded photos"),
    ] {
        call(
            &c,
            "upload_script",
            json!({"name": name, "runtime":"bash", "source":"echo hi\n", "description": desc}),
        )
        .await;
    }

    // Query shares NO words with description "a" (no "backup"/"database"/"s3"/"dump").
    let res = call(
        &c,
        "search_script",
        json!({"query": "archive my sql records to cloud object storage", "limit": 3}),
    )
    .await;
    eprintln!("search -> {res}");

    assert_eq!(res["mode"], "semantic", "embedder active");
    let results = res["results"].as_array().expect("results");
    assert!(!results.is_empty(), "semantic returned candidates");
    let top_desc = results[0]["desc"].as_str().unwrap_or("");
    assert!(
        top_desc.contains("back up") && top_desc.contains("database"),
        "backup/db intent ranked first semantically, got: {top_desc}"
    );

    c.cancel().await?;
    Ok(())
}
