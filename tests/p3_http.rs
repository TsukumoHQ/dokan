//! P3 operator surface + ops: bearer auth, trigger→run via HTTP API, Prometheus
//! /metrics, secrets, and relay egress (job result POSTed to the mesh).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use sqlx::Row;
use tokio::process::Command;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

const DB_URL: &str = "postgres://dokan:dokan@127.0.0.1:5499/dokan";

#[tokio::test]
async fn operator_surface_and_relay() -> anyhow::Result<()> {
    let port = free_port();
    let relay_port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let token = "testtok";

    // 1. Relay receiver: count POSTed job results.
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let relay = Router::new().route(
        "/",
        post(move |Json(_b): Json<serde_json::Value>| {
            let h = hits2.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                Json(json!({"ok": true}))
            }
        }),
    );
    let rl = tokio::net::TcpListener::bind(("127.0.0.1", relay_port)).await?;
    tokio::spawn(async move { axum::serve(rl, relay).await.unwrap() });

    // 2. Boot dokan in http mode with auth + relay.
    let mut child = Command::new(env!("CARGO_BIN_EXE_dokan"))
        .args([
            "--transport", "http",
            "--addr", &format!("127.0.0.1:{port}"),
            "--token", token,
            "--relay-url", &format!("http://127.0.0.1:{relay_port}/"),
        ])
        .kill_on_drop(true)
        .spawn()?;

    let cli = reqwest::Client::new();
    let auth = format!("Bearer {token}");

    // Wait for boot (authed /metrics).
    let mut up = false;
    for _ in 0..50 {
        if let Ok(r) = cli.get(format!("{base}/metrics")).header("authorization", &auth).send().await {
            if r.status().is_success() {
                up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(up, "dokan http did not come up");

    // 3. Auth gate: no token -> 401.
    let r = cli.get(format!("{base}/api/runs")).send().await?;
    assert_eq!(r.status(), 401, "unauthenticated rejected");

    // 4. Seed a script directly, then trigger it via the HTTP API.
    let pool = sqlx::postgres::PgPool::connect(DB_URL).await?;
    let script_id: i64 = sqlx::query(
        "INSERT INTO scripts (name, runtime, source, description) \
         VALUES ('http-test', 'bash', 'echo hi-http\n', 'p3 http') RETURNING id",
    )
    .fetch_one(&pool)
    .await?
    .get("id");

    let r = cli
        .post(format!("{base}/api/runs"))
        .header("authorization", &auth)
        .json(&json!({"script_id": script_id}))
        .send()
        .await?;
    assert!(r.status().is_success(), "authed trigger ok");
    let run_id = r.json::<serde_json::Value>().await?["run_id"].as_i64().unwrap();

    // 5. Poll the run list until it succeeds.
    let mut succeeded = false;
    for _ in 0..60 {
        let body: serde_json::Value = cli
            .get(format!("{base}/api/runs?limit=50"))
            .header("authorization", &auth)
            .send()
            .await?
            .json()
            .await?;
        if let Some(arr) = body["recent"].as_array() {
            if arr.iter().any(|r| r["run_id"].as_i64() == Some(run_id) && r["status"] == "succeeded") {
                succeeded = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(succeeded, "triggered run reached succeeded");

    // 5b. Cancel is wired and guards terminal runs: canceling a succeeded run -> 409.
    let r = cli
        .post(format!("{base}/api/runs/{run_id}/cancel"))
        .header("authorization", &auth)
        .send()
        .await?;
    assert_eq!(r.status(), 409, "cancel on terminal run rejected");
    assert_eq!(
        r.json::<serde_json::Value>().await?["status"], "succeeded",
        "409 body reports current status"
    );

    // 6. Relay received the finish notification. The POST is fire-and-forget from the
    //    job task, so poll rather than asserting on a single fixed sleep (flaky under load).
    let mut relayed = false;
    for _ in 0..40 {
        if hits.load(Ordering::SeqCst) >= 1 {
            relayed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(relayed, "relay egress fired");

    // 7. Metrics expose the run counter.
    let metrics = cli
        .get(format!("{base}/metrics"))
        .header("authorization", &auth)
        .send()
        .await?
        .text()
        .await?;
    assert!(metrics.contains("dokan_runs_finished_total"), "metrics: {metrics}");

    // 8. Secrets: write-only, names listed.
    cli.post(format!("{base}/api/secrets"))
        .header("authorization", &auth)
        .json(&json!({"name": "API_KEY", "value": "s3cr3t"}))
        .send()
        .await?;
    let secrets: serde_json::Value = cli
        .get(format!("{base}/api/secrets"))
        .header("authorization", &auth)
        .send()
        .await?
        .json()
        .await?;
    assert!(
        secrets["secrets"].as_array().map(|a| a.iter().any(|n| n == "API_KEY")).unwrap_or(false),
        "secret name listed (value not exposed): {secrets}"
    );

    let _ = child.kill().await;
    Ok(())
}
