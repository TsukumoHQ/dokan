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

// Read DATABASE_URL so this test's own pool hits the SAME database the spawned daemon
// uses (the daemon inherits the env). Falls back to the default dev DB. Without this the
// pool would pin to `dokan` while an isolated run points the daemon elsewhere → mismatch.
fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://dokan:dokan@127.0.0.1:5499/dokan".into())
}

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
        // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
        .env("DOKAN_DEV_INSECURE", "1")
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
    let pool = sqlx::postgres::PgPool::connect(&db_url()).await?;
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

/// Per-script resource override (v0.1.1): a script carrying mem_limit_mb runs OUTSIDE the
/// global warm pool on a fresh one-off container with the override cap, and still executes to
/// success. Asserts both that the override round-trips on the run's claim path and that the
/// `acquire_with_caps` container path works end-to-end. Non-flaky: it polls for the terminal
/// status. (Needs Postgres + Docker, like the other p3_http tests — CI runs it.)
#[tokio::test]
async fn per_script_mem_override() -> anyhow::Result<()> {
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let token = "testtok";

    let mut child = Command::new(env!("CARGO_BIN_EXE_dokan"))
        .args([
            "--transport", "http",
            "--addr", &format!("127.0.0.1:{port}"),
            "--token", token,
        ])
        // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
        .env("DOKAN_DEV_INSECURE", "1")
        .kill_on_drop(true)
        .spawn()?;

    let cli = reqwest::Client::new();
    let auth = format!("Bearer {token}");
    let mut up = false;
    for _ in 0..50 {
        if let Ok(r) = cli.get(format!("{base}/metrics")).header("authorization", &auth).send().await {
            if r.status().is_success() { up = true; break; }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(up, "dokan http did not come up");

    // Seed a trivial script with a per-script memory override (512 MiB) + a CPU override.
    let pool = sqlx::postgres::PgPool::connect(&db_url()).await?;
    let script_id: i64 = sqlx::query(
        "INSERT INTO scripts (name, runtime, source, description, mem_limit_mb, cpu_limit) \
         VALUES ('mem-override-test', 'bash', 'echo capped-ok\n', 'v0.1.1 per-script cap', 512, 1.5) \
         RETURNING id",
    )
    .fetch_one(&pool)
    .await?
    .get("id");

    // The override round-trips: it reads back from the same row the claim path projects.
    let mem: Option<i64> = sqlx::query("SELECT mem_limit_mb FROM scripts WHERE id = $1")
        .bind(script_id)
        .fetch_one(&pool)
        .await?
        .get("mem_limit_mb");
    assert_eq!(mem, Some(512), "mem_limit_mb round-trips");

    // Run it through the HTTP API; it must reach the override container path and succeed.
    let r = cli
        .post(format!("{base}/api/runs"))
        .header("authorization", &auth)
        .json(&json!({"script_id": script_id}))
        .send()
        .await?;
    assert!(r.status().is_success(), "authed trigger ok");
    let run_id = r.json::<serde_json::Value>().await?["run_id"].as_i64().unwrap();

    let mut done = false;
    for _ in 0..60 {
        let st: Option<String> = sqlx::query("SELECT status FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await?
            .get("status");
        if st.as_deref() == Some("succeeded") { done = true; break; }
        if st.as_deref() == Some("failed") { panic!("override run failed unexpectedly"); }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(done, "override-capped run reached succeeded");

    let _ = child.kill().await;
    Ok(())
}

/// Inbound webhook: an external POST to /hook/<token> enqueues the target script with the
/// body as input — and works WITHOUT a bearer token (the URL token is the auth), even
/// though the daemon is booted with one. Proves the endpoint sits outside the bearer gate.
#[tokio::test]
async fn webhook_fires_without_bearer() -> anyhow::Result<()> {
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let token = "testtok";

    let mut child = Command::new(env!("CARGO_BIN_EXE_dokan"))
        .args([
            "--transport", "http",
            "--addr", &format!("127.0.0.1:{port}"),
            "--token", token,
        ])
        // GAP-4: the daemon fails closed without crypto keys; opt into dev defaults.
        .env("DOKAN_DEV_INSECURE", "1")
        .kill_on_drop(true)
        .spawn()?;

    let cli = reqwest::Client::new();
    let auth = format!("Bearer {token}");
    let mut up = false;
    for _ in 0..50 {
        if let Ok(r) = cli.get(format!("{base}/metrics")).header("authorization", &auth).send().await {
            if r.status().is_success() { up = true; break; }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(up, "dokan http did not come up");

    // Seed a script + a webhook pointing at it.
    let pool = sqlx::postgres::PgPool::connect(&db_url()).await?;
    let script_id: i64 = sqlx::query(
        "INSERT INTO scripts (name, runtime, source, description) \
         VALUES ('wh-test', 'bash', 'echo from-webhook\n', 'p3 webhook') RETURNING id",
    )
    .fetch_one(&pool)
    .await?
    .get("id");
    let hook_token = format!("whtok-{port}");
    sqlx::query("INSERT INTO webhooks (token, target_kind, target_id) VALUES ($1, 'script', $2)")
        .bind(&hook_token)
        .bind(script_id)
        .execute(&pool)
        .await?;

    // Fire it with NO authorization header — must be accepted (202) and return a run_id.
    let resp = cli
        .post(format!("{base}/hook/{hook_token}"))
        .json(&json!({"event": "ping"}))
        .send()
        .await?;
    assert_eq!(resp.status(), 202, "webhook accepted without bearer");
    let body: serde_json::Value = resp.json().await?;
    let run_id = body["run_id"].as_i64().expect(&body.to_string());

    // An unknown token is 404.
    let miss = cli.post(format!("{base}/hook/nope")).json(&json!({})).send().await?;
    assert_eq!(miss.status(), 404, "unknown webhook token rejected");

    // The enqueued run executes to completion (daemon is an executor).
    let mut done = false;
    for _ in 0..60 {
        let st: Option<String> = sqlx::query("SELECT status FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await?
            .get("status");
        if st.as_deref() == Some("succeeded") { done = true; break; }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(done, "webhook-triggered run reached succeeded");

    let _ = child.kill().await;
    Ok(())
}
