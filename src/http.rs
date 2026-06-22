//! Thin operator surface (P3): run list, trigger, live log tail (SSE), secrets, and a
//! Prometheus `/metrics` endpoint. Deliberately minimal — humans operate here; all
//! analytical/heavy data belongs in Grafana (PRD §8). The agent uses MCP, not this.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream::Stream;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Deserialize;
use serde_json::json;

use crate::db::Db;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub metrics: PrometheusHandle,
}

pub fn operator_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/metrics", get(metrics))
        .route("/api/runs", get(list_runs).post(trigger_run))
        .route("/api/runs/{id}/logs", get(run_logs))
        .route("/api/runs/{id}/stream", get(run_stream))
        .route("/api/secrets", get(list_secrets).post(set_secret))
        .with_state(state)
}

/// Bearer-token gate (RBAC slice). No-op when DOKAN_TOKEN is unset. Full OAuth 2.1 is
/// available via rmcp for P4; this protects the HTTP surface today.
pub async fn auth(
    State(token): State<Option<String>>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(expected) = token {
        let ok = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.strip_prefix("Bearer ").unwrap_or(v) == expected)
            .unwrap_or(false);
        if !ok {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    next.run(req).await
}

async fn metrics(State(s): State<AppState>) -> impl IntoResponse {
    s.metrics.render()
}

async fn index(State(s): State<AppState>) -> Html<String> {
    let counts = s.db.run_status_counts().await.unwrap_or_default();
    let counts_html: String = counts
        .iter()
        .map(|(k, v)| format!("<span class=pill>{k}: {v}</span>"))
        .collect();
    // Self-refreshing run list — the whole thin UI in one page.
    let page = format!(
        r#"<!doctype html><html><head><meta charset=utf-8><title>dokan</title>
<style>
body{{font:14px ui-monospace,monospace;background:#0b0e14;color:#cbd5e1;margin:2rem;max-width:60rem}}
h1{{color:#7dd3fc}} a{{color:#7dd3fc}}
.pill{{background:#1e293b;padding:.2rem .5rem;border-radius:.4rem;margin-right:.4rem}}
table{{border-collapse:collapse;width:100%;margin-top:1rem}}
td,th{{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #1e293b}}
.s-succeeded{{color:#4ade80}} .s-failed{{color:#f87171}} .s-running{{color:#fbbf24}} .s-pending{{color:#94a3b8}}
</style></head><body>
<h1>dokan 導管</h1><p>agent-operated script runtime · <a href=/metrics>metrics</a></p>
<div>{counts_html}</div>
<table id=runs><thead><tr><th>run</th><th>script</th><th>status</th><th>exit</th></tr></thead><tbody></tbody></table>
<script>
async function tick(){{
  const r = await fetch('/api/runs?limit=30').then(x=>x.json());
  const tb = document.querySelector('#runs tbody');
  tb.innerHTML = (r.recent||[]).map(x=>
    `<tr><td><a href="/api/runs/${{x.run_id}}/logs">#${{x.run_id}}</a></td><td>${{x.script_id}}</td>`
    +`<td class="s-${{x.status}}">${{x.status}}</td><td>${{x.exit??''}}</td></tr>`).join('');
}}
tick(); setInterval(tick, 1500);
</script></body></html>"#
    );
    Html(page)
}

#[derive(Deserialize)]
struct ListQ {
    limit: Option<i64>,
}

async fn list_runs(State(s): State<AppState>, Query(q): Query<ListQ>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(30).clamp(1, 200);
    let counts = s.db.run_status_counts().await.unwrap_or_default();
    let counts_obj: serde_json::Map<String, serde_json::Value> =
        counts.into_iter().map(|(k, v)| (k, json!(v))).collect();
    let rows = s.db.list_runs(None, limit).await.unwrap_or_default();
    let recent: Vec<_> = rows
        .iter()
        .map(|r| json!({"run_id": r.id, "script_id": r.script_id, "status": r.status, "exit": r.exit_code}))
        .collect();
    Json(json!({"counts": counts_obj, "recent": recent}))
}

#[derive(Deserialize)]
struct TriggerBody {
    script_id: i64,
    input: Option<serde_json::Value>,
}

async fn trigger_run(
    State(s): State<AppState>,
    Json(b): Json<TriggerBody>,
) -> impl IntoResponse {
    let input = b.input.unwrap_or(json!({}));
    match s.db.insert_run(b.script_id, &input).await {
        Ok(run_id) => {
            metrics::counter!("dokan_runs_enqueued_total").increment(1);
            (StatusCode::OK, Json(json!({"run_id": run_id, "status": "pending"})))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

#[derive(Deserialize)]
struct LogQ {
    after: Option<i64>,
}

async fn run_logs(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    Query(q): Query<LogQ>,
) -> impl IntoResponse {
    let after = q.after.unwrap_or(0);
    let status = s.db.run_status(id).await.ok().flatten().unwrap_or_default();
    let lines = s.db.read_logs_after(id, after, 500).await.unwrap_or_default();
    let next = lines.last().map(|l| l.seq).unwrap_or(after);
    let rendered: Vec<String> = lines
        .iter()
        .map(|l| format!("{}|{}|{}", l.seq, l.stream, l.line))
        .collect();
    Json(json!({"status": status, "lines": rendered, "next_cursor": next}))
}

/// Live log tail over SSE — for the human UI only (PRD §8). Polls the DB and emits new
/// lines until the run reaches a terminal status.
async fn run_stream(
    State(s): State<AppState>,
    Path(id): Path<i64>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = futures_util::stream::unfold(
        (s.db, id, 0i64, false),
        |(db, id, cursor, done)| async move {
            if done {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
            let lines = db.read_logs_after(id, cursor, 500).await.unwrap_or_default();
            let next = lines.last().map(|l| l.seq).unwrap_or(cursor);
            let status = db.run_status(id).await.ok().flatten().unwrap_or_default();
            let rendered: Vec<String> = lines
                .iter()
                .map(|l| format!("{}|{}|{}", l.seq, l.stream, l.line))
                .collect();
            let terminal = matches!(status.as_str(), "succeeded" | "failed" | "canceled");
            let data = json!({"status": status, "lines": rendered, "cursor": next}).to_string();
            let event = Event::default().data(data);
            Some((Ok(event), (db, id, next, terminal)))
        },
    );
    Sse::new(stream)
}

async fn list_secrets(State(s): State<AppState>) -> impl IntoResponse {
    // Names only — values are write-only over this surface.
    let names = s.db.secret_names().await.unwrap_or_default();
    Json(json!({"secrets": names}))
}

#[derive(Deserialize)]
struct SecretBody {
    name: String,
    value: String,
}

async fn set_secret(
    State(s): State<AppState>,
    Json(b): Json<SecretBody>,
) -> impl IntoResponse {
    match s.db.upsert_secret(&b.name, &b.value).await {
        Ok(()) => (StatusCode::OK, Json(json!({"name": b.name, "status": "set"}))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))),
    }
}
