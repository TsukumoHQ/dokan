//! Thin operator surface (P3): run list, trigger, live log tail (SSE), secrets, and a
//! Prometheus `/metrics` endpoint. Deliberately minimal — humans operate here; all
//! analytical/heavy data belongs in Grafana (PRD §8). The agent uses MCP, not this.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::net::{IpAddr, SocketAddr};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::Stream;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Deserialize;
use serde_json::json;

use crate::db::Db;
use crate::exec::Executor;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub exec: Arc<Executor>,
    pub metrics: PrometheusHandle,
}

pub fn operator_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/metrics", get(metrics))
        .route("/api/runs", get(list_runs).post(trigger_run))
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/runs/{id}/logs", get(run_logs))
        .route("/api/runs/{id}/stream", get(run_stream))
        .route("/api/runs/{id}/receipt", get(run_receipt))
        .route("/api/runs/{id}/verify", get(verify_run))
        .route("/api/runs/{id}/reproduce", post(reproduce_run_ep))
        .route("/api/receipt/pubkey", get(receipt_pubkey))
        .route("/api/scripts", get(list_scripts))
        .route("/api/blobs", get(list_blobs))
        .route("/api/schedules", get(list_schedules))
        .route("/api/secrets", get(list_secrets).post(set_secret))
        .with_state(state)
}

/// Inbound webhook router. Mounted OUTSIDE the bearer gate — an external service (Stripe,
/// GitHub…) can't send DOKAN_TOKEN, so the unguessable token in the URL is the auth. The
/// request body becomes the run's input. Keep this separate so the auth layer never wraps it.
pub fn webhook_router(state: AppState) -> Router {
    Router::new()
        .route("/hook/{token}", post(webhook_fire))
        .with_state(state)
}

/// Liveness/readiness probe. Mounted OUTSIDE the bearer gate so a monitor, load-balancer, or
/// the auto-merge-on-green flow's health step can curl it with no token. 200 when Postgres
/// answers, 503 when it doesn't; always reports the running binary's version.
pub fn health_router(state: AppState) -> Router {
    Router::new().route("/health", get(health)).with_state(state)
}

async fn health(State(s): State<AppState>) -> impl IntoResponse {
    let db_up = s.db.ping().await;
    let code = if db_up { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (
        code,
        Json(json!({
            "status": if db_up { "ok" } else { "degraded" },
            "version": env!("CARGO_PKG_VERSION"),
            "db": if db_up { "up" } else { "down" },
        })),
    )
}

/// Fire an inbound webhook: resolve the token → enqueue the target script/flow with the
/// POST body as input. Non-blocking (202 + id); the worker/flow engine runs it.
async fn webhook_fire(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let now = now_secs();
    // Per-source-IP guard FIRST — the cheapest, broadest cap, so a hammering/enumerating client
    // is shed before any token parse or DB work. The IP is the real client (X-Forwarded-For only
    // when the direct peer is a trusted proxy — else the spoofable peer), so behind a tunnel the
    // limit keys on the actual client, not the shared proxy IP.
    let ip = client_ip(&headers, peer.ip(), &TRUSTED_PROXIES);
    if !WEBHOOK_IP_LIMITER.check_at(&ip, now) {
        metrics::counter!("dokan_webhook_rate_limited_total").increment(1);
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }
    // Reject a malformed token shape BEFORE touching the DB — same 404 as "unknown" (no leak),
    // but a probe never reaches the webhooks table.
    if !is_well_formed_webhook_token(&token) {
        return (StatusCode::NOT_FOUND, "no such webhook").into_response();
    }
    // Per-token flood guard.
    if !WEBHOOK_LIMITER.check_at(&token, now) {
        metrics::counter!("dokan_webhook_rate_limited_total").increment(1);
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }
    let Some((kind, target_id, agent_id)) =
        s.db.find_webhook_by_token(&token).await.ok().flatten()
    else {
        // Same response for unknown vs malformed: don't leak which tokens exist.
        return (StatusCode::NOT_FOUND, "no such webhook").into_response();
    };
    // Body → input: parse JSON if we can, else wrap the raw text so the script still sees it.
    let input = serde_json::from_slice::<serde_json::Value>(&body)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&body) }));

    // Same at-least-once dedup as the script path below: collapse a redelivery to the first
    // enqueue (provider delivery-id header, else hash of token+body).
    let idem = webhook_idempotency_key(&token, &headers, &body);
    if kind == "flow" {
        let Some(spec) = s.db.get_flow_spec(target_id).await.ok().flatten() else {
            return (StatusCode::NOT_FOUND, "flow gone").into_response();
        };
        // Atomic insert-or-return: an at-least-once redelivery (or a true near-simultaneous
        // race) collapses to the first flow_run via the partial UNIQUE index — exactly-once,
        // not best-effort check-then-insert.
        match s.db.insert_flow_run_idempotent(target_id, &spec, &input, &idem).await {
            Ok((id, true)) => {
                metrics::counter!("dokan_webhook_fires_total", "target" => "flow").increment(1);
                (StatusCode::ACCEPTED, Json(json!({"flow_run_id": id, "status": "pending"}))).into_response()
            }
            Ok((id, false)) => {
                metrics::counter!("dokan_webhook_dedup_total").increment(1);
                let status = s
                    .db
                    .find_flow_run_by_idempotency(&idem)
                    .await
                    .ok()
                    .flatten()
                    .map(|(_, st)| st)
                    .unwrap_or_else(|| "pending".to_string());
                (StatusCode::OK, Json(json!({"flow_run_id": id, "status": status, "idempotent": true}))).into_response()
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response(),
        }
    } else {
        // Script target: webhook providers (Stripe/Calendly/GitHub) deliver at-least-once and
        // RETRY on a slow/non-2xx response; the atomic insert-or-return collapses an identical
        // redelivery (or a concurrent race) to the first run instead of spawning a duplicate.
        match s.db.insert_run_idempotent(target_id, &input, agent_id.as_deref(), None, false, &idem).await {
            Ok((id, true)) => {
                metrics::counter!("dokan_webhook_fires_total", "target" => "script").increment(1);
                (StatusCode::ACCEPTED, Json(json!({"run_id": id, "status": "pending"}))).into_response()
            }
            Ok((id, false)) => {
                metrics::counter!("dokan_webhook_dedup_total").increment(1);
                let status = s
                    .db
                    .find_run_by_idempotency(&idem)
                    .await
                    .ok()
                    .flatten()
                    .map(|(_, st)| st)
                    .unwrap_or_else(|| "pending".to_string());
                (StatusCode::OK, Json(json!({"run_id": id, "status": status, "idempotent": true}))).into_response()
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response(),
        }
    }
}

/// Idempotency key for an inbound webhook delivery. Prefers a provider delivery-id header
/// (Stripe/GitHub/generic) so legitimate distinct events with identical bodies aren't
/// collapsed; falls back to a hash of (token + raw body) so a verbatim retry of a provider
/// that sends no delivery id still dedups. Namespaced by token so two webhooks never collide.
/// Enforced exactly-once: the key feeds an atomic insert-or-return against a partial UNIQUE
/// index, so even near-simultaneous retries collapse to a single run.
fn webhook_idempotency_key(token: &str, headers: &HeaderMap, body: &[u8]) -> String {
    const DELIVERY_HEADERS: [&str; 4] =
        ["idempotency-key", "x-idempotency-key", "x-github-delivery", "x-request-id"];
    for h in DELIVERY_HEADERS {
        if let Some(v) = headers.get(h).and_then(|v| v.to_str().ok()) {
            let v = v.trim();
            if !v.is_empty() {
                return format!("wh:{token}:{h}:{v}");
            }
        }
    }
    use sha2::{Digest, Sha256};
    let mut hsh = Sha256::new();
    hsh.update(token.as_bytes());
    hsh.update([0x1f]);
    hsh.update(body);
    format!("wh:{token}:body:{:x}", hsh.finalize())
}

/// Constant-time byte equality — no early-exit on the first differing byte, so a presented
/// token can't be recovered by timing the compare. (Lengths differing returns false up front;
/// token length is not the secret.) Dep-free; sufficient for this RBAC slice.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Does the `Authorization` header carry exactly `Bearer <expected>`? The `Bearer ` scheme is
/// REQUIRED (a bare token with no scheme is rejected as malformed), and the token is compared
/// in constant time.
fn bearer_matches(auth_header: Option<&str>, expected: &str) -> bool {
    let Some(h) = auth_header else {
        return false;
    };
    let Some(tok) = h.strip_prefix("Bearer ") else {
        return false; // missing/!Bearer scheme → malformed → reject
    };
    ct_eq(tok.as_bytes(), expected.as_bytes())
}

/// Cheap structural validation of a webhook token BEFORE the DB lookup: an opaque URL-safe
/// id of bounded length. `crypto::random_token` emits 32 lowercase hex, which satisfies this;
/// the bound is deliberately a bit wider than the generator so a differently-seeded token
/// still resolves, while a path-traversal probe (`../`), empty, oversized, or junk path is
/// rejected without touching the webhooks table. The 404 is identical to "unknown" (no leak).
fn is_well_formed_webhook_token(t: &str) -> bool {
    (6..=128).contains(&t.len())
        && t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Minimal fixed-window rate limiter (dep-free). `check_at` is true while a key is under `max`
/// hits inside `window_secs`; `now_secs` is injected so the policy is unit-testable without a
/// clock. The map is pruned when it grows large so a flood of distinct keys can't OOM it.
pub struct FixedWindowLimiter {
    window_secs: u64,
    max: u32,
    inner: Mutex<HashMap<String, (u64, u32)>>, // key -> (window_start_secs, count)
}

impl FixedWindowLimiter {
    pub fn new(window_secs: u64, max: u32) -> Self {
        Self { window_secs, max, inner: Mutex::new(HashMap::new()) }
    }

    pub fn check_at(&self, key: &str, now_secs: u64) -> bool {
        let mut m = self.inner.lock().unwrap();
        if m.len() > 4096 {
            let window = self.window_secs;
            m.retain(|_, (start, _)| now_secs.saturating_sub(*start) < window);
        }
        let e = m.entry(key.to_string()).or_insert((now_secs, 0));
        if now_secs.saturating_sub(e.0) >= self.window_secs {
            *e = (now_secs, 0); // window rolled over → reset
        }
        if e.1 >= self.max {
            return false;
        }
        e.1 += 1;
        true
    }
}

/// Per-token flood guard for the inbound webhook path: at most 20 fires/sec per token. Bounds
/// abuse of a leaked token without throttling legitimate providers.
static WEBHOOK_LIMITER: LazyLock<FixedWindowLimiter> = LazyLock::new(|| FixedWindowLimiter::new(1, 20));

/// Per-source-IP guard for the inbound webhook path (TSU-162): a coarser anti-DoS / anti-token-
/// enumeration cap — at most 100 requests / 10s from one IP (≈10/s sustained, generous for a
/// retrying provider) so a single hammering client can't overload the endpoint or probe tokens
/// fast. Keyed by the ConnectInfo peer IP; complements the per-token limiter.
static WEBHOOK_IP_LIMITER: LazyLock<FixedWindowLimiter> = LazyLock::new(|| FixedWindowLimiter::new(10, 100));

/// Trusted reverse-proxy / tunnel IPs (TSU-189), from `DOKAN_TRUSTED_PROXIES` (comma-separated).
/// Empty by default → X-Forwarded-For is NEVER trusted (the safe default: a spoofed XFF must not
/// let an attacker forge their source IP and dodge the per-IP rate limit). Set this ONLY to the
/// IP(s) of the proxy/tunnel actually in front of dokan.
static TRUSTED_PROXIES: LazyLock<std::collections::HashSet<IpAddr>> = LazyLock::new(|| {
    std::env::var("DOKAN_TRUSTED_PROXIES")
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| s.trim().parse::<IpAddr>().ok())
        .collect()
});

/// Resolve the real client IP for rate-limiting. If the direct peer is a TRUSTED proxy, take the
/// left-most X-Forwarded-For entry (the original client it forwarded for); otherwise use the peer
/// directly. A spoofed XFF from an UNTRUSTED peer is ignored — so it can't forge the rate-limit key.
fn client_ip(headers: &HeaderMap, peer: IpAddr, trusted: &std::collections::HashSet<IpAddr>) -> String {
    if trusted.contains(&peer)
        && let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(first) = xff.split(',').map(|s| s.trim()).find(|s| !s.is_empty())
    {
        return first.to_string();
    }
    peer.to_string()
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Bearer-token gate (RBAC slice). No-op when DOKAN_TOKEN is unset. Full OAuth 2.1 is
/// available via rmcp for P4; this protects the HTTP surface today. The token is compared in
/// constant time and the `Bearer` scheme is required.
pub async fn auth(
    State(token): State<Option<String>>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(expected) = token {
        let presented = headers.get("authorization").and_then(|v| v.to_str().ok());
        if !bearer_matches(presented, &expected) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    next.run(req).await
}

async fn metrics(State(s): State<AppState>) -> impl IntoResponse {
    s.metrics.render()
}

/// The whole thin operator UI — one self-contained page. No external assets (the runtime
/// is network-isolated): CSS/JS are inline. Counts and the run list are driven entirely by
/// `/api/runs` so they can never drift; clicking a run opens a live SSE log tail.
async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang=en>
<head>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<meta name=color-scheme content=dark>
<title>dokan</title>
<style>
  :root{
    --bg:#0a0c12; --panel:#11151f; --panel-2:#161b26; --line:#1f2632; --line-soft:#161b26;
    --fg:#e6edf6; --fg-dim:#8b97a8; --fg-faint:#5b6675;
    --accent:#22d3ee; --accent-dim:#0e7490;
    --ok:#22c55e; --warn:#f59e0b; --bad:#ef4444;
    --mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;
    --sans:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
    --pending:#f59e0b; --running:#f59e0b; --succeeded:#22c55e; --failed:#ef4444; --canceled:#5b6675;
    --radius:12px; --shadow:0 1px 0 rgba(255,255,255,.03),0 12px 32px -12px rgba(0,0,0,.6);
    --nav:200px;
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{font:15px/1.55 var(--sans); background:var(--bg); color:var(--fg);
    -webkit-font-smoothing:antialiased; min-height:100dvh}
  a{color:var(--accent); text-decoration:none}
  a:hover{text-decoration:underline}
  .tnum{font-variant-numeric:tabular-nums}
  :focus-visible{outline:2px solid var(--accent); outline-offset:2px; border-radius:6px}
  /* shell: sidebar + main */
  .app{display:grid; grid-template-columns:var(--nav) minmax(0,1fr); min-height:100dvh}
  /* sidebar */
  .sidebar{position:sticky; top:0; align-self:start; height:100dvh; display:flex; flex-direction:column;
    gap:.25rem; padding:1rem .75rem; background:var(--panel); border-right:1px solid var(--line)}
  .logo{display:flex; align-items:center; gap:.55rem; padding:.35rem .5rem .9rem}
  .logo .mark{width:22px; height:22px; border-radius:7px; flex:none;
    background:linear-gradient(150deg,var(--accent),var(--accent-dim));
    box-shadow:0 0 0 1px rgba(34,211,238,.3),0 0 18px -4px rgba(34,211,238,.55)}
  .logo .wm{font-size:1.18rem; font-weight:650; letter-spacing:-.02em; color:var(--fg)}
  .navlist{display:flex; flex-direction:column; gap:.12rem}
  .navlink{display:flex; align-items:center; gap:.6rem; padding:.5rem .6rem; border-radius:9px;
    color:var(--fg-dim); font-size:.88rem; font-weight:500; cursor:pointer; position:relative;
    border:1px solid transparent; transition:color .15s,background .15s,border-color .15s}
  .navlink:hover{color:var(--fg); background:var(--panel-2); text-decoration:none}
  .navlink .ic{width:17px; height:17px; flex:none; stroke:currentColor; fill:none; stroke-width:1.75;
    stroke-linecap:round; stroke-linejoin:round}
  .navlink.active{color:var(--fg); background:var(--panel-2); border-color:var(--line)}
  .navlink.active::before{content:""; position:absolute; left:-.75rem; top:.4rem; bottom:.4rem; width:3px;
    border-radius:0 3px 3px 0; background:var(--accent); box-shadow:0 0 12px -1px var(--accent)}
  .navlink.active .ic{color:var(--accent)}
  .sidefoot{margin-top:auto; padding:.6rem .5rem .1rem; display:flex; flex-direction:column; gap:.5rem}
  .zerollm{display:inline-flex; align-items:center; gap:.4rem; font-family:var(--mono); font-size:.66rem;
    letter-spacing:.04em; color:var(--fg-faint); text-transform:uppercase}
  .zerollm .d{width:6px; height:6px; border-radius:50%; background:var(--accent); flex:none;
    box-shadow:0 0 8px -1px var(--accent)}
  .sidefoot a{font-family:var(--mono); font-size:.74rem; color:var(--fg-dim)}
  .sidefoot a:hover{color:var(--accent)}
  /* main */
  .main{min-width:0; display:flex; flex-direction:column}
  .ribbon{position:sticky; top:0; z-index:20; display:flex; align-items:center; gap:1rem; flex-wrap:wrap;
    padding:.8rem 1.4rem; background:color-mix(in srgb,var(--bg) 86%,transparent);
    backdrop-filter:blur(8px); border-bottom:1px solid var(--line)}
  .conn{display:flex; align-items:center; gap:.45rem; font-family:var(--mono); font-size:.78rem;
    color:var(--fg-dim)}
  .conn .dot{width:8px; height:8px; border-radius:50%; background:var(--ok); flex:none;
    box-shadow:0 0 0 0 rgba(34,211,238,.5); animation:pulse 2.4s infinite}
  .conn.down .dot{background:var(--bad); animation:none}
  .conn .sep{color:var(--fg-faint)}
  .pills{display:flex; align-items:center; gap:.4rem; flex-wrap:wrap}
  .pill{display:inline-flex; align-items:center; gap:.4rem; padding:.28rem .55rem; border-radius:999px;
    border:1px solid var(--line); background:var(--panel); font-size:.78rem; color:var(--fg-dim);
    cursor:pointer; transition:border-color .15s,background .15s}
  .pill:hover{border-color:var(--fg-faint)}
  .pill[aria-pressed=true]{border-color:var(--accent); background:var(--panel-2); color:var(--fg)}
  .pill .d{width:8px; height:8px; border-radius:50%; flex:none; background:var(--fg-faint)}
  .pill .c{font-family:var(--mono); font-variant-numeric:tabular-nums; color:var(--fg); font-weight:600}
  .pill.p-running .d{background:var(--running)} .pill.p-pending .d{background:var(--pending)}
  .pill.p-succeeded .d{background:var(--succeeded)} .pill.p-failed .d{background:var(--failed)}
  .pill.p-running[aria-pressed=true] .d{animation:pulse 1.6s infinite}
  .spacer{margin-left:auto}
  /* buttons */
  .btn{appearance:none; font:inherit; cursor:pointer; display:inline-flex; align-items:center; gap:.45rem;
    border-radius:9px; padding:.42rem .8rem; font-size:.82rem; font-weight:550; border:1px solid var(--line);
    background:var(--panel-2); color:var(--fg);
    transition:border-color .15s,background .15s,transform .06s,color .15s}
  .btn:hover{border-color:var(--fg-faint)}
  .btn:active{transform:translateY(1px)}
  .btn-go{border-color:color-mix(in srgb,var(--accent) 45%,transparent);
    background:color-mix(in srgb,var(--accent) 13%,var(--panel)); color:var(--accent)}
  .btn-go:hover{background:color-mix(in srgb,var(--accent) 22%,var(--panel)); border-color:var(--accent)}
  .btn .ic{width:14px; height:14px; stroke:currentColor; fill:none; stroke-width:1.9;
    stroke-linecap:round; stroke-linejoin:round}
  /* content */
  .content{padding:1.4rem; max-width:80rem; width:100%}
  .panel[hidden]{display:none}
  .panel-h{display:flex; align-items:baseline; gap:.7rem; margin:.2rem 0 1rem}
  .panel-h h2{font-size:1.1rem; font-weight:620; letter-spacing:-.01em; margin:0}
  .panel-h .sub{color:var(--fg-faint); font-family:var(--mono); font-size:.76rem}
  /* card + table */
  .card{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    box-shadow:var(--shadow); overflow:hidden}
  .card-h{display:flex; align-items:center; justify-content:space-between; padding:.8rem 1.1rem;
    border-bottom:1px solid var(--line)}
  .card-h h3{font-size:.78rem; font-weight:600; color:var(--fg-dim); margin:0;
    text-transform:uppercase; letter-spacing:.06em}
  .filter-tag{font-family:var(--mono); font-size:.74rem; color:var(--fg-dim)}
  .filter-tag b{color:var(--fg)}
  .filter-tag button{appearance:none; background:none; border:0; color:var(--accent); cursor:pointer;
    font:inherit; padding:0 0 0 .4rem}
  table{border-collapse:collapse; width:100%}
  th,td{text-align:left; padding:.62rem 1.1rem; font-size:.86rem}
  thead th{color:var(--fg-faint); font-weight:500; font-size:.7rem; text-transform:uppercase;
    letter-spacing:.06em; border-bottom:1px solid var(--line)}
  tbody tr{border-bottom:1px solid var(--line-soft); transition:background .12s}
  tbody tr:last-child{border-bottom:0}
  tbody tr.clk{cursor:pointer}
  tbody tr.clk:hover{background:var(--panel-2)}
  tbody td{vertical-align:top}
  td.run{font-family:var(--mono); font-variant-numeric:tabular-nums; color:var(--accent); white-space:nowrap}
  td.run .clip{margin-left:.45rem; color:var(--fg-faint)}
  td.run .clip .ic{width:13px; height:13px; vertical-align:-2px; stroke:currentColor; fill:none;
    stroke-width:1.75; stroke-linecap:round; stroke-linejoin:round}
  td.script{max-width:30rem}
  td.script .sname{font-weight:550; color:var(--fg)}
  td.script .sid{font-family:var(--mono); font-size:.74rem; color:var(--fg-faint); margin-left:.5rem; font-variant-numeric:tabular-nums}
  td.script .sby{margin-left:.5rem; font-size:.7rem; color:var(--fg-dim); font-family:var(--mono);
    padding:.05rem .4rem; border:1px solid var(--line); border-radius:999px}
  td.script .sby::before{content:"by "; color:var(--fg-faint)}
  td.script .sdesc{margin-top:.2rem; font-size:.78rem; color:var(--fg-dim); line-height:1.4; overflow:hidden;
    text-overflow:ellipsis; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical}
  td.script .sprog{margin-top:.2rem; font-size:.78rem; color:var(--accent); font-family:var(--mono);
    overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:36ch}
  td.when{color:var(--fg-faint); font-family:var(--mono); font-size:.78rem; white-space:nowrap}
  td.exit{font-family:var(--mono); color:var(--fg-faint); text-align:right; font-variant-numeric:tabular-nums}
  td.act{text-align:right; white-space:nowrap; width:1%}
  .rowbtns{display:inline-flex; gap:.3rem; opacity:0; transition:opacity .12s}
  tbody tr:hover .rowbtns,tbody tr:focus-within .rowbtns{opacity:1}
  .iconbtn{appearance:none; cursor:pointer; width:28px; height:28px; display:inline-grid; place-items:center;
    border-radius:7px; border:1px solid var(--line); background:var(--panel); color:var(--fg-dim);
    transition:color .12s,border-color .12s,background .12s}
  .iconbtn:hover{color:var(--fg); border-color:var(--fg-faint); background:var(--panel-2)}
  .iconbtn.danger:hover{color:var(--failed); border-color:color-mix(in srgb,var(--failed) 50%,transparent)}
  .iconbtn .ic{width:14px; height:14px; stroke:currentColor; fill:none; stroke-width:1.9;
    stroke-linecap:round; stroke-linejoin:round}
  /* status badge */
  .badge{display:inline-flex; align-items:center; gap:.45rem; font-size:.8rem}
  .sdot{width:8px; height:8px; border-radius:50%; flex:none; background:var(--fg-faint)}
  .badge .sdot{box-shadow:0 0 0 3px color-mix(in srgb,currentColor 14%,transparent)}
  .s-pending .badge,.s-pending .sdot~*{color:var(--pending)}
  .s-pending .sdot{background:var(--pending)} .s-pending .badge{color:var(--pending)}
  .s-running .sdot{background:var(--running)} .s-running .badge{color:var(--running)}
  .s-succeeded .sdot{background:var(--succeeded)} .s-succeeded .badge{color:var(--succeeded)}
  .s-failed .sdot{background:var(--failed)} .s-failed .badge{color:var(--failed)}
  .s-canceled .sdot{background:var(--canceled)} .s-canceled .badge{color:var(--canceled)}
  .s-running .badge .sdot{animation:pulse 1.6s infinite}
  /* chips (script flags) */
  .chips{display:flex; flex-wrap:wrap; gap:.35rem; margin-top:.55rem}
  .chip{display:inline-flex; align-items:center; gap:.35rem; font-family:var(--mono); font-size:.7rem;
    padding:.12rem .45rem; border-radius:6px; border:1px solid var(--line); color:var(--fg-dim);
    background:var(--panel-2)}
  .chip.on{color:var(--accent); border-color:color-mix(in srgb,var(--accent) 40%,transparent);
    background:color-mix(in srgb,var(--accent) 10%,transparent)}
  .chip.off{color:var(--fg-faint)}
  .chip .ic{width:12px; height:12px; stroke:currentColor; fill:none; stroke-width:1.75;
    stroke-linecap:round; stroke-linejoin:round}
  /* scripts grid */
  .grid{display:grid; grid-template-columns:repeat(auto-fill,minmax(19rem,1fr)); gap:.85rem}
  .scard{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    padding:.9rem 1rem; box-shadow:var(--shadow); transition:border-color .15s}
  .scard:hover{border-color:var(--fg-faint)}
  .scard .top{display:flex; align-items:baseline; gap:.5rem}
  .scard .nm{font-weight:600; color:var(--fg)}
  .scard .rt{font-family:var(--mono); font-size:.7rem; color:var(--fg-faint); padding:.08rem .4rem;
    border:1px solid var(--line); border-radius:999px}
  .scard .id{margin-left:auto; font-family:var(--mono); font-size:.74rem; color:var(--fg-faint); font-variant-numeric:tabular-nums}
  .scard .ds{margin-top:.4rem; font-size:.8rem; color:var(--fg-dim); line-height:1.45; overflow:hidden;
    text-overflow:ellipsis; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical}
  /* secrets */
  .seclist{display:flex; flex-wrap:wrap; gap:.45rem; padding:1rem 1.1rem}
  .seckey{display:inline-flex; align-items:center; gap:.45rem; font-family:var(--mono); font-size:.8rem;
    color:var(--fg); padding:.3rem .6rem; border:1px solid var(--line); border-radius:8px; background:var(--panel-2)}
  .seckey .ic{width:13px; height:13px; color:var(--accent); stroke:currentColor; fill:none; stroke-width:1.75;
    stroke-linecap:round; stroke-linejoin:round}
  /* skeleton + empty */
  .skel{height:.9rem; border-radius:5px; width:60%; margin:1.1rem;
    background:linear-gradient(90deg,var(--panel-2),var(--line),var(--panel-2));
    background-size:200% 100%; animation:shimmer 1.3s infinite}
  .empty{padding:3rem 1rem; text-align:center; color:var(--fg-faint)}
  .empty .big{font-size:1rem; color:var(--fg-dim); margin-bottom:.3rem}
  .empty .ic{width:30px; height:30px; margin:0 auto .7rem; display:block; color:var(--fg-faint);
    stroke:currentColor; fill:none; stroke-width:1.5; stroke-linecap:round; stroke-linejoin:round}
  /* composer */
  .composer{overflow:hidden; max-height:0; opacity:0; transition:max-height .26s ease,opacity .2s}
  .composer.open{max-height:26rem; opacity:1}
  .composer .box{margin:1rem 1.4rem 0; background:var(--panel); border:1px solid var(--line);
    border-radius:var(--radius); box-shadow:var(--shadow); padding:1rem 1.1rem; display:grid; gap:.75rem}
  .composer .grid2{display:grid; grid-template-columns:9rem 1fr; gap:.75rem; align-items:start}
  .composer label{display:block; font-size:.72rem; text-transform:uppercase; letter-spacing:.06em;
    color:var(--fg-faint); margin-bottom:.35rem}
  .composer input,.composer textarea{width:100%; font-family:var(--mono); font-size:.84rem; color:var(--fg);
    background:var(--bg); border:1px solid var(--line); border-radius:8px; padding:.5rem .6rem; resize:vertical}
  .composer input:focus,.composer textarea:focus{outline:none; border-color:var(--accent)}
  .composer textarea{min-height:3.4rem; line-height:1.5}
  .composer .row{display:flex; align-items:center; gap:.6rem; justify-content:flex-end}
  .composer .hint{margin-right:auto; font-family:var(--mono); font-size:.74rem; color:var(--fg-faint)}
  .composer .hint.bad{color:var(--failed)}
  /* drawer */
  .scrim{position:fixed; inset:0; background:rgba(5,7,11,.6); backdrop-filter:blur(2px); opacity:0;
    pointer-events:none; transition:opacity .2s; z-index:40}
  .scrim.open{opacity:1; pointer-events:auto}
  .drawer{position:fixed; top:0; right:0; height:100dvh; width:min(46rem,100%); background:var(--panel);
    border-left:1px solid var(--line); z-index:50; transform:translateX(100%);
    transition:transform .24s cubic-bezier(.22,.61,.36,1); display:flex; flex-direction:column;
    box-shadow:-24px 0 60px -30px rgba(0,0,0,.8)}
  .drawer.open{transform:none}
  .drawer-h{display:flex; align-items:center; gap:.9rem; padding:1rem 1.2rem; border-bottom:1px solid var(--line)}
  .drawer-h .title{font-family:var(--mono); font-weight:600; font-size:1rem}
  .drawer-h .x{margin-left:auto; appearance:none; background:none; border:1px solid var(--line);
    color:var(--fg-dim); width:32px; height:32px; border-radius:8px; cursor:pointer; font-size:1.1rem}
  .drawer-h .x:hover{color:var(--fg); border-color:var(--fg-faint)}
  .tabs{display:flex; gap:.2rem; padding:.5rem .8rem 0; border-bottom:1px solid var(--line)}
  .tab{appearance:none; background:none; border:0; border-bottom:2px solid transparent; cursor:pointer;
    font:inherit; font-size:.82rem; font-weight:550; color:var(--fg-dim); padding:.45rem .7rem;
    transition:color .15s,border-color .15s}
  .tab:hover{color:var(--fg)}
  .tab.active{color:var(--fg); border-bottom-color:var(--accent)}
  .tabpane{flex:1; overflow:auto; min-height:0}
  .tabpane[hidden]{display:none}
  .log{height:100%; overflow:auto; padding:.9rem 1.2rem; margin:0; font-family:var(--mono); font-size:12.5px;
    line-height:1.7; white-space:pre-wrap; word-break:break-word; background:var(--bg)}
  .log .ln{display:block}
  .log .seq{color:var(--fg-faint); user-select:none; margin-right:.9rem; display:inline-block; min-width:2.5ch; text-align:right}
  .log .err{color:var(--failed)} .log .out{color:var(--fg)} .log .waiting{color:var(--fg-faint)}
  .rcpt{padding:1rem 1.2rem; display:flex; flex-direction:column; gap:.1rem}
  .rrow{display:flex; align-items:baseline; gap:.8rem; padding:.5rem 0; border-bottom:1px solid var(--line-soft)}
  .rrow:last-child{border-bottom:0}
  .rrow .k{font-size:.72rem; text-transform:uppercase; letter-spacing:.05em; color:var(--fg-faint);
    width:11rem; flex:none}
  .rrow .v{font-family:var(--mono); font-size:.82rem; color:var(--fg); word-break:break-all; font-variant-numeric:tabular-nums}
  .rrow .v .clip{margin-left:.4rem; color:var(--fg-faint); cursor:pointer}
  .rrow .v .clip .ic{width:13px; height:13px; vertical-align:-2px; stroke:currentColor; fill:none;
    stroke-width:1.75; stroke-linecap:round; stroke-linejoin:round}
  .detbadge{display:inline-flex; align-items:center; gap:.4rem; font-family:var(--mono); font-size:.74rem;
    padding:.15rem .5rem; border-radius:999px}
  .detbadge.det{color:var(--accent); border:1px solid color-mix(in srgb,var(--accent) 45%,transparent);
    background:color-mix(in srgb,var(--accent) 12%,transparent)}
  .detbadge.adv{color:var(--warn); border:1px solid color-mix(in srgb,var(--warn) 45%,transparent);
    background:color-mix(in srgb,var(--warn) 12%,transparent)}
  .artlist{padding:1rem 1.2rem; display:flex; flex-direction:column; gap:.4rem}
  .art{display:flex; align-items:center; gap:.7rem; padding:.55rem .7rem; border:1px solid var(--line);
    border-radius:9px; background:var(--panel-2)}
  .art .nm{font-weight:550; color:var(--fg); font-size:.84rem}
  .art .sha{margin-left:auto; font-family:var(--mono); font-size:.74rem; color:var(--fg-dim); cursor:pointer}
  .art .ic{width:15px; height:15px; color:var(--accent); flex:none; stroke:currentColor; fill:none;
    stroke-width:1.75; stroke-linecap:round; stroke-linejoin:round}
  /* toast */
  .toasts{position:fixed; bottom:1.25rem; left:50%; transform:translateX(-50%); z-index:60;
    display:flex; flex-direction:column; gap:.5rem; align-items:center; pointer-events:none}
  .toast{font-family:var(--mono); font-size:.8rem; padding:.5rem .85rem; border-radius:9px;
    background:var(--panel-2); border:1px solid var(--line); color:var(--fg); box-shadow:var(--shadow);
    animation:toastin .22s ease}
  .toast.ok{border-color:color-mix(in srgb,var(--ok) 50%,transparent); color:var(--ok)}
  .toast.err{border-color:color-mix(in srgb,var(--bad) 50%,transparent); color:var(--bad)}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(34,211,238,.5)}70%{box-shadow:0 0 0 6px rgba(34,211,238,0)}100%{box-shadow:0 0 0 0 rgba(34,211,238,0)}}
  @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
  @keyframes toastin{from{opacity:0; transform:translateY(6px)}to{opacity:1; transform:none}}
  @media (max-width:780px){
    .app{grid-template-columns:1fr}
    .sidebar{position:static; height:auto; flex-direction:row; flex-wrap:wrap; align-items:center; gap:.3rem}
    .navlist{flex-direction:row; flex-wrap:wrap}
    .navlink.active::before{display:none}
    .sidefoot{margin:0 0 0 auto; flex-direction:row; align-items:center; gap:.7rem; padding:0}
    .logo{padding:.35rem .5rem; width:100%}
  }
  @media (prefers-reduced-motion:reduce){*{animation:none!important; transition:none!important}}
</style>
</head>
<body>
<div class=app>
  <nav class=sidebar aria-label="sections">
    <div class=logo>
      <span class=mark aria-hidden=true></span>
      <span class=wm>dokan</span>
    </div>
    <div class=navlist id=nav role=tablist>
      <a class=navlink data-nav=runs href='#/runs' role=tab>
        <svg class=ic viewBox="0 0 24 24"><path d="M3 12h4l2.5 7 4-14 2.5 7h5"/></svg> Runs</a>
      <a class=navlink data-nav=scripts href='#/scripts' role=tab>
        <svg class=ic viewBox="0 0 24 24"><path d="M8 6l-5 6 5 6"/><path d="M16 6l5 6-5 6"/></svg> Scripts</a>
      <a class=navlink data-nav=schedules href='#/schedules' role=tab>
        <svg class=ic viewBox="0 0 24 24"><circle cx=12 cy=12 r=9/><path d="M12 7v5l3 2"/></svg> Schedules</a>
      <a class=navlink data-nav=secrets href='#/secrets' role=tab>
        <svg class=ic viewBox="0 0 24 24"><rect x=5 y=11 width=14 height=9 rx=2/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg> Secrets</a>
      <a class=navlink data-nav=artifacts href='#/artifacts' role=tab>
        <svg class=ic viewBox="0 0 24 24"><path d="M3 7l9-4 9 4v10l-9 4-9-4V7z"/><path d="M3 7l9 4 9-4"/><path d="M12 11v10"/></svg> Artifacts</a>
      <a class=navlink data-nav=flows href='#/flows' role=tab>
        <svg class=ic viewBox="0 0 24 24"><circle cx=6 cy=6 r=2.5/><circle cx=6 cy=18 r=2.5/><circle cx=18 cy=12 r=2.5/><path d="M8.5 6H14a2 2 0 0 1 2 2v1.5M8.5 18H14a2 2 0 0 0 2-2v-1.5"/></svg> Flows</a>
    </div>
    <div class=sidefoot>
      <span class=zerollm><span class=d></span> zero LLM inside</span>
      <a href=/metrics>/metrics</a>
    </div>
  </nav>

  <div class=main>
    <header class=ribbon aria-label="system status">
      <span class=conn id=conn><span class=dot></span> <span id=connLabel>connecting</span></span>
      <span class=sep aria-hidden=true>·</span>
      <div class=pills id=pills aria-label="run status counts"></div>
      <div class=spacer></div>
      <button class="btn btn-go" id=newRunBtn aria-expanded=false aria-controls=composer>
        <svg class=ic viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg> Trigger run
      </button>
    </header>

    <div class=composer id=composer>
      <div class=box>
        <div class=grid2>
          <div>
            <label for=scriptId>Script ID</label>
            <input id=scriptId type=number inputmode=numeric min=1 placeholder="e.g. 42" autocomplete=off>
          </div>
          <div>
            <label for=runInput>Input (JSON, optional)</label>
            <textarea id=runInput spellcheck=false placeholder="{ }"></textarea>
          </div>
        </div>
        <div class=row>
          <span class=hint id=composerHint>enqueues a run for the given script</span>
          <button class=btn id=composerCancel type=button>Cancel</button>
          <button class="btn btn-go" id=composerSubmit type=button>
            <svg class=ic viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg> Run
          </button>
        </div>
      </div>
    </div>

    <div class=content>
      <!-- RUNS -->
      <section class=panel data-panel=runs>
        <div class=panel-h><h2>Runs</h2><span class=sub id=runsSub></span></div>
        <div class=card>
          <div class=card-h><h3>Recent runs</h3><span class=filter-tag id=filterTag></span></div>
          <table>
            <thead><tr><th>Run</th><th>Script</th><th>Status</th><th class=when>When</th><th class=exit>Exit</th><th class=act></th></tr></thead>
            <tbody id=rows><tr><td colspan=6><div class=skel></div></td></tr></tbody>
          </table>
        </div>
      </section>

      <!-- SCRIPTS -->
      <section class=panel data-panel=scripts hidden>
        <div class=panel-h><h2>Scripts</h2><span class=sub id=scriptsSub></span></div>
        <div class=grid id=scriptGrid></div>
      </section>

      <!-- SCHEDULES -->
      <section class=panel data-panel=schedules hidden>
        <div class=panel-h><h2>Schedules</h2><span class=sub id=schedSub></span></div>
        <div class=card>
          <div class=card-h><h3>Cron jobs</h3></div>
          <table>
            <thead><tr><th>Script</th><th>Cron</th><th>Next fire</th><th class=exit>Script ID</th></tr></thead>
            <tbody id=schedRows></tbody>
          </table>
        </div>
      </section>

      <!-- SECRETS -->
      <section class=panel data-panel=secrets hidden>
        <div class=panel-h><h2>Secrets</h2><span class=sub>names only — values are write-only here</span></div>
        <div class=card>
          <div class=card-h><h3>Set a secret</h3></div>
          <div class=box style="margin:0;border:0;box-shadow:none;display:grid;gap:.75rem;padding:1rem 1.1rem">
            <div class=grid2 style="grid-template-columns:14rem 1fr">
              <div>
                <label for=secName>Name</label>
                <input id=secName placeholder="OPENAI_API_KEY" autocomplete=off spellcheck=false>
              </div>
              <div>
                <label for=secVal>Value</label>
                <input id=secVal type=password placeholder="sk-…" autocomplete=off spellcheck=false>
              </div>
            </div>
            <div class=row>
              <span class=hint id=secHint>stored encrypted; injected as an env var into matching runs</span>
              <button class="btn btn-go" id=secSubmit type=button>Set secret</button>
            </div>
          </div>
        </div>
        <div class=card style="margin-top:.85rem">
          <div class=card-h><h3>Provisioned</h3><span class=filter-tag id=secCount></span></div>
          <div class=seclist id=secList></div>
        </div>
      </section>

      <!-- ARTIFACTS -->
      <section class=panel data-panel=artifacts hidden>
        <div class=panel-h><h2>Artifacts</h2><span class=sub id=blobSub></span></div>
        <div class=card>
          <div class=card-h><h3>Blob store</h3></div>
          <table>
            <thead><tr><th>Handle</th><th class=exit>Size</th><th class=when>Created</th><th class=when>Last used</th></tr></thead>
            <tbody id=blobRows></tbody>
          </table>
        </div>
      </section>

      <!-- FLOWS -->
      <section class=panel data-panel=flows hidden>
        <div class=panel-h><h2>Flows</h2><span class=sub>declarative DAGs</span></div>
        <div class=card>
          <div class=empty>
            <svg class=ic viewBox="0 0 24 24"><circle cx=6 cy=6 r=2.5/><circle cx=6 cy=18 r=2.5/><circle cx=18 cy=12 r=2.5/><path d="M8.5 6H14a2 2 0 0 1 2 2v1.5M8.5 18H14a2 2 0 0 0 2-2v-1.5"/></svg>
            <div class=big>Wire flows over MCP</div>
            <div>Compose a DAG with the <span style="font-family:var(--mono);color:var(--fg-dim)">compose_flow</span> tool, then run it with <span style="font-family:var(--mono);color:var(--fg-dim)">run_flow</span>. Flow runs surface in the runs feed.</div>
          </div>
        </div>
      </section>
    </div>
  </div>
</div>

<div class=toasts id=toasts aria-live=polite></div>

<div class=scrim id=scrim></div>
<aside class=drawer id=drawer role=dialog aria-modal=true aria-label="run detail" tabindex=-1>
  <div class=drawer-h>
    <span class=title id=drawerTitle>run</span>
    <span class=badge id=drawerStatus></span>
    <button class="iconbtn danger" id=drawerCancel aria-label="cancel run" hidden style="margin-left:auto">
      <svg class=ic viewBox="0 0 24 24"><rect x=6 y=6 width=12 height=12 rx=1.5/></svg>
    </button>
    <button class=x id=drawerClose aria-label="close">&times;</button>
  </div>
  <div class=tabs role=tablist aria-label="run views">
    <button class="tab active" data-tab=logs role=tab aria-selected=true>Logs</button>
    <button class=tab data-tab=receipt role=tab aria-selected=false>Receipt</button>
    <button class=tab data-tab=artifacts role=tab aria-selected=false>Artifacts</button>
  </div>
  <pre class="log tabpane" id=drawerLog data-pane=logs></pre>
  <div class="tabpane" id=drawerReceipt data-pane=receipt hidden></div>
  <div class="tabpane" id=drawerArtifacts data-pane=artifacts hidden></div>
</aside>

<script>
const ORDER=['running','pending','succeeded','failed','canceled'];
const PANELS=['runs','scripts','schedules','secrets','artifacts','flows'];
const TERMINAL=['succeeded','failed','canceled'];
let filter=null, es=null;
const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const $=id=>document.getElementById(id);
const ICLIP='<svg class=ic viewBox="0 0 24 24"><rect x=9 y=9 width=11 height=11 rx=2/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>';

function ago(iso){
  if(!iso) return '';
  const s=Math.max(0,(Date.now()-new Date(iso).getTime())/1000);
  if(s<60) return Math.floor(s)+'s ago';
  if(s<3600) return Math.floor(s/60)+'m ago';
  if(s<86400) return Math.floor(s/3600)+'h ago';
  return Math.floor(s/86400)+'d ago';
}
function when(iso){
  if(!iso) return '';
  try{ return new Date(iso).toLocaleString() }catch(_){ return iso }
}
function hsize(n){
  n=Number(n)||0;
  if(n<1024) return n+' B';
  if(n<1048576) return (n/1024).toFixed(1)+' KiB';
  if(n<1073741824) return (n/1048576).toFixed(1)+' MiB';
  return (n/1073741824).toFixed(2)+' GiB';
}
const trunc=(s,n)=>{ s=String(s||''); return s.length>n?s.slice(0,n)+'…':s; };
function toast(msg,kind){
  const t=document.createElement('div');
  t.className='toast'+(kind?' '+kind:''); t.textContent=msg;
  $('toasts').appendChild(t);
  setTimeout(()=>{ t.style.transition='opacity .25s'; t.style.opacity='0';
    setTimeout(()=>t.remove(),250); },2600);
}
function copy(text,note){
  navigator.clipboard?.writeText(String(text)).then(
    ()=>toast(note||'copied','ok'), ()=>toast('copy failed','err'));
}
async function act(url,body){
  const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},
    body:body?JSON.stringify(body):undefined});
  let d={}; try{ d=await r.json() }catch(_){}
  if(!r.ok) throw new Error(d.error||('HTTP '+r.status));
  return d;
}
async function reRun(scriptId){
  try{ const d=await act('/api/runs',{script_id:Number(scriptId)});
    toast('run #'+d.run_id+' enqueued','ok'); tick(); openDrawer(d.run_id,'pending');
  }catch(e){ toast(e.message,'err'); }
}
async function cancelRun(id){
  try{ await act('/api/runs/'+id+'/cancel'); toast('run #'+id+' canceled','ok'); tick(); }
  catch(e){ toast(e.message,'err'); }
}

/* ---- routing ---- */
function currentRoute(){ const h=(location.hash||'').replace('#/',''); return PANELS.includes(h)?h:'runs'; }
function route(){
  const r=currentRoute();
  for(const p of PANELS){
    const sec=document.querySelector('[data-panel='+p+']'); if(sec) sec.hidden=(p!==r);
    const n=document.querySelector('[data-nav='+p+']');
    if(n){ n.classList.toggle('active',p===r); n.setAttribute('aria-selected',p===r?'true':'false'); }
  }
  loadPanel(r);
}
function loadPanel(r){
  if(r==='scripts') fetchScripts();
  else if(r==='schedules') fetchSchedules();
  else if(r==='secrets') fetchSecrets();
  else if(r==='artifacts') fetchBlobs();
}
addEventListener('hashchange',route);

/* ---- runs feed (also drives the ribbon) ---- */
let last={counts:{},recent:[]};
function tick(){
  return fetch('/api/runs?limit=50').then(x=>x.json()).then(d=>{
    setConn(true); last=d; renderPills(d.counts||{}); renderRows(d.recent||[]);
  }).catch(()=>setConn(false));
}
function setConn(ok){
  const c=$('conn'); c.classList.toggle('down',!ok);
  $('connLabel').textContent=ok?'live':'reconnecting';
}
function renderPills(counts){
  const tile=(k,n)=>`<button class="pill p-${k}" data-f="${k}" aria-pressed="${filter===k}">`
    +`<span class=d></span>${esc(k)} <span class=c>${n||0}</span></button>`;
  $('pills').innerHTML=['running','pending','succeeded','failed'].map(k=>tile(k,counts[k])).join('');
  for(const b of $('pills').querySelectorAll('.pill'))
    b.onclick=()=>{ const f=b.dataset.f; filter=(f===filter)?null:f;
      if(currentRoute()!=='runs') location.hash='#/runs'; renderRows(last.recent||[]); renderPills(last.counts||{}); };
  const total=Object.values(counts).reduce((a,b)=>a+b,0);
  $('runsSub').textContent=total+' run'+(total===1?'':'s');
}
function renderRows(recent){
  const list=filter?recent.filter(r=>r.status===filter):recent;
  const ft=$('filterTag');
  ft.innerHTML=filter?`filtered: <b>${esc(filter)}</b><button id=clearF>clear</button>`:'';
  if(filter && $('clearF')) $('clearF').onclick=()=>{ filter=null; renderRows(last.recent||[]); renderPills(last.counts||{}); };
  if(!list.length){
    $('rows').innerHTML=`<tr><td colspan=6><div class=empty>`
      +`<div class=big>${recent.length?'No runs match this filter':'No runs yet'}</div>`
      +`<div>${recent.length?'Try clearing the filter.':'Trigger a script to see it here.'}</div></div></td></tr>`;
    return;
  }
  const ICX='<svg class=ic viewBox="0 0 24 24"><rect x=6 y=6 width=12 height=12 rx=1.5/></svg>';
  const IRUN='<svg class=ic viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg>';
  const IFILE='<svg class=ic viewBox="0 0 24 24"><path d="M21 15l-5-5L5 21"/><path d="M14 4h6v6"/><path d="M4 4h6v6H4z"/></svg>';
  $('rows').innerHTML=list.map(r=>{
    const live=!TERMINAL.includes(r.status);
    const nblobs=r.input_blobs&&typeof r.input_blobs==='object'?Object.keys(r.input_blobs).length:0;
    const cancel=live?`<button class="iconbtn danger" data-act=cancel data-id="${r.run_id}" title="cancel run" aria-label="cancel run #${r.run_id}">${ICX}</button>`:'';
    return `<tr class="clk s-${esc(r.status)}" data-id="${r.run_id}" data-st="${esc(r.status)}" tabindex=0>`
    +`<td class=run>#${r.run_id}${nblobs?`<span class=clip title="${nblobs} input file${nblobs===1?'':'s'}">${IFILE}</span>`:''}</td>`
    +`<td class=script><span class=sname>${esc(r.script_name||('script '+r.script_id))}</span>`
    +`<span class=sid>#${r.script_id}</span>`
    +(r.created_by?`<span class=sby>${esc(r.created_by)}</span>`:'')
    +(r.script_desc?`<div class=sdesc>${esc(r.script_desc)}</div>`:'')
    +(r.progress&&live?`<div class=sprog title="live progress">▸ ${esc(r.progress)}</div>`:'')+`</td>`
    +`<td><span class=badge><span class=sdot></span>${esc(r.status)}</span></td>`
    +`<td class=when title="${esc(r.created_at||'')}">${esc(ago(r.created_at))}</td>`
    +`<td class="exit tnum">${r.exit??'·'}</td>`
    +`<td class=act><span class=rowbtns>${cancel}`
    +`<button class=iconbtn data-act=rerun data-sid="${r.script_id}" title="run this script again" aria-label="re-run script ${r.script_id}">${IRUN}</button>`
    +`<button class=iconbtn data-act=copy data-id="${r.run_id}" title="copy run id" aria-label="copy run id #${r.run_id}">${ICLIP}</button>`
    +`</span></td></tr>`;
  }).join('');
  for(const tr of $('rows').querySelectorAll('tr[data-id]')){
    const open=e=>{ if(e.target.closest('[data-act]')) return; openDrawer(tr.dataset.id, tr.dataset.st); };
    tr.onclick=open;
    tr.onkeydown=e=>{ if(e.key==='Enter') open(e); };
  }
  for(const b of $('rows').querySelectorAll('[data-act]'))
    b.onclick=e=>{ e.stopPropagation(); const a=b.dataset.act;
      if(a==='cancel') cancelRun(b.dataset.id);
      else if(a==='rerun') reRun(b.dataset.sid);
      else if(a==='copy') copy(b.dataset.id,'copied #'+b.dataset.id); };
}

/* ---- scripts ---- */
function fetchScripts(){
  $('scriptGrid').innerHTML='<div class=skel></div>';
  fetch('/api/scripts').then(x=>x.json()).then(d=>{
    const list=d.scripts||[]; $('scriptsSub').textContent=list.length+' script'+(list.length===1?'':'s');
    if(!list.length){ $('scriptGrid').innerHTML='<div class=card><div class=empty><div class=big>No scripts</div><div>Upload one over MCP with upload_script.</div></div></div>'; return; }
    const INET='<svg class=ic viewBox="0 0 24 24"><circle cx=12 cy=12 r=9/><path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/></svg>';
    $('scriptGrid').innerHTML=list.map(s=>{
      const chips=[];
      chips.push(`<span class="chip ${s.network?'on':'off'}">${INET}network ${s.network?'on':'off'}</span>`);
      if(s.mem_limit_mb!=null) chips.push(`<span class="chip">mem ${s.mem_limit_mb} MiB</span>`);
      if(s.cpu_limit!=null) chips.push(`<span class="chip">cpu ${s.cpu_limit}</span>`);
      if(s.feed_prev_result) chips.push(`<span class="chip on">feed_prev_result</span>`);
      return `<div class=scard><div class=top><span class=nm>${esc(s.name)}</span>`
        +`<span class=rt>${esc(s.runtime)}</span><span class=id>#${s.id}</span></div>`
        +(s.desc?`<div class=ds>${esc(s.desc)}</div>`:'')
        +`<div class=chips>${chips.join('')}</div></div>`;
    }).join('');
  }).catch(()=>{ $('scriptGrid').innerHTML='<div class=card><div class=empty>failed to load scripts</div></div>'; });
}

/* ---- schedules ---- */
function fetchSchedules(){
  fetch('/api/schedules').then(x=>x.json()).then(d=>{
    const list=d.schedules||[]; $('schedSub').textContent=list.length+' active';
    if(!list.length){ $('schedRows').innerHTML='<tr><td colspan=4><div class=empty><div class=big>No schedules</div><div>Cron a script over MCP to see it here.</div></div></td></tr>'; return; }
    $('schedRows').innerHTML=list.map(s=>
      `<tr><td><span class=sname>${esc(s.script_name||('script '+s.script_id))}</span></td>`
      +`<td class=run style="color:var(--accent)">${esc(s.cron)}</td>`
      +`<td class=when>${esc(cronHint(s.cron))}</td>`
      +`<td class="exit tnum">#${s.script_id}</td></tr>`).join('');
  }).catch(()=>{});
}
function cronHint(c){
  const p=String(c||'').trim().split(/\s+/);
  if(p[0]==='*'&&p[1]==='*') return 'every minute';
  if(p[1]==='*') return 'hourly';
  if(/^\d+$/.test(p[0])&&/^\d+$/.test(p[1])) return 'daily '+p[1].padStart(2,'0')+':'+p[0].padStart(2,'0');
  return '—';
}

/* ---- secrets ---- */
function fetchSecrets(){
  fetch('/api/secrets').then(x=>x.json()).then(d=>{
    const names=d.secrets||[]; $('secCount').innerHTML=names.length?`<b>${names.length}</b>`:'';
    const IKEY='<svg class=ic viewBox="0 0 24 24"><circle cx=8 cy=15 r=4/><path d="M10.8 12.2L20 3M17 6l2 2"/></svg>';
    $('secList').innerHTML=names.length?names.map(n=>`<span class=seckey>${IKEY}${esc(n)}</span>`).join('')
      :'<div class=empty style="width:100%"><div class=big>No secrets</div><div>Set one above.</div></div>';
  }).catch(()=>{});
}
async function submitSecret(){
  const name=$('secName').value.trim(), value=$('secVal').value;
  const hint=$('secHint'); hint.classList.remove('bad');
  if(!name){ hint.textContent='name required'; hint.classList.add('bad'); return; }
  if(!value){ hint.textContent='value required'; hint.classList.add('bad'); return; }
  try{ await act('/api/secrets',{name,value});
    toast('secret '+name+' set','ok'); $('secName').value=''; $('secVal').value='';
    hint.textContent='stored encrypted; injected as an env var into matching runs'; fetchSecrets();
  }catch(e){ hint.textContent=e.message; hint.classList.add('bad'); }
}

/* ---- artifacts ---- */
function fetchBlobs(){
  $('blobRows').innerHTML='<tr><td colspan=4><div class=skel></div></td></tr>';
  fetch('/api/blobs').then(x=>x.json()).then(d=>{
    const list=d.blobs||[]; $('blobSub').textContent=list.length+' blob'+(list.length===1?'':'s');
    if(!list.length){ $('blobRows').innerHTML='<tr><td colspan=4><div class=empty><div class=big>No artifacts</div><div>Upload bytes over MCP with upload_blob.</div></div></td></tr>'; return; }
    $('blobRows').innerHTML=list.map(b=>
      `<tr><td class=run>${esc(trunc(b.handle,16))}<span class=clip data-copy="${esc(b.handle)}" title="copy full handle">${ICLIP}</span></td>`
      +`<td class="exit tnum">${esc(hsize(b.size))}</td>`
      +`<td class=when title="${esc(b.created_at)}">${esc(ago(b.created_at))}</td>`
      +`<td class=when title="${esc(b.last_used_at)}">${esc(ago(b.last_used_at))}</td></tr>`).join('');
    for(const c of $('blobRows').querySelectorAll('[data-copy]'))
      c.onclick=()=>copy(c.dataset.copy,'copied handle');
  }).catch(()=>{ $('blobRows').innerHTML='<tr><td colspan=4><div class=empty>failed to load</div></td></tr>'; });
}

/* ---- drawer (tabbed: logs / receipt / artifacts) ---- */
let openId=null, openRun=null, lastFocus=null;
function openDrawer(id, status){
  openId=id; lastFocus=document.activeElement;
  openRun=(last.recent||[]).find(r=>String(r.run_id)===String(id))||null;
  $('drawerTitle').textContent='run #'+id;
  setDrawerStatus(status);
  selectTab('logs');
  $('drawerReceipt').dataset.loaded=''; $('drawerArtifacts').dataset.loaded='';
  const log=$('drawerLog'); log.innerHTML='<span class="ln waiting">connecting…</span>';
  $('scrim').classList.add('open'); $('drawer').classList.add('open');
  $('drawer').focus();
  if(es) es.close();
  let cleared=false;
  es=new EventSource('/api/runs/'+id+'/stream');
  es.onmessage=ev=>{
    let m; try{ m=JSON.parse(ev.data) }catch(_){ return }
    if(!cleared){ log.innerHTML=''; cleared=true; }
    setDrawerStatus(m.status);
    const near=log.scrollTop+log.clientHeight >= log.scrollHeight-40;
    for(const raw of (m.lines||[])){
      const p=raw.split('|'), seq=p[0], stream=p[1], line=p.slice(2).join('|');
      const div=document.createElement('span'); div.className='ln';
      div.innerHTML=`<span class=seq>${esc(seq)}</span><span class="${stream==='stderr'?'err':'out'}">${esc(line)}</span>`;
      log.appendChild(div);
    }
    if(near) log.scrollTop=log.scrollHeight;
    if(TERMINAL.includes(m.status)){ es.close(); es=null; }
  };
  es.onerror=()=>{ if(es){ es.close(); es=null; } };
}
function selectTab(name){
  for(const t of $('drawer').querySelectorAll('.tab')){
    const on=t.dataset.tab===name; t.classList.toggle('active',on); t.setAttribute('aria-selected',on?'true':'false');
  }
  for(const p of $('drawer').querySelectorAll('.tabpane')) p.hidden=(p.dataset.pane!==name);
  if(name==='receipt') loadReceipt();
  if(name==='artifacts') loadArtifacts();
}
for(const t of document.querySelectorAll('.drawer .tab')) t.onclick=()=>selectTab(t.dataset.tab);

function rrow(k,vHtml){ return `<div class=rrow><div class=k>${esc(k)}</div><div class=v>${vHtml}</div></div>`; }
function shaCell(v){ if(v==null) return '—';
  return `${esc(trunc(v,20))}<span class=clip data-copy="${esc(v)}" title="copy">${ICLIP}</span>`; }
function loadReceipt(){
  const el=$('drawerReceipt'); if(el.dataset.loaded==='1') return;
  el.innerHTML='<div class=empty>loading receipt…</div>';
  fetch('/api/runs/'+openId+'/receipt').then(r=>{
    if(r.status===404) return null; if(!r.ok) throw 0; return r.json();
  }).then(rc=>{
    el.dataset.loaded='1';
    if(!rc){ el.innerHTML='<div class=empty><div class=big>No receipt</div><div>A tamper-evident receipt is written when the run finishes.</div></div>'; return; }
    const det=rc.deterministic;
    const badge=`<span class="detbadge ${det?'det':'adv'}">${det?'deterministic':'advisory'}</span>`;
    let h='<div class=rcpt>';
    h+=rrow('verdict', badge+(rc.network?' <span style="color:var(--fg-faint);font-family:var(--mono);font-size:.74rem">network-enabled</span>':''));
    h+=rrow('exit', `<span class=tnum>${rc.exit??'—'}</span>`);
    h+=rrow('image digest', shaCell(rc.image_digest));
    h+=rrow('source sha256', shaCell(rc.source_sha256));
    h+=rrow('input sha256', shaCell(rc.input_sha256));
    h+=rrow('output sha256', shaCell(rc.output_sha256));
    h+=rrow('secrets generation', `<span class=tnum>${rc.secrets_generation??'—'}</span>`);
    h+=rrow('hermetic', rc.hermetic?'<span class="detbadge det">yes · network-disabled</span>':'<span class="detbadge adv">no</span>');
    h+=rrow('hmac ('+esc(rc.alg||'?')+')', shaCell(rc.sig));
    const ed=rc.ed25519||{}; const dsig=(rc.dsse&&rc.dsse.signatures&&rc.dsse.signatures[0]&&rc.dsse.signatures[0].sig)||'';
    if(dsig) h+=rrow('ed25519 ('+esc(ed.keyid||'?')+')', shaCell(dsig));
    const ib=rc.input_blobs; const keys=ib&&typeof ib==='object'?Object.keys(ib):[];
    if(keys.length){
      let inner=keys.map(k=>`<div style="margin:.15rem 0">${esc(k)} → <span style="color:var(--fg-dim)">${esc(trunc(ib[k],16))}</span></div>`).join('');
      h+=rrow('input blobs', inner);
    }
    h+='</div>';
    el.innerHTML=h;
    for(const c of el.querySelectorAll('[data-copy]')) c.onclick=()=>copy(c.dataset.copy,'copied');
  }).catch(()=>{ el.dataset.loaded=''; el.innerHTML='<div class=empty>failed to load receipt</div>'; });
}
function loadArtifacts(){
  const el=$('drawerArtifacts'); if(el.dataset.loaded==='1') return; el.dataset.loaded='1';
  const ib=openRun&&openRun.input_blobs; const keys=ib&&typeof ib==='object'?Object.keys(ib):[];
  const IFILE='<svg class=ic viewBox="0 0 24 24"><path d="M14 3v5h5"/><path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/></svg>';
  if(!keys.length){ el.innerHTML='<div class=empty><div class=big>No input files</div><div>This run was triggered without /input artifacts.</div></div>'; return; }
  el.innerHTML='<div class=artlist>'+keys.map(k=>
    `<div class=art>${IFILE}<span class=nm>/input/${esc(k)}</span>`
    +`<span class=sha data-copy="${esc(ib[k])}" title="copy handle">${esc(trunc(ib[k],18))}</span></div>`).join('')+'</div>';
  for(const c of el.querySelectorAll('[data-copy]')) c.onclick=()=>copy(c.dataset.copy,'copied handle');
}
function setDrawerStatus(st){
  const el=$('drawerStatus'); el.className='badge';
  $('drawer').className='drawer open s-'+st;
  el.innerHTML=`<span class=sdot></span>${esc(st||'')}`;
  $('drawerCancel').hidden=!st||TERMINAL.includes(st);
}
$('drawerCancel').onclick=()=>{ if(openId!=null) cancelRun(openId); };
function closeDrawer(){
  $('scrim').classList.remove('open'); $('drawer').classList.remove('open');
  if(es){ es.close(); es=null; }
  if(lastFocus&&lastFocus.focus) lastFocus.focus();
}
$('drawerClose').onclick=closeDrawer;
$('scrim').onclick=closeDrawer;
addEventListener('keydown',e=>{
  if(e.key==='Escape'){ if($('drawer').classList.contains('open')) closeDrawer(); else setComposer(false); }
});
/* focus trap within the open drawer */
$('drawer').addEventListener('keydown',e=>{
  if(e.key!=='Tab') return;
  const f=$('drawer').querySelectorAll('button,[href],input,[tabindex]:not([tabindex="-1"])');
  const vis=[...f].filter(el=>el.offsetParent!==null||el===document.activeElement);
  if(!vis.length) return;
  const first=vis[0], lastEl=vis[vis.length-1];
  if(e.shiftKey&&document.activeElement===first){ e.preventDefault(); lastEl.focus(); }
  else if(!e.shiftKey&&document.activeElement===lastEl){ e.preventDefault(); first.focus(); }
});

/* ---- composer ---- */
function setComposer(open){
  $('composer').classList.toggle('open',open);
  $('newRunBtn').setAttribute('aria-expanded',open);
  if(open) setTimeout(()=>$('scriptId').focus(),60);
}
$('newRunBtn').onclick=()=>setComposer(!$('composer').classList.contains('open'));
$('composerCancel').onclick=()=>setComposer(false);
async function submitComposer(){
  const sid=Number($('scriptId').value);
  const hint=$('composerHint'); hint.classList.remove('bad');
  if(!sid||sid<1){ hint.textContent='enter a valid script id'; hint.classList.add('bad'); return; }
  let input; const raw=$('runInput').value.trim();
  if(raw){ try{ input=JSON.parse(raw); }
    catch(_){ hint.textContent='input is not valid JSON'; hint.classList.add('bad'); return; } }
  try{
    const d=await act('/api/runs',input!==undefined?{script_id:sid,input}:{script_id:sid});
    toast('run #'+d.run_id+' enqueued','ok');
    $('scriptId').value=''; $('runInput').value=''; hint.textContent='enqueues a run for the given script';
    setComposer(false); if(currentRoute()!=='runs') location.hash='#/runs'; tick(); openDrawer(d.run_id,'pending');
  }catch(e){ hint.textContent=e.message; hint.classList.add('bad'); }
}
$('composerSubmit').onclick=submitComposer;
$('composer').addEventListener('keydown',e=>{
  if((e.metaKey||e.ctrlKey)&&e.key==='Enter') submitComposer();
});
$('secSubmit').onclick=submitSecret;
$('secVal').addEventListener('keydown',e=>{ if(e.key==='Enter') submitSecret(); });

/* ---- boot ---- */
route();
tick(); setInterval(()=>{ tick(); if(currentRoute()==='schedules') fetchSchedules(); },1600);
</script>
</body>
</html>"#;

#[derive(Deserialize)]
struct ListQ {
    limit: Option<i64>,
}

async fn list_runs(State(s): State<AppState>, Query(q): Query<ListQ>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(30).clamp(1, 200);
    let counts = s.db.run_status_counts().await.unwrap_or_default();
    let counts_obj: serde_json::Map<String, serde_json::Value> =
        counts.into_iter().map(|(k, v)| (k, json!(v))).collect();
    let rows = s.db.list_runs(None, None, limit).await.unwrap_or_default();
    let recent: Vec<_> = rows
        .iter()
        .map(|r| json!({
            "run_id": r.id, "script_id": r.script_id, "status": r.status, "outcome": r.outcome(),
            "exit": r.exit_code, "error": r.error, "created_at": r.created_at.to_rfc3339(),
            "script_name": r.script_name, "script_desc": r.script_description,
            "created_by": r.script_created_by, "progress": r.progress,
            "input_blobs": r.input_blobs,
        }))
        .collect();
    Json(json!({"counts": counts_obj, "recent": recent}))
}

/// Cancel an in-flight run: kill its container (best-effort) and mark it canceled.
/// Mirrors the MCP `cancel` tool so the human UI and the agent stay in lockstep.
async fn cancel_run(State(s): State<AppState>, Path(id): Path<i64>) -> impl IntoResponse {
    let status = s.db.run_status(id).await.ok().flatten().unwrap_or_default();
    if matches!(status.as_str(), "succeeded" | "failed" | "canceled") {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "run already terminal", "status": status})),
        );
    }
    s.exec.cancel(id).await;
    let _ = s
        .db
        .finish_run(id, "canceled", None, Some("canceled by operator"))
        .await;
    (StatusCode::OK, Json(json!({"run_id": id, "status": "canceled"})))
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
    match s.db.insert_run(b.script_id, &input, None).await {
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
    let names = s.db.secret_names(None).await.unwrap_or_default();
    Json(json!({"secrets": names}))
}

async fn list_schedules(State(s): State<AppState>) -> impl IntoResponse {
    let rows = s.db.list_schedules().await.unwrap_or_default();
    let items: Vec<_> = rows
        .iter()
        .map(|sc| json!({
            "schedule_id": sc.id, "script_id": sc.script_id,
            "script_name": sc.script_name, "cron": sc.cron,
        }))
        .collect();
    Json(json!({"schedules": items}))
}

/// Script catalog with runtime-policy flags — drives the Scripts panel's flag chips
/// (network on/off, mem/cpu overrides, feed_prev_result). Bodiless.
async fn list_scripts(State(s): State<AppState>, Query(q): Query<ListQ>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 500);
    let (rows, _total) = s.db.list_scripts_full(limit).await.unwrap_or_default();
    let scripts: Vec<_> = rows
        .iter()
        .map(|sc| json!({
            "id": sc.id, "name": sc.name, "runtime": sc.runtime, "desc": sc.description,
            "network": sc.network, "mem_limit_mb": sc.mem_limit_mb,
            "cpu_limit": sc.cpu_limit, "feed_prev_result": sc.feed_prev_result,
        }))
        .collect();
    Json(json!({"scripts": scripts}))
}

/// Inventory of the content-addressed blob store (no bytes) — the Artifacts panel.
async fn list_blobs(State(s): State<AppState>, Query(q): Query<ListQ>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 500);
    let (rows, _total) = s.db.list_blobs(limit).await.unwrap_or_default();
    let blobs: Vec<_> = rows
        .iter()
        .map(|b| json!({
            "handle": b.sha, "size": b.size,
            "created_at": b.created_at.to_rfc3339(),
            "last_used_at": b.last_used_at.to_rfc3339(),
        }))
        .collect();
    Json(json!({"blobs": blobs}))
}

/// A run's tamper-evident reproducibility receipt (404 if none yet — pending/running, or a run
/// whose receipt wasn't captured). Rendered in the drawer's Receipt tab.
async fn run_receipt(State(s): State<AppState>, Path(id): Path<i64>) -> impl IntoResponse {
    match s.db.run_receipt(id).await.ok().flatten() {
        Some(r) => (StatusCode::OK, Json(r)),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no receipt for this run"})),
        ),
    }
}

/// Public, key-free verification of a run's receipt — the third-party `verify` path. Checks the
/// Ed25519/DSSE signature against the receipt's embedded public key and that the signed in-toto
/// Statement attests this receipt's output. No re-execution (that's `reproduce`).
async fn verify_run(State(s): State<AppState>, Path(id): Path<i64>) -> impl IntoResponse {
    match s.db.run_receipt(id).await.ok().flatten() {
        Some(r) => {
            let rep = crate::receipt::verify_receipt(&r);
            (
                StatusCode::OK,
                Json(json!({
                    "run_id": id,
                    "ok": rep.ok(),
                    "ed25519_valid": rep.ed25519_valid,
                    "binding_consistent": rep.binding_consistent,
                    "hermetic": rep.hermetic,
                    "deterministic": r.get("deterministic").and_then(|v| v.as_bool()).unwrap_or(false),
                    "keyid": rep.keyid,
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no receipt for this run"})),
        ),
    }
}

/// Reproduce a run by RE-EXECUTION: re-run the recorded invocation and byte-compare its output to
/// the receipt → REPRODUCED / DIVERGED / TAMPERED / INCONCLUSIVE. Blocks up to ~120s for the
/// re-run (the cockpit "reproduce" action); the MCP `reproduce` tool exposes a tunable timeout.
async fn reproduce_run_ep(State(s): State<AppState>, Path(id): Path<i64>) -> impl IntoResponse {
    match crate::mcp::reproduce_run(&s.db, &s.exec, id, 120, None).await {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

/// The daemon's Ed25519 PUBLIC verifying key — anyone can fetch it to verify receipts offline.
async fn receipt_pubkey(State(_s): State<AppState>) -> impl IntoResponse {
    let signer = crate::receipt::Signer::from_env();
    (
        StatusCode::OK,
        Json(json!({
            "alg": "ed25519",
            "keyid": signer.ed_keyid(),
            "public_key": signer.ed_public_b64(),
            "encoding": "base64",
        })),
    )
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
    match s.db.upsert_secret(&b.name, &b.value, None).await {
        Ok(()) => (StatusCode::OK, Json(json!({"name": b.name, "status": "set"}))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipset(ips: &[&str]) -> std::collections::HashSet<IpAddr> {
        ips.iter().map(|s| s.parse().unwrap()).collect()
    }
    fn xff(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", v.parse().unwrap());
        h
    }

    #[test]
    fn client_ip_ignores_xff_from_untrusted_peer() {
        let peer: IpAddr = "203.0.113.9".parse().unwrap();
        // Peer is NOT a trusted proxy → a spoofed XFF must be ignored (use the real peer).
        assert_eq!(client_ip(&xff("1.2.3.4"), peer, &ipset(&["10.0.0.1"])), "203.0.113.9");
        // Empty trusted set → never trust XFF.
        assert_eq!(client_ip(&xff("1.2.3.4"), peer, &ipset(&[])), "203.0.113.9");
    }

    #[test]
    fn client_ip_uses_xff_client_from_trusted_proxy() {
        let proxy: IpAddr = "10.0.0.1".parse().unwrap();
        let trusted = ipset(&["10.0.0.1"]);
        // Trusted proxy → take the left-most XFF entry (the original client).
        assert_eq!(client_ip(&xff("1.2.3.4, 10.0.0.1"), proxy, &trusted), "1.2.3.4");
        assert_eq!(client_ip(&xff("  9.9.9.9 "), proxy, &trusted), "9.9.9.9");
        // Trusted proxy but no XFF header → fall back to the peer.
        assert_eq!(client_ip(&HeaderMap::new(), proxy, &trusted), "10.0.0.1");
    }

    #[test]
    fn ct_eq_matches_only_identical_equal_length() {
        assert!(ct_eq(b"secret-token", b"secret-token"));
        assert!(!ct_eq(b"secret-token", b"secret-toker"));
        assert!(!ct_eq(b"short", b"longer-value"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn bearer_requires_scheme_and_exact_token() {
        assert!(bearer_matches(Some("Bearer abc123"), "abc123"));
        // A bare token with no scheme is now rejected (was accepted pre-hardening).
        assert!(!bearer_matches(Some("abc123"), "abc123"));
        assert!(!bearer_matches(Some("Bearer wrong"), "abc123"));
        assert!(!bearer_matches(Some("Basic abc123"), "abc123"));
        assert!(!bearer_matches(None, "abc123"));
        // Scheme is case-sensitive per RFC 6750 usage here.
        assert!(!bearer_matches(Some("bearer abc123"), "abc123"));
    }

    #[test]
    fn webhook_token_shape_accepts_opaque_ids_rejects_junk() {
        // The real generator (32 lowercase hex) passes.
        assert!(is_well_formed_webhook_token("0123456789abcdef0123456789abcdef"));
        // Differently-seeded but URL-safe ids pass (e.g. the integration-test seeds).
        assert!(is_well_formed_webhook_token("whtok-54321"));
        assert!(is_well_formed_webhook_token("whflow-54321"));
        // Junk / probes are rejected before any DB lookup.
        assert!(!is_well_formed_webhook_token(""));
        assert!(!is_well_formed_webhook_token("nope")); // too short (<6) → 404, never hits DB
        assert!(!is_well_formed_webhook_token(&"a".repeat(129))); // oversized
        assert!(!is_well_formed_webhook_token("../../etc/passwd")); // path traversal
        assert!(!is_well_formed_webhook_token("has space")); // non-url-safe
    }

    #[test]
    fn limiter_allows_under_max_then_blocks_in_window() {
        let lim = FixedWindowLimiter::new(1, 3);
        assert!(lim.check_at("k", 100));
        assert!(lim.check_at("k", 100));
        assert!(lim.check_at("k", 100));
        assert!(!lim.check_at("k", 100), "4th hit in the window is blocked");
    }

    #[test]
    fn limiter_resets_after_window_and_is_per_key() {
        let lim = FixedWindowLimiter::new(1, 1);
        assert!(lim.check_at("k", 100));
        assert!(!lim.check_at("k", 100), "second hit same window blocked");
        assert!(lim.check_at("k", 101), "new window resets the counter");
        // Distinct keys have independent budgets.
        assert!(lim.check_at("other", 101));
    }
}
