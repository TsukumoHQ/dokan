//! Thin operator surface (P3): run list, trigger, live log tail (SSE), secrets, and a
//! Prometheus `/metrics` endpoint. Deliberately minimal — humans operate here; all
//! analytical/heavy data belongs in Grafana (PRD §8). The agent uses MCP, not this.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
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

/// Fire an inbound webhook: resolve the token → enqueue the target script/flow with the
/// POST body as input. Non-blocking (202 + id); the worker/flow engine runs it.
async fn webhook_fire(
    State(s): State<AppState>,
    Path(token): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let Some((kind, target_id, agent_id)) =
        s.db.find_webhook_by_token(&token).await.ok().flatten()
    else {
        // Same response for unknown vs malformed: don't leak which tokens exist.
        return (StatusCode::NOT_FOUND, "no such webhook").into_response();
    };
    // Body → input: parse JSON if we can, else wrap the raw text so the script still sees it.
    let input = serde_json::from_slice::<serde_json::Value>(&body)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&body) }));

    if kind == "flow" {
        let Some(spec) = s.db.get_flow_spec(target_id).await.ok().flatten() else {
            return (StatusCode::NOT_FOUND, "flow gone").into_response();
        };
        match s.db.insert_flow_run(target_id, &spec, &input).await {
            Ok(id) => {
                metrics::counter!("dokan_webhook_fires_total", "target" => "flow").increment(1);
                (StatusCode::ACCEPTED, Json(json!({"flow_run_id": id, "status": "pending"}))).into_response()
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response(),
        }
    } else {
        match s.db.insert_run(target_id, &input, agent_id.as_deref()).await {
            Ok(id) => {
                metrics::counter!("dokan_webhook_fires_total", "target" => "script").increment(1);
                (StatusCode::ACCEPTED, Json(json!({"run_id": id, "status": "pending"}))).into_response()
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response(),
        }
    }
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
    --bg:#0a0c12; --panel:#11151f; --panel-2:#161b26; --line:#1f2632;
    --line-soft:#161b26; --fg:#e2e8f0; --fg-dim:#8b97a8; --fg-faint:#5b6675;
    --accent:#7dd3fc; --mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;
    --sans:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    --pending:#94a3b8; --running:#fbbf24; --succeeded:#4ade80; --failed:#f87171; --canceled:#64748b;
    --radius:12px; --shadow:0 1px 0 rgba(255,255,255,.03),0 12px 32px -12px rgba(0,0,0,.6);
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{
    font:15px/1.55 var(--sans); background:var(--bg); color:var(--fg);
    -webkit-font-smoothing:antialiased; min-height:100dvh;
  }
  .wrap{max-width:74rem; margin:0 auto; padding:2.25rem 1.5rem 4rem}
  /* header */
  header{display:flex; align-items:center; justify-content:space-between; gap:1rem; flex-wrap:wrap}
  .brand{display:flex; align-items:baseline; gap:.6rem}
  .brand h1{font-size:1.35rem; font-weight:650; letter-spacing:-.01em; margin:0}
  .brand .jp{color:var(--accent); font-weight:600}
  .brand small{color:var(--fg-faint); font-family:var(--mono); font-size:.74rem}
  .tools{display:flex; align-items:center; gap:1rem}
  .live{display:flex; align-items:center; gap:.45rem; color:var(--fg-dim); font-size:.8rem;
    font-family:var(--mono)}
  .live .dot{width:7px; height:7px; border-radius:50%; background:var(--succeeded);
    box-shadow:0 0 0 0 rgba(74,222,128,.5); animation:pulse 2s infinite}
  a{color:var(--accent); text-decoration:none}
  a:hover{text-decoration:underline}
  .ghost{color:var(--fg-dim); font-family:var(--mono); font-size:.8rem; padding:.3rem .55rem;
    border:1px solid var(--line); border-radius:8px}
  .ghost:hover{color:var(--fg); border-color:var(--fg-faint); text-decoration:none}
  /* stat cards */
  .stats{display:grid; grid-template-columns:repeat(auto-fit,minmax(8.5rem,1fr));
    gap:.75rem; margin:1.75rem 0}
  .stat{appearance:none; text-align:left; cursor:pointer; font:inherit;
    background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    padding:.85rem 1rem; transition:border-color .15s,background .15s,transform .06s}
  .stat:hover{border-color:var(--fg-faint); background:var(--panel-2)}
  .stat:active{transform:translateY(1px)}
  .stat[aria-pressed=true]{border-color:var(--accent); background:var(--panel-2)}
  .stat .n{font-family:var(--mono); font-size:1.6rem; font-weight:600; line-height:1.1;
    font-variant-numeric:tabular-nums}
  .stat .k{display:flex; align-items:center; gap:.4rem; margin-top:.3rem;
    font-size:.78rem; color:var(--fg-dim); text-transform:capitalize}
  .sdot{width:8px; height:8px; border-radius:50%; flex:none; background:var(--fg-faint)}
  .s-pending .sdot,.k-pending .sdot{background:var(--pending)}
  .s-running .sdot,.k-running .sdot{background:var(--running)}
  .s-succeeded .sdot,.k-succeeded .sdot{background:var(--succeeded)}
  .s-failed .sdot,.k-failed .sdot{background:var(--failed)}
  .s-canceled .sdot,.k-canceled .sdot{background:var(--canceled)}
  /* table */
  .card{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    box-shadow:var(--shadow); overflow:hidden}
  .card-h{display:flex; align-items:center; justify-content:space-between;
    padding:.85rem 1.1rem; border-bottom:1px solid var(--line)}
  .card-h h2{font-size:.82rem; font-weight:600; color:var(--fg-dim); margin:0;
    text-transform:uppercase; letter-spacing:.06em}
  .filter-tag{font-family:var(--mono); font-size:.74rem; color:var(--fg-dim)}
  .filter-tag b{color:var(--fg)}
  .filter-tag button{appearance:none; background:none; border:0; color:var(--accent);
    cursor:pointer; font:inherit; padding:0 0 0 .4rem}
  table{border-collapse:collapse; width:100%}
  th,td{text-align:left; padding:.65rem 1.1rem; font-size:.86rem}
  thead th{color:var(--fg-faint); font-weight:500; font-size:.72rem;
    text-transform:uppercase; letter-spacing:.06em; border-bottom:1px solid var(--line)}
  tbody tr{border-bottom:1px solid var(--line-soft); cursor:pointer;
    transition:background .12s}
  tbody tr:last-child{border-bottom:0}
  tbody tr:hover{background:var(--panel-2)}
  tbody td{vertical-align:top}
  td.run{font-family:var(--mono); font-variant-numeric:tabular-nums; color:var(--accent);
    vertical-align:top}
  td.script{vertical-align:top; max-width:30rem}
  td.script .sname{font-weight:550; color:var(--fg)}
  td.script .sid{font-family:var(--mono); font-size:.74rem; color:var(--fg-faint); margin-left:.5rem;
    font-variant-numeric:tabular-nums}
  td.script .sby{margin-left:.5rem; font-size:.7rem; color:var(--fg-dim); font-family:var(--mono);
    padding:.05rem .4rem; border:1px solid var(--line); border-radius:999px}
  td.script .sby::before{content:"by "; color:var(--fg-faint)}
  td.script .sdesc{margin-top:.2rem; font-size:.78rem; color:var(--fg-dim); line-height:1.4;
    overflow:hidden; text-overflow:ellipsis; display:-webkit-box; -webkit-line-clamp:2;
    -webkit-box-orient:vertical}
  td.script .sprog{margin-top:.2rem; font-size:.78rem; color:var(--accent); font-family:var(--mono);
    overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:36ch}
  td.exit{font-family:var(--mono); color:var(--fg-faint); text-align:right}
  .badge{display:inline-flex; align-items:center; gap:.45rem; font-size:.8rem}
  .badge .sdot{box-shadow:0 0 0 3px color-mix(in srgb,currentColor 14%,transparent)}
  .s-pending .badge{color:var(--pending)} .s-running .badge{color:var(--running)}
  .s-succeeded .badge{color:var(--succeeded)} .s-failed .badge{color:var(--failed)}
  .s-canceled .badge{color:var(--canceled)}
  .s-running .badge .sdot{animation:pulse 1.6s infinite}
  /* skeleton + empty */
  .skel{height:.9rem; border-radius:5px; width:60%;
    background:linear-gradient(90deg,var(--panel-2),var(--line),var(--panel-2));
    background-size:200% 100%; animation:shimmer 1.3s infinite}
  .empty{padding:3rem 1rem; text-align:center; color:var(--fg-faint)}
  .empty .big{font-size:1rem; color:var(--fg-dim); margin-bottom:.3rem}
  /* drawer */
  .scrim{position:fixed; inset:0; background:rgba(5,7,11,.6); backdrop-filter:blur(2px);
    opacity:0; pointer-events:none; transition:opacity .2s; z-index:40}
  .scrim.open{opacity:1; pointer-events:auto}
  .drawer{position:fixed; top:0; right:0; height:100dvh; width:min(46rem,100%);
    background:var(--panel); border-left:1px solid var(--line); z-index:50;
    transform:translateX(100%); transition:transform .24s cubic-bezier(.22,.61,.36,1);
    display:flex; flex-direction:column; box-shadow:-24px 0 60px -30px rgba(0,0,0,.8)}
  .drawer.open{transform:none}
  .drawer-h{display:flex; align-items:center; gap:.9rem; padding:1rem 1.2rem;
    border-bottom:1px solid var(--line)}
  .drawer-h .title{font-family:var(--mono); font-weight:600; font-size:1rem}
  .drawer-h .x{margin-left:auto; appearance:none; background:none; border:1px solid var(--line);
    color:var(--fg-dim); width:32px; height:32px; border-radius:8px; cursor:pointer; font-size:1.1rem}
  .drawer-h .x:hover{color:var(--fg); border-color:var(--fg-faint)}
  .log{flex:1; overflow:auto; padding:.9rem 1.2rem; margin:0;
    font-family:var(--mono); font-size:12.5px; line-height:1.7; white-space:pre-wrap;
    word-break:break-word; background:var(--bg)}
  .log .ln{display:block}
  .log .seq{color:var(--fg-faint); user-select:none; margin-right:.9rem;
    display:inline-block; min-width:2.5ch; text-align:right}
  .log .err{color:var(--failed)}
  .log .out{color:var(--fg)}
  .log .waiting{color:var(--fg-faint)}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(74,222,128,.5)}
    70%{box-shadow:0 0 0 6px rgba(74,222,128,0)}100%{box-shadow:0 0 0 0 rgba(74,222,128,0)}}
  @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
  /* identity: conduit rule under the brand + primary action */
  .brand{position:relative}
  .brand::after{content:""; position:absolute; left:0; right:-1.5rem; bottom:-.5rem; height:1px;
    background:linear-gradient(90deg,var(--accent),transparent 70%); opacity:.5}
  .btn{appearance:none; font:inherit; cursor:pointer; display:inline-flex; align-items:center;
    gap:.45rem; border-radius:9px; padding:.42rem .8rem; font-size:.82rem; font-weight:550;
    border:1px solid var(--line); background:var(--panel-2); color:var(--fg);
    transition:border-color .15s,background .15s,transform .06s,color .15s}
  .btn:hover{border-color:var(--fg-faint)}
  .btn:active{transform:translateY(1px)}
  .btn:focus-visible{outline:2px solid var(--accent); outline-offset:2px}
  .btn-go{border-color:color-mix(in srgb,var(--succeeded) 45%,transparent);
    background:color-mix(in srgb,var(--succeeded) 14%,var(--panel)); color:var(--succeeded)}
  .btn-go:hover{background:color-mix(in srgb,var(--succeeded) 22%,var(--panel));
    border-color:var(--succeeded)}
  .btn .ic{width:14px; height:14px; stroke:currentColor; fill:none; stroke-width:2;
    stroke-linecap:round; stroke-linejoin:round}
  /* new-run panel */
  .composer{overflow:hidden; max-height:0; opacity:0; transition:max-height .26s ease,opacity .2s,margin .26s}
  .composer.open{max-height:24rem; opacity:1; margin:1.25rem 0 .25rem}
  .composer .box{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    box-shadow:var(--shadow); padding:1rem 1.1rem; display:grid; gap:.75rem}
  .composer .grid{display:grid; grid-template-columns:9rem 1fr; gap:.75rem; align-items:start}
  .composer label{display:block; font-size:.72rem; text-transform:uppercase; letter-spacing:.06em;
    color:var(--fg-faint); margin-bottom:.35rem}
  .composer input,.composer textarea{width:100%; font-family:var(--mono); font-size:.84rem;
    color:var(--fg); background:var(--bg); border:1px solid var(--line); border-radius:8px;
    padding:.5rem .6rem; resize:vertical}
  .composer input:focus,.composer textarea:focus{outline:none; border-color:var(--accent)}
  .composer textarea{min-height:3.4rem; line-height:1.5}
  .composer .row{display:flex; align-items:center; gap:.6rem; justify-content:flex-end}
  .composer .hint{margin-right:auto; font-family:var(--mono); font-size:.74rem; color:var(--fg-faint)}
  .composer .hint.bad{color:var(--failed)}
  /* table: when + actions */
  td.when{color:var(--fg-faint); font-family:var(--mono); font-size:.78rem; white-space:nowrap}
  td.act{text-align:right; white-space:nowrap; width:1%}
  .rowbtns{display:inline-flex; gap:.3rem; opacity:0; transition:opacity .12s}
  tbody tr:hover .rowbtns,tbody tr:focus-within .rowbtns{opacity:1}
  .iconbtn{appearance:none; cursor:pointer; width:28px; height:28px; display:inline-grid;
    place-items:center; border-radius:7px; border:1px solid var(--line); background:var(--panel);
    color:var(--fg-dim); transition:color .12s,border-color .12s,background .12s}
  .iconbtn:hover{color:var(--fg); border-color:var(--fg-faint); background:var(--panel-2)}
  .iconbtn:focus-visible{outline:2px solid var(--accent); outline-offset:1px}
  .iconbtn.danger:hover{color:var(--failed); border-color:color-mix(in srgb,var(--failed) 50%,transparent)}
  .iconbtn .ic{width:14px; height:14px; stroke:currentColor; fill:none; stroke-width:2;
    stroke-linecap:round; stroke-linejoin:round}
  /* toast */
  .toasts{position:fixed; bottom:1.25rem; left:50%; transform:translateX(-50%); z-index:60;
    display:flex; flex-direction:column; gap:.5rem; align-items:center; pointer-events:none}
  .toast{font-family:var(--mono); font-size:.8rem; padding:.5rem .85rem; border-radius:9px;
    background:var(--panel-2); border:1px solid var(--line); color:var(--fg);
    box-shadow:var(--shadow); animation:toastin .22s ease}
  .toast.ok{border-color:color-mix(in srgb,var(--succeeded) 50%,transparent); color:var(--succeeded)}
  .toast.err{border-color:color-mix(in srgb,var(--failed) 50%,transparent); color:var(--failed)}
  @keyframes toastin{from{opacity:0; transform:translateY(6px)}to{opacity:1; transform:none}}
  /* cockpit: system ribbon — read-only at-a-glance pulse */
  .ribbon{display:flex; flex-wrap:wrap; gap:0; margin:1.5rem 0 .25rem;
    background:var(--panel); border:1px solid var(--line); border-radius:var(--radius);
    box-shadow:var(--shadow); overflow:hidden}
  .kpi{flex:1 1 0; min-width:7.5rem; padding:.7rem 1rem; border-right:1px solid var(--line-soft);
    display:flex; flex-direction:column; gap:.15rem}
  .kpi:last-child{border-right:0}
  .kpi .v{font-family:var(--mono); font-size:1.25rem; font-weight:600; line-height:1.1;
    font-variant-numeric:tabular-nums; color:var(--fg)}
  .kpi .l{font-size:.68rem; text-transform:uppercase; letter-spacing:.07em; color:var(--fg-faint)}
  .kpi.live-k .v{color:var(--running)} .kpi.ok-k .v{color:var(--succeeded)}
  .kpi .v.zero{color:var(--fg-faint)}
  /* cockpit grid: runs (main) + schedules (rail) */
  .cockpit{display:grid; grid-template-columns:minmax(0,2.2fr) minmax(15rem,1fr); gap:1rem;
    align-items:start; margin-top:1.25rem}
  @media (max-width:900px){ .cockpit{grid-template-columns:1fr} }
  /* schedules rail */
  .sched-list{display:flex; flex-direction:column}
  .sched{display:flex; align-items:baseline; gap:.5rem; padding:.6rem 1.1rem;
    border-bottom:1px solid var(--line-soft)}
  .sched:last-child{border-bottom:0}
  .sched .nm{font-weight:550; font-size:.84rem; color:var(--fg); flex:1; min-width:0;
    overflow:hidden; text-overflow:ellipsis; white-space:nowrap}
  .sched .cr{font-family:var(--mono); font-size:.72rem; color:var(--accent);
    background:color-mix(in srgb,var(--accent) 12%,transparent); padding:.1rem .4rem; border-radius:6px}
  .sched .sid{font-family:var(--mono); font-size:.7rem; color:var(--fg-faint)}
  @media (max-width:560px){ td.exit,th.exit,td.when,th.when{display:none} .wrap{padding:1.5rem 1rem 3rem}
    .composer .grid{grid-template-columns:1fr} .rowbtns{opacity:1} .kpi{min-width:5.5rem} }
  @media (prefers-reduced-motion:reduce){ *{animation:none!important; transition:none!important} }
</style>
</head>
<body>
<div class=wrap>
  <header>
    <div class=brand>
      <h1>dokan <span class=jp>導管</span></h1>
      <small>agent-operated script runtime</small>
    </div>
    <div class=tools>
      <span class=live><span class=dot></span> live</span>
      <a class=ghost href=/metrics>metrics</a>
      <button class="btn btn-go" id=newRunBtn aria-expanded=false aria-controls=composer>
        <svg class=ic viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg> New run
      </button>
    </div>
  </header>

  <div class=composer id=composer>
    <div class=box>
      <div class=grid>
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

  <div class=ribbon id=ribbon aria-label="system pulse"></div>

  <div class=stats id=stats aria-label="run status counts"></div>

  <div class=cockpit>
    <section class=card>
      <div class=card-h>
        <h2>Recent runs</h2>
        <span class=filter-tag id=filterTag></span>
      </div>
      <table>
        <thead><tr><th>Run</th><th>Script</th><th>Status</th><th class=when>When</th><th class=exit>Exit</th><th class=act></th></tr></thead>
        <tbody id=rows></tbody>
      </table>
    </section>

    <section class=card>
      <div class=card-h>
        <h2>Schedules</h2>
        <span class=filter-tag id=schedCount></span>
      </div>
      <div class=sched-list id=sched></div>
    </section>
  </div>
</div>

<div class=toasts id=toasts aria-live=polite></div>

<div class=scrim id=scrim></div>
<aside class=drawer id=drawer role=dialog aria-modal=true aria-label="run logs">
  <div class=drawer-h>
    <span class=title id=drawerTitle>run</span>
    <span class=badge id=drawerStatus></span>
    <button class="btn iconbtn danger" id=drawerCancel aria-label="cancel run" hidden style="margin-left:auto">
      <svg class=ic viewBox="0 0 24 24"><rect x=6 y=6 width=12 height=12 rx=1.5/></svg>
    </button>
    <button class=x id=drawerClose aria-label="close">&times;</button>
  </div>
  <pre class=log id=drawerLog></pre>
</aside>

<script>
const ORDER=['running','pending','succeeded','failed','canceled'];
let filter=null, es=null, firstPaint=true;

const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const $=id=>document.getElementById(id);
const TERMINAL=['succeeded','failed','canceled'];

function ago(iso){
  if(!iso) return '';
  const s=Math.max(0,(Date.now()-new Date(iso).getTime())/1000);
  if(s<60) return Math.floor(s)+'s ago';
  if(s<3600) return Math.floor(s/60)+'m ago';
  if(s<86400) return Math.floor(s/3600)+'h ago';
  return Math.floor(s/86400)+'d ago';
}
function toast(msg,kind){
  const t=document.createElement('div');
  t.className='toast'+(kind?' '+kind:''); t.textContent=msg;
  $('toasts').appendChild(t);
  setTimeout(()=>{ t.style.transition='opacity .25s'; t.style.opacity='0';
    setTimeout(()=>t.remove(),250); },2600);
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
function copyId(id){
  navigator.clipboard?.writeText(String(id)).then(()=>toast('copied #'+id,'ok'),
    ()=>toast('copy failed','err'));
}

function renderStats(counts){
  const keys=[...new Set([...ORDER,...Object.keys(counts)])].filter(k=>counts[k]);
  const total=Object.values(counts).reduce((a,b)=>a+b,0);
  const card=(key,label,n,cls)=>
    `<button class="stat ${cls}" aria-pressed="${filter===key}" data-f="${key??''}">`
    +`<div class=n>${n}</div><div class=k>`
    +(key?`<span class=sdot></span>`:``)+`${esc(label)}</div></button>`;
  let html=card(null,'all',total,'k-all');
  for(const k of keys) html+=card(k,k,counts[k],'k-'+k);
  $('stats').innerHTML=html;
  for(const b of $('stats').querySelectorAll('.stat'))
    b.onclick=()=>{ const f=b.dataset.f||null; filter=(f===filter)?null:f; render(last); };
}

function renderRows(recent){
  const list=filter?recent.filter(r=>r.status===filter):recent;
  const ft=$('filterTag');
  ft.innerHTML=filter?`filtered: <b>${esc(filter)}</b><button id=clearF>clear</button>`:'';
  if(filter) $('clearF').onclick=()=>{ filter=null; render(last); };
  if(!list.length){
    $('rows').innerHTML=`<tr><td colspan=6><div class=empty>`
      +`<div class=big>${recent.length?'No runs match this filter':'No runs yet'}</div>`
      +`<div>${recent.length?'Try clearing the filter.':'Trigger a script to see it here.'}</div>`
      +`</div></td></tr>`;
    return;
  }
  const ICX=`<svg class=ic viewBox="0 0 24 24"><rect x=6 y=6 width=12 height=12 rx=1.5/></svg>`;
  const IRUN=`<svg class=ic viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg>`;
  const ICOPY=`<svg class=ic viewBox="0 0 24 24"><rect x=9 y=9 width=11 height=11 rx=2/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>`;
  $('rows').innerHTML=list.map(r=>{
    const live=!TERMINAL.includes(r.status);
    const cancel=live?`<button class="iconbtn danger" data-act=cancel data-id="${r.run_id}" `
      +`title="cancel run" aria-label="cancel run #${r.run_id}">${ICX}</button>`:'';
    return `<tr class="s-${esc(r.status)}" data-id="${r.run_id}" data-st="${esc(r.status)}">`
    +`<td class=run>#${r.run_id}</td>`
    +`<td class=script>`
    +`<span class=sname>${esc(r.script_name||('script '+r.script_id))}</span>`
    +`<span class=sid>#${r.script_id}</span>`
    +(r.created_by?`<span class=sby>${esc(r.created_by)}</span>`:'')
    +(r.script_desc?`<div class=sdesc>${esc(r.script_desc)}</div>`:'')
    +(r.progress&&live?`<div class=sprog title="live progress">▸ ${esc(r.progress)}</div>`:'')
    +`</td>`
    +`<td><span class=badge><span class=sdot></span>${esc(r.status)}</span></td>`
    +`<td class=when title="${esc(r.created_at||'')}">${esc(ago(r.created_at))}</td>`
    +`<td class=exit>${r.exit??'·'}</td>`
    +`<td class=act><span class=rowbtns>${cancel}`
    +`<button class=iconbtn data-act=rerun data-sid="${r.script_id}" title="run this script again" `
    +`aria-label="re-run script ${r.script_id}">${IRUN}</button>`
    +`<button class=iconbtn data-act=copy data-id="${r.run_id}" title="copy run id" `
    +`aria-label="copy run id #${r.run_id}">${ICOPY}</button>`
    +`</span></td></tr>`;
  }).join('');
  for(const tr of $('rows').querySelectorAll('tr[data-id]'))
    tr.onclick=e=>{ if(e.target.closest('[data-act]')) return; openDrawer(tr.dataset.id, tr.dataset.st); };
  for(const b of $('rows').querySelectorAll('[data-act]'))
    b.onclick=e=>{ e.stopPropagation();
      const a=b.dataset.act;
      if(a==='cancel') cancelRun(b.dataset.id);
      else if(a==='rerun') reRun(b.dataset.sid);
      else if(a==='copy') copyId(b.dataset.id);
    };
}

let last={counts:{},recent:[]}, schedules=[];
function render(d){ last=d; renderRibbon(); renderStats(d.counts||{}); renderRows(d.recent||[]); }

/* system ribbon: read-only at-a-glance pulse derived from status counts + schedules */
function renderRibbon(){
  const c=last.counts||{};
  const run=c.running||0, pend=c.pending||0, ok=c.succeeded||0, fail=c.failed||0;
  const total=Object.values(c).reduce((a,b)=>a+b,0);
  const rate=(ok+fail)?Math.round(ok/(ok+fail)*100):null;
  const tile=(v,l,cls)=>
    `<div class="kpi ${cls||''}"><div class="v ${(v===0||v==='—')?'zero':''}">${v}</div>`
    +`<div class=l>${l}</div></div>`;
  $('ribbon').innerHTML=
    tile(run,'active',run?'live-k':'')
    +tile(pend,'queued')
    +tile(rate===null?'—':rate+'%','success',(rate!==null&&rate>=90)?'ok-k':'')
    +tile(total,'total runs')
    +tile(schedules.length,'schedules');
}

/* schedules rail — live crons (name · cron · script id) */
function renderSchedules(){
  $('schedCount').innerHTML=schedules.length?`<b>${schedules.length}</b> active`:'';
  const el=$('sched');
  if(!schedules.length){
    el.innerHTML=`<div class=empty><div class=big>No schedules</div>`
      +`<div>Cron a script to see it here.</div></div>`;
    return;
  }
  el.innerHTML=schedules.map(s=>
    `<div class=sched><span class=nm title="${esc(s.script_name||'')}">`
    +`${esc(s.script_name||('script '+s.script_id))}</span>`
    +`<span class=cr>${esc(s.cron)}</span><span class=sid>#${s.script_id}</span></div>`).join('');
}
async function fetchSchedules(){
  try{
    const d=await fetch('/api/schedules').then(x=>x.json());
    schedules=d.schedules||[]; renderSchedules(); renderRibbon();
  }catch(e){ /* keep last good frame */ }
}

async function tick(){
  try{
    const d=await fetch('/api/runs?limit=50').then(x=>x.json());
    firstPaint=false; render(d);
  }catch(e){ /* keep last good frame; transient fetch error */ }
}

/* live log drawer over SSE */
let openId=null;
function openDrawer(id, status){
  openId=id;
  $('drawerTitle').textContent='run #'+id;
  setDrawerStatus(status);
  const log=$('drawerLog');
  log.innerHTML='<span class="ln waiting">connecting…</span>';
  $('scrim').classList.add('open'); $('drawer').classList.add('open');
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
      const div=document.createElement('span');
      div.className='ln';
      div.innerHTML=`<span class=seq>${esc(seq)}</span>`
        +`<span class="${stream==='stderr'?'err':'out'}">${esc(line)}</span>`;
      log.appendChild(div);
    }
    if(near) log.scrollTop=log.scrollHeight;
    if(['succeeded','failed','canceled'].includes(m.status)){ es.close(); es=null; }
  };
  es.onerror=()=>{ if(es){ es.close(); es=null; } };
}
function setDrawerStatus(st){
  const el=$('drawerStatus');
  el.className='badge';
  el.closest('.drawer').className='drawer open s-'+st;
  el.innerHTML=`<span class=sdot></span>${esc(st||'')}`;
  $('drawerCancel').hidden=!st||TERMINAL.includes(st);
}
$('drawerCancel').onclick=()=>{ if(openId!=null) cancelRun(openId); };
function closeDrawer(){
  $('scrim').classList.remove('open'); $('drawer').classList.remove('open');
  if(es){ es.close(); es=null; }
}
$('drawerClose').onclick=closeDrawer;
$('scrim').onclick=closeDrawer;
addEventListener('keydown',e=>{ if(e.key==='Escape') closeDrawer(); });

/* new-run composer */
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
  let input;
  const raw=$('runInput').value.trim();
  if(raw){ try{ input=JSON.parse(raw); }
    catch(_){ hint.textContent='input is not valid JSON'; hint.classList.add('bad'); return; } }
  try{
    const d=await act('/api/runs',input!==undefined?{script_id:sid,input}:{script_id:sid});
    toast('run #'+d.run_id+' enqueued','ok');
    $('scriptId').value=''; $('runInput').value=''; hint.textContent='enqueues a run for the given script';
    setComposer(false); tick(); openDrawer(d.run_id,'pending');
  }catch(e){ hint.textContent=e.message; hint.classList.add('bad'); }
}
$('composerSubmit').onclick=submitComposer;
$('composer').addEventListener('keydown',e=>{
  if((e.metaKey||e.ctrlKey)&&e.key==='Enter') submitComposer();
  if(e.key==='Escape') setComposer(false);
});

$('rows').innerHTML='<tr><td colspan=6 style="padding:1.1rem"><div class=skel></div></td></tr>';
renderRibbon();
tick(); setInterval(tick,1500);
fetchSchedules(); setInterval(fetchSchedules,5000);
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
    let rows = s.db.list_runs(None, limit).await.unwrap_or_default();
    let recent: Vec<_> = rows
        .iter()
        .map(|r| json!({
            "run_id": r.id, "script_id": r.script_id, "status": r.status,
            "exit": r.exit_code, "error": r.error, "created_at": r.created_at.to_rfc3339(),
            "script_name": r.script_name, "script_desc": r.script_description,
            "created_by": r.script_created_by, "progress": r.progress,
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
