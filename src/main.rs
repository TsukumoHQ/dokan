//! dokan — agent-operated runtime for deterministic scripts in Docker.
//! MCP-first control plane. Zero LLM inside.

mod cron;
mod db;
mod embed;
mod exec;
mod flow;
mod http;
mod mcp;
mod pool;
mod worker;

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cron::Cron;
use crate::db::Db;
use crate::exec::Executor;
use crate::mcp::Dokan;
use crate::worker::Worker;

#[derive(Parser, Debug)]
#[command(name = "dokan", version, about = "Agent-operated script runtime (MCP-first)")]
struct Cli {
    /// Transport: `stdio` for a local agent, `http` for remote agents.
    #[arg(long, default_value = "http", env = "DOKAN_TRANSPORT")]
    transport: String,

    /// HTTP bind address (http transport only).
    #[arg(long, default_value = "127.0.0.1:8088", env = "DOKAN_ADDR")]
    addr: String,

    /// Postgres connection URL.
    #[arg(
        long,
        default_value = "postgres://dokan:dokan@127.0.0.1:5499/dokan",
        env = "DATABASE_URL"
    )]
    database_url: String,

    /// Runtimes this process's worker advertises (comma-separated). Empty = no worker.
    #[arg(long, default_value = "python,node,bash", env = "DOKAN_CAPS", value_delimiter = ',')]
    caps: Vec<String>,

    /// Max concurrent jobs per worker.
    #[arg(long, default_value_t = 8, env = "DOKAN_CONCURRENCY")]
    concurrency: usize,

    /// Warm idle containers to keep per image.
    #[arg(long, default_value_t = 2, env = "DOKAN_WARM_IDLE")]
    warm_idle: usize,

    /// Enable local semantic search (loads the BGE embedding model).
    #[arg(long, env = "DOKAN_EMBED")]
    embed: bool,

    /// Embedding model cache directory.
    #[arg(long, default_value = ".fastembed_cache", env = "DOKAN_EMBED_CACHE")]
    embed_cache: String,

    /// Relay endpoint: job results POSTed here for the mesh. Optional.
    #[arg(long, env = "DOKAN_RELAY_URL")]
    relay_url: Option<String>,

    /// Bearer token required on the HTTP surface (MCP + UI). Unset = open.
    #[arg(long, env = "DOKAN_TOKEN")]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // On stdio, logs MUST go to stderr — stdout is the MCP wire.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "dokan=info".into()))
        .with_writer(std::io::stderr)
        .init();

    let db = Db::connect(&cli.database_url).await?;
    db.migrate().await?;
    tracing::info!("db connected + migrated");

    // Prometheus recorder (global). /metrics renders from this handle. Latency series get
    // explicit buckets (default exporter has none → no quantiles), and every metric is
    // described once so /metrics carries # HELP/# TYPE for scrapers and dashboards.
    let metrics_handle = {
        use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
        PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Suffix("_seconds".to_string()),
                &[
                    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
                ],
            )?
            .install_recorder()?
    };
    describe_metrics();

    // Sampler: snapshot DB-derived gauges (queue depth by status, enabled schedules) every
    // few seconds. Cheap, and gives backlog/saturation series the event counters can't.
    {
        let db = db.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                if let Ok(counts) = db.run_status_counts().await {
                    for (status, n) in counts {
                        metrics::gauge!("dokan_runs", "status" => status).set(n as f64);
                    }
                }
                if let Ok(schedules) = db.enabled_schedules().await {
                    metrics::gauge!("dokan_schedules_enabled").set(schedules.len() as f64);
                }
            }
        });
    }

    let exec = Arc::new(Executor::connect(cli.warm_idle, cli.relay_url.clone())?);
    tracing::info!(warm_idle = cli.warm_idle, "docker connected, warm pool armed");

    // Optional local embeddings for semantic search.
    let embedder = if cli.embed {
        match crate::embed::Embedder::try_load(&cli.embed_cache) {
            Ok(e) => {
                tracing::info!("semantic search enabled (BGE-small)");
                Some(e)
            }
            Err(e) => {
                tracing::warn!("embedder load failed, falling back to substring: {e}");
                None
            }
        }
    } else {
        None
    };

    // Cron scheduler: ticks enqueue runs the workers then claim.
    let cron = Cron::start(db.clone()).await?;
    tracing::info!("cron scheduler started");

    // In-process worker. Multi-host scale = run more dokan processes against the same
    // Postgres; SKIP LOCKED keeps claims disjoint.
    if !cli.caps.is_empty() {
        Worker::new(db.clone(), exec.clone(), cli.caps.clone(), cli.concurrency).spawn();
        // Flow engine drives DAGs by enqueuing each step as a normal run.
        flow::FlowEngine::new(db.clone(), exec.clone()).start().await?;
        tracing::info!("flow engine started");
    }

    match cli.transport.as_str() {
        "stdio" => serve_stdio(db, exec, cron, embedder).await,
        "http" => serve_http(db, exec, cron, embedder, metrics_handle, cli.token, &cli.addr).await,
        other => anyhow::bail!("unknown transport: {other} (use stdio|http)"),
    }
}

async fn serve_stdio(
    db: Db,
    exec: Arc<Executor>,
    cron: Arc<Cron>,
    embedder: Option<crate::embed::Embedder>,
) -> Result<()> {
    use rmcp::transport::stdio;
    use rmcp::ServiceExt;

    let server = Dokan::new(db, exec, Some(cron), embedder);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn serve_http(
    db: Db,
    exec: Arc<Executor>,
    cron: Arc<Cron>,
    embedder: Option<crate::embed::Embedder>,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    token: Option<String>,
    addr: &str,
) -> Result<()> {
    use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
    use rmcp::transport::streamable_http_server::StreamableHttpService;

    let db_for_mcp = db.clone();
    let exec_for_ui = exec.clone();
    let service = StreamableHttpService::new(
        move || {
            Ok(Dokan::new(
                db_for_mcp.clone(),
                exec.clone(),
                Some(cron.clone()),
                embedder.clone(),
            ))
        },
        LocalSessionManager::default().into(),
        Default::default(),
    );

    let state = http::AppState {
        db,
        exec: exec_for_ui,
        metrics: metrics_handle,
    };
    // MCP control plane + thin operator UI, behind one bearer-token gate.
    let app = axum::Router::new()
        .nest_service("/mcp", service)
        .merge(http::operator_router(state))
        .layer(axum::middleware::from_fn_with_state(token, http::auth));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("dokan listening: MCP http://{addr}/mcp · UI http://{addr}/");
    axum::serve(listener, app).await?;
    Ok(())
}

/// One-time # HELP/# TYPE descriptions for every metric dokan emits. Kept central so the
/// `/metrics` surface is self-documenting and units are unambiguous to scrapers.
fn describe_metrics() {
    use metrics::Unit;
    // Run lifecycle.
    metrics::describe_counter!("dokan_runs_enqueued_total", Unit::Count, "Runs enqueued onto the queue (API trigger or cron)");
    metrics::describe_counter!("dokan_runs_claimed_total", Unit::Count, "Runs claimed off the queue by a worker");
    metrics::describe_counter!("dokan_run_attempts_total", Unit::Count, "Execution attempts (includes retries) labeled by attempt number");
    metrics::describe_counter!("dokan_runs_retried_total", Unit::Count, "Runs requeued for a transient-failure retry");
    metrics::describe_counter!("dokan_runs_finished_total", Unit::Count, "Runs reaching a terminal status, by status");
    metrics::describe_counter!("dokan_run_internal_errors_total", Unit::Count, "Runs that failed inside dokan (could not execute) rather than the script exiting nonzero");
    metrics::describe_counter!("dokan_run_timeouts_total", Unit::Count, "Runs killed for exceeding the execution timeout");
    metrics::describe_histogram!("dokan_run_duration_seconds", Unit::Seconds, "Wall-clock per run from acquire to discard, by runtime and status");
    metrics::describe_counter!("dokan_log_lines_total", Unit::Count, "Log lines persisted, by stream (stdout/stderr)");
    metrics::describe_gauge!("dokan_runs", Unit::Count, "Current run count by status (sampled from the queue)");
    metrics::describe_gauge!("dokan_runs_active", Unit::Count, "Runs currently executing in a container");
    metrics::describe_gauge!("dokan_schedules_enabled", Unit::Count, "Enabled cron schedules");
    // Warm pool.
    metrics::describe_counter!("dokan_pool_acquire_total", Unit::Count, "Container acquisitions, by result (warm hit / cold create)");
    metrics::describe_histogram!("dokan_pool_acquire_seconds", Unit::Seconds, "Time to obtain a ready container (warm pop is ~0; cold includes create/pull)");
    metrics::describe_counter!("dokan_pool_containers_created_total", Unit::Count, "Idle containers created");
    metrics::describe_histogram!("dokan_pool_create_seconds", Unit::Seconds, "Time to create+start an idle container (includes image pull on first use)");
    metrics::describe_counter!("dokan_pool_image_pulls_total", Unit::Count, "Container images pulled from the registry");
    metrics::describe_counter!("dokan_pool_containers_discarded_total", Unit::Count, "Containers removed (after a run or as stale idle)");
    metrics::describe_gauge!("dokan_pool_idle_containers", Unit::Count, "Warm idle containers currently buffered, by image");
    // Cron + flows.
    metrics::describe_counter!("dokan_cron_runs_enqueued_total", Unit::Count, "Runs enqueued by the cron scheduler");
    metrics::describe_counter!("dokan_flow_runs_finished_total", Unit::Count, "Flow runs reaching a terminal status, by status");
    metrics::describe_counter!("dokan_flow_steps_finished_total", Unit::Count, "Flow steps reaching a terminal status, by status");
}
