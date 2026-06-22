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

    // Prometheus recorder (global). /metrics renders from this handle.
    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()?;

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
