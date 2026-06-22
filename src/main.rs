//! dokan — agent-operated runtime for deterministic scripts in Docker.
//! MCP-first control plane. Zero LLM inside.

mod db;
mod exec;
mod mcp;

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::db::Db;
use crate::exec::Executor;
use crate::mcp::Dokan;

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

    let exec = Arc::new(Executor::connect()?);
    tracing::info!("docker connected");

    match cli.transport.as_str() {
        "stdio" => serve_stdio(db, exec).await,
        "http" => serve_http(db, exec, &cli.addr).await,
        other => anyhow::bail!("unknown transport: {other} (use stdio|http)"),
    }
}

async fn serve_stdio(db: Db, exec: Arc<Executor>) -> Result<()> {
    use rmcp::transport::stdio;
    use rmcp::ServiceExt;

    let server = Dokan::new(db, exec);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn serve_http(db: Db, exec: Arc<Executor>, addr: &str) -> Result<()> {
    use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
    use rmcp::transport::streamable_http_server::StreamableHttpService;

    let service = StreamableHttpService::new(
        move || Ok(Dokan::new(db.clone(), exec.clone())),
        LocalSessionManager::default().into(),
        Default::default(),
    );

    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("dokan MCP listening on http://{addr}/mcp");
    axum::serve(listener, app).await?;
    Ok(())
}
