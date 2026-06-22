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
mod scale;
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

    /// Min concurrent jobs (autoscale floor; the controller raises it under load).
    #[arg(long, default_value_t = 8, env = "DOKAN_CONCURRENCY")]
    concurrency: usize,

    /// Max concurrent jobs (autoscale ceiling; host-safety cap on parallel containers).
    #[arg(long, default_value_t = 32, env = "DOKAN_MAX_CONCURRENCY")]
    max_concurrency: usize,

    /// Min warm idle containers per image (autoscale floor).
    #[arg(long, default_value_t = 2, env = "DOKAN_WARM_IDLE")]
    warm_idle: usize,

    /// Max warm idle containers per image (autoscale ceiling).
    #[arg(long, default_value_t = 16, env = "DOKAN_MAX_WARM")]
    max_warm: usize,

    /// Per-job memory cap (MiB). The cgroup OOM-kills a job that exceeds it (exit 137).
    #[arg(long, default_value_t = 1024, env = "DOKAN_MEM_LIMIT_MB")]
    mem_limit_mb: i64,

    /// Per-job CPU cap (cores; fractional allowed, e.g. 1.5).
    #[arg(long, default_value_t = 2.0, env = "DOKAN_CPU_LIMIT")]
    cpu_limit: f64,

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

    let mem_bytes = cli.mem_limit_mb * 1024 * 1024;
    let nano_cpus = (cli.cpu_limit * 1e9) as i64;
    let exec = Arc::new(Executor::connect(
        cli.warm_idle,
        mem_bytes,
        nano_cpus,
        cli.relay_url.clone(),
    )?);
    tracing::info!(
        warm_idle = cli.warm_idle,
        mem_limit_mb = cli.mem_limit_mb,
        cpu_limit = cli.cpu_limit,
        "docker connected, warm pool armed"
    );

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
    // Executor = a process that advertises at least one non-empty runtime. An empty
    // DOKAN_CAPS (or one that parses to a single "" token) means control-plane only: no
    // worker, no flow engine, no warm pool — it just enqueues/reads over shared Postgres.
    let is_executor = cli.caps.iter().any(|c| !c.trim().is_empty());
    if is_executor {
        // This process is the executor: it owns the Docker host. Sweep containers orphaned
        // by a prior crashed executor, then arm the warm pool. Control-plane-only instances
        // (empty caps — e.g. per-agent stdio dokans) skip all of this and never touch the
        // warm pool, so N co-located agents don't fight over one Docker.
        let swept = exec.sweep_orphans().await;
        if swept > 0 {
            tracing::info!(containers = swept, "swept orphaned warm containers at startup");
        }
        exec.arm_pool();
        exec.prewarm(); // pull + warm runtime images now, not on the first job
        // Autoscale concurrency + warm depth via Little's Law (L = λW). --concurrency and
        // --warm-idle are the floors; the controller raises both toward L under load.
        let conc = scale::Concurrency::new(cli.concurrency, cli.max_concurrency);
        Worker::new(db.clone(), exec.clone(), cli.caps.clone(), conc.clone()).spawn();
        scale::spawn_autoscaler(
            db.clone(),
            exec.clone(),
            conc,
            scale::ScaleCfg {
                conc_floor: cli.concurrency,
                conc_max: cli.max_concurrency,
                warm_floor: cli.warm_idle,
                warm_max: cli.max_warm,
                headroom: 1.3,
            },
        );
        // Flow engine drives DAGs by enqueuing each step as a normal run.
        flow::FlowEngine::new(db.clone(), exec.clone()).start().await?;
        tracing::info!("flow engine started");

        // Orphan reaper: a worker/engine that dies mid-run leaves rows stuck `running`.
        // Lease-based reclaim hands them back to `pending` once past a generous lease
        // (well beyond the hard job timeout), without disturbing healthy concurrent
        // workers — the safety net that makes multi-worker against one Postgres correct.
        {
            let db = db.clone();
            let lease = (exec::DEFAULT_TIMEOUT_SECS * 2) as f64; // 2× the hard job timeout
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    tick.tick().await;
                    match db.reap_orphan_runs(lease).await {
                        Ok(n) if n > 0 => {
                            tracing::warn!(runs = n, "reaped orphaned runs (dead worker)");
                            metrics::counter!("dokan_runs_reaped_total").increment(n);
                        }
                        Err(e) => tracing::error!("reap_orphan_runs: {e}"),
                        _ => {}
                    }
                    match db.reap_orphan_flow_runs(lease).await {
                        Ok(n) if n > 0 => {
                            tracing::warn!(flow_runs = n, "reaped orphaned flow_runs (dead engine)");
                            metrics::counter!("dokan_flow_runs_reaped_total").increment(n);
                        }
                        Err(e) => tracing::error!("reap_orphan_flow_runs: {e}"),
                        _ => {}
                    }
                }
            });
        }
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
    metrics::describe_counter!("dokan_runs_reaped_total", Unit::Count, "Runs reclaimed from a dead worker (stuck running past the lease)");
    metrics::describe_counter!("dokan_flow_runs_reaped_total", Unit::Count, "Flow runs reclaimed from a dead engine (stale heartbeat past the lease)");
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
