//! dokan — agent-operated runtime for deterministic scripts in Docker.
//! MCP-first control plane. Zero LLM inside.

use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

// Modules now live in the `dokan` library crate (see src/lib.rs); the binary drives them.
use dokan::cron::Cron;
use dokan::db::Db;
use dokan::exec::Executor;
use dokan::mcp::Dokan;
use dokan::worker::Worker;
use dokan::{crypto, embed, exec, flow, http, receipt, scale};

#[derive(Parser, Debug)]
#[command(name = "dokan", version, about = "Agent-operated script runtime (MCP-first)")]
struct Cli {
    /// Optional subcommand. With NO subcommand, dokan boots the daemon (default behavior) — so
    /// `dokan --transport http --addr ...` is unchanged, which is how launchd invokes it.
    #[command(subcommand)]
    command: Option<Commands>,

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

    /// Retention: delete logs + terminal runs older than this many days (0 = keep forever).
    #[arg(long, default_value_t = 7.0, env = "DOKAN_RETENTION_DAYS")]
    retention_days: f64,

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

    /// Fail runs stuck `pending` longer than this (never claimed — no capable worker, or a
    /// dead enqueuer). 0 = keep forever. Generous so a healthy backlog is never touched.
    #[arg(long, default_value_t = 1800.0, env = "DOKAN_PENDING_TIMEOUT_SECS")]
    pending_timeout_secs: f64,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Self-update from the latest TsukumoHQ/dokan GitHub release.
    Update {
        /// Bypass the dev-build and no-downgrade guards.
        #[arg(long)]
        force: bool,
        /// Report whether an update is available and exit; do not install.
        #[arg(long)]
        check: bool,
    },
    /// Print this daemon's Ed25519 PUBLIC verifying key — share it so third parties can verify
    /// receipts offline. Reads the key from DOKAN_RECEIPT_ED25519_SECRET (or the dev key).
    Pubkey,
    /// Verify a receipt JSON with its embedded public key — no daemon, no shared secret, no
    /// re-execution. Exit 0 = verified (Ed25519 sig valid + bound to its output), 1 = failed.
    Verify {
        /// Path to a receipt JSON file, or '-' to read from stdin.
        receipt: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // On stdio, logs MUST go to stderr — stdout is the MCP wire.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "dokan=info".into()))
        .with_writer(std::io::stderr)
        .init();

    // Subcommand routing. `update` runs the self-updater and EXITS before any daemon work or the
    // security preflight — it must work on a binary that isn't configured to serve. With no
    // subcommand we fall through to the daemon path, byte-for-byte unchanged.
    match cli.command {
        Some(Commands::Update { force, check }) => {
            let code = dokan::update::run(force, check).await;
            std::process::exit(code);
        }
        Some(Commands::Pubkey) => {
            let s = receipt::Signer::from_env();
            println!(
                "{}",
                serde_json::json!({
                    "alg": "ed25519",
                    "keyid": s.ed_keyid(),
                    "public_key": s.ed_public_b64(),
                    "encoding": "base64",
                })
            );
            std::process::exit(0);
        }
        Some(Commands::Verify { receipt: path }) => {
            let raw = if path == "-" {
                use std::io::Read;
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                buf
            } else {
                std::fs::read_to_string(&path)?
            };
            let r: serde_json::Value = serde_json::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("receipt is not valid JSON: {e}"))?;
            let rep = receipt::verify_receipt(&r);
            println!(
                "{}",
                serde_json::json!({
                    "ok": rep.ok(),
                    "ed25519_valid": rep.ed25519_valid,
                    "binding_consistent": rep.binding_consistent,
                    "hermetic": rep.hermetic,
                    "keyid": rep.keyid,
                })
            );
            std::process::exit(if rep.ok() { 0 } else { 1 });
        }
        None => {}
    }

    // Fail closed on missing crypto keys (GAP-4). Refuse to boot insecurely unless the
    // operator explicitly opts in with DOKAN_DEV_INSECURE=1. Runs before any DB/Docker work
    // so an insecure misconfiguration is caught at the door, loudly and actionably.
    preflight_security()?;

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
        match embed::Embedder::try_load(&cli.embed_cache) {
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
        exec.resolve_digests().await; // stable cache key from the first run (T1)

        // Executor registry: heartbeat so the live fleet is observable (T4).
        {
            let db = db.clone();
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into());
            let id = format!("{host}:{}", std::process::id());
            let caps = cli.caps.join(",");
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tick.tick().await;
                    let _ = db.executor_heartbeat(&id, &host, &caps).await;
                }
            });
        }
        // Autoscale concurrency + warm depth via Little's Law (L = λW). --concurrency and
        // --warm-idle are the floors; the controller raises both toward L under load.
        let conc = scale::Concurrency::new(cli.concurrency, cli.max_concurrency);
        Worker::new(db.clone(), exec.clone(), cli.caps.clone(), conc.clone()).spawn();
        scale::spawn_autoscaler(
            db.clone(),
            exec.clone(),
            conc.clone(),
            scale::ScaleCfg {
                conc_floor: cli.concurrency,
                conc_max: cli.max_concurrency,
                warm_floor: cli.warm_idle,
                warm_max: cli.max_warm,
                headroom: 1.3,
            },
        );
        // Retention GC: keep Postgres bounded by deleting logs + terminal runs past the TTL.
        if cli.retention_days > 0.0 {
            let db = db.clone();
            let days = cli.retention_days;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
                loop {
                    tick.tick().await;
                    match db.gc_old(days).await {
                        Ok((r, l)) if r > 0 || l > 0 => {
                            tracing::info!(runs = r, logs = l, "retention GC");
                            metrics::counter!("dokan_gc_runs_total").increment(r);
                            metrics::counter!("dokan_gc_logs_total").increment(l);
                        }
                        Err(e) => tracing::error!("retention GC: {e}"),
                        _ => {}
                    }
                }
            });
        }
        // Flow engine drives DAGs. It shares the worker's concurrency cap (`conc`) so a
        // large map fan-out can't spawn unbounded containers and swamp the host.
        flow::FlowEngine::new(db.clone(), exec.clone(), conc).start().await?;
        tracing::info!("flow engine started");

        // Orphan reaper: a worker/engine that dies mid-run leaves rows stuck `running`.
        // Lease-based reclaim hands them back to `pending` once past a generous lease
        // (well beyond the hard job timeout), without disturbing healthy concurrent
        // workers — the safety net that makes multi-worker against one Postgres correct.
        {
            let db = db.clone();
            let lease = (exec::DEFAULT_TIMEOUT_SECS * 2) as f64; // 2× the hard job timeout
            let pending_to = cli.pending_timeout_secs;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    tick.tick().await;
                    // Retire pending runs that were never claimed (zombies), bounding the
                    // queue. Opt-out with 0.
                    if pending_to > 0.0 {
                        match db.fail_stale_pending(pending_to).await {
                            Ok(n) if n > 0 => {
                                tracing::warn!(runs = n, "failed stale pending runs (unclaimed)");
                                metrics::counter!("dokan_runs_unclaimed_total").increment(n);
                            }
                            Err(e) => tracing::error!("fail_stale_pending: {e}"),
                            _ => {}
                        }
                    }
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
    embedder: Option<embed::Embedder>,
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
    embedder: Option<embed::Embedder>,
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
    let protected = axum::Router::new()
        .nest_service("/mcp", service)
        .merge(http::operator_router(state.clone()))
        .layer(axum::middleware::from_fn_with_state(token, http::auth));
    // Inbound webhooks sit OUTSIDE the bearer gate (the URL token is their auth), so an
    // external service can reach /hook/<token> without DOKAN_TOKEN.
    let app = axum::Router::new()
        .merge(http::webhook_router(state))
        .merge(protected);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("dokan listening: MCP http://{addr}/mcp · UI http://{addr}/");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Boot-time security gate (GAP-4): both crypto keys are validated together, before the
/// daemon does anything. Missing keys are insecure defaults — plaintext secrets at rest and
/// forgeable receipts — so we FAIL CLOSED by default. `DOKAN_DEV_INSECURE=1` is the single,
/// explicit escape hatch for local dev + CI; with it set, the per-key `from_env()` paths warn
/// loudly and proceed with the insecure behavior. The dev/plaintext code paths are retained,
/// now gated behind this flag rather than reachable by accident.
fn preflight_security() -> Result<()> {
    if crypto::dev_insecure() {
        tracing::warn!(
            "DOKAN_DEV_INSECURE set — booting in INSECURE dev mode. Any unset crypto key falls \
             back to an insecure default (plaintext secrets / public receipt key). Never set this \
             in production."
        );
        return Ok(());
    }

    let mut missing = Vec::new();
    if !crypto::secret_key_configured() {
        missing.push(
            "DOKAN_SECRET_KEY — without it, secrets are stored in PLAINTEXT at rest (a DB dump \
             leaks every API key).",
        );
    }
    if !receipt::Signer::key_configured() {
        missing.push(
            "DOKAN_RECEIPT_KEY — without it, receipts are HMAC'd with a PUBLIC dev key, so they \
             are forgeable and NOT tamper-evident.",
        );
    }
    if !receipt::Signer::ed_key_configured() {
        missing.push(
            "DOKAN_RECEIPT_ED25519_SECRET — without it, receipts are Ed25519-signed with a PUBLIC \
             dev key, so the third-party public-verify story is void (anyone can forge). Set it to \
             a base64 32-byte seed.",
        );
    }

    if !missing.is_empty() {
        anyhow::bail!(
            "refusing to start insecurely (GAP-4 fail-closed) — required crypto key(s) missing:\n  \
             - {}\n\nFix: set the key(s) above to strong secret values. For local dev or CI where \
             this is acceptable, set DOKAN_DEV_INSECURE=1 to explicitly opt into the insecure \
             defaults.",
            missing.join("\n  - ")
        );
    }
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
    metrics::describe_counter!("dokan_runs_unclaimed_total", Unit::Count, "Runs failed for sitting pending past the timeout (never claimed)");
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
    metrics::describe_counter!("dokan_flow_steps_finished_total", Unit::Count, "Flow steps reaching a terminal status, by status (succeeded/failed/skipped)");
    metrics::describe_counter!("dokan_flow_compensations_total", Unit::Count, "Saga compensation script runs (rollback of a succeeded step after the flow failed), by result");
    metrics::describe_counter!("dokan_flow_steps_recalled_total", Unit::Count, "Flow steps served from the content-addressed cache (partial flow recall) instead of running a container");
}
