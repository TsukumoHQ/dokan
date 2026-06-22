//! Worker loop: claims pending runs from the Postgres queue (`FOR UPDATE SKIP LOCKED`)
//! and executes them. Each worker advertises capabilities (runtimes it can serve); the
//! claim filters on them, so scheduling is just the queue — no central dispatcher.
//!
//! Multiple workers (this process or others) can run concurrently and safely against
//! the same queue. Concurrency per worker is bounded by a semaphore; overflow stays
//! queued in Postgres.

use std::sync::Arc;
use std::time::Duration;

use crate::db::Db;
use crate::exec::Executor;
use crate::scale::Concurrency;

const MAX_ATTEMPTS: i32 = 3;
const IDLE_POLL: Duration = Duration::from_millis(300);
/// Fallback re-check window when waiting on LISTEN/NOTIFY — bounds the cost of a missed
/// notification without the constant churn of tight polling.
const FALLBACK_POLL: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct Worker {
    db: Db,
    exec: Arc<Executor>,
    caps: Vec<String>,
    /// Resizable by the autoscaler (Little's Law).
    slots: Arc<Concurrency>,
}

impl Worker {
    pub fn new(db: Db, exec: Arc<Executor>, caps: Vec<String>, slots: Arc<Concurrency>) -> Self {
        Self {
            db,
            exec,
            caps,
            slots,
        }
    }

    /// Spawn the claim/execute loop. Returns immediately; runs until process exit.
    pub fn spawn(self) {
        tokio::spawn(async move {
            tracing::info!(caps = ?self.caps, "worker started");
            // Wake on enqueue via LISTEN/NOTIFY instead of polling; the fallback timeout
            // covers any missed notify (and cron/other inserts). Degrades to polling if the
            // listener can't connect. (Perf #1.)
            let mut listener = match self.db.run_queue_listener().await {
                Ok(l) => Some(l),
                Err(e) => {
                    tracing::warn!("run-queue listener unavailable, polling: {e}");
                    None
                }
            };
            loop {
                // Block until a slot is free, then try to claim work.
                let permit = match self.slots.acquire().await {
                    Some(p) => p,
                    None => break,
                };
                match self.db.claim_run(&self.caps).await {
                    Ok(Some(job)) => {
                        metrics::counter!("dokan_runs_claimed_total").increment(1);
                        let db = self.db.clone();
                        let exec = self.exec.clone();
                        tokio::spawn(async move {
                            let _permit = permit; // released on drop
                            let attempt = db.mark_attempt(job.run_id).await.unwrap_or(1);
                            metrics::counter!("dokan_run_attempts_total",
                                "attempt" => attempt.to_string()).increment(1);
                            exec.run(
                                &db,
                                job.run_id,
                                &job.runtime,
                                &job.source,
                                &job.input,
                                job.agent_id.as_deref(),
                            )
                            .await;
                            // Retry ONLY genuine infra/internal failures. A run that
                            // produced an exit_code ran to completion — its nonzero is a
                            // deterministic verdict (a monitor/gate finding), not a crash;
                            // retrying would reprint findings and waste compute. Only a
                            // NULL exit_code (couldn't execute / timeout) is transient.
                            if let Ok(Some((status, exit_code))) =
                                db.run_outcome(job.run_id).await
                            {
                                if status == "failed"
                                    && exit_code.is_none()
                                    && attempt < MAX_ATTEMPTS
                                {
                                    tracing::warn!(run_id = job.run_id, attempt, "retrying (infra failure)");
                                    metrics::counter!("dokan_runs_retried_total").increment(1);
                                    let _ = db.requeue(job.run_id).await;
                                }
                            }
                        });
                    }
                    Ok(None) => {
                        drop(permit);
                        // Idle: wait for an enqueue notification, or wake after a short
                        // fallback to re-check (covers missed notifies + already-pending rows).
                        match &mut listener {
                            Some(l) => {
                                let _ = tokio::time::timeout(FALLBACK_POLL, l.recv()).await;
                            }
                            None => tokio::time::sleep(IDLE_POLL).await,
                        }
                    }
                    Err(e) => {
                        drop(permit);
                        tracing::error!("claim error: {e}");
                        tokio::time::sleep(IDLE_POLL).await;
                    }
                }
            }
        });
    }
}
