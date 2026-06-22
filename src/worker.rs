//! Worker loop: claims pending runs from the Postgres queue (`FOR UPDATE SKIP LOCKED`)
//! and executes them. Each worker advertises capabilities (runtimes it can serve); the
//! claim filters on them, so scheduling is just the queue — no central dispatcher.
//!
//! Multiple workers (this process or others) can run concurrently and safely against
//! the same queue. Concurrency per worker is bounded by a semaphore; overflow stays
//! queued in Postgres.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::db::Db;
use crate::exec::Executor;

const MAX_ATTEMPTS: i32 = 3;
const IDLE_POLL: Duration = Duration::from_millis(300);

#[derive(Clone)]
pub struct Worker {
    db: Db,
    exec: Arc<Executor>,
    caps: Vec<String>,
    slots: Arc<Semaphore>,
}

impl Worker {
    pub fn new(db: Db, exec: Arc<Executor>, caps: Vec<String>, concurrency: usize) -> Self {
        Self {
            db,
            exec,
            caps,
            slots: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// Spawn the claim/execute loop. Returns immediately; runs until process exit.
    pub fn spawn(self) {
        tokio::spawn(async move {
            tracing::info!(caps = ?self.caps, "worker started");
            loop {
                // Block until a slot is free, then try to claim work.
                let permit = match self.slots.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
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
                            exec.run(&db, job.run_id, &job.runtime, &job.source, &job.input)
                                .await;
                            // Retry transient failures up to MAX_ATTEMPTS.
                            if let Ok(Some(status)) = db.run_status(job.run_id).await {
                                if status == "failed" && attempt < MAX_ATTEMPTS {
                                    tracing::warn!(run_id = job.run_id, attempt, "retrying");
                                    metrics::counter!("dokan_runs_retried_total").increment(1);
                                    let _ = db.requeue(job.run_id).await;
                                }
                            }
                        });
                    }
                    Ok(None) => {
                        drop(permit);
                        tokio::time::sleep(IDLE_POLL).await;
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
