//! Cron scheduling. Each schedule tick enqueues a pending run (status='pending') —
//! the worker pool then claims and executes it like any other job. tokio-cron-scheduler
//! uses 6-field cron with a leading seconds column.

use std::sync::Arc;

use anyhow::Result;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::db::Db;

pub struct Cron {
    sched: JobScheduler,
    db: Db,
}

impl Cron {
    /// Build the scheduler, load enabled schedules from the DB, and start ticking.
    pub async fn start(db: Db) -> Result<Arc<Self>> {
        let sched = JobScheduler::new().await?;
        let cron = Arc::new(Self { sched, db });
        for s in cron.db.enabled_schedules().await? {
            if let Err(e) = cron.add_job(s.script_id, &s.cron, s.input.clone()).await {
                tracing::warn!("skip schedule {}: {e}", s.id);
            }
        }
        cron.sched.start().await?;
        Ok(cron)
    }

    /// Register a cron job that enqueues a run for `script_id` on each tick.
    pub async fn add_job(
        &self,
        script_id: i64,
        cron: &str,
        input: serde_json::Value,
    ) -> Result<()> {
        let db = self.db.clone();
        let job = Job::new_async(cron, move |_uuid, _l| {
            let db = db.clone();
            let input = input.clone();
            Box::pin(async move {
                match db.insert_run(script_id, &input).await {
                    Ok(run_id) => tracing::info!(script_id, run_id, "cron enqueued run"),
                    Err(e) => tracing::error!("cron enqueue failed: {e}"),
                }
            })
        })?;
        self.sched.add(job).await?;
        Ok(())
    }
}
