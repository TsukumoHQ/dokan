//! Cron scheduling. Each schedule tick enqueues a pending run (status='pending') —
//! the worker pool then claims and executes it like any other job. tokio-cron-scheduler
//! uses 6-field cron with a leading seconds column.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::db::Db;

pub struct Cron {
    sched: JobScheduler,
    db: Db,
    // schedule_id -> live job uuid, so unschedule can stop the ticking job too.
    jobs: Mutex<HashMap<i64, Uuid>>,
}

impl Cron {
    /// Build the scheduler, load enabled schedules from the DB, and start ticking.
    pub async fn start(db: Db) -> Result<Arc<Self>> {
        let sched = JobScheduler::new().await?;
        let cron = Arc::new(Self {
            sched,
            db,
            jobs: Mutex::new(HashMap::new()),
        });
        for s in cron.db.enabled_schedules().await? {
            if let Err(e) = cron.add_job(s.id, s.script_id, &s.cron, s.input.clone()).await {
                tracing::warn!("skip schedule {}: {e}", s.id);
            }
        }
        cron.sched.start().await?;
        Ok(cron)
    }

    /// Register a cron job that enqueues a run for `script_id` on each tick.
    pub async fn add_job(
        &self,
        schedule_id: i64,
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
                    Ok(run_id) => {
                        metrics::counter!("dokan_cron_runs_enqueued_total").increment(1);
                        tracing::info!(script_id, run_id, "cron enqueued run");
                    }
                    Err(e) => tracing::error!("cron enqueue failed: {e}"),
                }
            })
        })?;
        let uuid = job.guid();
        self.sched.add(job).await?;
        self.jobs.lock().unwrap().insert(schedule_id, uuid);
        Ok(())
    }

    /// Stop and forget a schedule: remove the live job and disable it in the DB so it
    /// does not reload on the next boot.
    pub async fn remove(&self, schedule_id: i64) -> Result<bool> {
        let uuid = self.jobs.lock().unwrap().remove(&schedule_id);
        if let Some(uuid) = uuid {
            let _ = self.sched.remove(&uuid).await;
        }
        let n = self.db.set_schedule_enabled(schedule_id, false).await?;
        Ok(n > 0)
    }
}
