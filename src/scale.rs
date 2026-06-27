//! Autoscaling via Little's Law. The queue is an M/M/c-ish system: the average number of
//! jobs in the system L = λ·W, where λ is the arrival rate (runs enqueued / s) and W is the
//! mean time a job spends in service. To keep the queue stable we need roughly L servers,
//! so the controller sizes BOTH the worker concurrency (parallel execs) and the warm-pool
//! depth (ready containers) to L·headroom, clamped to a host-safe range. λ is EWMA-smoothed
//! to avoid flapping; when arrivals fall, L falls and both scale back toward the floor.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::db::Db;
use crate::exec::Executor;

/// Resizable concurrency limiter for the worker. Grow = add permits; shrink = forget
/// currently-available permits (in-use ones fall away as their jobs finish).
pub struct Concurrency {
    sem: Arc<Semaphore>,
    current: AtomicUsize,
    max: usize,
}

impl Concurrency {
    pub fn new(initial: usize, max: usize) -> Arc<Self> {
        let max = max.max(1);
        let initial = initial.clamp(1, max);
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(initial)),
            current: AtomicUsize::new(initial),
            max,
        })
    }

    /// Acquire one slot; held until the returned permit drops. None only if the semaphore
    /// was closed (never, in practice).
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.sem.clone().acquire_owned().await.ok()
    }

    #[allow(dead_code)]
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Resize toward `target` (clamped to [1, max]). Returns the resulting size.
    pub fn set(&self, target: usize) -> usize {
        let target = target.clamp(1, self.max);
        let cur = self.current.load(Ordering::Relaxed);
        if target > cur {
            self.sem.add_permits(target - cur);
            self.current.store(target, Ordering::Relaxed);
        } else if target < cur {
            let removed = self.sem.forget_permits(cur - target);
            self.current.fetch_sub(removed, Ordering::Relaxed);
        }
        self.current.load(Ordering::Relaxed)
    }
}

/// Autoscaler bounds + responsiveness.
pub struct ScaleCfg {
    pub conc_floor: usize,
    pub conc_max: usize,
    pub warm_floor: usize,
    pub warm_max: usize,
    /// Multiplier on L so utilization stays < 1 (e.g. 1.3 → ρ ≈ 0.77).
    pub headroom: f64,
}

/// Spawn the controller (executor-only). Every few seconds it measures λ and W, applies
/// Little's Law, and retunes concurrency + warm depth.
pub fn spawn_autoscaler(db: Db, exec: Arc<Executor>, conc: Arc<Concurrency>, cfg: ScaleCfg) {
    tokio::spawn(async move {
        const WINDOW_SECS: i64 = 10; // arrival-rate window
        const ALPHA: f64 = 0.4; // EWMA weight on the newest sample
        const NOMINAL_W: f64 = 0.4; // assumed service time before we have data
        let mut lambda = 0.0f64;
        let mut tick = tokio::time::interval(Duration::from_secs(3));
        let mut last_log = (0usize, 0usize);
        loop {
            tick.tick().await;
            let arrivals = db.arrivals_last_secs(WINDOW_SECS).await.unwrap_or(0) as f64;
            let inst_lambda = arrivals / WINDOW_SECS as f64;
            lambda = ALPHA * inst_lambda + (1.0 - ALPHA) * lambda;
            let w = db
                .mean_run_duration_secs(60)
                .await
                .ok()
                .flatten()
                .unwrap_or(NOMINAL_W)
                .clamp(0.05, 600.0);
            // Little's Law: jobs-in-system needed to serve the arrival rate.
            let l = lambda * w;
            let target = (l * cfg.headroom).ceil() as usize;
            let conc_set = conc.set(target.clamp(cfg.conc_floor, cfg.conc_max));
            let warm_set = exec.set_warm_target(target.clamp(cfg.warm_floor, cfg.warm_max));

            metrics::gauge!("dokan_autoscale_arrival_rate").set(lambda);
            metrics::gauge!("dokan_autoscale_in_system").set(l);
            metrics::gauge!("dokan_autoscale_concurrency").set(conc_set as f64);
            metrics::gauge!("dokan_autoscale_warm_target").set(warm_set as f64);
            if (conc_set, warm_set) != last_log {
                tracing::info!(
                    lambda = format!("{lambda:.2}/s"),
                    w = format!("{w:.2}s"),
                    l = format!("{l:.1}"),
                    concurrency = conc_set,
                    warm = warm_set,
                    "autoscale"
                );
                last_log = (conc_set, warm_set);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrency_grows_clamps_and_shrinks() {
        let c = Concurrency::new(2, 8);
        assert_eq!(c.current(), 2);
        assert_eq!(c.set(6), 6, "grow");
        assert_eq!(c.set(100), 8, "clamp to max");
        // Hold all 8 permits.
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(c.acquire().await.unwrap());
        }
        // Shrink while all are in use: nothing is available to forget, so size is unchanged
        // (in-use permits fall away only as they release).
        assert_eq!(c.set(3), 8, "shrink is best-effort under full load");
        // Release them, then shrink for real.
        held.clear();
        assert_eq!(c.set(3), 3, "forgets now-available permits");
        assert_eq!(c.set(0), 1, "clamp to floor 1");
    }
}
