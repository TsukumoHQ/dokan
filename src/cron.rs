//! Cron scheduling. Each schedule tick enqueues a pending run (status='pending') —
//! the worker pool then claims and executes it like any other job. Cron expressions are
//! 6-field with a leading seconds column.
//!
//! Two tickers, one contract:
//! - **UTC schedules** (no `timezone`, the historical behavior) tick on tokio-cron-scheduler,
//!   exactly as before.
//! - **Zoned schedules** (`timezone = "Europe/Zurich"`, any IANA name) tick on a small task of
//!   ours that asks croner for the next wall-clock occurrence in that zone (chrono-tz). We do
//!   NOT use tokio-cron-scheduler's `new_async_tz`: it converts the zone to a FixedOffset once,
//!   when the job is created, so a 07:30 schedule created in winter fires at 08:30 local all
//!   summer. croner resolves every occurrence in the zone itself, with the OCPS / Vixie-cron
//!   DST rules: a fixed time that falls in the spring-forward gap fires at the first valid
//!   time after the gap, and a fixed time repeated by the fall-back overlap fires once.
//!
//! A tick enqueues with the schedule's options (the same ones run_script takes): `agent_id`
//! (scoped secrets + quota, checked at every tick), input `files`, `capture_output`, and an
//! optional per-tick idempotency key.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::db::{Db, Schedule};

/// Longest single sleep of a zoned ticker. tokio's clock is monotonic and does not advance
/// while the host sleeps (a laptop lid), so the wall clock is re-read at least this often.
const ZONED_MAX_SLEEP: Duration = Duration::from_secs(30);

/// Parse a cron expression with the options tokio-cron-scheduler uses (seconds required,
/// day-of-month AND day-of-week), so a zoned schedule reads its fields exactly like a UTC one.
pub fn parse_cron(expr: &str) -> std::result::Result<croner::Cron, String> {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Required)
        .dom_and_dow(true)
        .build()
        .parse(expr)
        .map_err(|e| e.to_string())
}

/// Parse an IANA time-zone name (e.g. `Europe/Zurich`). Case-sensitive, as in the tz database.
pub fn parse_timezone(name: &str) -> std::result::Result<Tz, String> {
    Tz::from_str(name.trim()).map_err(|_| {
        format!("unknown time zone `{name}` (expected an IANA name such as Europe/Zurich)")
    })
}

/// Next fire time strictly after `after`, reading the cron fields as wall-clock time in `tz`
/// (None = UTC). DST transitions follow croner's rules (see the module doc).
pub fn next_fire(
    cron: &croner::Cron,
    tz: Option<Tz>,
    after: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match tz {
        Some(tz) => cron
            .find_next_occurrence(&after.with_timezone(&tz), false)
            .ok()
            .map(|t| t.with_timezone(&Utc)),
        None => cron.find_next_occurrence(&after, false).ok(),
    }
}

/// Idempotency key of one tick: same schedule + same scheduled second → same key, so daemons
/// sharing the database enqueue one run per tick.
pub fn tick_key(schedule_id: i64, tick_epoch: i64) -> String {
    format!("schedule:{schedule_id}:{tick_epoch}")
}

/// Next fire of a stored schedule (for list_schedules / the cockpit). None if unparsable.
pub fn schedule_next_fire(s: &Schedule, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let cron = parse_cron(&s.cron).ok()?;
    let tz = match s.timezone.as_deref() {
        Some(name) => Some(parse_timezone(name).ok()?),
        None => None,
    };
    next_fire(&cron, tz, now)
}

/// Enqueue the run of one tick. With an `agent_id`, the agent's quota is checked first — the
/// same check as run_script — and a refused tick is recorded as a failed run (never started)
/// rather than silently dropped. Returns the enqueued run id, if any.
pub async fn enqueue_tick(db: &Db, s: &Schedule, tick_epoch: i64) -> Option<i64> {
    let key = s.idempotency.then(|| tick_key(s.id, tick_epoch));
    if let Some(aid) = s.agent_id.as_deref() {
        match crate::quota::check(db, aid).await {
            Ok(None) => {}
            Ok(Some(refusal)) => {
                metrics::counter!("dokan_cron_ticks_refused_total", "reason" => refusal.code())
                    .increment(1);
                tracing::warn!(
                    schedule_id = s.id,
                    agent_id = aid,
                    "cron tick refused: {}",
                    refusal.reason()
                );
                if let Err(e) = db
                    .insert_refused_run(
                        s.script_id,
                        &s.input,
                        Some(aid),
                        &refusal.reason(),
                        key.as_deref(),
                    )
                    .await
                {
                    tracing::error!(schedule_id = s.id, "recording refused tick failed: {e}");
                }
                return None;
            }
            Err(e) => {
                tracing::error!(
                    schedule_id = s.id,
                    "cron quota check failed, tick skipped: {e}"
                );
                return None;
            }
        }
    }
    let res = match key.as_deref() {
        Some(k) => db
            .insert_run_idempotent(
                s.script_id,
                &s.input,
                s.agent_id.as_deref(),
                s.input_blobs.as_ref(),
                s.capture_output,
                k,
            )
            .await
            .map(|(id, created)| created.then_some(id)),
        None => db
            .insert_run_with_blobs(
                s.script_id,
                &s.input,
                s.agent_id.as_deref(),
                s.input_blobs.as_ref(),
                s.capture_output,
            )
            .await
            .map(Some),
    };
    match res {
        Ok(Some(run_id)) => {
            metrics::counter!("dokan_cron_runs_enqueued_total").increment(1);
            tracing::info!(
                schedule_id = s.id,
                script_id = s.script_id,
                run_id,
                "cron enqueued run"
            );
            Some(run_id)
        }
        Ok(None) => {
            tracing::info!(
                schedule_id = s.id,
                tick_epoch,
                "cron tick already enqueued (idempotent)"
            );
            None
        }
        Err(e) => {
            tracing::error!(schedule_id = s.id, "cron enqueue failed: {e}");
            None
        }
    }
}

/// The live ticker behind one schedule, so `remove` can stop it.
enum Ticker {
    /// tokio-cron-scheduler job (UTC schedules).
    Utc(Uuid),
    /// Our zoned ticker task.
    Zoned(tokio::task::AbortHandle),
}

pub struct Cron {
    sched: JobScheduler,
    db: Db,
    // schedule_id -> live ticker, so unschedule can stop the ticking job too.
    jobs: Mutex<HashMap<i64, Ticker>>,
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
            if let Err(e) = cron.add(&s).await {
                tracing::warn!("skip schedule {}: {e}", s.id);
            }
        }
        cron.sched.start().await?;
        Ok(cron)
    }

    /// Register the ticker of a stored schedule: each tick enqueues a run with its options.
    pub async fn add(&self, s: &Schedule) -> Result<()> {
        let ticker = match s.timezone.as_deref() {
            None => {
                let db = self.db.clone();
                let spec = Arc::new(s.clone());
                let job = Job::new_async(s.cron.as_str(), move |_uuid, _l| {
                    let db = db.clone();
                    let spec = spec.clone();
                    Box::pin(async move {
                        // Ticks fire just after the scheduled second; flooring recovers it.
                        enqueue_tick(&db, &spec, Utc::now().timestamp()).await;
                    })
                })?;
                let uuid = job.guid();
                self.sched.add(job).await?;
                Ticker::Utc(uuid)
            }
            Some(name) => {
                let cron = parse_cron(&s.cron).map_err(|e| anyhow!("schedule {}: {e}", s.id))?;
                let tz = parse_timezone(name).map_err(|e| anyhow!("schedule {}: {e}", s.id))?;
                let task = tokio::spawn(zoned_ticker(self.db.clone(), s.clone(), cron, tz));
                Ticker::Zoned(task.abort_handle())
            }
        };
        if let Some(old) = self.jobs.lock().unwrap().insert(s.id, ticker) {
            // Re-adding the same id replaces its ticker (never two tickers for one schedule).
            self.stop(old);
        }
        Ok(())
    }

    fn stop(&self, t: Ticker) {
        match t {
            Ticker::Utc(uuid) => {
                let sched = self.sched.clone();
                tokio::spawn(async move {
                    let _ = sched.remove(&uuid).await;
                });
            }
            Ticker::Zoned(h) => h.abort(),
        }
    }

    /// Stop and forget a schedule: remove the live job and disable it in the DB so it
    /// does not reload on the next boot.
    pub async fn remove(&self, schedule_id: i64) -> Result<bool> {
        let ticker = self.jobs.lock().unwrap().remove(&schedule_id);
        match ticker {
            Some(Ticker::Utc(uuid)) => {
                let _ = self.sched.remove(&uuid).await;
            }
            Some(Ticker::Zoned(h)) => h.abort(),
            None => {}
        }
        let n = self.db.set_schedule_enabled(schedule_id, false).await?;
        Ok(n > 0)
    }
}

/// Zoned ticker: sleep (in bounded slices, re-reading the wall clock) until the next local
/// occurrence, enqueue, repeat. Missed ticks (host asleep) coalesce into one late tick, like
/// the UTC ticker and launchd's StartCalendarInterval.
async fn zoned_ticker(db: Db, s: Schedule, cron: croner::Cron, tz: Tz) {
    let mut after = Utc::now();
    loop {
        let Some(next) = next_fire(&cron, Some(tz), after) else {
            tracing::warn!(
                schedule_id = s.id,
                "zoned schedule has no next occurrence; ticker stopped"
            );
            return;
        };
        loop {
            let now = Utc::now();
            if now >= next {
                break;
            }
            let left = (next - now)
                .to_std()
                .unwrap_or_default()
                .min(ZONED_MAX_SLEEP);
            tokio::time::sleep(left).await;
        }
        enqueue_tick(&db, &s, next.timestamp()).await;
        after = Utc::now().max(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    fn zurich() -> Option<Tz> {
        Some(parse_timezone("Europe/Zurich").unwrap())
    }

    #[test]
    fn utc_is_the_default() {
        let c = parse_cron("0 30 7 * * *").unwrap();
        assert_eq!(
            next_fire(&c, None, utc(2026, 1, 10, 0, 0)),
            Some(utc(2026, 1, 10, 7, 30))
        );
    }

    #[test]
    fn zurich_follows_winter_and_summer_time() {
        let c = parse_cron("0 30 7 * * *").unwrap();
        // CET (UTC+1) in January, CEST (UTC+2) in July: same 07:30 on the wall clock.
        assert_eq!(
            next_fire(&c, zurich(), utc(2026, 1, 10, 0, 0)),
            Some(utc(2026, 1, 10, 6, 30))
        );
        assert_eq!(
            next_fire(&c, zurich(), utc(2026, 7, 10, 0, 0)),
            Some(utc(2026, 7, 10, 5, 30))
        );
    }

    #[test]
    fn zurich_daily_tick_across_both_dst_switches() {
        let c = parse_cron("0 30 7 * * *").unwrap();
        // Spring forward 2026-03-29 (02:00 CET -> 03:00 CEST).
        let sat = next_fire(&c, zurich(), utc(2026, 3, 28, 0, 0)).unwrap();
        assert_eq!(sat, utc(2026, 3, 28, 6, 30));
        assert_eq!(next_fire(&c, zurich(), sat), Some(utc(2026, 3, 29, 5, 30)));
        // Fall back 2026-10-25 (03:00 CEST -> 02:00 CET).
        let sat = next_fire(&c, zurich(), utc(2026, 10, 24, 0, 0)).unwrap();
        assert_eq!(sat, utc(2026, 10, 24, 5, 30));
        assert_eq!(next_fire(&c, zurich(), sat), Some(utc(2026, 10, 25, 6, 30)));
    }

    #[test]
    fn fixed_time_in_spring_gap_fires_once_right_after_the_gap() {
        // 02:30 does not exist on 2026-03-29 in Zurich: fire at 03:00 CEST (01:00 UTC).
        let c = parse_cron("0 30 2 * * *").unwrap();
        let t = next_fire(&c, zurich(), utc(2026, 3, 28, 12, 0)).unwrap();
        assert_eq!(t, utc(2026, 3, 29, 1, 0));
        // ...and the next day is back to 02:30 CEST.
        assert_eq!(next_fire(&c, zurich(), t), Some(utc(2026, 3, 30, 0, 30)));
    }

    #[test]
    fn fixed_time_in_fall_overlap_fires_once() {
        // 02:30 happens twice on 2026-10-25 in Zurich (CEST then CET): fire only the first.
        let c = parse_cron("0 30 2 * * *").unwrap();
        let t = next_fire(&c, zurich(), utc(2026, 10, 24, 12, 0)).unwrap();
        assert_eq!(t, utc(2026, 10, 25, 0, 30));
        assert_eq!(next_fire(&c, zurich(), t), Some(utc(2026, 10, 26, 1, 30)));
    }

    #[test]
    fn rejects_unknown_zone_and_bad_cron() {
        assert!(parse_timezone("Europe/Atlantis").is_err());
        assert!(parse_timezone("CET+1").is_err());
        assert!(parse_cron("0 61 7 * * *").is_err());
        assert!(parse_cron("not a cron").is_err());
    }

    #[test]
    fn tick_key_is_stable_per_schedule_and_second() {
        assert_eq!(tick_key(7, 1_800_000_000), "schedule:7:1800000000");
        assert_ne!(tick_key(7, 1), tick_key(8, 1));
    }
}
