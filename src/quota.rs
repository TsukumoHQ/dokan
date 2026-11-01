//! Per-agent backpressure, shared by every path that enqueues a run on an agent's behalf:
//! `run_script` (MCP) and a schedule tick that carries an `agent_id`. One check, one set of
//! limits — a schedule can never give an agent more than a direct `run_script` would.

use anyhow::Result;
use serde_json::json;

use crate::db::Db;

/// Default per-agent in-flight (pending+running) cap — backpressure so one agent can't
/// swamp the shared runtime. Generous; tighten per deployment if needed.
pub const AGENT_MAX_CONCURRENT: i64 = 25;

/// Per-agent compute budget: max container service-seconds over the rolling window. Cheap
/// runaway guard the agent can see (whoami) and reason about. Generous default.
pub const AGENT_COMPUTE_BUDGET_SECS: f64 = 3600.0;
pub const AGENT_BUDGET_WINDOW_SECS: i64 = 86400;

/// Why an enqueue was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    /// Too many runs in flight for this agent.
    Concurrency { in_flight: i64 },
    /// Rolling compute budget exhausted.
    Budget { spent: f64 },
}

impl Refusal {
    /// Stable machine-readable code (the `error` field of the MCP envelope).
    pub fn code(&self) -> &'static str {
        match self {
            Refusal::Concurrency { .. } => "quota_exceeded",
            Refusal::Budget { .. } => "budget_exceeded",
        }
    }

    /// The uniform error envelope `run_script` returns (shape unchanged since v0.4).
    pub fn envelope(&self, agent_id: &str) -> serde_json::Value {
        match self {
            Refusal::Concurrency { in_flight } => json!({
                "error": self.code(), "message": "this agent has too many runs in flight",
                "hint": "wait for in-flight runs to finish or back off", "agent_id": agent_id,
                "in_flight": in_flight, "limit": AGENT_MAX_CONCURRENT
            }),
            Refusal::Budget { spent } => json!({
                "error": self.code(), "message": "this agent exhausted its rolling compute budget",
                "hint": "retry after the budget window rolls over", "agent_id": agent_id,
                "compute_seconds_24h": spent, "budget": AGENT_COMPUTE_BUDGET_SECS
            }),
        }
    }

    /// One-line reason recorded on a refused scheduled run (`runs.error`).
    pub fn reason(&self) -> String {
        match self {
            Refusal::Concurrency { in_flight } => {
                format!("quota_exceeded: {in_flight} runs in flight (limit {AGENT_MAX_CONCURRENT})")
            }
            Refusal::Budget { spent } => format!(
                "budget_exceeded: {spent:.0} s of compute in 24 h (budget {AGENT_COMPUTE_BUDGET_SECS:.0} s)"
            ),
        }
    }
}

/// Pure decision from the two measured inputs — unit-tested.
pub fn decide(in_flight: i64, spent: f64) -> Option<Refusal> {
    if in_flight >= AGENT_MAX_CONCURRENT {
        return Some(Refusal::Concurrency { in_flight });
    }
    if spent >= AGENT_COMPUTE_BUDGET_SECS {
        return Some(Refusal::Budget { spent });
    }
    None
}

/// Measure the agent's usage and decide. `Ok(None)` = the enqueue may proceed.
pub async fn check(db: &Db, agent_id: &str) -> Result<Option<Refusal>> {
    let in_flight = db.agent_running_count(agent_id).await?;
    if in_flight >= AGENT_MAX_CONCURRENT {
        return Ok(decide(in_flight, 0.0));
    }
    let spent = db
        .agent_compute_seconds(agent_id, AGENT_BUDGET_WINDOW_SECS)
        .await?;
    Ok(decide(in_flight, spent))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_both_limits_passes() {
        assert_eq!(decide(0, 0.0), None);
        assert_eq!(
            decide(AGENT_MAX_CONCURRENT - 1, AGENT_COMPUTE_BUDGET_SECS - 1.0),
            None
        );
    }

    #[test]
    fn concurrency_checked_first() {
        let r = decide(AGENT_MAX_CONCURRENT, AGENT_COMPUTE_BUDGET_SECS).unwrap();
        assert_eq!(r.code(), "quota_exceeded");
        assert_eq!(r.envelope("a")["limit"], AGENT_MAX_CONCURRENT);
    }

    #[test]
    fn budget_refusal_shape() {
        let r = decide(0, AGENT_COMPUTE_BUDGET_SECS).unwrap();
        assert_eq!(r.code(), "budget_exceeded");
        assert_eq!(r.envelope("a")["agent_id"], "a");
        assert!(r.reason().starts_with("budget_exceeded"));
    }
}
