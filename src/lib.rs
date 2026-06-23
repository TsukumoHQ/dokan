//! dokan library crate.
//!
//! All modules live here (not in the binary) so integration tests can unit-test internals
//! directly — e.g. `db` query methods like `gc_old` — instead of only exercising them over
//! the MCP wire. `src/main.rs` is a thin binary that drives this crate.

pub mod cron;
pub mod crypto;
pub mod db;
pub mod embed;
pub mod exec;
pub mod flow;
pub mod http;
pub mod mcp;
pub mod pool;
pub mod receipt;
pub mod scale;
pub mod worker;
