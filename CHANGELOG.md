# Changelog

All notable changes to dokan are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
from `0.1.0` onward.

## [Unreleased]

## [0.1.0] — 2026-06-25

First tagged release. dokan has been built and run in production against the
team's own agent fleet; this version makes the public presentation match that
reality (a real tag, release, and a <15-minute quickstart).

### Added
- **Deterministic Docker job execution.** One job = one fresh, network-isolated
  container (`alpine` / `python:*-slim` / `node:*-slim`), discarded after the
  run, with per-job CPU/memory caps and a hard timeout.
- **MCP control plane.** Agents upload, run, schedule, wire, and read jobs over
  MCP (Streamable HTTP or stdio) — `upload_script`, `run_script`, `read_logs`,
  `wait_for`, `search_script`, `get_script`, `list_runs`, `cancel`, and more.
  Every response is engineered to be token-frugal.
- **Flow engine.** `compose_flow` / `run_flow` execute a validated acyclic DAG
  wired over MCP, with `when` branches, `map` fan-out, `compensate` (saga
  rollback), retries, and step-boundary durability (a crashed engine resumes;
  succeeded steps are skipped).
- **Triggers.** 6-field cron schedules and inbound webhooks
  (`POST /hook/<token>`).
- **Secrets.** Write-only secret store injected as job env vars; values are
  never returned or logged.
- **Structured results + receipts.** Jobs emit a `::dokan:result:: {json}` line
  captured as the run's structured result and POSTed to the relay on completion;
  completed runs carry a receipt signed with `DOKAN_RECEIPT_KEY`.
- **Content-addressed cache.** Unchanged work is never recomputed.
- **Operator cockpit** at `/` and Prometheus metrics at `/metrics`.
- **Flagship example** (`examples/flagship/`): a self-contained fraud-triage DAG
  runnable with no API keys and no job network — the one-command "wow", proven
  green in CI (`tests/p2_flows.rs::flagship_demo_flow`).

### Known limitations
- Single-tenant trust model: secrets are global to all jobs and the MCP plane is
  unauthenticated on loopback. Not yet turnkey multi-tenant (no SSO/RBAC/HA).
  See [SECURITY.md](SECURITY.md).

[Unreleased]: https://github.com/TsukumoHQ/dokan/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.1.0
