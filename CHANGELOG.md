# Changelog

All notable changes to dokan are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
from `0.1.0` onward.

## [Unreleased]

## [0.5.0] — 2026-11-02

Long scheduled jobs: a job that runs 30 minutes every morning, as an agent, with its code
mounted from a blob, at a local wall-clock time.

### Added
- **Per-script timeout.** `upload_script(..., timeout_secs)` sets the job's wall-clock limit
  (stored in `scripts.timeout_secs`, shown by `get_script` and the cockpit). Bounded by a new
  daemon setting `--max-timeout-secs` / `DOKAN_MAX_TIMEOUT_SECS` (default **3600 s**): a larger
  value is refused with `invalid_timeout`, and limits are clamped again at exec time. `whoami`
  reports `limits.default_timeout_secs` (300) and `limits.max_timeout_secs`.
- **Schedules take run_script's options.** `schedule(..., agent_id, files, capture_output,
  idempotency)` stores them on the schedule and applies them to every tick: the agent's scoped
  secrets and quota, input blobs mounted read-only at `/input`, a captured `/output`, and an
  optional per-tick key `schedule:<id>:<tick epoch>` so daemons sharing one database enqueue a
  single run per tick. Files are validated at creation (unknown handle → no schedule).
- **Time zone per schedule.** `schedule(..., timezone="Europe/Zurich")` (any IANA name) reads
  the cron fields as local wall-clock time. DST follows croner's OCPS rules: a fixed time in the
  spring-forward gap fires right after the gap, a fixed time repeated by the fall-back overlap
  fires once. `schedule` and `list_schedules` return `next_fire` (UTC); the cockpit shows it.

### Changed
- **Run lease = heartbeat.** The executor bumps `runs.heartbeat_at` every 15 s while a job runs,
  and the orphan reaper reclaims a `running` run only when its heartbeat (or `started_at`, for
  rows from an older executor) is older than 600 s. A live long job is no longer handed to a
  second worker after 10 minutes; a dead worker's run is still reclaimed one lease later.
- A timeout of a script that declared its own `timeout_secs` is **final** (one attempt). A
  script without one keeps the previous behavior (a timeout is retried as an infra failure).
- A tick of a schedule with an `agent_id` passes the same quota check as `run_script`; a refused
  tick is recorded as a failed, never-started run whose `error` gives the reason.
- The blob GC keeps blobs mounted by an enabled schedule.
- `schedule` parses the cron before storing it, so an unparsable expression no longer leaves an
  enabled row behind.

### Notes
- Zoned schedules tick on dokan's own loop (croner + chrono-tz), not on tokio-cron-scheduler's
  `new_async_tz`, which freezes the UTC offset when the job is created and so drifts by one hour
  across DST. UTC schedules (no `timezone`) keep the tokio-cron-scheduler path unchanged.
- Migration is additive and idempotent (`ADD COLUMN IF NOT EXISTS`, nullable or default false):
  existing scripts keep 300 s, existing schedules keep UTC, no agent, no files.
- There is no `update_schedule`: to change a schedule (new code blob after a deploy),
  `unschedule` then `schedule` again.

[0.5.0]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.5.0

## [0.2.1] — 2026-06-26

### Added
- **Cockpit redesign.** The operator UI is rebuilt as a sidebar-nav, hash-routed SPA
  (still one self-contained HTML string, no external assets): dokan cyan `#22d3ee`
  brand accent, OLED dark, panels for **Runs · Scripts · Schedules · Secrets ·
  Artifacts · Flows**, and a tabbed run drawer — **Logs** (live SSE), **Receipt**
  (deterministic/advisory badge + hashes + `input_blobs`), **Artifacts** (input files).
  Scripts surface their flags (network, mem/cpu override, `feed_prev_result`); Artifacts
  shows the blob inventory. Accessible (focus rings, drawer focus-trap, reduced-motion).
- `GET /api/scripts`, `GET /api/blobs`, `GET /api/runs/{id}/receipt`, and a `list_blobs`
  MCP tool (blob inventory, token-frugal).


## [0.2.0] — 2026-06-26

### Added
- **Run artifacts — input files.** Feed a job real documents (a PDF, dataset, `.md`)
  without stuffing them into `DOKAN_INPUT`. New `upload_blob(data, filename?)` stores
  bytes in a content-addressed blob store (blake3; re-uploading identical bytes dedups
  to one row) and returns a handle. Pass handles to `run_script` via
  `files={"<name>": "<handle>"}`; dokan materializes each file **read-only at
  `/input/<name>`** in the container. `download_blob(handle)` fetches bytes back. Blob
  shas fold into the run's receipt (and the cache key), so a job that reads `/input`
  stays a pure function of its declared inputs — **determinism, cache, and a portable
  receipt are preserved**, unlike a host bind-mount. Cap: 32 MiB per blob.
- This is the "upload into the job, don't mount the host out" design (`docs/specs/
  v0.2.0-run-artifacts.md`) — the convergent pattern of E2B / Modal / OpenAI Code
  Interpreter, kept hermetic à la Bazel/Nix.

### Changed
- A run carrying input files bypasses the warm pool and cold-creates a one-off container
  with the `/input` bind (same mechanism as a per-script cap override). No-files runs are
  byte-identical to before. The only bind-mount in dokan is of a dokan-owned,
  content-addressed, ephemeral dir — not a user host path.

### Notes
- Output artifacts (`/output` capture) are deferred to a follow-up. `run_flow files:`
  (flow-level inputs visible to every step) is stubbed (`TODO`).

[0.2.0]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.2.0

## [0.1.2] — 2026-06-26

### Added
- **Last-result-as-input** (stateful monitors on a stateless runtime). `upload_script`
  accepts optional `feed_prev_result` (default `false`). When `true`, dokan injects the
  most-recent prior run's structured result of the same script into the next run's
  `DOKAN_INPUT.prev_result` (any exit code — a monitor's exit-1 verdict still carries its
  state; `null` on the first run). Lets a monitor keep a cross-run diff
  (`read prev_result.state → diff → emit new state + exit nonzero on change`) without host
  files or an external store — staying deterministic and isolated. `false` = unchanged
  behavior for every existing script. Surfaced on `get_script`.

[0.1.2]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.1.2

## [0.1.1] — 2026-06-25

### Added
- **Per-script resource override.** `upload_script` now accepts optional
  `mem_limit_mb` (MiB) and `cpu_limit` (cores); a script with either set runs on
  a fresh one-off container with those caps instead of the executor's global
  `--mem-limit-mb` / `--cpu-limit` default. A missing dimension falls back to the
  global default. Fixes heavier jobs that OOM'd (exit 137) under the shared cap —
  e.g. a memory-hungry monitor — without raising the cap for every job.
  Surfaced on `get_script`. `NULL` = global default (unchanged behavior).

### Changed
- Warm-pool container creation refactored into a shared `create_one` helper.
  Scripts with an override **bypass the warm pool** (which stays global-only) and
  cold-create a dedicated container, so the common no-override path is unchanged.

[0.1.1]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.1.1

## [0.1.0] — 2026-06-25

**First tagged release — beta / preview.** dokan has been built and run in
production against the team's own agent fleet; this version makes the public
presentation match that reality (a real tag, release, and a <15-minute
quickstart). It is published as honest beta/preview — the release exists for
OSS hygiene; a GA designation comes later.

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

[Unreleased]: https://github.com/TsukumoHQ/dokan/compare/v0.2.1...HEAD
[0.1.0]: https://github.com/TsukumoHQ/dokan/releases/tag/v0.1.0
