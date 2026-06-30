---
name: dokan
description: Operate the dokan runtime — run deterministic scripts in Docker over MCP, compose DAG flows, schedule cron, set secrets, return structured results, read logs token-frugally. Use when the user wants to run/deploy a script or job, build a pipeline/flow/DAG of steps, schedule recurring work, provision API keys for a job, build an event-driven monitor, or check run status/logs on dokan. Triggers — "run this script on dokan", "upload + run", "compose a flow", "schedule a job", "wire a pipeline", "set a secret/API key", "alert on change", "check the run/logs", "dokan".
---

# Operating dokan

dokan is the **passive pipe**; you are the **operator**. It runs deterministic scripts in Docker. **No LLM runs inside** — all intelligence is yours, applied at author/trigger time, never as a node in a flow. Every MCP response is engineered for low tokens — respect that.

## Connect (one shared executor)

dokan instances are **co-located** — they share one Docker host and one Postgres. There is exactly **one executor** (the process that owns Docker and runs jobs); everyone else is control-plane. So **do not spawn your own dokan** — talk to the shared executor's HTTP MCP. `.mcp.json` points at `http://127.0.0.1:8088/mcp`; tools appear as `mcp__dokan__*`. (If you must run your own dokan, set `DOKAN_CAPS=""` so it stays control-plane only and never competes for the Docker host.)

## Core loop (single script)

1. `upload_script(name, runtime, source, description, created_by?, upsert?)` → `script_id`. Runtimes: `python` | `node` | `bash`. **Always write a 1-line `description`** (powers semantic `search_script`). Set `created_by` to tag provenance (shown in the UI). **Pass `upsert=true`** to re-provision by name idempotently — same name → same `script_id`, updated in place (or no-op if unchanged). A plain re-upload of an existing name spawns a NEW id and returns a `warning` — don't accumulate orphans.
2. `run_script(script_id, input?)` → `run_id` **immediately, never blocks**. `input` is arbitrary JSON, reaches the job as env **`DOKAN_INPUT`** (a JSON string — **not** stdin, **not** argv).
3. Poll: `wait_for(run_id, timeout?)` (long-poll, fewest round-trips) **or** `read_logs(run_id, after_cursor)` (cursor; pass back `next_cursor`). Both return the job's structured `result` when present (see below).
4. `cancel(run_id)` kills the container. `delete_script(script_id)` removes a script and cascades its runs/logs/schedules (refused if a flow depends on it) — use it to clean up orphans.

Logs are CSV-ish `seq|stream|text`, error-first. Don't re-read from cursor 0 — thread `next_cursor`.

## Exit code = verdict (monitors, read this)

A run that **ran to completion and exited nonzero is a deterministic verdict — NOT a crash, and NOT retried.** So a monitor/gate can legitimately `exit 1` / `exit 2` to signal "found something" and it runs **exactly once** (no 3× reprint). Only genuine infra failures (NULL exit: container vanished / timeout) retry. You no longer need a monitor(exit0)/strict(exit1) split.

`exit 137` = the job hit the memory cap (cgroup OOM). Per-job caps default **1024 MiB / 2.0 CPU**; ask the operator to raise `--mem-limit-mb` / `--cpu-limit` for heavy jobs.

## Structured result + event-driven monitors

Print a line `::dokan:result:: {json}` on **stdout**. dokan captures the **last** one as the run's structured result — it is **not** logged, is returned by `wait_for`/`read_logs`, and is **POSTed to the relay** on completion. That is how a monitor ALERTS event-driven: emit a finding, the agent reacts to the relay POST — no polling, no parsing stdout.

```bash
if [ "$changed" = true ]; then
  echo "::dokan:result:: {\"alert\":true,\"items\":$items}"
  exit 1            # nonzero = finding; runs once
fi
echo "::dokan:result:: {\"alert\":false}"
```

## Secrets (API keys for jobs)

`set_secret(name, value)` once → available to a job as a **tmpfs file at `/run/secrets/<name>`** (mode 0400) and, for back-compat, as an env var (e.g. `$OPENAI_API_KEY`). **Write-only**: values are never returned or logged. `list_secrets()` shows names only. Read them in-script via `/run/secrets/<name>` or normal env (`os.environ["OPENAI_API_KEY"]`). Use for OPENAI/PERPLEXITY/GEMINI/SERPAPI, Metricool `MC_USER_ID`/`MC_USER_TOKEN`, etc. By default a job gets all globals; scope it with a **per-script allowlist** — `upload_script(..., secrets=["openai_key"])` injects only those (defense-in-depth on a trusted single-tenant box).

## Host data (trovex store, etc.)

The container is network-isolated: **no host FS, no host MCP** inside it. To use host-side data (e.g. the trovex store), fetch it on the host yourself and pass it in via `DOKAN_INPUT`. Don't expect in-container access to host services.

## Flows (DAG)

Wire-over-MCP, the differentiator — don't hand-code orchestration.

```
compose_flow(name, spec) → flow_id      # validated acyclic
  spec = { "steps": [
    {"id":"fetch","script_id":1},
    {"id":"shape","script_id":2,"depends_on":["fetch"]},
    {"id":"ship","script_id":3,"depends_on":["shape"]}
  ]}
run_flow(flow_id, input?) → flow_run_id  # immediate; engine drives the DAG
get_flow_run(flow_run_id) → status + per-step status/output
```

Each step is one container run, seeing `{flow_input, deps:{upstream_id: last_stdout}, step}` as `DOKAN_INPUT`. Durability is at the **step boundary** — a crashed engine resumes; succeeded steps are skipped. **Steps must be idempotent** (a dying step re-runs).

## Schedule

`schedule(script_id, cron, input?)` — **6-field cron, leading seconds** (`0 */5 * * * *` = every 5 min). A 5-field expression is **rejected loudly** (it would otherwise silently never fire). Each tick enqueues a run. `list_schedules()` shows id + **script name** + cron. `unschedule(schedule_id)` stops a cron (always clean up test crons).

## Find existing work

`search_script(query, limit?)` → ranked IDs + 1-line desc, never bodies. **Typo-tolerant** (pg_trgm fuzzy) when no embedder, semantic with `--embed` (the `mode` field tells you). `get_script(id, include_source=true)` only when you must read the body.

## Token discipline (baked into the contract — follow it)

- IDs/handles over payloads. Never pull a script body unless editing it.
- Always paginate logs with `next_cursor`; request only needed fields.
- `list_runs` returns server-side status counts + recent rows, not every row.

## Human surface (not yours)

The executor serves a single-page **cockpit** at `/` (status ribbon, run list + live SSE log tail, schedules rail, trigger, secrets) and raw Prometheus at `/metrics`. Deep analytics → **Grafana**: `docker compose -f observability/docker-compose.yml up -d` (Grafana on :3300, Prometheus on :9490) ships a provisioned "dokan — runtime" dashboard. You operate via MCP; humans watch the cockpit/Grafana.

## Preflight (if tools missing or server won't boot)

The stack needs Postgres + Docker + the running executor:
- `docker compose up -d` (starts `dokan-db` on :5499)
- `cargo build`, then run the executor: `target/debug/dokan --transport http --addr 127.0.0.1:8088`
- export `DOCKER_HOST` if Docker isn't at `/var/run/docker.sock` (Colima/Desktop)

A `SessionStart` hook (`.claude/hooks/dokan-preflight.sh`) checks these and auto-starts the DB.
