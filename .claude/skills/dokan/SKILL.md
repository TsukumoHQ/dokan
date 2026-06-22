---
name: dokan
description: Operate the dokan runtime — run deterministic scripts in Docker over MCP, compose DAG flows, schedule cron, read logs token-frugally. Use when the user wants to run/deploy a script or job, build a pipeline/flow/DAG of steps, schedule recurring work, or check run status/logs on dokan. Triggers — "run this script on dokan", "upload + run", "compose a flow", "schedule a job", "wire a pipeline", "check the run/logs", "dokan".
---

# Operating dokan

dokan is the **passive pipe**; you are the **operator**. It runs deterministic scripts in Docker. **No LLM runs inside** — all intelligence is yours, applied at author/trigger time, never as a node in a flow. Every MCP response is engineered for low tokens — respect that.

## Connect

MCP server `dokan` is wired in `.mcp.json` (stdio, local). Tools appear as `mcp__dokan__*`. If absent, the stack isn't up — see Preflight below.

## Core loop (single script)

1. `upload_script(name, runtime, source, description)` → `script_id`. Runtimes: `python` | `node` | `bash`. **Always write a 1-line `description`** — it powers semantic `search_script`.
2. `run_script(script_id, input?)` → `run_id` **immediately, never blocks**. `input` is arbitrary JSON, reaches the job as env `DOKAN_INPUT`.
3. Poll: `wait_for(run_id, timeout?)` (long-poll, fewest round-trips) **or** `read_logs(run_id, after_cursor)` (cursor; pass back `next_cursor`).
4. `cancel(run_id)` kills the container.

Logs are CSV-ish `seq|stream|text`, error-first. Don't re-read from cursor 0 — thread `next_cursor`.

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

Each step is one container run. A step sees `{flow_input, deps:{upstream_id: last_stdout}, step}` as `DOKAN_INPUT`. Durability is at the **step boundary** — a crashed engine resumes; succeeded steps are skipped. **Steps must be idempotent** (a dying step re-runs).

## Schedule

`schedule(script_id, cron, input?)` — 6-field cron, **leading seconds** (`0 */5 * * * *` = every 5 min). Each tick enqueues a run. `list_schedules()` to view.

## Find existing work

`search_script(query, limit?)` → ranked IDs + 1-line desc, never bodies. Semantic when the server runs `--embed`, else substring (`mode` field tells you). `get_script(id, include_source=true)` only when you must read the body.

## Token discipline (baked into the contract — follow it)

- IDs/handles over payloads. Never pull a script body unless editing it.
- Always paginate logs with `next_cursor`; request only needed fields.
- `list_runs` returns server-side status counts + recent rows, not every row.

## Human surface (not yours)

`dokan --transport http` serves a thin UI at `/` (run list, trigger, live SSE tail, secrets) and `/metrics` (Prometheus). Heavy/analytical data → Grafana. You operate via MCP; humans watch the UI.

## Preflight (if tools missing or server won't boot)

The stack needs Postgres + Docker + the built binary:
- `docker compose up -d` (starts `dokan-db` on :5499)
- `cargo build` (produces `target/debug/dokan`)
- export `DOCKER_HOST` if Docker isn't at `/var/run/docker.sock` (Colima/Desktop)

A `SessionStart` hook (`.claude/hooks/dokan-preflight.sh`) checks these and auto-starts the DB.
