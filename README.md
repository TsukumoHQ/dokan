<p align="center"><img src="assets/dokan-wordmark.png" alt="dokan" width="680"></p>

# dokan (導管)

> Agent-operated runtime for **deterministic scripts in Docker**, with an **MCP-first control plane**. Zero LLM inside. Apache-2.0.

Your coding agent uploads, runs, and reads logs over MCP. No UI clicks. The platform is the passive pipe; the agent is the orchestrator. See [PRD.md](PRD.md) for the full thesis.

## Status

**P0–P3 shipped and tested** (8 integration tests, all green against live Postgres + Docker).

```
✅ WEDGE PROVEN: agent uploaded + ran + read logs over MCP, zero UI.
```

- **P0 Proof** — agent uploads + runs + reads logs over MCP, zero UI.
- **P1 Engine** — SKIP LOCKED queue, warm pool, capability routing, cron, retries.
- **P2 Flows** — declarative `compose_flow` DAG, step-boundary durability, dep passing.
- **P3 Scale/ops** — semantic search (fastembed+pgvector), Prometheus metrics, thin UI + SSE tail, secrets, relay egress, bearer auth.

## Architecture (this slice)

- **Single Rust daemon** (`dokan`) — `axum` + `rmcp` MCP server, `stdio` (local) or Streamable HTTP (remote).
- **State** — Postgres (`sqlx`, runtime queries, no offline cache needed). Tables: `scripts`, `runs`, `logs`.
- **Execution** — `bollard`: one job = one clean container (`python:3.12-slim` / `node:22-slim` / `alpine`), discarded after. Per-job memory + CPU caps and a hard timeout. Code is trusted → raw containers, no micro-VM.
- **Logs** — container stdout/stderr streamed line-by-line into Postgres, served back to the agent cursor-paginated.

## MCP surface (token-frugal contract)

| Tool | Returns |
|---|---|
| `search_script` | ranked IDs + 1-line desc, `"showing X of Y"` |
| `get_script` | projected metadata; body only with `include_source=true` |
| `upload_script` | `script_id` + version |
| `run_script` | `run_id` immediately — **never blocks** |
| `read_logs` | new lines since `after_cursor`, `next_cursor`, status; CSV-ish `seq\|stream\|text` |
| `wait_for` | long-poll to terminal status + tail |
| `list_runs` | server-side status counts + recent rows |
| `cancel` | kill container + mark canceled |
| `compose_flow` | declarative DAG spec → `flow_id` (validated acyclic) — wire-over-MCP |
| `run_flow` / `get_flow_run` | run a DAG; poll overall + per-step status/output |
| `schedule` / `list_schedules` | cron a script (6-field, leading seconds) |

Server instructions ship in-band so the agent self-limits (paginate, project fields, don't fetch bodies). Semantic ranking via local embeddings (`--embed`), substring fallback otherwise.

## Operator surface (HTTP, humans)

`dokan --transport http` also serves a thin UI + API behind an optional bearer token (`DOKAN_TOKEN`):

| Route | Purpose |
|---|---|
| `GET /` | self-refreshing run list |
| `GET/POST /api/runs` | list / trigger a run |
| `GET /api/runs/{id}/logs` | cursor logs |
| `GET /api/runs/{id}/stream` | live SSE log tail |
| `GET/POST /api/secrets` | write-only secrets (injected as job env) |
| `GET /metrics` | Prometheus (`dokan_runs_*`) → Grafana |

Job results POST to `DOKAN_RELAY_URL` on completion (mesh egress).

## Quickstart

```sh
# 1. state store
docker compose up -d

# 2. build
cargo build

# 3. prove the wedge end-to-end (needs Docker; honors $DOCKER_HOST)
export DOCKER_HOST=unix:///path/to/docker.sock   # Colima/Docker Desktop; omit if /var/run/docker.sock
cargo test --test smoke -- --nocapture
```

### Wire into Claude Code

Local (stdio): see [.mcp.json](.mcp.json) — point `DOCKER_HOST` at your socket.

Remote (HTTP):

```sh
dokan --transport http --addr 127.0.0.1:8088   # MCP at http://127.0.0.1:8088/mcp
```

## Config

| Flag / env | Default |
|---|---|
| `--transport` / `DOKAN_TRANSPORT` | `http` (`stdio` for local agents) |
| `--addr` / `DOKAN_ADDR` | `127.0.0.1:8088` |
| `--database-url` / `DATABASE_URL` | `postgres://dokan:dokan@127.0.0.1:5499/dokan` |
| `DOCKER_HOST` | local socket if unset |

## Worker / semantic / ops config (P1–P3)

| Flag / env | Purpose |
|---|---|
| `--caps` / `DOKAN_CAPS` | runtimes this worker serves (routing) |
| `--concurrency` / `DOKAN_CONCURRENCY` | max concurrent jobs |
| `--warm-idle` / `DOKAN_WARM_IDLE` | warm containers kept per image |
| `--embed` / `DOKAN_EMBED` | enable semantic search |
| `--relay-url` / `DOKAN_RELAY_URL` | mesh egress endpoint |
| `--token` / `DOKAN_TOKEN` | bearer token for the HTTP surface |

## Roadmap (per PRD §12)

- **P0 — Proof** ✅ agent-operated run+logs over MCP.
- **P1 — Engine** ✅ `SKIP LOCKED` queue, warm pool, capability routing, cron, retries, resource caps.
- **P2 — Flows** ✅ declarative `compose_flow`, DAG, step-boundary durability, retries.
- **P3 — Scale/ops** ✅ semantic registry (fastembed+pgvector), Prometheus metrics, thin UI + SSE, secrets, relay egress, bearer auth. Multi-worker = run more processes vs the same Postgres.
- **P4 — Enterprise** (next) — SSO/OAuth 2.1 (available via rmcp), audit, HA, persistent-service engine, micro-VM isolation, Loki shipping + Grafana dashboards.

## License

Apache-2.0.
