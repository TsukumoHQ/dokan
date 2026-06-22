# dokan (導管)

> Agent-operated runtime for **deterministic scripts in Docker**, with an **MCP-first control plane**. Zero LLM inside. Apache-2.0.

Your coding agent uploads, runs, and reads logs over MCP. No UI clicks. The platform is the passive pipe; the agent is the orchestrator. See [PRD.md](PRD.md) for the full thesis.

## Status

**P0 — Proof shipped.** The wedge (PRD §11 step 4) is implemented and passing end-to-end: an MCP client stands up a script and runs it with zero human interaction.

```
✅ WEDGE PROVEN: agent uploaded + ran + read logs over MCP, zero UI.
```

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

Server instructions ship in-band so the agent self-limits (paginate, project fields, don't fetch bodies).

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

## Roadmap (per PRD §12)

- **P0 — Proof** ✅ single-host, one runtime, agent-operated run+logs over MCP.
- **P1 — Engine** — `SKIP LOCKED` queue, warm pool (deadpool/bollard), multi-worker capability routing, cron, resource caps.
- **P2 — Flows** — declarative `compose_flow`, DAG, step-boundary durability, retries.
- **P3 — Scale/ops** — N workers, semantic registry (fastembed+pgvector), Grafana/Loki, thin UI, relay egress, OAuth/RBAC.
- **P4 — Enterprise** — SSO, audit, HA, persistent-service engine, micro-VM isolation for untrusted code.

## License

Apache-2.0.
