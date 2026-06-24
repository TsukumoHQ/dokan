<p align="center"><img src="assets/wordmark.png" alt="dokan" width="280"></p>

<p align="center"><b>Your AI coding agent builds and runs the workflow. You don't click.</b></p>

<p align="center">Agent-operated automation runtime · deterministic scripts in Docker · <b>zero LLM inside</b> · Apache-2.0</p>

---

**dokan** is an automation runtime built for the agent era. Instead of a human clicking through a UI, your coding agent stands up, runs, and schedules workflows itself by talking to dokan over MCP. The platform runs deterministic code in clean containers and **burns zero tokens**: the expensive intelligence stays in your agent, outside the runtime.

Think *Sidekiq/cron for AI agents*: the agent scripts the mechanical 80%, dokan executes it cheaply and reliably, you don't touch a dashboard.

## Why dokan
- **Agent-operated.** your agent uploads, wires, triggers, reads logs over MCP. No UI.
- **Zero LLM inside = zero token burn.** deterministic code, not LLM-in-the-loop. The platform never spends tokens to run your workflows.
- **Deterministic + reliable.** one job = one clean Docker container, per-job CPU/mem caps, timeouts, retries, content-addressed cache (never recompute unchanged work).
- **Real triggers.** cron + inbound webhooks (POST /hook/<token>, Stripe/Calendly/GitHub-ready).
- **Token-frugal.** every MCP response engineered for an agent's context budget.

## Quickstart
Prereqs: Docker running and a Rust toolchain. The daemon's default `DATABASE_URL` already points at the compose database, so there's nothing to configure.
```sh
docker compose up -d            # Postgres state store (pgvector) on :5499
cargo build --release
./target/release/dokan          # HTTP daemon on 127.0.0.1:8088 — UI at /, MCP at /mcp
```
Schema migrations apply automatically on boot.

## Wire into your agent (MCP)
Point your agent's MCP config at the daemon:
```jsonc
"dokan": { "type": "http", "url": "http://127.0.0.1:8088/mcp" }
```
Your agent now has the full dokan toolset over MCP.

## MCP surface (token-frugal)
| Tool | Returns |
|---|---|
| search_script | ranked IDs + 1-line desc |
| upload_script | script_id + version |
| run_script | run_id immediately, never blocks |
| read_logs / wait_for | cursor logs / long-poll to terminal + tail |
| schedule / list_schedules | cron a script (6-field) |
| compose_flow / run_flow | declarative DAG, wired over MCP |
| create_webhook | inbound HTTP trigger to a script/flow |
| set_secret / list_secrets | write-only secrets, injected as job env |
| cancel · list_runs · get_script | … |

Server instructions ship in-band so the agent self-limits.

## How it works
Single Rust daemon (axum + rmcp MCP server, stdio or Streamable HTTP). State in Postgres. Execution via Docker: one job, one clean container (python:3.12-slim / node:22-slim / alpine), discarded after, per-job caps + hard timeout. Logs stream into Postgres, served cursor-paginated. Thin operator cockpit at / + Prometheus at /metrics.

## Status
Active development, built and run in production by the team that makes it (we run our own agent fleet's automation on dokan). **Ready for: demos, design partners, technical early adopters.** Not yet turnkey multi-tenant enterprise (no SSO/RBAC/HA), out of scope while we serve internal teams. Honest about where it is.

## License
Apache-2.0. Use it, embed it, build on it.

---
*Part of the [tsukumo](https://tsukumo.ch) suite: open tools for running AI agents well at scale.*
