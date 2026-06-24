# Dokan (導管) — Product Requirements Document

> **Name:** `dokan` — Japanese 導管 "conduit / duct". The platform is the **passive pipe** the agent operates through; the agent is the orchestrator, not Dokan.
> **Status:** Draft v0.1 — pre-build, scoping
> **Date:** 2026-06-22
> **One-liner:** An Apache-2.0, self-hostable, **agent-operated** runtime for **deterministic scripts in Docker**, with an **MCP-first control plane**. An external coding agent (Claude Code) is the primary operator — it searches, uploads, wires, triggers, and reads logs over MCP. **Zero LLM runs inside the platform.** Every MCP response is engineered for **low token usage**.

---

## 1. The insight

Existing workflow platforms are built **UI-first for humans** (Windmill, n8n) or **SDK-code-first for developers** (Temporal, Inngest, Trigger.dev, Mastra). The agent era inverts the operator: the human's coding agent should stand up and run workflows by *talking to a control plane*, not by clicking a builder or compiling an SDK.

Two cost realities drive the design:

1. **Determinism is free; LLM tokens are not.** The platform must run the **80% deterministic** work (fetch, transform, rules, ship) as plain scripts at **zero token cost**. The **20% intelligence** lives *outside* — in the operating agent at author/trigger time, never as a node inside a flow.
2. **The consumer is an agent, so the API surface is a token budget.** Every byte a tool returns eats the agent's context window. Token-frugality is a **first-class product feature**, not an optimization.

**"Agentic-first" here means agent-OPERATED, not agent-executing.** No agent runs *inside* a flow. The platform is a deterministic execution substrate that an external agent drives over MCP.

---

## 2. The wedge — what makes us not-a-Windmill-clone

Research confirms the **mechanism is not novel** (tiny alpha MIT npm projects ship "agent creates cron jobs over MCP" today), but the **productized, well-positioned version does not exist**. White space is real along **four axes simultaneously** — no funded product owns the intersection:

1. **License purity** — Apache-2.0. Only Trigger.dev (Apache-2.0) and Temporal (MIT) are cleanly permissive. Windmill (AGPL/EE), n8n (non-OSI fair-code), Inngest core (SSPL), Restate (BSL), Mastra (EE) all carry baggage.
2. **Zero-LLM-inside** — the anti-thesis. Windmill, n8n, Inngest/AgentKit, Mastra all *sell* LLM-in-workflow as the core feature. No serious self-hostable player owns "the platform runs zero LLMs; deterministic scripts only."
3. **Upload-script-and-wire-over-MCP** — every mature platform is code-first: you author SDK/TS code and deploy. None lets an agent upload arbitrary **polyglot Docker scripts** and **wire flows declaratively over MCP**.
4. **Token-frugal MCP + agent-as-primary-operator** — existing MCP servers are dashboard/CLI-first with MCP bolted on; none designed responses for an agent's token budget.

**Honest framing:** differentiation is **execution + positioning**, not core-idea novelty. The two to out-position:
- **Trigger.dev v4** (closest threat) — Apache-2.0, Docker exec, official prod MCP server. *Beat it on:* declarative wire-over-MCP (it is TS-code-first, flows-in-code), zero-LLM positioning, token-frugal responses, agent-as-primary-operator (its MCP is bolted on).
- **Windmill** (best MCP today) — *beat it on:* Apache-2.0 vs AGPL, Docker-per-job vs per-language runtimes, zero-LLM vs LLM-laden core, agent-first vs human-UI-first.

**Positioning line:** *"Sidekiq/Cronicle for agents."* Your coding agent builds the workflow; you don't click.

---

## 3. Target user

- **Dev teams** running fleets of coding agents. Reference scale: **~14 devs × 4–5 agents each ≈ 70 concurrent operators** hitting the control plane.
- **Trust model: internal only.** Scripts are written by the team + their agents. Not untrusted third-party code. → isolation is **opt-in**, not mandatory (changes the security posture materially).
- Part of a **multi-product mesh**: other products trigger scripts via API; results flow back to the team's **relay**. Dokan is a **node in the mesh**, not an island.

---

## 4. Design principles

1. **Token-frugal by default** (the differentiator — see §7 for the contract). Summary-first, IDs-not-blobs, cursor pagination, field projection, tail/error-first truncation, budgets-with-notes.
2. **Code-first definitions, git-native.** Flows are code/declarative specs reviewable in PRs — not drag-drop.
3. **Thin UI on purpose.** The UI only *operates* (run list, trigger, log tail, secrets). All analytical/complex data → **Grafana**. We never build an app-builder (Windmill's heaviest weight).
4. **Deterministic core, zero LLM inside.** Intelligence is the operator's, applied at the edge.
5. **Open-core, Apache-2.0 core.** Sell cloud/enterprise (SSO, RBAC, audit, HA) on top.
6. **MCP-first; UI/API are secondary surfaces over the same core.**

---

## 5. Architecture

```
                 ┌─────────────────────────────────────────────┐
   Claude Code   │              CONTROL PLANE (daemon)          │
   agents  ──────┤  MCP server (Streamable HTTP) + REST API     │
   (≈70)   MCP   │  scheduler · queue · state · log router      │
                 │  semantic search (script registry)           │
   other         │                                              │
   products ─────┤  REST API  ──► results ──► RELAY (mesh)       │
                 └───────────────┬─────────────────────────────┘
                                 │  Postgres (queue + state + vectors)
                                 │  FOR UPDATE SKIP LOCKED
                 ┌───────────────┴─────────────────────────────┐
                 │            WORKER NODES (N hosts)            │
                 │  Docker warm pool (pre-baked images)        │
                 │  pool warm · run clean · discard            │
                 │  stdout ──► log router ──► Loki             │
                 └─────────────────────────────────────────────┘

   Observability:  metrics ──► Prometheus ──► Grafana (heavy data)
                   thin web UI ◄── SSE (live tail, humans only)
```

- **Control plane** = single Rust daemon. Deploy = binary + Postgres + Docker (`docker compose up`, not `curl | sh` — be honest, it is not a 1-binary toy at this scale).
- **State/queue** = **Postgres** (SQLite dies at ~70 concurrent writers). Queue is hand-rolled `FOR UPDATE SKIP LOCKED`.
- **Workers** = N hosts with Docker; pull jobs, run containers from a warm pool, stream stdout back.
- **Relay integration** = REST egress; job results posted to the team's existing relay for cross-product coordination.

---

## 6. Execution model

**Locked defaults (veto open):**

1. **Docker, warm pool.** "Pool warm, run clean, discard." Latency cost is image-pull + init, *not* namespace creation — a pool of pre-baked idle containers kills the 2–3s → <300ms gap. Each job gets a clean fs/PID/net namespace; containers are **not** reused dirty in-place. Since code is **trusted**, raw containers suffice (no gVisor/Firecracker needed; those were only required for untrusted multi-tenant code).
2. **Batch-long first; persistent services = phase 2.** v1 = jobs that run (seconds → hours) then exit. Persistent always-on services are a *second engine* (supervisor/restart-policy semantics) — deferred to avoid doubling scope.
3. **Durability at the STEP boundary.** A flow is a DAG of steps (each step = one container run). The engine checkpoints state in Postgres *between* steps; on crash it resumes at the last completed step. **Inside** a long step there is no magic — it re-runs if it dies, so **steps must be idempotent**. This is the deliberate escape from the Temporal trap (mid-script durability would force a constraining replayable SDK — the exact "usine à gaz" we're fleeing).

**Multilang:** the *orchestrator* is one binary; **runtimes are declared per worker.** A worker advertises capabilities (`python3.12`, `rust`, `node`...); the scheduler routes jobs to capable workers. Rust scripts ship as **pre-built artifacts** (we do not compile arbitrary Rust in the hot path); Python via `uv`-managed envs baked into images.

**Resource safety (mandatory per job):** hard timeout, memory cap, CPU cap (cgroups), kill-switch, bounded worker concurrency (queue the overflow). Optional `sandbox: gvisor|firecracker` flag reserved for future untrusted workloads.

---

## 7. MCP surface — the token-frugal contract

The control plane's MCP server **is the product API.** It must obey a strict token budget. Backed by 2026 research (Anthropic *Code execution with MCP*; MCP spec 2025-11-25; GitHub MCP server-instructions; Axiom wide-events).

**Core tools (signatures are illustrative, to be grilled):**

| Tool | Returns (token-frugal) |
|---|---|
| `search_script(query, fields?, limit?)` | Ranked **IDs + 1-line desc** only. Never script bodies. Semantic (local embeddings). `"showing 10 of 240"`. |
| `get_script(id, fields?)` | Projected fields on demand; full body only when explicitly requested. |
| `upload_script(name, runtime, source\|artifact, meta)` | Returns `script_id` + version. |
| `configure(id, params)` | Compact ack: id + status enum. |
| `compose_flow(spec)` | Declarative DAG spec → `flow_id`. **Wire-over-MCP** — the key differentiator vs code-first rivals. |
| `run_script(id, input)` / `run_flow(id, input)` | **Returns `run_id` immediately. Never blocks.** |
| `read_logs(run_id, after_cursor?, fields?, limit?)` | **Tail-first, error-first.** New lines since cursor + `next_cursor` + status. Tabular as CSV-ish, not repeated-key JSON. |
| `wait_for(run_id, until?, timeout?)` | Long-poll: blocks until status change/timeout; returns tail + status. Fewer round-trips for long jobs. |
| `list_runs(filter?, fields?, limit?)` | Server-side aggregation: counts + last-failure, not every row. Cursor-paginated. |
| `cancel(run_id)` | Compact ack. |

**Rules baked into the server:**
- **IDs/handles over payloads** everywhere.
- **Field projection** (`fields`/`view`) on every read — the highest-leverage lever (−80–90% tokens).
- **Cursor pagination** (opaque `nextCursor`, never offsets) everywhere.
- **Tail-first/error-first** truncation for logs; budget cap per response with `"showing X of Y"` notes.
- **Ship server instructions in-band** (GitHub MCP lesson) so the agent self-limits ("always paginate", "request only needed fields").
- **Assume lazy tool-schema loading** (Tool Search, GA Feb 2026) — keeps the multi-tool surface from costing ~55k boot tokens.

**Log-following pattern (no raw SSE for the agent):** `run_script` → `run_id`; then cursor `read_logs(after=...)` polling (default, lowest controllable cost) or `wait_for` long-poll (fewer turns). MCP progress notifications available as a cheap heartbeat but are *status*, not bulk log delivery, and not every client renders them into the model context.

**Transport:** MCP **Streamable HTTP** for remote agents (single endpoint; SSE used *internally* for server→client streaming), **stdio** for local. HTTP+SSE transport is deprecated — do not build on it. Claude Code: `stdio` local, `http` remote.

**Tool-set is static per binary — reconnect on ship.** dokan's tools are compiled into the daemon (`#[tool]` methods on `Dokan`), so adding a tool means a **new binary → daemon restart**, which tears down every Streamable-HTTP MCP session. A dead session can't be pushed to, and `notifications/tools/list_changed` only helps a *live* server whose tool set mutates in-process — ours doesn't — so dokan deliberately does **not** advertise `tools.listChanged` (advertising a capability it never exercises would mislead clients). The contract is therefore operational: after a tool ship + restart, connected agents must **reconnect** their dokan MCP (Claude Code: `/mcp`; a relay/gateway proxy: refresh its upstream connection) to re-`initialize` and pick up the new `tools/list`. **Announce the restart on the fleet channel** so agents reconnect instead of running against a stale tool list. A true zero-reconnect story would need a stable MCP gateway in front of the daemon that survives restarts and re-exposes tools — an edge/relay concern, out of the single-binary scope.

---

## 8. Observability & UI

| Channel | Transport | Audience |
|---|---|---|
| Agent ← logs/state | **MCP tool calls** (cursor reads) | Claude Code |
| Human ← live tail | **SSE / WebSocket** | thin web UI |
| Analytics ← metrics | **Loki + Prometheus → Grafana** | dashboards |

- **Thin UI** = run list, trigger, live log tail, secrets management. Nothing analytical.
- **Grafana owns complex data.** We ship dashboards, we don't build a viz layer.
- Logs: jobs' stdout shipped to Loki; queryable in Grafana; persisted + cursor-served to agents.

---

## 9. Tech stack (validated 2026, all permissive — Apache-2.0 clean)

| Layer | Crate / tool | Ver | License | Note / gotcha |
|---|---|---|---|---|
| Docker control | **bollard** | 0.21 | Apache-2.0 | Demux multiplexed log frames; conn pool defaults to 1 thread — tune. Pre-1.0 churn. |
| Queue | **hand-rolled `SKIP LOCKED`** (sqlx) | — | — | <100 LOC; canonical & multi-worker-safe. **Avoid apalis** (1.0-rc churning 6 mo). `pgmq` (now `pgmq/pgmq`) if SQS semantics wanted. |
| MCP server | **rmcp** (official) | 1.7 | Apache-2.0 | Stable Streamable HTTP, progress notifs, **OAuth 2.1** built-in; handles 70 agents single-process. `LocalSessionManager` = in-memory → sticky sessions if behind LB. |
| HTTP/API | **axum** | 0.8 | MIT | SSE for UI. 0.8 path syntax `/{id}` (old `/:id` panics at boot). |
| DB | **sqlx** | 0.9 | MIT/Apache | Commit `.sqlx` for offline builds; missing `runtime-*` feature = runtime panic. |
| Semantic search | **fastembed** + **pgvector** | 5.17 / 0.4 | Apache-2.0 / MIT-Apache | Local embeddings (zero API cost). **Not pure Rust** (pulls `ort`) — pre-stage ONNX runtime/weights for CI/musl/air-gapped. pgvector: index opclass must match operator or silent seq-scan. |
| Cron | **tokio-cron-scheduler** | 0.15 | Apache/MIT | 6-field cron with leading **seconds**; needs live Tokio daemon (≠ launchd model). |
| Container pool | **deadpool::managed::Manager** over bollard | — | — | No warm-pool crate exists — implement the Manager yourself. |
| Observability | tracing + tracing-opentelemetry + metrics-exporter-prometheus | — | permissive | OTLP → Prometheus → Grafana. |

> Reference (do **not** copy — AGPL): Windmill's Rust backend for queue/worker patterns. Read for ideas, license-incompatible to lift.

---

## 10. Non-goals (v1)

- ❌ No app-builder / visual no-code UI (Windmill's weight).
- ❌ No LLM execution inside flows.
- ❌ No mid-script durability / replayable-SDK (Temporal trap).
- ❌ No untrusted multi-tenant code execution (no gVisor/Firecracker in v1 — trusted team only).
- ❌ No persistent always-on services (phase 2).
- ❌ No 10-language breadth chase — start with Python + one more, expand by worker capability.

---

## 11. MVP — vertical slice that proves the wedge

Build in this order. The wedge is proven at **step 4**, before any UI or queue sophistication.

1. **`axum` + `rmcp`** → MCP server exposing `upload_script`, `run_script`, `read_logs`, `list_runs`.
2. **`bollard`** → run one Python script in one container; capture + cursor-serve logs.
3. **`sqlx` + Postgres** → persist scripts, runs, logs, cursors.
4. **Claude Code connects over MCP → uploads + runs + reads logs, with NO UI.**
   → *This is the demo.* An agent stands up and runs a workflow, zero human clicks. If this lands, the rest is known execution.
5. Then, in order: hand-rolled queue (`SKIP LOCKED`) → warm pool (deadpool/bollard) → `compose_flow` DAG + step-boundary checkpointing → semantic `search_script` → Grafana/Loki wiring → thin UI → relay egress.

---

## 12. Roadmap phases

- **P0 — Proof (MVP §11):** single-host, one runtime, agent-operated run+logs over MCP.
- **P1 — Engine:** Postgres queue, warm pool, multi-worker routing by capability, cron, resource caps.
- **P2 — Flows:** declarative `compose_flow`, DAG, step-boundary durability, retries.
- **P3 — Scale/ops:** N worker nodes, semantic registry, Grafana/Loki, thin UI, relay mesh egress, OAuth/RBAC.
- **P4 — Open-core/enterprise:** SSO, audit, HA, persistent-service engine, optional micro-VM isolation for untrusted workloads.

---

## 13. Open questions / risks

1. **Trigger.dev is genuinely close** (Apache-2.0 + Docker + prod MCP). Wedge must stay sharp: declarative-wire-over-MCP + zero-LLM + token-frugal + agent-first. Validate the gap is felt, not theoretical.
2. **Infra honesty:** at 70 agents + Postgres + Docker + Grafana, infra weight ≈ Windmill. "Light" is **DX/agent-operated**, not ops footprint. Don't oversell "lightweight."
3. **MCP surface design** is the real product — §7 signatures need a dedicated grilling pass.
4. **Idempotency burden** on script authors (step-boundary durability) — document patterns, provide helpers.
5. **fastembed** dependency weight (`ort`) — consider remote embedding or a lighter local model if CI/musl friction bites.
6. **Worker capability routing** complexity — keep declarative and simple.
7. **Name** — `dokan` (導管) validated.

---

## 14. Licensing / business

- **Core: Apache-2.0** (patent grant, commercial-friendly, genuinely differentiated vs the AGPL/SSPL/BSL field).
- **Open-core:** monetize cloud + enterprise (SSO, RBAC, audit logs, HA, managed workers, micro-VM isolation).
- Adoption-first: permissive license + dev-team-native + agent-operated as the growth wedge.

---

## Sources

Competitor landscape, Rust stack validation, and MCP token-frugality were researched via web (June 2026) across: Anthropic *Code execution with MCP*; MCP spec 2025-11-25 (Transports, Pagination, Progress); Claude Code MCP docs; GitHub MCP changelog; Axiom wide-events; crates.io/docs.rs for all crate versions/licenses; Trigger.dev, Windmill, Temporal, Inngest, Restate, Mastra, Kestra, n8n docs/licenses. Full citation set retained in research notes.
