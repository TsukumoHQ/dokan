# Security Policy

## Supported versions

dokan is in active `0.x` development. Security fixes land on the latest
`0.x` release line; there is no long-term-support branch yet.

| Version | Supported |
|---------|-----------|
| 0.2.x   | ✅ latest  |
| < 0.2   | ❌ (superseded) |

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report privately via GitHub's
[private vulnerability reporting](https://github.com/TsukumoHQ/dokan/security/advisories/new)
("Report a vulnerability" on the repo's Security tab). If that is unavailable,
email **security@tsukumo.ch** with:

- a description of the issue and its impact,
- the version / commit you tested,
- reproduction steps or a proof of concept.

We aim to acknowledge within **72 hours** and to ship a fix or mitigation for
confirmed, in-scope issues on a best-effort basis. Please give us a reasonable
window to remediate before any public disclosure.

## Trust model (read this before deploying)

dokan is built for a **trusted, single-tenant operator** — your own agent
fleet on a host you control. Several deliberate design choices follow from
that and are **not** vulnerabilities:

- **Secret scoping is advisory provenance, not an isolation boundary.**
  `set_secret` injects globals into *every* job container; an optional per-agent
  scope (`agent_secrets`) narrows extras to one `agent_id`. But `agent_id` is a
  caller-supplied, **unauthenticated** arg on the unauthenticated control plane —
  a caller can pass any id. So per-agent scoping is **defense-in-depth on a
  trusted box, not a guarantee** against a malicious caller reading another
  agent's scoped secrets. Only run scripts you trust on a box that holds secrets.
  (True non-spoofable isolation = per-agent auth tokens, a planned upgrade.)
- **The MCP control plane is unauthenticated** and binds to `127.0.0.1:8088`
  by default. It assumes anything that can reach it is already trusted. **Do
  not expose `:8088` to an untrusted network** — put it behind your own
  auth/proxy if you must.
- **Jobs run arbitrary code** you (or your agent) upload. That is the point.
  Isolation is the container boundary, not a sandbox against a malicious
  operator.

## What dokan does defend

- **Containment.** Each job runs in a fresh container with **no host filesystem
  and no host services**, discarded after the run. Outbound network is **on by
  default** (most monitors need it); set `network=false` per job for a hermetic,
  no-outbound-network run (the container is then fully network-isolated and its
  output is a deterministic function of its inputs).
- **Resource caps.** Per-job CPU and memory limits (cgroup) and a hard
  timeout — a runaway or OOM job is killed, not allowed to starve the host.
- **Write-only secrets.** Secret values are never returned by any MCP tool,
  never logged, and not echoed back; `list_secrets` exposes names only.
- **Tamper-evident run receipts.** Completed runs carry a receipt keyed with
  `DOKAN_RECEIPT_KEY` (HMAC), so a run's result can be checked as produced by
  this executor by anyone holding the key. This is integrity/tamper-evidence,
  **not** a public, third-party-verifiable signature — that needs an asymmetric
  scheme (on the roadmap).
- **Inbound webhook tokens.** `POST /hook/<token>` triggers require an
  unguessable per-webhook token.

## Hardening checklist for operators

- Keep `:8088` bound to loopback (the default); never publish it directly.
- Set a strong, persistent `DOKAN_RECEIPT_KEY` in production (a random
  per-process key is used otherwise, so receipts aren't verifiable across
  restarts).
- Treat the host as holding all job secrets — restrict who can upload scripts.
- Keep the runtime images (`alpine`, `python:*-slim`, `node:*-slim`) current.
