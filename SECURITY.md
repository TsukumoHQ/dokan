# Security Policy

## Supported versions

dokan is in active `0.x` development. Security fixes land on the latest
`0.x` release line; there is no long-term-support branch yet.

| Version | Supported |
|---------|-----------|
| 0.4.x   | ✅ latest  |
| < 0.4   | ❌ (superseded) |

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

- **Secret scoping is defense-in-depth, not an isolation boundary.**
  Secrets are injected as **tmpfs files at `/run/secrets/<name>`** (mode 0400,
  never written to the image layer) plus, for back-compat, env vars. A **per-script
  allowlist** (`upload_script(..., secrets=[...])`) limits a script to the named
  secrets; an optional per-agent scope (`agent_secrets`) narrows extras to one
  `agent_id`. But `agent_id` is a caller-supplied, **unauthenticated** arg on the
  unauthenticated control plane — a caller can pass any id. So scoping is
  **defense-in-depth on a trusted box, not a guarantee** against a malicious caller.
  Only run scripts you trust on a box that holds secrets. (True non-spoofable
  isolation = per-agent auth tokens, a planned upgrade.)
- **The MCP control plane is unauthenticated** and binds to `127.0.0.1:8088`
  by default. It assumes anything that can reach it is already trusted. **Do
  not expose `:8088` to an untrusted network** — put it behind your own
  auth/proxy if you must.
- **Jobs run arbitrary code** you (or your agent) upload. That is the point.
  Isolation is the container boundary, not a sandbox against a malicious
  operator.

## What dokan does defend

- **Containment.** Each job runs in a fresh container with **no host filesystem
  and no host services**, discarded after the run. Outbound network is **disabled
  by default** (hermetic-by-default, v0.4.0): a job is fully network-isolated and
  its output is a deterministic function of its inputs unless it opts in with
  `network=true` (most monitors need it).
- **Container hardening (v0.4.0).** Jobs run **non-root** (uid 65534), on a
  **read-only root filesystem**, with **all Linux capabilities dropped**
  (`cap_drop: ALL`) and **`no-new-privileges`**. The only writable surfaces are a
  `/tmp` tmpfs, an opt-in `/output` bind, and the `/run/secrets` tmpfs.
- **Resource caps.** Per-job CPU and memory limits (cgroup) and a hard
  timeout — a runaway or OOM job is killed, not allowed to starve the host.
- **Write-only secrets.** Secret values are never returned by any MCP tool,
  never logged, and not echoed back; `list_secrets` exposes names only.
- **Tamper-evident, publicly-verifiable run receipts.** Completed runs carry a
  receipt that is HMAC-keyed with `DOKAN_RECEIPT_KEY` (integrity for key-holders)
  **and Ed25519-signed inside an in-toto / DSSE envelope** (v0.4.0). A third party
  can `verify` it **offline with only the public key** — no shared secret — and
  `DOKAN_TRUSTED_RECEIPT_KEYS` pins the keys allowed to sign. `reproduce`
  re-executes a network-off run and byte-compares the output against the receipt.
- **Inbound webhook tokens.** `POST /hook/<token>` triggers require an
  unguessable per-webhook token.

## Hardening checklist for operators

- Keep `:8088` bound to loopback (the default); never publish it directly.
- Set strong, persistent crypto keys in production — the daemon **fails closed**
  without them (v0.4.0): `DOKAN_SECRET_KEY` (seals secrets at rest, else they're
  stored plaintext), `DOKAN_RECEIPT_KEY` (HMAC), and `DOKAN_RECEIPT_ED25519_SECRET`
  (base64 32-byte seed — the public-verify signing key). `DOKAN_DEV_INSECURE=1`
  opts into insecure dev defaults; never set it in production. The one-command
  installer generates and persists all three for you.
- Treat the host as holding all job secrets — restrict who can upload scripts.
- Keep the runtime images (`alpine`, `python:*-slim`, `node:*-slim`) current.
