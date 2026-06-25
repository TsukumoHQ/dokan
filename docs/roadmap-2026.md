# dokan roadmap — grounded research synthesis (2026-06)

Status: **proposal for CTO** · Origin: dokan-core, owner-prompted · Method: 5 parallel web-research
agents (cockpit UX, reproducibility/verify, agent-runtime landscape, output-artifacts + secret-scoping,
dark-dashboard design). Every claim below is sourced in the per-section links.

---

## 0. The strategic finding (read this first)

Across the landscape research, **nobody owns dokan's wedge.** The 2026 agent-infra space splits into
durable-execution engines (Temporal, Inngest, Trigger.dev, Restate, DBOS, Hatchet, Windmill), agent
sandboxes (E2B, Modal, Daytona, Fly), and agent frameworks (Mastra, LangGraph). **Every one of them
embeds LLM calls *into* the runtime** (`step.ai`, managed agents). dokan inverts it: the intelligence
stays in the agent at the edge; the runtime is a deterministic executor that **cannot burn tokens**.

That is the literal productization of Anthropic's **"code execution with MCP"** thesis (Nov 2025:
agents offload deterministic work to a sandbox and filter before the context window, ~98% token
savings) — which describes the offload target but ships no runtime. dokan is that runtime.

**Three defensible angles (lead with these):**
1. **Zero-LLM-inside as an architectural guarantee**, not a feature — the deterministic offload the
   code-execution-with-MCP pattern needs.
2. **Agent-*operated*, not MCP-wrapped** — Temporal/Trigger bolt an MCP server onto a human-authored
   workflow; dokan's agent *is the operator* (stands up, wires, schedules). "authored-by-agent vs
   operated-by-agent."
3. **Verifiable + sovereign** — local-first + content-addressed cache + a re-verifiable receipt. The
   provenance/receipts trend is real and accelerating (SLSA, in-toto, Sigstore, the AAR signed-receipt
   draft, "verifiability-first agents") but lives in research/security vendors, **not in the runtime
   layer**. dokan baking it in is unique.

**Avoid as headline claims (saturated):** "durable execution for agents", "secure sandbox" (E2B/Modal
knife-fight), "agent framework / orchestration".

Sources: [Anthropic code-execution-with-MCP](https://www.anthropic.com/engineering/code-execution-with-mcp) ·
[durable-execution-for-agents critique](https://greenabstracts.substack.com/p/the-durable-execution-stack-for-agents) ·
[O'Reilly AI Agents Stack 2026](https://www.oreilly.com/radar/the-ai-agents-stack-2026-edition/) ·
[Temporal MCP server (bolt-on proof)](https://temporal.io/code-exchange/temporal-mcp-server) ·
[Verifiability-First Agents](https://arxiv.org/pdf/2512.17259) · [sandbox wars](https://agentmarketcap.ai/blog/2026/04/07/ai-agent-sandbox-infrastructure-e2b-modal-daytona-fly-machines-secure-code-execution)

---

## 1. `verify` / `reproduce` — the differentiator primitive  ⭐ top pick

The reproducibility research found **Rosalind** (a deterministic genomics engine) — a near 1:1 mirror of
dokan's design, which validates the whole concept. The key distinction, which we must not conflate:

- **`verify`** = re-hash inputs/outputs + check the receipt signature, **no re-run**. Offline, instant.
  **Table-stakes** (SLSA, GitHub attestations, Turborepo all do this).
- **`reproduce`** (a.k.a. `verify --rerun`) = content-locate the inputs by hash, **re-execute** the
  exact invocation, **byte-compare** the output against the receipt. **The differentiator** — valued
  precisely in regulated/audit/trustless contexts, and *no provenance-signing tool offers it as a
  runtime primitive because they don't own execution.* dokan does.

Borrow Rosalind's exit-code vocabulary verbatim: **0 REPRODUCED / 5 TAMPERED / 6 DIVERGED /
7 INCONCLUSIVE**. Positioning: **"verify by re-execution, not by trust"** — SLSA verifies a *document*;
dokan re-runs and byte-compares.

**Honesty rails (the claim breaks without these):**
- **Determinism is a property of the workload, not just the runtime.** dokan guarantees hermetic
  isolation + pinned image; it can't stop user code using wall-clock / unseeded RNG / map ordering.
  Scope it: "the runtime is deterministic; your output is reproducible iff your code is" — and let
  `reproduce` be the thing that *proves which*.
- **The INCONCLUSIVE case is real:** gzip/tar/timestamped outputs aren't byte-stable even when
  logically identical → needs a canonicalization story (that's why exit 7 exists).

Sources: [Rosalind (verify + reproduce + exit codes)](https://github.com/logannye/rosalind) ·
[reproducible-builds.org](https://reproducible-builds.org/) · [rebuilderd](https://github.com/kpcyrd/rebuilderd) ·
[Nix --rebuild --check](https://nix.dev/manual/nix/2.32/advanced-topics/diff-hook.html) ·
[SLSA verifying-artifacts (no rebuild)](https://slsa.dev/spec/v1.0/verifying-artifacts)

---

## 2. Receipt hardening — align to in-toto / SLSA  ⭐

Current receipt = `HMAC(image digest + source + input + output + input-file hashes)`. Solid
content-addressing core; gaps ranked:

| Gap | Severity | Fix |
|---|---|---|
| **HMAC (symmetric) ≠ "signed"** | **High** | HMAC only proves integrity to secret-holders (this is *exactly* Turborepo's model, which explicitly disclaims it as "not a security feature"). **Stop saying "signed"; say "tamper-evident."** For public, third-party-verifiable provenance → **Ed25519 / DSSE** (optionally Sigstore-keyless). |
| **Hermeticity not an explicit claim** | **High (upside)** | Record `network: disabled` as a first-class signed predicate → maps to **SLSA L4 hermetic**, and it's what *licenses* "soundly deterministic." Make it the headline, not an asterisk. |
| No in-toto envelope | Med | Wrap as an in-toto Statement (`subject`=output digest, `predicateType: dokan.dev/Run/v1`) → unlocks `cosign`/`gh attestation`-style tooling interop. |
| No invocation record (cmd/args/env) | Med | SLSA verifiers match `externalParameters`; without the exact invocation the receipt isn't the thing you can re-execute (ties to §1). |
| No builder identity / version, timestamps | Low | Record dokan + runtime version, `startedOn`/`finishedOn`, `invocationId`. Cheap SLSA alignment. |

Sources: [in-toto Statement spec](https://github.com/in-toto/attestation/blob/main/spec/v1/statement.md) ·
[SLSA provenance fields](https://slsa.dev/spec/v1.0/provenance) ·
[Turborepo HMAC "not a security feature"](https://turborepo.dev/docs/core-concepts/remote-caching) ·
[Bazel AC-poisoning vs CAS](https://jmmv.dev/2025/09/bazel-remote-execution.html) ·
[GitHub artifact attestations UX](https://docs.github.com/en/actions/concepts/security/artifact-attestations)

---

## 3. Output artifacts — `/output` (symmetry with `/input`)  ⭐

Mirror the input design (just shipped). Two camps in prior art: *explicit manifest* (GH Actions, Modal,
E2B) vs *magic directory* (OpenAI whole-FS, Daytona `/logs/artifacts/`). For an agent-driven runtime
the **magic-dir + content-addressed blob** model wins (the agent doesn't know filenames ahead of time).

- **Writable `/output`** in every container; **auto-capture everything** written there at exit into the
  *same* content-addressed blob store `/input` uses (dedup free).
- **Manifest as the run result**: `[{path, sha256, size, content_type}]` — token-frugal handle the
  operator polls; bytes fetched separately (`download_blob`, like `include_source=true` is opt-in).
- **Caps (enforce, don't trust):** per-file (~100 MB), per-run total (~512 MB–1 GB), **file-count**
  (GH Actions caps 500/job). Stream on capture; reject over-cap with a clear error in the result.
- **Retention:** 7-day TTL default (Modal=7d, GH=90d), refcount + TTL GC.
- **Avoid** whole-container-FS capture (noisy, leaks scratch, hard to cap).

Sources: [GH Actions artifacts](https://docs.github.com/en/actions/tutorials/store-and-share-data) ·
[Daytona auto-collect](https://www.daytona.io/docs/en/file-system-operations/) ·
[Modal Volumes](https://modal.com/docs/guide/volumes) ·
[OpenAI Container Files API](https://developers.openai.com/api/docs/guides/tools-code-interpreter)

---

## 4. Secret scoping — retire the single-tenant limitation  ⭐

Today secrets are **global** (every secret → env var in every container). dokan runs **arbitrary
untrusted code**, so this is both a blast-radius problem and the single-tenant ceiling. Adopt the
**Modal model**: named secrets + **per-script allowlist**.

- `secrets: ["openai_key", "stripe_key"]` on the script/run → only the declared secrets are injected
  (least privilege). Namespace by tenant (`tenant/name`) → the path to multi-tenant.
- **Injection: tmpfs file mounts at `/run/secrets/<name>`, NOT env vars.** Env is the *worst* channel
  for untrusted code — readable by every process, leaks into logs / crash dumps / `docker inspect`,
  propagates to children. Mount secrets **outside** `/output` so a job can't copy them into a captured
  artifact. Mask known secret values in log capture.
- **Migration (no flag-day):** Phase 0 opt-in (`secrets` declared → only those; omitted → global +
  deprecation warning). Phase 1 `strict_secret_scoping` flag (deny-by-default). Phase 2 default-on,
  global injection becomes a removed escape hatch.

Sources: [Modal Secrets (per-fn allowlist)](https://modal.com/docs/guide/secrets) ·
[GitHub least-privilege secrets](https://github.blog/security/application-security/implementing-least-privilege-for-secrets-in-github-actions/) ·
[OWASP Secrets Management (env vs file)](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) ·
[Vault dynamic secrets](https://developer.hashicorp.com/vault/tutorials/db-credentials/database-secrets) ·
[Doppler scoping](https://docs.doppler.com/docs/secrets)

---

## 5. Cockpit — beyond the redesign

The redesign (sidebar nav, cyan #22d3ee, tabbed run drawer with the receipt, scripts/artifacts panels)
is built (branch `feat/cockpit-redesign`). The UX research says the **next** layer is what makes an ops
cockpit best-in-class — and points at dokan's moat:

- **Run waterfall timeline** — one span per step, x=time, width=duration, color=status. Now the default
  expectation (Temporal Timeline, Inngest/Trigger.dev OTel waterfall). **Table-stakes; highest-leverage
  single thing.** Surface queue-wait vs execution as segments of the same bar.
- **Fan-out `{n,ok,failed}` rollup → lazy-expand to children** — Temporal's Compact view; **exactly
  dokan's map model**, validates the instinct.
- **💎 Cache "recalled vs fresh" badge** — tag each step `FRESH` / `CACHED (recalled)` with the input
  hash + **time-saved** ("recalled in 4ms, would've taken ~30s"). The UX research's verdict: this is
  the **most under-served area across all 8 tools** (Temporal memoizes but doesn't *show* it) and
  directly dramatizes dokan's determinism promise. Cheap if we already key on input hashes.
- **Retry attempts as separate spans**; **replay-from-step with edited payload** (Trigger.dev — the
  most-loved debugging affordance; later, gated on the saga/checkpoint model we mostly have).
- **Receipt-verify in the UI** — one click re-checks the receipt → a "verified" badge. Pairs with §1/§2.

**Design craft rails** (from the dark-dashboard research, to keep it off the generic-AI-UI baseline):
4px grid + density toggle (32/40px rows); hover = 1px cyan left-rail + ~3% bg lift, **never a full cyan
row**; mono+tabular-nums for all machine data; **middle-truncated hashes** (`a1b2…9f3c`) + click-to-copy;
`—` for empty cells; status = a small saturated **dot**, not a loud pill; drawer = overlay (~180ms
ease-out), not a route change; **cyan is a scalpel** — one place per screen (near-black amplifies
saturation → neon if oversprayed).

Sources: [Temporal workflow-UI](https://temporal.io/blog/the-dark-magic-of-workflow-exploration) ·
[Inngest traces](https://www.inngest.com/blog/enhanced-observability-traces-and-metrics) ·
[Trigger.dev run inspector](https://trigger.dev/changelog/run-page-inspector) ·
[Content-addressed memoization](https://dev.to/shobande_femi/content-addressed-memoization-for-durable-execution-4h2h) ·
[Linear redesign](https://linear.app/now/how-we-redesigned-the-linear-ui) ·
[Vercel Geist Table](https://vercel.com/geist/table) · [anti-AI-UI tells](https://dev.to/olehvolos/users-can-tell-when-your-ui-was-ai-generated-and-they-dont-like-it-33kn)

---

## Proposed sequencing (for CTO to rule)

The thread that ties §1–§5 together: **dokan's differentiation is "verifiable deterministic zero-LLM
runtime," and both the platform and the UI should dramatize it.**

1. **Ship the cockpit redesign** (built, polish pass vs the craft rails) — closes the 4-feature UI debt.
2. **Output artifacts `/output`** — symmetric with input, unblocks real agent doc-workflows (cc-process-miner).
3. **`reproduce` primitive + receipt hardening** (Ed25519 + hermetic claim + in-toto) — *the* wedge;
   turns "reproducible" from a claim into a runtime command. Pair with the **cache-recall badge** in the UI.
4. **Secret scoping** (Modal-style allowlist + file mounts) — retires the single-tenant ceiling, real
   security win.

§1–§2 are the highest-differentiation; §3–§4 are table-stakes-with-upside. Each gets its own spec before build.
