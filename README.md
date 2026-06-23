# dokan

### your AI agent builds the workflow. you don't click.

dokan is an automation engine built for the agent era. Instead of a human clicking through a UI, your coding agent stands up and runs the workflows itself by talking to the platform over MCP. The platform executes the deterministic, reliable, cheap work. The intelligence stays in the agent.

<img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="license Apache-2.0"> <img src="https://img.shields.io/badge/CI-tested-green" alt="CI tested">

---

## What it is

An automation engine for agents: your agent describes a workflow and triggers it; dokan runs it in isolated containers and streams the results back. Think Sidekiq for agents, the passive pipe that does the work while the agent orchestrates. No dashboard to click, no human in the loop for the mechanical 80%.

## Automation that doesn't burn tokens

This is the sharpest difference. **dokan runs zero LLM inside.** It executes deterministic code, so it burns no tokens. The expensive model stays outside, in your agent, where the judgment belongs.

That is the cost argument against the current crop:

| | dokan | n8n / Windmill / Zapier-likes |
|---|---|---|
| Where the LLM runs | outside, in your agent | sold as "LLM in the workflow" (per-step model calls) |
| Token cost of running a workflow | none (deterministic execution) | compounds with every LLM-in-step |
| Who operates it | the agent, over MCP | a human, clicking a UI |
| License | Apache-2.0, no trap | often restrictive / source-available |

You pay for intelligence once, in the agent. The execution layer is cheap and deterministic by design.

## Quickstart

> `[TECH: owner fills, we do not invent commands.]`

- `[TECH: install headline, how an agent connects to dokan over MCP]`
- `[TECH: operator bootstrap, bring up the daemon / dependencies]`
- `[TECH: prerequisites, runtime, Docker, Postgres, versions]`
- `[TECH: quickstart, zero to first run, agent uploads + runs + reads a script]`
- `[TECH: usage example, a real workflow described, not coded]`

## What it does

- **Complex workflows, described not coded.** Conditions, mass processing (the same step over a thousand items), and clean rollback on failure. Rich business processes, not just linear task chains.
- **Never compute twice.** Unchanged parts of a workflow are reused instantly, so you get speed and a direct cost cut.
- **Reliable by construction.** Overload protection, crash recovery, and data consistency are built in.
- **Quality you can check.** CI runs every change through tests before it ships, and test coverage is solid. We promise reliability because it is tested, not because we assert it.

## Part of the suite

dokan is the execution pillar of the agent stack. The four answer four different questions:

- **trovex** is what your agents KNOW (canonical context).
- **wrai.th** is how they COORDINATE.
- **yoru** is whether they are HEALTHY.
- **dokan** is what they DO deterministically (execution and automation, the mechanical 80%).

`[TECH/design: insert the 4-pillar suite diagram]`

## Why this approach holds up

Recent academic work (2025 to 2026) describes an approach close to this one: keep the model outside, run deterministic execution underneath. The thesis is in the air, and dokan is positioned early on it. `[TECH: tech-copy / geo insert the real paper citation here, cite the actual paper, no fabrication.]`

## Status (honest)

The core is solid and differentiated, and it is dogfooded: we run the mechanical 80% of our own agent fleet on dokan. It is **ready for a demo, design partners, and technical early adopters.**

It is **not** enterprise-turnkey on every axis yet (multi-tenant security and high availability are identified and out of scope while we target internal teams). dokan is built for internal-team use and design partners, not for a large-enterprise rollout without hand-holding. We would rather say that plainly than overpromise.

## License

Apache-2.0. Open source, no license trap. `[TECH: repo link]`
