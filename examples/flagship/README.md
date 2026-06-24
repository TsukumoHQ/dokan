# Flagship demo — a real DAG in one command

A self-contained dokan flow that shows the engine's headline features in ~10 seconds, with
**no API keys and no network in the jobs** — so anyone who cloned the repo gets the exact
same result. It's a tiny **order fraud-triage** pipeline:

```
intake ──▶ score ──▶ summarize ──▶ alert
           (map:        (structured   (when:
            one run       result +       deps.summarize
            per order)    branch token)  == "FLAGGED")
```

- **intake** emits a deterministic batch of orders.
- **score** is a `map` step — the flow fans it out into **one container per order** (throttled
  by the shared concurrency permit, not all at once).
- **summarize** sees the whole batch *and* every score, emits a structured `::dokan:result::`,
  and prints a branch token (`FLAGGED` / `CLEAN`) as its last line.
- **alert** has a `when` gate — it runs **only** on a flagged batch; on a clean one it's skipped.

## Run it

Start a daemon (see the repo Quickstart), then:

```sh
./examples/flagship/run.sh
```

(Needs `curl` + `jq`. Point at a non-default daemon with `DOKAN_ADDR=host:port`.)

The script talks to the daemon over **MCP** — exactly the way your agent would: `upload_script`
×4 → `compose_flow` → `run_flow` → `get_flow_run`.

## Expected output

The default batch (5 orders) contains one high-value, high-risk-geo order, so the branch fires:

```json
{
  "status": "succeeded",
  "steps": [
    { "id": "intake",    "status": "succeeded" },
    { "id": "score",     "status": "succeeded", "map": { "n": 5, "ok": 5, "failed": 0 } },
    { "id": "summarize", "status": "succeeded", "out": "FLAGGED" },
    { "id": "alert",     "status": "succeeded" }
  ]
}
```

`summarize`'s structured result (`{ orders: 5, flagged: 1, max_score: 81, threshold: 70 }`) is
captured off stdout — fetch it with `get_flow_run` / the run's signed receipt, not by scraping logs.

## Why it's the "wow"

- **Wired over MCP, not code-first.** The agent declares the DAG; dokan runs it.
- **`map` fan-out + `when` branch** in four small scripts — the patterns real pipelines need.
- **Deterministic + offline.** Every job is `network:false`, so the whole run is a pure
  function of its input: re-running recalls from the content-addressed cache, and each step
  carries a signed reproducibility receipt. The demo is the proof.

The same flow runs in CI as `flagship_demo_flow` in `tests/p2_flows.rs`.
