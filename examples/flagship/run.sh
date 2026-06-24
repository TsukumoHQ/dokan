#!/usr/bin/env bash
# dokan flagship demo — drive a real DAG over MCP, the way an agent does it.
# Uploads 4 deterministic steps, wires them into a flow (map fan-out + a `when` branch),
# runs it, and prints the result. No secrets, no network in the jobs → fully reproducible.
#
# Prereq: a running dokan daemon (see repo Quickstart). Then:  ./examples/flagship/run.sh
# Override the address with DOKAN_ADDR=host:port. Needs curl + jq.
set -euo pipefail

ADDR="${DOKAN_ADDR:-127.0.0.1:8088}"
URL="http://$ADDR/mcp"
DIR="$(cd "$(dirname "$0")" && pwd)"
ACCEPT="application/json, text/event-stream"
SID=""

# One MCP JSON-RPC round-trip over Streamable HTTP. Captures the session id off the first
# response, returns the single SSE `data:` JSON line.
rpc() {
  local hdr body
  hdr="$(mktemp)"
  body="$(curl -s -D "$hdr" -H "Content-Type: application/json" -H "Accept: $ACCEPT" \
    ${SID:+-H "Mcp-Session-Id: $SID"} -X POST "$URL" --data "$1")"
  if [ -z "$SID" ]; then
    SID="$(grep -i '^mcp-session-id:' "$hdr" | tr -d '\r' | awk '{print $2}')"
  fi
  rm -f "$hdr"
  # notifications return an empty body — tolerate "no SSE data line" without tripping pipefail.
  printf '%s' "$body" | { grep '^data: {' || true; } | sed 's/^data: //' | tail -1
}

# Call a tool, return the inner text payload (dokan tools return JSON as text content).
tool() {
  rpc "$(jq -cn --arg n "$1" --argjson a "$2" \
    '{jsonrpc:"2.0",id:1,method:"tools/call",params:{name:$n,arguments:$a}}')" \
    | jq -r '.result.content[0].text'
}

# Upload one step's source (idempotent), print its script_id.
upload() {
  local src; src="$(cat "$DIR/$2")"
  tool upload_script \
    "$(jq -n --arg n "$1" --arg s "$src" '{name:$n,runtime:"node",source:$s,network:false,upsert:true}')" \
    | jq -r '.script_id'
}

echo "▸ dokan flagship — fraud-triage flow (deterministic, offline)"

# 1. MCP handshake.
rpc '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"flagship-demo","version":"1"}}}' >/dev/null
rpc '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null

# 2. Upload the 4 steps.
INTAKE="$(upload flagship-intake 01-intake.js)"
SCORE="$(upload flagship-score 02-score.js)"
SUM="$(upload flagship-summarize 03-summarize.js)"
ALERT="$(upload flagship-alert 04-alert.js)"
echo "  uploaded: intake=$INTAKE score=$SCORE summarize=$SUM alert=$ALERT"

# 3. Compose the DAG: intake -> score (map per order) -> summarize -> alert (when FLAGGED).
SPEC="$(jq -n --argjson i "$INTAKE" --argjson s "$SCORE" --argjson m "$SUM" --argjson a "$ALERT" '{
  steps: [
    { id: "intake",    script_id: $i },
    { id: "score",     script_id: $s, depends_on: ["intake"], map: "deps.intake" },
    { id: "summarize", script_id: $m, depends_on: ["intake","score"] },
    { id: "alert",     script_id: $a, depends_on: ["summarize"], when: { ref: "deps.summarize", op: "eq", value: "FLAGGED" } }
  ]}')"
FLOW="$(tool compose_flow "$(jq -cn --argjson spec "$SPEC" '{name:"fraud-triage",spec:$spec}')" | jq -r '.flow_id')"
echo "  flow_id=$FLOW"

# 4. Run it (batch of 5 orders).
RUN="$(tool run_flow "$(jq -cn --argjson f "$FLOW" '{flow_id:$f,input:{count:5}}')" | jq -r '.flow_run_id')"
echo "  flow_run_id=$RUN — running..."

# 5. Poll to terminal.
OUT="{}"
for _ in $(seq 1 60); do
  OUT="$(tool get_flow_run "$(jq -cn --argjson r "$RUN" '{flow_run_id:$r}')")"
  ST="$(printf '%s' "$OUT" | jq -r '.status')"
  if [ "$ST" = "succeeded" ] || [ "$ST" = "failed" ]; then break; fi
  sleep 1
done

echo ""
echo "▸ flow result (note: 'score' collapses its fan-out into {n,ok,failed}):"
printf '%s' "$OUT" | jq '{status, steps}'
