#!/usr/bin/env bash
# dokan SessionStart preflight. Fires ONCE per session. Stays SILENT when healthy —
# only speaks (into context) when something needs doing. Auto-starts the DB (cheap);
# never auto-builds (slow). Keep it fast and quiet — hooks that chatter get ignored.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 0

issues=()

# DB on :5499 — auto-start if down (fast).
if ! docker exec dokan-db pg_isready -U dokan >/dev/null 2>&1; then
  docker compose up -d >/dev/null 2>&1
  sleep 2
  docker exec dokan-db pg_isready -U dokan >/dev/null 2>&1 \
    && issues+=("started dokan-db") \
    || issues+=("dokan-db not reachable — run: docker compose up -d")
fi

# Binary present?
[ -x target/debug/dokan ] || issues+=("binary missing — run: cargo build")

# Docker socket reachable (Colima/Desktop live outside /var/run).
if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  guess=$(docker context inspect 2>/dev/null | grep -o 'unix://[^"]*' | head -1)
  [ -n "$guess" ] && issues+=("export DOCKER_HOST=$guess")
fi

# Silent on success; one terse block when there's something to know.
if [ ${#issues[@]} -gt 0 ]; then
  printf 'dokan preflight:\n'
  printf '  - %s\n' "${issues[@]}"
fi
exit 0
