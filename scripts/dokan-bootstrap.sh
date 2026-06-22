#!/bin/bash
# dokan-bootstrap — bring up the deps so any agent's stdio dokan MCP works.
# Run at login by the LaunchAgent (com.tsukumo.dokan-bootstrap), idempotent.
# The dokan binary itself is spawned per-session by Claude Code (stdio MCP);
# this only guarantees its two deps are up: colima (Docker) + dokan-db (Postgres 5499).
set -u
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"; }

# 1) Docker engine (colima) — start if not running. Idempotent: no-op when up.
if ! colima status >/dev/null 2>&1; then
  log "colima down -> starting"
  colima start
else
  log "colima already running"
fi

# 2) dokan-db Postgres — reboot-safe: if the container exists (even stopped or
# created outside compose), just start it; otherwise create it via compose.
# Avoids the "name in use" conflict when the container wasn't compose-managed.
cd "$HOME/Projects/dokan" || { log "dokan dir missing"; exit 1; }
if docker ps -a --format '{{.Names}}' | grep -qx dokan-db; then
  log "dokan-db container exists -> docker start"
  docker start dokan-db >/dev/null 2>&1 || true
else
  log "dokan-db missing -> docker compose up -d"
  docker compose up -d
fi

# 3) Wait for Postgres on 5499 to accept connections (max ~30s).
for i in $(seq 1 30); do
  if nc -z 127.0.0.1 5499 2>/dev/null; then log "dokan-db ready on 5499"; exit 0; fi
  sleep 1
done
log "WARN: dokan-db not ready after 30s"
exit 0
