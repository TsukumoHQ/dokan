#!/bin/bash
# dokan-daemon — bring up deps, then exec dokan as a PERSISTENT HTTP daemon.
# launchd (com.tsukumo.dokan) runs this with KeepAlive=true: if dokan exits,
# launchd restarts it. HTTP (not stdio) so dokan's CRON scheduler runs 24/7
# independent of any agent session, and all leads share ONE script store.
set -u
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export DATABASE_URL="postgres://dokan:dokan@127.0.0.1:5499/dokan"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"; }
TOKEN="$(cat "$HOME/.config/dokan/token")"

# 1) Docker engine (colima) up.
if ! colima status >/dev/null 2>&1; then log "colima down -> starting"; colima start; else log "colima up"; fi

# 2) dokan-db Postgres up (reboot-safe: start existing container, else compose create).
cd "$HOME/Projects/dokan" || { log "dokan dir missing"; exit 1; }
if docker ps -a --format '{{.Names}}' | grep -qx dokan-db; then
  docker start dokan-db >/dev/null 2>&1 || true
else
  docker compose up -d
fi

# 3) Wait for Postgres on 5499.
for i in $(seq 1 30); do nc -z 127.0.0.1 5499 2>/dev/null && { log "dokan-db ready"; break; }; sleep 1; done

# 4) Exec dokan as the foreground HTTP daemon (launchd watches this PID).
# No --token: dokan binds 127.0.0.1 (localhost-only), so the bearer gate adds no
# real protection but blocks the browser UI (browsers can't send an Authorization
# header). Dropping it lets the thin UI open directly at http://127.0.0.1:8088/.
log "exec dokan http :8088 (no token, localhost-only)"
exec ./target/debug/dokan --transport http --addr 127.0.0.1:8088
