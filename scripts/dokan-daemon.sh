#!/bin/bash
# dokan-daemon — bring up deps, then exec dokan as a PERSISTENT HTTP daemon.
# launchd (com.tsukumo.dokan) runs this with KeepAlive=true: if dokan exits,
# launchd restarts it. HTTP (not stdio) so dokan's CRON scheduler runs 24/7
# independent of any agent session, and all leads share ONE script store.
set -u
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
# 2026-11-15 (task b71bf299): host migrated colima -> Docker Desktop; the old colima
# socket was dead, which stranded dokan-db and wedged the connection pool for ~4.5 days
# (every MCP call: "pool timed out while waiting for an open connection"). Target the live
# Docker Desktop socket so bootstrap + exec reach the running runtime. Honor an already-set
# DOCKER_HOST (default context / CI); default to the Docker Desktop socket via $HOME, so this
# is portable instead of hardcoding one machine's absolute path.
export DOCKER_HOST="${DOCKER_HOST:-unix://$HOME/.docker/run/docker.sock}"
export DATABASE_URL="postgres://dokan:dokan@127.0.0.1:5499/dokan"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"; }
TOKEN="$(cat "$HOME/.config/dokan/token")"

# 1) Docker engine must already be reachable. Do NOT boot colima here: this host runs Docker
# Desktop, and starting colima spins up a SECOND engine whose stale socket is exactly what
# stranded dokan-db and wedged the pool for ~4.5 days (b71bf299). Require a live engine on the
# configured DOCKER_HOST; fail loud if none so launchd retries instead of forking a rival runtime.
docker info >/dev/null 2>&1 || { log "docker engine unreachable on DOCKER_HOST=$DOCKER_HOST — start Docker Desktop"; exit 1; }

# 2) dokan-db Postgres up (reboot-safe: start existing container, else compose create). A failure
# here must exit — never fall through to exec dokan against a DB that isn't coming up.
cd "$HOME/Projects/dokan" || { log "dokan dir missing"; exit 1; }
if docker ps -a --format '{{.Names}}' | grep -qx dokan-db; then
  docker start dokan-db >/dev/null 2>&1 || { log "docker start dokan-db failed"; exit 1; }
else
  docker compose up -d || { log "docker compose up failed"; exit 1; }
fi

# 3) Wait for Postgres on 5499, then HARD-GATE on it. A reachable dokan server in front of a dead
# DB pool is the exact outage we just fixed — so if 5499 never answers, exit (launchd restarts the
# whole bring-up) rather than exec dokan on a dead DB.
for i in $(seq 1 30); do nc -z 127.0.0.1 5499 2>/dev/null && { log "dokan-db ready"; break; }; sleep 1; done
nc -z 127.0.0.1 5499 2>/dev/null || { log "dokan-db not reachable on 5499 after 30s — aborting"; exit 1; }

# 4) Exec dokan as the foreground HTTP daemon (launchd watches this PID).
# No --token: dokan binds 127.0.0.1 (localhost-only), so the bearer gate adds no
# real protection but blocks the browser UI (browsers can't send an Authorization
# header). Dropping it lets the thin UI open directly at http://127.0.0.1:8088/.
log "exec dokan http :8088 (no token, localhost-only)"
exec ./target/debug/dokan --transport http --addr 127.0.0.1:8088
