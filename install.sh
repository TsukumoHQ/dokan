#!/bin/sh
# dokan one-command installer.
#
#   curl -fsSL https://raw.githubusercontent.com/TsukumoHQ/dokan/main/install.sh | sh
#
# Stands up the runtime and lands the Claude Code operator skill on a clean machine.
# Idempotent: re-running is safe (skips what is already in place, never clobbers state).
#
# What it does, in order:
#   1. detect your OS/arch and the matching release asset
#   2. download + SHA-256-verify the dokan binary into ~/.local/bin
#   3. install the operator skill into ~/.claude/skills/dokan (where Claude Code loads it)
#   4. generate per-install crypto keys (secure-by-default; persisted 0600 in ~/.dokan/dokan.env)
#   5. start Postgres (docker compose) and the dokan daemon on 127.0.0.1:8088
#
# Tunables (env):
#   DOKAN_VERSION=v0.4.0      pin a release tag (default: latest)
#   DOKAN_BIN_DIR=~/.local/bin
#   DOKAN_HOME=~/.dokan        where the compose file + daemon log live
#   DOKAN_SKILL_DIR=~/.claude/skills/dokan
#   DOKAN_SKIP_RUNTIME=1       install the binary + skill only; do not touch Docker
#                              (used by the clean-room install-smoke; no Docker needed)
set -eu

REPO="TsukumoHQ/dokan"
BIN_DIR="${DOKAN_BIN_DIR:-$HOME/.local/bin}"
HOME_DIR="${DOKAN_HOME:-$HOME/.dokan}"
SKILL_DIR="${DOKAN_SKILL_DIR:-$HOME/.claude/skills/dokan}"
ADDR="127.0.0.1:8088"

# --- pretty, tty-aware output -------------------------------------------------
if [ -t 1 ]; then B="$(printf '\033[1m')"; G="$(printf '\033[32m')"; Y="$(printf '\033[33m')"; R="$(printf '\033[31m')"; X="$(printf '\033[0m')"; else B=""; G=""; Y=""; R=""; X=""; fi
info() { printf '%s==>%s %s\n' "$G" "$X" "$1"; }
warn() { printf '%s warn%s %s\n' "$Y" "$X" "$1" >&2; }
# die ALWAYS exits non-zero with a human message — never a raw trace.
die()  { printf '%serror%s %s\n' "$R" "$X" "$1" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

# --- 0. baseline tools --------------------------------------------------------
have curl || die "curl is required but not found. Install curl, then re-run."
if have sha256sum; then SHA="sha256sum"
elif have shasum; then SHA="shasum -a 256"
else die "need sha256sum or shasum to verify the download. Install coreutils, then re-run."
fi

# --- 1. platform → release asset (mirrors src/update.rs::asset_name_for) -------
os="$(uname -s)"; arch="$(uname -m)"
case "$os" in
  Darwin) os_t="apple-darwin" ;;
  Linux)  os_t="unknown-linux-gnu" ;;
  *) die "unsupported OS '$os'. dokan ships macOS and Linux binaries; build from source instead: https://github.com/$REPO" ;;
esac
case "$arch" in
  arm64|aarch64) arch_t="aarch64" ;;
  x86_64|amd64)  arch_t="x86_64" ;;
  *) die "unsupported architecture '$arch'. Build from source instead: https://github.com/$REPO" ;;
esac
ASSET="dokan-${arch_t}-${os_t}"

# --- 2. resolve version (follow the /latest redirect — no API token, no jq) ----
TAG="${DOKAN_VERSION:-}"
if [ -z "$TAG" ]; then
  info "Resolving latest release…"
  loc="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest" 2>/dev/null || true)"
  TAG="${loc##*/tag/}"
  [ -n "$TAG" ] && [ "$TAG" != "$loc" ] || die "could not resolve the latest release tag. Set DOKAN_VERSION=vX.Y.Z and re-run."
fi
BASE="https://github.com/$REPO/releases/download/$TAG"

# --- 3. download + verify the binary -----------------------------------------
tmp="$(mktemp -d "${TMPDIR:-/tmp}/dokan-install.XXXXXX")" || die "could not create a temp dir."
trap 'rm -rf "$tmp"' EXIT INT TERM
info "Downloading dokan $TAG ($ASSET)…"
curl -fsSL "$BASE/$ASSET" -o "$tmp/dokan" \
  || die "download failed for $BASE/$ASSET. Check the tag exists for your platform, or set DOKAN_VERSION."
if curl -fsSL "$BASE/SHA256SUMS" -o "$tmp/SHA256SUMS" 2>/dev/null; then
  want="$(grep " ${ASSET}\$" "$tmp/SHA256SUMS" 2>/dev/null | awk '{print $1}' | head -n1)"
  if [ -n "$want" ]; then
    got="$($SHA "$tmp/dokan" | awk '{print $1}')"
    [ "$want" = "$got" ] || die "checksum mismatch for $ASSET (expected $want, got $got). Aborting — not installing an unverified binary."
    info "Checksum verified."
  else
    warn "no checksum line for $ASSET in SHA256SUMS — skipping verification."
  fi
else
  warn "SHA256SUMS not published for $TAG — skipping checksum verification."
fi

# --- 4. install the binary ----------------------------------------------------
mkdir -p "$BIN_DIR" || die "cannot create $BIN_DIR."
chmod +x "$tmp/dokan"
mv -f "$tmp/dokan" "$BIN_DIR/dokan" || die "cannot install into $BIN_DIR (permission denied?)."
info "Installed ${B}$BIN_DIR/dokan${X}"
case ":$PATH:" in
  *":$BIN_DIR:"*) : ;;
  *) warn "$BIN_DIR is not on your PATH. Add it:  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# --- 5. land the Claude Code operator skill (pinned to the installed tag) ------
mkdir -p "$SKILL_DIR" || die "cannot create $SKILL_DIR."
if curl -fsSL "https://raw.githubusercontent.com/$REPO/$TAG/.claude/skills/dokan/SKILL.md" -o "$SKILL_DIR/SKILL.md"; then
  info "Operator skill installed ${B}$SKILL_DIR/SKILL.md${X} (Claude Code loads it from here)"
else
  warn "could not fetch the operator skill for $TAG — the runtime still works; you can re-run later."
fi

# --- 6. runtime (Postgres + daemon) ------------------------------------------
if [ "${DOKAN_SKIP_RUNTIME:-0}" = "1" ]; then
  info "DOKAN_SKIP_RUNTIME=1 — skipping Docker/daemon. Binary + skill are in place."
  info "Done. Start the runtime yourself with: docker compose up -d && dokan --transport http --addr $ADDR"
  exit 0
fi

have docker || die "Docker is required to run the dokan runtime, but 'docker' was not found.
  Install Docker Desktop (or colima/podman), start it, then re-run this installer.
  Or install the binary only:  DOKAN_SKIP_RUNTIME=1 sh install.sh"
docker info >/dev/null 2>&1 || die "Docker is installed but not running (could not reach the Docker daemon).
  Start Docker Desktop / 'colima start', then re-run.
  If you use colima/podman, also: export DOCKER_HOST=unix://\$HOME/.colima/default/docker.sock"

# Compose file lives under DOKAN_HOME so a binary-only install still has one.
mkdir -p "$HOME_DIR"
cat > "$HOME_DIR/docker-compose.yml" <<'YAML'
services:
  db:
    image: pgvector/pgvector:pg16
    container_name: dokan-db
    environment:
      POSTGRES_USER: dokan
      POSTGRES_PASSWORD: dokan
      POSTGRES_DB: dokan
    ports: ["5499:5432"]
    volumes: ["dokan_pg:/var/lib/postgresql/data"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dokan"]
      interval: 5s
      timeout: 3s
      retries: 10
volumes:
  dokan_pg:
YAML

if docker compose version >/dev/null 2>&1; then DC="docker compose"
elif have docker-compose; then DC="docker-compose"
else die "need Docker Compose (v2 plugin or docker-compose). Update Docker Desktop, then re-run."
fi

info "Starting Postgres (dokan-db on :5499)…"
( cd "$HOME_DIR" && $DC up -d ) || die "could not start Postgres via Docker Compose. Is port 5499 free?"

# Provision crypto keys so the daemon boots SECURE-BY-DEFAULT (it fails closed otherwise).
# Generated ONCE and persisted; never regenerated on re-run (rotating them would orphan
# already-sealed secrets + receipts). Values are written to a 0600 file, never printed.
ENV_FILE="$HOME_DIR/dokan.env"
if [ ! -f "$ENV_FILE" ]; then
  info "Generating crypto keys (secrets-at-rest + receipt signing)…"
  gen() { head -c 32 /dev/urandom | base64 | tr -d '\n'; }
  ( umask 077; {
      printf 'DOKAN_SECRET_KEY=%s\n' "$(gen)"
      printf 'DOKAN_RECEIPT_KEY=%s\n' "$(gen)"
      printf 'DOKAN_RECEIPT_ED25519_SECRET=%s\n' "$(gen)"
    } > "$ENV_FILE" )
  info "Keys written to ${B}$ENV_FILE${X} (0600). Back this up — losing it makes sealed secrets unreadable."
fi
# Load the keys into the environment the daemon inherits (no values echoed).
set -a; . "$ENV_FILE"; set +a

# If the daemon already answers, this is a re-run — leave it.
if curl -fsS "http://$ADDR/health" >/dev/null 2>&1 || curl -fsS "http://$ADDR/" >/dev/null 2>&1; then
  info "dokan daemon already up at http://$ADDR — nothing to restart."
else
  info "Starting the dokan daemon…"
  ( "$BIN_DIR/dokan" --transport http --addr "$ADDR" >"$HOME_DIR/daemon.log" 2>&1 & echo $! >"$HOME_DIR/daemon.pid" )
  i=0
  while [ "$i" -lt 30 ]; do
    if curl -fsS "http://$ADDR/health" >/dev/null 2>&1 || curl -fsS "http://$ADDR/" >/dev/null 2>&1; then break; fi
    i=$((i+1)); sleep 1
  done
  if [ "$i" -ge 30 ]; then
    die "daemon did not come up on http://$ADDR within 30s. Last log lines:
$(tail -n 20 "$HOME_DIR/daemon.log" 2>/dev/null)"
  fi
fi

cat <<DONE

${G}${B}dokan is up.${X}  cockpit: http://$ADDR/   ·   MCP: http://$ADDR/mcp

Wire it into your agent (Claude Code):
  claude mcp add --transport http dokan http://$ADDR/mcp

Logs:  tail -f $HOME_DIR/daemon.log
Stop:  kill \$(cat $HOME_DIR/daemon.pid) ; (cd $HOME_DIR && $DC down)
DONE
