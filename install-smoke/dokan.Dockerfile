# Clean-room install smoke for dokan (Linux / x86_64).
#
# Proves the documented one-command install lands the binary + operator skill with ZERO errors
# on a bare image, and that re-running is idempotent. This is the Linux clean-install signal that
# catches "skills/hooks didn't land / install errored" — most of the 3h-to-run pain.
#
# The runtime (Postgres + executor) needs a Docker daemon, which a bare build container does not
# have, so this room runs the installer with DOKAN_SKIP_RUNTIME=1 (binary + skill only). The
# daemon-up + run-a-job path is covered by a Docker-enabled pass (host or DinD), flagged separately.
#
#   docker build -f install-smoke/dokan.Dockerfile -t dokan-install-smoke .
#
# A successful build == the gate is green. (CI: build on every release tag; red == blocked release.)
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive HOME=/root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Run ONLY the documented install command — nothing pre-installed beyond curl + CA certs.
COPY install.sh /opt/dokan/install.sh
RUN DOKAN_SKIP_RUNTIME=1 sh /opt/dokan/install.sh

# Assert contract (shared with TSU-224): exits 0, CLI on PATH + --version, skill landed where
# Claude Code resolves it, and a second run is idempotent (no error / no clobber).
ENV PATH="/root/.local/bin:${PATH}"
RUN set -eux; \
    command -v dokan; \
    dokan --version; \
    test -f /root/.claude/skills/dokan/SKILL.md; \
    grep -q 'name: dokan' /root/.claude/skills/dokan/SKILL.md; \
    DOKAN_SKIP_RUNTIME=1 sh /opt/dokan/install.sh; \
    dokan --version; \
    echo "INSTALL-SMOKE OK"
