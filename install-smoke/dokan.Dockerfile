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
COPY install-smoke/assert.sh /assert.sh
RUN DOKAN_SKIP_RUNTIME=1 sh /opt/dokan/install.sh

# Idempotency: a second run must not error or clobber.
RUN DOKAN_SKIP_RUNTIME=1 sh /opt/dokan/install.sh

# Shared assert contract (TSU-224). dokan's operator install lands the skill only — it ships no
# Claude Code hooks to users — so HOOKS_* are unset (n/a).
ENV PATH="/root/.local/bin:${PATH}"
RUN TOOL=dokan TOOL_BIN=dokan SKILL_FILE=/root/.claude/skills/dokan/SKILL.md sh /assert.sh \
 && echo "INSTALL-SMOKE OK"
