# Release-gate clean room (TSU-224). Installs the JUST-BUILT binary (DOKAN_BINARY) on a LEAN base
# with NO libssl, proving the binary is self-contained + portable — this is exactly what would have
# caught the native-tls/libssl.so.3 regression. release.yml builds this after `build` and before
# publish; a red build BLOCKS the release.
#
#   docker build -f install-smoke/dokan-release-gate.Dockerfile \
#     --build-arg TAG=$TAG -t dokan-release-gate .       # expects ./dokan-gate-bin in the context
ARG BASE=debian:bookworm-slim
FROM ${BASE}
ARG TAG
ENV DEBIAN_FRONTEND=noninteractive HOME=/root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY install.sh /opt/install.sh
COPY install-smoke/assert.sh /assert.sh
COPY dokan-gate-bin /opt/dokan-gate-bin

# Install the provided binary + the skill (skill fetched from the tag). No runtime (no Docker here).
RUN DOKAN_BINARY=/opt/dokan-gate-bin DOKAN_VERSION="${TAG}" DOKAN_SKIP_RUNTIME=1 sh /opt/install.sh

# Assert the binary RUNS on this lean base (the portability proof) + the skill landed.
ENV PATH="/root/.local/bin:${PATH}"
RUN TOOL=dokan TOOL_BIN=dokan SKILL_FILE=/root/.claude/skills/dokan/SKILL.md sh /assert.sh \
 && echo "RELEASE-GATE OK"
