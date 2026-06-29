#!/usr/bin/env python3
# dokan install-smoke — DOGFOOD form (TSU-224). Runs as a dokan job (runtime=python, network=true).
#
# The job container IS the bare clean room. It asserts the same contract install.sh enforces, but
# via the stdlib (job images ship no curl): the platform binary downloads + its SHA-256 matches
# SHA256SUMS, the binary actually runs (`--version` exits 0) from the exec-mounted /tmp, and the
# Claude Code operator skill is fetchable + well-formed. Emits a structured result (pass/fail + what
# is missing); the run's receipt makes it tamper-evident.
#
# Runtime MUST be `python` (python:3.12-slim ships libssl.so.3, which the dokan binary links;
# node:22-slim / debian-slim do NOT — see the portability note in TSU-224).
#
# INPUT (DOKAN_INPUT, optional): {"version": "vX.Y.Z"} to pin a tag (default: latest).
import hashlib, json, os, platform, subprocess, urllib.request

REPO = "TsukumoHQ/dokan"
TOOL = "dokan"
# Match the asset to the job container's arch (the executor host's arch). arm64 Linux needs the
# aarch64 asset, which only exists once release.yml builds all 4 triples (TSU-224 / PR #60).
TRIPLE = {"x86_64": "x86_64-unknown-linux-gnu", "aarch64": "aarch64-unknown-linux-gnu"}.get(platform.machine())
ASSET = f"dokan-{TRIPLE}"
missing, version = [], None


def get(url, binary=False):
    with urllib.request.urlopen(url, timeout=30) as r:  # follows redirects
        return (r.read() if binary else r.read().decode()), r.geturl(), r.status


def done(passed, tag):
    print(f"::dokan:result:: {json.dumps({'tool': TOOL, 'pass': passed, 'tag': tag, 'version': version, 'missing': missing})}")
    raise SystemExit(0 if passed else 1)


def main():
    global version
    if not TRIPLE:
        missing.append(f"unsupported arch {platform.machine()}")
        return done(False, "")
    tag = (json.loads(os.environ.get("DOKAN_INPUT") or "{}").get("version") or "").strip()
    if not tag:
        _, final, _ = get(f"https://github.com/{REPO}/releases/latest")
        tag = final.split("/tag/")[-1].strip()
    if not tag or "/tag/" in tag:
        missing.append("could not resolve a release tag")
        return done(False, tag)
    base = f"https://github.com/{REPO}/releases/download/{tag}"

    # 1. binary downloads
    try:
        binb, _, _ = get(f"{base}/{ASSET}", binary=True)
    except Exception as e:
        missing.append(f"binary {ASSET} not downloadable ({e})")
        return done(False, tag)

    # 2. checksum verifies against SHA256SUMS (never run an unverified binary)
    try:
        sums, _, _ = get(f"{base}/SHA256SUMS")
        line = next((l for l in sums.splitlines() if l.strip().replace("*", "").endswith(ASSET)), "")
        want = line.split()[0] if line else ""
        got = hashlib.sha256(binb).hexdigest()
        if not want:
            missing.append(f"no SHA256SUMS line for {ASSET}")
        elif want.lower() != got:
            missing.append(f"checksum mismatch ({want} != {got})")
    except Exception as e:
        missing.append(f"SHA256SUMS not fetchable ({e})")
    if missing:
        return done(False, tag)

    # 3. binary runs (--version exits 0) from the exec-mounted /tmp
    with open("/tmp/dokan", "wb") as f:
        f.write(binb)
    os.chmod("/tmp/dokan", 0o755)
    p = subprocess.run(["/tmp/dokan", "--version"], capture_output=True, text=True)
    if p.returncode != 0:
        missing.append(f"'/tmp/dokan --version' exit {p.returncode}: {(p.stderr or '').strip()[:200]}")
    else:
        version = (p.stdout or "").strip()
        print(version)

    # 4. operator skill is fetchable + well-formed
    try:
        skill, _, _ = get(f"https://raw.githubusercontent.com/{REPO}/{tag}/.claude/skills/{TOOL}/SKILL.md")
        os.makedirs(f"/tmp/.claude/skills/{TOOL}", exist_ok=True)
        with open(f"/tmp/.claude/skills/{TOOL}/SKILL.md", "w") as f:
            f.write(skill)
        if "name: dokan" not in skill:
            missing.append("skill SKILL.md missing `name: dokan`")
    except Exception as e:
        missing.append(f"operator skill SKILL.md not fetchable ({e})")

    done(not missing, tag)


main()
