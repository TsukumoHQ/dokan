# Suite install-smoke contract (TSU-224)

A bare-image clean room per suite tool (**trovex · yoru · dokan · wraith**) that runs **only the
tool's one documented install command** and asserts it actually worked. Real signal: a new user
spent 3h getting the suite running — this drives that to <5 min/tool and keeps it there.

## The contract every tool's clean room MUST satisfy

1. **Bare base.** `FROM` a minimal base image with nothing preinstalled beyond the tool's
   documented prereqs (+ `curl`/`ca-certificates` to fetch). No prior state.
2. **One command.** Run **exactly** the README quickstart install command — nothing more.
3. **Asserts** (use the shared `assert.sh` so all four are identical):
   - install **exits 0**, no error strings in output;
   - the **CLI is on PATH** and `--version`/`--help` exits 0;
   - the **Claude Code skill landed** where Claude resolves it (`$HOME/.claude/skills/<tool>/…` exists);
   - **hooks registered** in `settings.json` (only for tools that ship hooks);
   - **idempotent** — running the install a second time does not error or clobber.
4. **CI gate** on every release tag — a red install **blocks the release**.
5. **Also a dokan script** per tool (dogfood — deterministic Docker is dokan's job): receipt =
   pass/fail + what's missing.

## How a lane wires its container

Copy `assert.sh` in and run it with your tool's parameters after the install command:

```dockerfile
FROM <bare base>
RUN <install prereqs: curl ca-certificates …>
COPY install-smoke/assert.sh /assert.sh
RUN <THE ONE documented install command>
RUN <run it AGAIN>            # idempotency: must not error/clobber
ENV PATH="<wherever the CLI lands>:${PATH}"
RUN TOOL=<tool> TOOL_BIN=<cli> SKILL_FILE=$HOME/.claude/skills/<tool>/SKILL.md \
    [ HOOKS_FILE=$HOME/.claude/settings.json HOOKS_GREP='<hook id>' ] \
    sh /assert.sh
```

`assert.sh` params: `TOOL`, `TOOL_BIN`, `SKILL_FILE` (required); `VERSION_FLAG` (default
`--version`); `HOOKS_FILE` + `HOOKS_GREP` (only if the tool ships hooks).

## Per-lane ownership

| Tool   | Lane             | Ticket   | Container status |
|--------|------------------|----------|------------------|
| dokan  | dokan-core       | TSU-222  | ✅ `install-smoke/dokan.Dockerfile` (this repo) |
| trovex | trovex-fullstack | TSU-220  | lane builds against this contract |
| yoru   | yoru-dev         | TSU-221  | lane builds against this contract |
| wrai.th| wraith-dev       | TSU-223  | lane builds against this contract |

## Scope note

Docker proves the **Linux clean-install** (catches "skills/hooks didn't land / errored" — most of
the 3h). **macOS-path-specific** gaps still need one real mac pass — flag those separately; a Linux
green does NOT claim a clean mac.
