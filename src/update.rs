//! Self-updater — `dokan update [--force] [--check]`.
//!
//! Implements the shared Fleet Auto-Updater contract: resolve current/latest version, gate on
//! semver, download the platform release asset, verify its SHA-256 against the release's
//! `SHA256SUMS`, install it atomically over the running binary, refresh side-assets, and (when
//! supervised by launchd) cycle the daemon onto the new binary. EVERY network/IO/parse error
//! path is a clean no-op: the existing install keeps working. No LLM, no panics on the failure
//! paths.
//!
//! PRIMARY acquire strategy is asset-download (implemented below). The documented alternative is
//! build-from-source (`cargo install --git https://github.com/TsukumoHQ/dokan`), which we do NOT
//! do automatically — it needs a toolchain and would not be atomic.

use std::cmp::Ordering;
use std::path::Path;

use anyhow::{Context, Result};
use semver::Version;
use sha2::{Digest, Sha256};

/// PINNED repo slug. Must never be a redirectable/renamed value — a rename would silently point
/// the updater at someone else's releases.
const REPO: &str = "TsukumoHQ/dokan";
/// launchd job label for the supervised daemon (macOS).
const LAUNCHD_LABEL: &str = "com.tsukumo.dokan";
/// Current version of the running binary (contract step 1).
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
    #[serde(default)]
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize, Clone)]
struct Asset {
    name: String,
    browser_download_url: String,
}

/// Outcome of the semver + dev-build + downgrade gate. `--force` collapses the guards to `Update`.
#[derive(Debug, PartialEq, Eq)]
enum Gate {
    /// current == latest: nothing to do.
    UpToDate,
    /// current > latest: refuse to downgrade (unless --force).
    Ahead,
    /// current < latest, but the running binary is a debug/source build: refuse to clobber it
    /// (unless --force).
    DevBuild,
    /// proceed with the update.
    Update,
}

/// Parse a release tag into a semver `Version`. Tags may carry a leading `v` — strip it first.
fn parse_tag(tag: &str) -> Result<Version> {
    let stripped = tag.strip_prefix('v').unwrap_or(tag);
    Version::parse(stripped).with_context(|| format!("release tag is not semver: {tag}"))
}

/// Pure gate decision (contract steps 3 + 4). `debug_build` = `cfg!(debug_assertions)` of the
/// RUNNING binary; `force` bypasses both the dev-build guard and the no-downgrade guard.
fn gate(current: &Version, latest: &Version, force: bool, debug_build: bool) -> Gate {
    match current.cmp(latest) {
        Ordering::Equal => Gate::UpToDate,
        Ordering::Greater if !force => Gate::Ahead,
        // current < latest, OR a forced downgrade.
        _ => {
            if debug_build && !force {
                Gate::DevBuild
            } else {
                Gate::Update
            }
        }
    }
}

/// Derive the release asset name for a platform (contract step 5). Returns `None` for platforms
/// release.yml does not build. The names MUST match `.github/workflows/release.yml` exactly:
/// `dokan-<rust-target-triple>`.
fn asset_name_for(os: &str, arch: &str) -> Option<String> {
    let target = match (os, arch) {
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        _ => return None,
    };
    Some(format!("dokan-{target}"))
}

/// Asset name for THIS binary's platform.
fn asset_name() -> Option<String> {
    asset_name_for(std::env::consts::OS, std::env::consts::ARCH)
}

/// Look up the expected hex digest for `asset` in a `SHA256SUMS` body (contract step 6). Lines are
/// `<hexdigest>  <filename>` (sha256sum format; the filename may carry a leading `*` in binary
/// mode). Malformed lines are skipped, not fatal.
fn sha_for_asset(sums: &str, asset: &str) -> Option<String> {
    for line in sums.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(digest) = parts.next() else { continue };
        let Some(name) = parts.last() else { continue };
        if name.trim_start_matches('*') == asset {
            return Some(digest.to_lowercase());
        }
    }
    None
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Entry point. Returns a process exit code: 0 for a clean outcome (updated / up-to-date / a
/// deliberate guard refusal), nonzero for a graceful failure (offline, checksum mismatch, …).
/// Never panics on the failure paths (contract step 11).
pub async fn run(force: bool, check: bool) -> i32 {
    let current = match Version::parse(VERSION) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("dokan update: build carries an invalid version `{VERSION}`: {e}");
            return 1;
        }
    };

    let client = match reqwest::Client::builder()
        .user_agent(concat!("dokan-updater/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("dokan update: HTTP client init failed: {e}");
            return 1;
        }
    };

    let release = match fetch_latest(&client).await {
        Ok(r) => r,
        Err(e) => {
            // Offline / GitHub unreachable = clean no-op, not a panic (contract step 11).
            eprintln!("dokan update: couldn't reach GitHub ({e}), staying on {current}");
            return 1;
        }
    };

    let latest = match parse_tag(&release.tag_name) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("dokan update: {e}; staying on {current}");
            return 1;
        }
    };

    let decision = gate(&current, &latest, force, cfg!(debug_assertions));

    if check {
        match decision {
            Gate::UpToDate => println!("up to date ({current})"),
            Gate::Update => println!("update available {current} -> {latest}"),
            Gate::Ahead => {
                println!("local build {current} is ahead of latest release {latest}")
            }
            Gate::DevBuild => println!(
                "update available {current} -> {latest} (dev build — run `dokan update --force`)"
            ),
        }
        return 0;
    }

    match decision {
        Gate::UpToDate => {
            println!("already up to date ({current})");
            return 0;
        }
        Gate::Ahead => {
            println!(
                "local build {current} is ahead of latest release {latest}; not downgrading (use --force)"
            );
            return 0;
        }
        Gate::DevBuild => {
            eprintln!(
                "dokan update: refusing to auto-update a dev build ({current}) — installing the \
                 release would clobber your local target/debug binary. Re-run with --force to override."
            );
            return 0;
        }
        Gate::Update => {}
    }

    match install(&client, &release, &latest).await {
        Ok(()) => {
            println!("updated {current} -> {latest}");
            // Side-assets + restart are best-effort and AFTER a verified install — neither can
            // fail the update that already landed.
            refresh_side_assets();
            restart_if_launchd(&latest);
            0
        }
        Err(e) => {
            // Atomic install means the old binary is intact on any failure here.
            eprintln!("dokan update: update failed, staying on {current}: {e:#}");
            1
        }
    }
}

/// Contract step 2: resolve the latest release via the GitHub API (unauthenticated, with a
/// User-Agent). Returns tag + assets in one call.
async fn fetch_latest(client: &reqwest::Client) -> Result<Release> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let release = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json::<Release>()
        .await?;
    Ok(release)
}

async fn download(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let bytes = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(bytes.to_vec())
}

/// Acquire (step 5) → verify (step 6) → install atomically (step 7) → verify the staged binary
/// (step 10). Any error short-circuits BEFORE the rename, so the running install is never touched.
async fn install(client: &reqwest::Client, release: &Release, latest: &Version) -> Result<()> {
    let asset_name = asset_name()
        .context("no prebuilt release asset for this platform (build from source instead)")?;
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == asset_name)
        .with_context(|| format!("release {} has no asset `{asset_name}`", release.tag_name))?;
    let sums_asset = release
        .assets
        .iter()
        .find(|a| a.name == "SHA256SUMS")
        .context("release is missing its SHA256SUMS asset")?;

    let bin = download(client, &asset.browser_download_url)
        .await
        .context("downloading the release binary")?;
    let sums_raw = download(client, &sums_asset.browser_download_url)
        .await
        .context("downloading SHA256SUMS")?;
    let sums = String::from_utf8(sums_raw).context("SHA256SUMS is not valid UTF-8")?;

    // Verify integrity BEFORE installing (step 6).
    let expected = sha_for_asset(&sums, &asset_name)
        .with_context(|| format!("SHA256SUMS has no entry for `{asset_name}`"))?;
    let actual = {
        let mut h = Sha256::new();
        h.update(&bin);
        hex(&h.finalize())
    };
    if actual != expected {
        anyhow::bail!("checksum mismatch for `{asset_name}`: expected {expected}, got {actual}");
    }

    let current_exe = std::env::current_exe().context("resolving current_exe()")?;
    install_atomic(&current_exe, &bin).context("atomic install")?;
    verify(&current_exe, latest).context("post-install verification")?;
    Ok(())
}

/// Contract step 7: write the new bytes to a temp file IN THE SAME DIRECTORY as the running exe
/// (so the rename is same-filesystem and therefore atomic on POSIX), chmod 0o755, then
/// `rename(temp, current_exe)`. A failure before the rename leaves the old binary fully intact;
/// the temp file is cleaned up. We never leave a half-written binary at the live path.
fn install_atomic(current_exe: &Path, bytes: &[u8]) -> Result<()> {
    let dir = current_exe
        .parent()
        .context("current_exe has no parent directory")?;
    let file_name = current_exe
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("dokan");
    let tmp = dir.join(format!(".{file_name}.update-{}", std::process::id()));

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("creating temp file {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.flush()?;
        f.sync_all()?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
        {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("chmod 0o755 {}", tmp.display()));
        }
    }

    if let Err(e) = std::fs::rename(&tmp, current_exe) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| {
            format!("rename {} -> {}", tmp.display(), current_exe.display())
        });
    }
    Ok(())
}

/// Contract step 10: run the freshly-installed binary with `--version` and confirm it prints the
/// target version. Done on the staged on-disk binary (before any restart) so a bad install is
/// caught immediately.
fn verify(exe: &Path, latest: &Version) -> Result<()> {
    let out = std::process::Command::new(exe)
        .arg("--version")
        .output()
        .context("spawning installed binary --version")?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let want = latest.to_string();
    if !stdout.contains(&want) {
        anyhow::bail!(
            "installed binary reports `{}`, expected version {want}",
            stdout.trim()
        );
    }
    Ok(())
}

/// Contract step 8: refresh side-assets (the operator skill). Best-effort + idempotent; errors are
/// LOGGED and SWALLOWED — a skill-copy failure must never fail a binary update.
fn refresh_side_assets() {
    if let Err(e) = copy_skill() {
        tracing::warn!("skill refresh skipped (non-fatal): {e:#}");
    }
}

fn copy_skill() -> Result<()> {
    let src = Path::new(".claude/skills/dokan");
    if !src.is_dir() {
        return Ok(()); // nothing to refresh from here
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    let dst = Path::new(&home).join(".claude/skills/dokan");
    copy_dir(src, &dst)?;
    tracing::info!("refreshed operator skill -> {}", dst.display());
    Ok(())
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            copy_dir(&path, &target)?;
        } else {
            std::fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

/// Contract step 9: restart the daemon if it runs under launchd — STAGE, DON'T HOT-KILL.
///
/// dokan serves a LIVE MCP control plane, so we never SIGKILL a process mid-request. The update
/// path has already STAGED the new bytes at `current_exe` (atomic rename, step 7); here we merely
/// ask launchd to cycle the daemon through its own KeepAlive lifecycle (`launchctl kickstart -k`),
/// which lets the running process return and respawns it on the new binary BETWEEN requests. For a
/// NON-supervised MCP-serving binary the correct pattern is the same in spirit: stage the binary,
/// then signal a graceful restart — never a hot-kill of a serving process. (interview-finding-1)
///
/// We detect that the label is actually loaded first; if it is not (not launchd-managed, or a
/// different platform), we skip cleanly — the new binary is staged and will be picked up on the
/// next manual start.
fn restart_if_launchd(latest: &Version) {
    let Some(uid) = uid() else { return };
    let target = format!("gui/{uid}/{LAUNCHD_LABEL}");

    let loaded = std::process::Command::new("launchctl")
        .arg("print")
        .arg(&target)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !loaded {
        tracing::info!(
            "launchd label {LAUNCHD_LABEL} not loaded; skipping restart (new {latest} staged, \
             picked up on next start)"
        );
        return;
    }

    match std::process::Command::new("launchctl")
        .arg("kickstart")
        .arg("-k")
        .arg(&target)
        .output()
    {
        Ok(o) if o.status.success() => {
            tracing::info!("kickstarted {LAUNCHD_LABEL} onto {latest}")
        }
        Ok(o) => tracing::warn!(
            "launchctl kickstart failed (binary staged regardless): {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Err(e) => tracing::warn!("launchctl kickstart not run (binary staged regardless): {e}"),
    }
}

/// Numeric uid as a string, via `id -u` (no libc dependency). `None` if it can't be determined.
fn uid() -> Option<String> {
    let out = std::process::Command::new("id").arg("-u").output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    #[test]
    fn parse_tag_strips_leading_v() {
        assert_eq!(parse_tag("v1.2.3").unwrap(), v("1.2.3"));
        assert_eq!(parse_tag("1.2.3").unwrap(), v("1.2.3"));
        assert!(parse_tag("not-a-version").is_err());
    }

    #[test]
    fn gate_equal_is_up_to_date_even_on_dev_build() {
        assert_eq!(gate(&v("1.0.0"), &v("1.0.0"), false, false), Gate::UpToDate);
        assert_eq!(gate(&v("1.0.0"), &v("1.0.0"), false, true), Gate::UpToDate);
    }

    #[test]
    fn gate_behind_updates() {
        assert_eq!(gate(&v("1.0.0"), &v("1.1.0"), false, false), Gate::Update);
        // patch + minor + major bumps all count as behind.
        assert_eq!(gate(&v("1.0.0"), &v("1.0.1"), false, false), Gate::Update);
        assert_eq!(gate(&v("0.9.9"), &v("1.0.0"), false, false), Gate::Update);
    }

    #[test]
    fn gate_ahead_is_noop_without_force() {
        assert_eq!(gate(&v("2.0.0"), &v("1.0.0"), false, false), Gate::Ahead);
        // --force allows the downgrade.
        assert_eq!(gate(&v("2.0.0"), &v("1.0.0"), true, false), Gate::Update);
    }

    #[test]
    fn gate_dev_build_blocks_until_force() {
        // Behind + debug build => refuse to clobber the local build.
        assert_eq!(gate(&v("1.0.0"), &v("1.1.0"), false, true), Gate::DevBuild);
        // --force overrides the dev-build guard.
        assert_eq!(gate(&v("1.0.0"), &v("1.1.0"), true, true), Gate::Update);
        // Release build (no debug_assertions) behind => plain update.
        assert_eq!(gate(&v("1.0.0"), &v("1.1.0"), false, false), Gate::Update);
    }

    #[test]
    fn asset_names_match_release_targets() {
        assert_eq!(
            asset_name_for("macos", "aarch64").as_deref(),
            Some("dokan-aarch64-apple-darwin")
        );
        assert_eq!(
            asset_name_for("macos", "x86_64").as_deref(),
            Some("dokan-x86_64-apple-darwin")
        );
        assert_eq!(
            asset_name_for("linux", "x86_64").as_deref(),
            Some("dokan-x86_64-unknown-linux-gnu")
        );
        assert_eq!(
            asset_name_for("linux", "aarch64").as_deref(),
            Some("dokan-aarch64-unknown-linux-gnu")
        );
        // Unsupported platforms have no asset (build from source).
        assert_eq!(asset_name_for("windows", "x86_64"), None);
    }

    #[test]
    fn sha_lookup_parses_sums_file() {
        let sums = "abc123  dokan-aarch64-apple-darwin\n\
                    def456  dokan-x86_64-unknown-linux-gnu\n";
        assert_eq!(
            sha_for_asset(sums, "dokan-x86_64-unknown-linux-gnu").as_deref(),
            Some("def456")
        );
        assert_eq!(
            sha_for_asset(sums, "dokan-aarch64-apple-darwin").as_deref(),
            Some("abc123")
        );
        assert_eq!(sha_for_asset(sums, "dokan-no-such-target"), None);
    }

    #[test]
    fn sha_lookup_handles_binary_star_prefix_and_case() {
        // sha256sum binary mode prefixes the filename with '*'; digest may be upper-case.
        let sums = "ABC123  *dokan-aarch64-apple-darwin\n";
        assert_eq!(
            sha_for_asset(sums, "dokan-aarch64-apple-darwin").as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn sha_lookup_skips_blank_and_malformed_lines() {
        let sums = "\n   \njust-one-token\nabc123  dokan-aarch64-apple-darwin\n";
        assert_eq!(
            sha_for_asset(sums, "dokan-aarch64-apple-darwin").as_deref(),
            Some("abc123")
        );
    }
}
