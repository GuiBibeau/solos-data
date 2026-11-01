//! Build-time inputs for `solos-data` (ADR-0021).
//!
//! Embeds the git commit in `SOLOS_DATA_GIT_SHA` so receipts stop saying `unknown` when the
//! binary is copied off the build box. Precedence: a non-empty `SOLOS_DATA_GIT_SHA` in the
//! build environment (used verbatim, so a release build can pin the exact commit it was cut
//! from), then `git rev-parse HEAD` of the repository that contains this crate, then
//! `unknown` when git or the repository is unavailable (a source export).
//!
//! The repository is found by walking up from the crate directory: `<ancestor>/.git` is a
//! directory in a normal checkout, or a `gitdir: <path>` pointer file in a linked worktree.
//! The commit is re-read when the ref moves: `HEAD`, `packed-refs`, and `refs/` of the git
//! directory are declared as rerun inputs. In a linked worktree the branch refs live in the
//! common directory, so its refs are declared too (`commondir` resolves it).

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sha = match std::env::var("SOLOS_DATA_GIT_SHA") {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() {
                rev_parse(&manifest).unwrap_or_else(|| "unknown".to_owned())
            } else {
                value.to_owned()
            }
        }
        Err(_) => rev_parse(&manifest).unwrap_or_else(|| "unknown".to_owned()),
    };
    println!("cargo:rerun-if-env-changed=SOLOS_DATA_GIT_SHA");
    println!("cargo:rustc-env=SOLOS_DATA_GIT_SHA={sha}");
}

/// `git rev-parse HEAD` of the repository holding `dir`, with rerun inputs declared.
fn rev_parse(dir: &Path) -> Option<String> {
    let git = git_dir(dir)?;
    declare_rerun(&git);
    if let Some(common) = common_dir(&git) {
        declare_rerun(&common);
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (text.len() == 40 && text.bytes().all(|b| b.is_ascii_hexdigit())).then_some(text)
}

/// Declare `HEAD`, `packed-refs`, and `refs/` of one git directory as rerun inputs.
fn declare_rerun(git: &Path) {
    for input in ["HEAD", "packed-refs"] {
        let path = git.join(input);
        if path.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    let refs = git.join("refs");
    if refs.is_dir() {
        println!("cargo:rerun-if-changed={}", refs.display());
    }
}

/// The git directory of the repository holding `dir`: the nearest ancestor whose `.git` is a
/// directory (a normal checkout) or a `gitdir:` pointer file (a linked worktree). The pointer
/// may name an absolute path or one relative to the ancestor that holds it.
fn git_dir(dir: &Path) -> Option<PathBuf> {
    let mut ancestor = dir.to_path_buf();
    loop {
        let dot = ancestor.join(".git");
        if dot.is_dir() {
            return Some(dot);
        }
        if let Ok(text) = std::fs::read_to_string(&dot)
            && let Some(pointed) = text.trim().strip_prefix("gitdir:")
        {
            let pointed = PathBuf::from(pointed.trim());
            let resolved = if pointed.is_absolute() {
                pointed
            } else {
                ancestor.join(pointed)
            };
            if resolved.is_dir() {
                return Some(resolved);
            }
        }
        ancestor = ancestor.parent()?.to_path_buf();
    }
}

/// The common directory of a linked worktree's git directory (`commondir` file), where the
/// branch refs actually live.
fn common_dir(git: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(git.join("commondir")).ok()?;
    let pointed = PathBuf::from(text.trim());
    let resolved = if pointed.is_absolute() {
        pointed
    } else {
        git.join(pointed)
    };
    resolved.is_dir().then_some(resolved)
}
