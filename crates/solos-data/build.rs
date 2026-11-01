//! Build-time inputs for `solos-data` (ADR-0021).
//!
//! Embeds the git commit in `SOLOS_DATA_GIT_SHA` so receipts stop saying `unknown` when the
//! binary is copied off the build box. Precedence: a non-empty `SOLOS_DATA_GIT_SHA` in the
//! build environment (used verbatim, so a release build can pin the exact commit it was cut
//! from), then `git rev-parse HEAD` of the repository that contains this crate, then
//! `unknown` when git or the repository is unavailable (a source export).
//!
//! The commit is re-read when the ref moves: `HEAD`, `packed-refs`, and `refs/` of the git
//! directory are declared as rerun inputs. In a linked worktree `<crate>/.git` is a file
//! (`gitdir: <path>`), so the git directory is resolved through it.

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
    println!("cargo:rustc-env=SOLOS_DATA_GIT_SHA={sha}");
}

/// `git rev-parse HEAD` of the repository holding `dir`, with rerun inputs declared.
fn rev_parse(dir: &Path) -> Option<String> {
    let git = git_dir(dir)?;
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

/// The git directory of the repository holding `dir`: `<dir>/.git` when that is a directory
/// (a normal checkout), or the path named by the worktree's `gitdir:` pointer file.
fn git_dir(dir: &Path) -> Option<PathBuf> {
    let dot = dir.join(".git");
    if dot.is_dir() {
        return Some(dot);
    }
    let text = std::fs::read_to_string(&dot).ok()?;
    let pointed = text.trim().strip_prefix("gitdir:")?.trim();
    let path = PathBuf::from(pointed);
    path.is_dir().then_some(path)
}
