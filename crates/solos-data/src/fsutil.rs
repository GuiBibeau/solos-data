//! File helpers shared by the collector and the decoder: streaming SHA-256, fsync of files and
//! directories, and the write-tmp-fsync-rename pattern every published artefact uses.

use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

thread_local! {
    /// When set, the next `sync_all` on this thread fails. Tests inject a fatal fsync; production
    /// never sets it.
    static FAIL_FSYNC: Cell<bool> = const { Cell::new(false) };
}

/// Fail `sync_all` on this thread until cleared. For tests. A lane that hits the failure must
/// exit and restart from the watermark, not retry it (ADR-0016).
pub fn inject_fsync_failure(fail: bool) {
    FAIL_FSYNC.with(|flag| flag.set(fail));
}

fn sync_handle(file: &File) -> io::Result<()> {
    if FAIL_FSYNC.with(|flag| flag.get()) {
        return Err(io::Error::other("fsync failed: injected"));
    }
    file.sync_all()
        .map_err(|error| io::Error::new(error.kind(), format!("fsync failed: {error}")))
}

/// SHA-256 of a file, hex, streamed.
pub fn file_hash(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// SHA-256 of bytes, hex.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// fsync a file or a directory. A failure is an I/O error whose message starts with `fsync failed`,
/// which the collector treats as fatal.
pub fn sync_path(path: &Path) -> io::Result<()> {
    sync_handle(&File::open(path)?)
}

/// Create `path` and every missing ancestor. Each newly created directory's parent is fsynced, so
/// the new directory entry itself is durable. Directories that already exist are left alone.
pub fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path.to_path_buf();
    loop {
        if cursor.as_os_str().is_empty() {
            break;
        }
        if cursor.try_exists()? {
            break;
        }
        let parent = parent_dir(&cursor);
        missing.push(cursor);
        match parent {
            Some(parent) => cursor = parent,
            None => break,
        }
    }
    for dir in missing.iter().rev() {
        match std::fs::create_dir(dir) {
            Ok(()) => {
                let parent = dir.parent();
                let parent = parent.filter(|parent| !parent.as_os_str().is_empty());
                sync_path(parent.unwrap_or(Path::new(".")))?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn parent_dir(path: &Path) -> Option<std::path::PathBuf> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        None
    } else {
        Some(parent.to_path_buf())
    }
}

/// Write `body` to `path` durably: `.tmp` sibling, fsync, rename, fsync the directory.
pub fn write_durable(path: &Path, body: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(body)?;
        sync_handle(&file)?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_path(parent)?;
    }
    Ok(())
}

/// Write `body` to `path` through a `.tmp` rename without fsync, as the TypeScript status
/// snapshots do.
pub fn write_atomic(path: &Path, body: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)
}

/// `<path>.tmp`.
#[must_use]
pub fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    std::path::PathBuf::from(name)
}

/// Lexically normalize `.` and `..` without touching the file system.
#[must_use]
pub fn normalize(path: &Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `path.resolve(base, path)`: absolute paths stand, relative ones join `base`; both normalized.
#[must_use]
pub fn resolve(base: &Path, path: &Path) -> std::path::PathBuf {
    if path.is_absolute() {
        normalize(path)
    } else {
        normalize(&base.join(path))
    }
}

/// Manifest path and directory-walk path in one form: relative paths join `root`, then symlinks
/// are resolved when the file exists. A data directory opened through a link matches rows stored
/// with the other spelling.
#[must_use]
pub fn manifest_path(root: &Path, stored: &Path) -> std::path::PathBuf {
    let resolved = resolve(root, stored);
    std::fs::canonicalize(&resolved).unwrap_or(resolved)
}

/// Whether `path` lies inside `root` lexically (`!relative(root, path).startsWith('..')`).
#[must_use]
pub fn inside(root: &Path, path: &Path) -> bool {
    normalize(path).starts_with(normalize(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_and_normalizes() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(normalize(Path::new("/a/b/../c/./d")), Path::new("/a/c/d"));
        assert!(inside(Path::new("/root"), Path::new("/root/x/y")));
        assert!(!inside(Path::new("/root"), Path::new("/root/../other")));
        assert_eq!(
            resolve(Path::new("/root"), Path::new("staging/f.parquet")),
            Path::new("/root/staging/f.parquet")
        );
    }

    #[test]
    fn durable_directory_creation_is_idempotent_and_an_injected_fsync_fails() {
        let root = std::env::temp_dir().join(format!("solos-mkdir-{}", uuid::Uuid::new_v4()));
        let nested = root.join("staging/transactions/epoch=0");
        create_dir_all_durable(&nested).unwrap();
        create_dir_all_durable(&nested).unwrap();
        assert!(nested.is_dir());
        inject_fsync_failure(true);
        let failed = create_dir_all_durable(&root.join("new/child"));
        inject_fsync_failure(false);
        let message = failed.unwrap_err().to_string();
        assert!(message.contains("fsync failed"), "{message}");
        let _ = std::fs::remove_dir_all(root);
    }
}
