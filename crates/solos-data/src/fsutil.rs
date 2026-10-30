//! File helpers shared by the collector and the decoder: streaming SHA-256, fsync of files and
//! directories, and the write-tmp-fsync-rename pattern every published artefact uses.

use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

/// Which fsync call an injected failure targets (issue #56). Tests inject a fatal fsync at one
/// step; production never sets one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncStep {
    /// The next fsync of any kind.
    Any,
    /// The temporary file's fsync, before the rename.
    TempFile,
    /// The directory fsync after the rename.
    DirAfterRename,
    /// The parent-directory fsync inside `create_dir_all_durable` (staging directories).
    ParentDir,
}

/// The I/O failure a test has armed. Production never arms one; a lane that hits an injected or
/// real failure must not retry it (ADR-0016).
#[cfg(feature = "failpoints")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FsyncFault {
    /// A generic fsync failure with no OS error code (the pre-existing failpoint).
    Generic,
    /// EIO, as a failing disk would return.
    Eio,
    /// ENOSPC, as a full disk would return.
    Enospc,
}

#[cfg(feature = "failpoints")]
thread_local! {
    /// When `Some`, the next matching fsync on this thread fails with the armed fault. The arm
    /// fires once: the first matching fsync consumes it. A test that arms one and then keeps
    /// running past the expected failure should call [`clear_fsync_failure`].
    static FSYNC_FAULT: std::cell::Cell<Option<(FsyncStep, FsyncFault)>> =
        const { std::cell::Cell::new(None) };
    /// When `Some`, the next durable write in [`write_durable`] fails with ENOSPC (fires once).
    static WRITE_FAULT: std::cell::Cell<Option<FsyncFault>> =
        const { std::cell::Cell::new(None) };
}

/// Fail the next fsync of `step` on this thread, once, with a generic injected failure. The first
/// matching fsync consumes the arm. A lane that hits the failure must exit and restart from the
/// watermark, not retry it (ADR-0016).
#[cfg(feature = "failpoints")]
pub fn inject_fsync_failure(step: FsyncStep) {
    FSYNC_FAULT.with(|slot| slot.set(Some((step, FsyncFault::Generic))));
}

/// Fail the next fsync of `step` on this thread, once, with EIO, as a failing disk would return.
/// The first matching fsync consumes the arm.
#[cfg(feature = "failpoints")]
pub fn inject_fsync_eio(step: FsyncStep) {
    FSYNC_FAULT.with(|slot| slot.set(Some((step, FsyncFault::Eio))));
}

/// Fail the next fsync of `step` on this thread, once, with ENOSPC, as a full disk would return.
/// The first matching fsync consumes the arm.
#[cfg(feature = "failpoints")]
pub fn inject_fsync_enospc(step: FsyncStep) {
    FSYNC_FAULT.with(|slot| slot.set(Some((step, FsyncFault::Enospc))));
}

/// Fail the next durable write in [`write_durable`] on this thread, once, with ENOSPC. The error
/// keeps its OS error code (`ErrorKind::StorageFull` / `ENOSPC`), so callers match it as a typed
/// `StoreError::Io` and a failed write is retryable.
#[cfg(feature = "failpoints")]
pub fn inject_write_enospc() {
    WRITE_FAULT.with(|slot| slot.set(Some(FsyncFault::Enospc)));
}

/// Clear any armed fsync fault without firing it.
#[cfg(feature = "failpoints")]
pub fn clear_fsync_failure() {
    FSYNC_FAULT.with(|slot| slot.set(None));
}

/// Clear any armed durable-write fault without firing it.
#[cfg(feature = "failpoints")]
pub fn clear_write_failure() {
    WRITE_FAULT.with(|slot| slot.set(None));
}

/// The OS-level error an armed [`FsyncFault`] represents, before the production wrap in
/// [`wrap_fsync`].
#[cfg(feature = "failpoints")]
fn fault_error(fault: FsyncFault) -> io::Error {
    match fault {
        FsyncFault::Generic => io::Error::other("injected"),
        FsyncFault::Eio => io::Error::from_raw_os_error(nix::libc::EIO),
        FsyncFault::Enospc => io::Error::from_raw_os_error(nix::libc::ENOSPC),
    }
}

/// The production wrap that makes an fsync failure fatal: `fatal_class` matches the `fsync
/// failed` prefix (ADR-0016). Injected faults go through this same wrap, so deleting it must turn
/// both the real and the injected fsync tests red.
fn wrap_fsync(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("fsync failed: {error}"))
}

#[cfg(feature = "failpoints")]
fn sync_all(file: &File, fault: Option<FsyncFault>) -> io::Result<()> {
    // An injected fault replaces the syscall result, then both paths go through the one
    // production wrap below, so an injected failure is classified exactly like a real one.
    let result = match fault {
        Some(fault) => Err(fault_error(fault)),
        None => file.sync_all(),
    };
    result.map_err(wrap_fsync)
}

#[cfg(not(feature = "failpoints"))]
fn sync_all(file: &File) -> io::Result<()> {
    file.sync_all().map_err(wrap_fsync)
}

#[cfg(feature = "failpoints")]
fn sync_handle(file: &File, step: FsyncStep) -> io::Result<()> {
    let fault = FSYNC_FAULT.with(|slot| {
        let Some((target, fault)) = slot.get() else {
            return None;
        };
        if target == step || target == FsyncStep::Any {
            slot.set(None); // fire once
            Some(fault)
        } else {
            None
        }
    });
    sync_all(file, fault)
}

#[cfg(not(feature = "failpoints"))]
fn sync_handle(file: &File, _step: FsyncStep) -> io::Result<()> {
    sync_all(file)
}

/// The armed durable-write fault, if any, consumed on the first durable write.
#[cfg(feature = "failpoints")]
fn write_failpoint() -> Option<io::Error> {
    WRITE_FAULT.with(|slot| slot.take().map(fault_error))
}

#[cfg(not(feature = "failpoints"))]
fn write_failpoint() -> Option<io::Error> {
    None
}

// Test-only guard: once [`install_fsutil_write_guard`] sets a root on this thread, an fsutil
// helper write outside that root panics. It is a narrow assertion on the fsutil helpers only
// (`write_durable`, `write_atomic`, `create_dir_all_durable`): it compares paths as text, is
// thread-local, and does not see `sync_path`, plain `std::fs`, renames, or DuckDB COPY/temp
// writes. Tests install it around their temp dir so a stray fsutil write fails the test instead
// of landing elsewhere. Production never installs it.
#[cfg(feature = "failpoints")]
thread_local! {
    static WRITE_GUARD: std::cell::RefCell<Option<std::path::PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Restrict the fsutil write helpers (`write_durable`, `write_atomic`,
/// `create_dir_all_durable`) on this thread to `root` until [`clear_fsutil_write_guard`]. Test
/// helper; a narrow assertion, not suite-wide coverage.
#[cfg(feature = "failpoints")]
pub fn install_fsutil_write_guard(root: &Path) {
    WRITE_GUARD.with(|guard| *guard.borrow_mut() = Some(root.to_path_buf()));
}

/// Lift the restriction installed by [`install_fsutil_write_guard`]. Test helper.
#[cfg(feature = "failpoints")]
pub fn clear_fsutil_write_guard() {
    WRITE_GUARD.with(|guard| *guard.borrow_mut() = None);
}

#[cfg(feature = "failpoints")]
fn guard_inside(path: &Path) {
    let Some(root) = WRITE_GUARD.with(|guard| guard.borrow().clone()) else {
        return;
    };
    assert!(
        inside(&root, path),
        "test wrote outside its temp dir: {} (guard root {})",
        path.display(),
        root.display()
    );
}

#[cfg(not(feature = "failpoints"))]
fn guard_inside(_path: &Path) {}

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
    sync_handle(&File::open(path)?, FsyncStep::Any)
}

/// fsync a temporary file before its rename ([`FsyncStep::TempFile`]).
pub fn sync_tmp_file(path: &Path) -> io::Result<()> {
    sync_handle(&File::open(path)?, FsyncStep::TempFile)
}

/// fsync a directory after a rename ([`FsyncStep::DirAfterRename`]).
pub fn sync_dir_after_rename(path: &Path) -> io::Result<()> {
    sync_handle(&File::open(path)?, FsyncStep::DirAfterRename)
}

/// Create `path` and every missing ancestor. Each newly created directory's parent is fsynced, so
/// the new directory entry itself is durable. Directories that already exist are left alone.
pub fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    guard_inside(path);
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
                sync_handle(
                    &File::open(parent.unwrap_or(Path::new(".")))?,
                    FsyncStep::ParentDir,
                )?;
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
    guard_inside(path);
    let tmp = tmp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        if let Some(error) = write_failpoint() {
            return Err(error);
        }
        file.write_all(body)?;
        sync_handle(&file, FsyncStep::TempFile)?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_handle(&File::open(parent)?, FsyncStep::DirAfterRename)?;
    }
    Ok(())
}

/// Write `body` to `path` through a `.tmp` rename without fsync, as the TypeScript status
/// snapshots do.
pub fn write_atomic(path: &Path, body: &[u8]) -> io::Result<()> {
    guard_inside(path);
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
        inject_fsync_failure(FsyncStep::ParentDir);
        let failed = create_dir_all_durable(&root.join("new/child"));
        clear_fsync_failure();
        let message = failed.unwrap_err().to_string();
        assert!(message.contains("fsync failed"), "{message}");
        let _ = std::fs::remove_dir_all(root);
    }
}
