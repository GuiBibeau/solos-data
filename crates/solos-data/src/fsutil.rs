//! File helpers shared by the collector and the decoder: streaming SHA-256, fsync of files and
//! directories, and the write-tmp-fsync-rename pattern every published artefact uses.

use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

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

/// fsync a file or a directory.
pub fn sync_path(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
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
        file.sync_all()?;
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
}
