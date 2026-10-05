//! Reader leases: a reader drops a `{pid}` file in `<root>/.readers` while it uses a catalog, and
//! cleanup refuses to unlink superseded files while any live process holds one.

use std::path::Path;

/// Run `read` while holding a lease in `root`.
pub fn with_read_lease<T, E: From<std::io::Error>>(
    root: &Path,
    read: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let directory = root.join(".readers");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join(format!(
        "{}-{}.json",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        set_private(&file)?;
        file.write_all(format!("{{\"pid\":{}}}", std::process::id()).as_bytes())?;
    }
    let result = read();
    let _ = std::fs::remove_file(&path);
    result
}

#[cfg(unix)]
fn set_private(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private(_file: &std::fs::File) -> std::io::Result<()> {
    Ok(())
}

/// Whether any lease belongs to a live process. Unreadable leases count as readers; leases of
/// dead processes are removed.
pub fn has_readers(root: &Path) -> std::io::Result<bool> {
    let directory = root.join(".readers");
    std::fs::create_dir_all(&directory)?;
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Ok(true),
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            return Ok(true);
        };
        let Some(pid) = value.get("pid").and_then(serde_json::Value::as_i64) else {
            return Ok(true);
        };
        if pid < 1 {
            return Ok(true);
        }
        if process_alive(pid) {
            return Ok(true);
        }
        let _ = std::fs::remove_file(&path);
    }
    Ok(false)
}

#[cfg(unix)]
fn process_alive(pid: i64) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return true;
    };
    match nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(nix::errno::Errno::ESRCH) => false,
        Err(_) => true,
    }
}

#[cfg(not(unix))]
fn process_alive(_pid: i64) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_blocks_cleanup_only_while_held() {
        let root = std::env::temp_dir().join(format!("solos-lease-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        with_read_lease::<_, std::io::Error>(&root, || {
            assert!(has_readers(&root).unwrap());
            Ok(())
        })
        .unwrap();
        assert!(!has_readers(&root).unwrap());
        std::fs::write(root.join(".readers/stale.json"), "{\"pid\":2147483646}").unwrap();
        assert!(!has_readers(&root).unwrap());
        std::fs::write(root.join(".readers/bad.json"), "not json").unwrap();
        assert!(has_readers(&root).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }
}
