//! Object storage for sealed raw files. Production talks to Cloudflare R2 through rclone.
//! Tests use [`MemoryStore`]. Credentials stay in the child process environment and never
//! appear in a log line or an error string.

use crate::jsonout::safe_error;
use crate::store::StoreError;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Size and, when the backend stored one, the SHA-256 of an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectHead {
    /// Object size in bytes.
    pub size: u64,
    /// Stored SHA-256 hex, when the listing carries one. Absent means the caller streams.
    pub sha256: Option<String>,
}

/// A readable object store. `head` must not download the body.
pub trait ObjectStore {
    /// Metadata for `key`, or `None` when the object is absent.
    ///
    /// # Errors
    ///
    /// A transport or listing failure. An absent object is `Ok(None)`, not an error.
    fn head(&self, key: &str) -> Result<Option<ObjectHead>, StoreError>;

    /// Stream the object body into `dest`.
    ///
    /// # Errors
    ///
    /// The object is missing, or the read failed.
    fn write_to(&self, key: &str, dest: &mut dyn Write) -> Result<(), StoreError>;
}

/// One object held in memory.
struct MemObject {
    bytes: Vec<u8>,
    stored_sha256: Option<String>,
    size_override: Option<u64>,
}

/// In-memory store for tests. Never opens a network connection.
pub struct MemoryStore {
    objects: Mutex<BTreeMap<String, MemObject>>,
    writes: AtomicU64,
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            writes: AtomicU64::new(0),
        }
    }

    /// Insert or replace `key`. `size_override` is the size `head` reports.
    pub fn insert(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
        stored_sha256: Option<String>,
        size_override: Option<u64>,
    ) {
        let mut guard = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(
            key.into(),
            MemObject {
                bytes,
                stored_sha256,
                size_override,
            },
        );
    }

    /// How many times [`ObjectStore::write_to`] has streamed a body.
    #[must_use]
    pub fn write_to_calls(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStore for MemoryStore {
    fn head(&self, key: &str) -> Result<Option<ObjectHead>, StoreError> {
        let guard = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.get(key).map(|object| ObjectHead {
            size: object.size_override.unwrap_or(object.bytes.len() as u64),
            sha256: object.stored_sha256.clone(),
        }))
    }

    fn write_to(&self, key: &str, dest: &mut dyn Write) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let guard = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(object) = guard.get(key) else {
            return Err(StoreError::Check(format!("object store missing {key}")));
        };
        dest.write_all(&object.bytes)?;
        Ok(())
    }
}

/// rclone talking to R2, or to `SOLOS_BACKUP_DEST` when that is set.
pub struct RcloneStore {
    bin: PathBuf,
    dest: String,
    /// Passed only to the rclone child. Not written to logs.
    env: Vec<(String, String)>,
}

impl RcloneStore {
    /// The backup destination. Reads `SOLOS_BACKUP_ENV` (default `~/.config/solos-data/backup.env`)
    /// unless `SOLOS_BACKUP_DEST` is already set.
    ///
    /// # Errors
    ///
    /// The env file is missing, unreadable, or lacks a required key. The message names the key
    /// and does not include its value.
    pub fn open_default() -> Result<Self, StoreError> {
        if let Ok(dest) = std::env::var("SOLOS_BACKUP_DEST")
            && !dest.is_empty()
        {
            return Ok(Self {
                bin: rclone_bin(),
                dest,
                env: Vec::new(),
            });
        }
        let path = match std::env::var("SOLOS_BACKUP_ENV") {
            Ok(value) if !value.is_empty() => PathBuf::from(value),
            _ => home_config()?.join("solos-data/backup.env"),
        };
        let text = std::fs::read_to_string(&path)
            .map_err(|_| StoreError::Check("backup env file is missing or unreadable".into()))?;
        let map = parse_env_file(&text)?;
        for key in [
            "R2_ACCOUNT_ID",
            "R2_BUCKET",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
        ] {
            if map.get(key).is_none_or(String::is_empty) {
                return Err(StoreError::Check(format!("backup env is missing {key}")));
            }
        }
        let account = &map["R2_ACCOUNT_ID"];
        let bucket = &map["R2_BUCKET"];
        let env = vec![
            ("RCLONE_CONFIG".into(), String::new()),
            ("RCLONE_CONFIG_R2_TYPE".into(), "s3".into()),
            ("RCLONE_CONFIG_R2_PROVIDER".into(), "Cloudflare".into()),
            (
                "RCLONE_CONFIG_R2_ENDPOINT".into(),
                format!("https://{account}.r2.cloudflarestorage.com"),
            ),
            (
                "RCLONE_CONFIG_R2_ACCESS_KEY_ID".into(),
                map["R2_ACCESS_KEY_ID"].clone(),
            ),
            (
                "RCLONE_CONFIG_R2_SECRET_ACCESS_KEY".into(),
                map["R2_SECRET_ACCESS_KEY"].clone(),
            ),
            ("RCLONE_CONFIG_R2_NO_CHECK_BUCKET".into(), "true".into()),
        ];
        Ok(Self {
            bin: rclone_bin(),
            dest: format!("r2:{bucket}"),
            env,
        })
    }

    fn remote(&self, key: &str) -> String {
        format!(
            "{}/{}",
            self.dest.trim_end_matches('/'),
            key.trim_start_matches('/')
        )
    }

    fn command(&self, args: &[String]) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.args(args);
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        cmd.stdin(Stdio::null());
        cmd
    }
}

impl ObjectStore for RcloneStore {
    fn head(&self, key: &str) -> Result<Option<ObjectHead>, StoreError> {
        let remote = self.remote(key);
        let output = self
            .command(&["lsjson".into(), remote])
            .output()
            .map_err(|_| StoreError::Check("rclone failed to start".into()))?;
        if !output.status.success() {
            return absent_or_error("head", &output);
        }
        parse_lsjson(&String::from_utf8_lossy(&output.stdout))
    }

    fn write_to(&self, key: &str, dest: &mut dyn Write) -> Result<(), StoreError> {
        let remote = self.remote(key);
        let mut child = self
            .command(&["cat".into(), remote])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| StoreError::Check("rclone failed to start".into()))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| StoreError::Check("rclone stdout missing".into()))?;
        let copied = std::io::copy(&mut stdout, dest);
        let status = child
            .wait()
            .map_err(|_| StoreError::Check("rclone wait failed".into()))?;
        let mut stderr = Vec::new();
        if let Some(mut err) = child.stderr.take() {
            let _ = err.read_to_end(&mut stderr);
        }
        copied.map_err(|_| StoreError::Check("object store read failed".into()))?;
        if status.success() {
            return Ok(());
        }
        let code = status.code().unwrap_or(-1);
        Err(StoreError::Check(format!(
            "object store read failed ({code}): {}",
            safe_error(&String::from_utf8_lossy(&stderr))
        )))
    }
}

fn absent_or_error(
    step: &str,
    output: &std::process::Output,
) -> Result<Option<ObjectHead>, StoreError> {
    let code = output.status.code().unwrap_or(-1);
    let stderr = safe_error(&String::from_utf8_lossy(&output.stderr));
    let lower = stderr.to_ascii_lowercase();
    // rclone exit 3 is "directory not found". A network failure must not look like a missing
    // object, or a drop would proceed with no copy.
    if code == 3 || lower.contains("not found") || lower.contains("does not exist") {
        return Ok(None);
    }
    Err(StoreError::Check(format!(
        "object store {step} failed ({code}): {stderr}"
    )))
}

fn home_config() -> Result<PathBuf, StoreError> {
    let home = std::env::var("HOME").map_err(|_| {
        StoreError::Check("HOME is not set; cannot find the backup env file".into())
    })?;
    Ok(PathBuf::from(home).join(".config"))
}

fn rclone_bin() -> PathBuf {
    if let Ok(path) = std::env::var("RCLONE")
        && !path.is_empty()
    {
        return PathBuf::from(path);
    }
    if let Ok(home) = std::env::var("HOME") {
        let candidate = PathBuf::from(home).join(".local/bin/rclone");
        if candidate.is_file() {
            return candidate;
        }
    }
    PathBuf::from("rclone")
}

/// `KEY=VALUE` lines. A malformed line's error does not include the line, so a secret on it
/// cannot leak into a log.
pub(crate) fn parse_env_file(text: &str) -> Result<BTreeMap<String, String>, StoreError> {
    let mut map = BTreeMap::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(StoreError::Check(format!(
                "backup env line {} is not KEY=VALUE",
                index + 1
            )));
        };
        if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(StoreError::Check(format!(
                "backup env line {} has a bad key",
                index + 1
            )));
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        map.insert(key.to_owned(), value.to_owned());
    }
    Ok(map)
}

/// Parse `rclone lsjson` output. An empty list is an absent object.
pub(crate) fn parse_lsjson(text: &str) -> Result<Option<ObjectHead>, StoreError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(trimmed)
        .map_err(|_| StoreError::Check("object store listing was not json".into()))?;
    let item = if let Some(items) = value.as_array() {
        items
            .iter()
            .find(|item| item.get("IsDir").and_then(Value::as_bool) != Some(true))
            .cloned()
    } else if value.is_object() {
        Some(value)
    } else {
        None
    };
    let Some(item) = item else {
        return Ok(None);
    };
    let size = item
        .get("Size")
        .and_then(json_u64)
        .ok_or_else(|| StoreError::Check("object store listing has no size".into()))?;
    let sha256 = item
        .get("Hashes")
        .and_then(|hashes| hashes.get("SHA-256").or_else(|| hashes.get("sha256")))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(Some(ObjectHead { size, sha256 }))
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|n| u64::try_from(n).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parse_errors_do_not_echo_the_line() {
        let err = parse_env_file("R2_SECRET_ACCESS_KEY not-a-pair super-secret").unwrap_err();
        let text = err.to_string();
        assert!(!text.contains("super-secret"), "{text}");
        assert!(!text.contains("not-a-pair"), "{text}");
        let map = parse_env_file("R2_BUCKET=example-bucket\n# comment\n").unwrap();
        assert_eq!(
            map.get("R2_BUCKET").map(String::as_str),
            Some("example-bucket")
        );
    }

    #[test]
    fn lsjson_reads_size_and_optional_sha_and_an_empty_list_is_absent() {
        let head = parse_lsjson(
            r#"[{"Path":"a.parquet","Size":12,"IsDir":false,"Hashes":{"SHA-256":"abc"}}]"#,
        )
        .unwrap();
        assert_eq!(
            head,
            Some(ObjectHead {
                size: 12,
                sha256: Some("abc".into()),
            })
        );
        assert_eq!(parse_lsjson("[]").unwrap(), None);
    }
}
