//! SHA-256 manifests for hourly fill files.
//!
//! Accepts a `sha256sum` file (`<hex>  <path>` or `<hex> *<path>`) and a JSON manifest: an
//! object of path to hex, or `{ "files": [ { "path", "sha256" } ] }`. A path counts when it
//! ends in `YYYYMMDD/H.lz4` with `H` unpadded (`0`..`23`). Other entries are ignored. Two
//! checksums for one hour are an error.

use crate::store::StoreError;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// Hour key (`YYYYMMDD/H`) to lowercase sha256 hex.
pub type HourSums = BTreeMap<String, String>;

/// Load checksums. Hour keys are `YYYYMMDD/H`.
///
/// # Errors
///
/// The file cannot be read, a fill entry's digest is not 64 hex characters, or two entries
/// name the same hour.
pub fn load_checksums(path: &Path) -> Result<HourSums, StoreError> {
    let text = std::fs::read_to_string(path)?;
    let trimmed = text.trim_start();
    let mut out = HourSums::new();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        let value: Value = serde_json::from_str(trimmed)
            .map_err(|e| StoreError::Check(format!("checksum manifest: {e}")))?;
        ingest_json(&mut out, &value)?;
    } else {
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let Some(sha) = parts.next() else { continue };
            let Some(file) = parts.next() else {
                return Err(StoreError::Check(format!(
                    "checksum line {}: expected sha256 and path",
                    index + 1
                )));
            };
            if parts.next().is_some() {
                return Err(StoreError::Check(format!(
                    "checksum line {}: extra fields",
                    index + 1
                )));
            }
            ingest(&mut out, file, sha)?;
        }
    }
    Ok(out)
}

fn ingest_json(out: &mut HourSums, value: &Value) -> Result<(), StoreError> {
    match value {
        Value::Array(items) => {
            for item in items {
                ingest_json_item(out, item)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            if let Some(files) = map.get("files").or_else(|| map.get("objects")) {
                return ingest_json(out, files);
            }
            for (path, sha) in map {
                let Some(sha) = sha.as_str() else {
                    return Err(StoreError::Check(format!(
                        "checksum for {path} is not a string"
                    )));
                };
                ingest(out, path, sha)?;
            }
            Ok(())
        }
        _ => Err(StoreError::Check(
            "checksum manifest is not an object or array".into(),
        )),
    }
}

fn ingest_json_item(out: &mut HourSums, item: &Value) -> Result<(), StoreError> {
    let Some(object) = item.as_object() else {
        return Err(StoreError::Check(
            "checksum manifest entry is not an object".into(),
        ));
    };
    let path = object
        .get("path")
        .or_else(|| object.get("name"))
        .or_else(|| object.get("file"))
        .or_else(|| object.get("key"))
        .and_then(Value::as_str);
    let sha = object
        .get("sha256")
        .or_else(|| object.get("sha"))
        .or_else(|| object.get("hash"))
        .and_then(Value::as_str);
    let (Some(path), Some(sha)) = (path, sha) else {
        return Err(StoreError::Check(
            "checksum manifest entry needs a path and a sha256".into(),
        ));
    };
    ingest(out, path, sha)
}

fn ingest(out: &mut HourSums, path: &str, sha: &str) -> Result<(), StoreError> {
    let Some(key) = hour_key_of_path(path) else {
        return Ok(());
    };
    let sha = sha.trim().to_ascii_lowercase();
    if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(StoreError::Check(format!(
            "checksum for {key} is not sha256"
        )));
    }
    if out.insert(key.clone(), sha).is_some() {
        return Err(StoreError::Check(format!("duplicate checksum for {key}")));
    }
    Ok(())
}

/// `YYYYMMDD/H` when `path` ends in that hour's `.lz4` name. `H` is unpadded.
#[must_use]
pub fn hour_key_of_path(path: &str) -> Option<String> {
    let path = path.trim().trim_start_matches('*');
    let mut parts: Vec<&str> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    let file = parts.pop()?;
    let date = parts.pop()?;
    let stem = file.strip_suffix(".lz4")?;
    let hour: u32 = stem.parse().ok()?;
    if stem != hour.to_string() || hour > 23 {
        return None;
    }
    hour_key(date, hour).ok()
}

/// `YYYYMMDD/H` for an unpadded hour in `0..=23`.
///
/// # Errors
///
/// `date` is not eight digits or `hour` is above 23.
pub fn hour_key(date: &str, hour: u32) -> Result<String, String> {
    if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) || hour > 23 {
        return Err(format!("bad hour {date}/{hour}"));
    }
    Ok(format!("{date}/{hour}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_and_both_manifest_shapes() {
        assert_eq!(
            hour_key_of_path("node_fills_by_block/hourly/20261022/0.lz4").as_deref(),
            Some("20261022/0")
        );
        assert_eq!(
            hour_key_of_path("*20261022/23.lz4").as_deref(),
            Some("20261022/23")
        );
        assert!(hour_key_of_path("20261022/00.lz4").is_none());
        assert!(hour_key_of_path("20261022/24.lz4").is_none());
        let root = std::env::temp_dir().join(format!(
            "hl-sums-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let sha = "ab".repeat(32);
        let sums = root.join("SHA256SUMS");
        std::fs::write(
            &sums,
            format!("{sha}  node_fills_by_block/hourly/20261022/12.lz4\n{sha} *20261022/13.lz4\n"),
        )
        .unwrap();
        let loaded = load_checksums(&sums).unwrap();
        assert_eq!(
            loaded.get("20261022/12").map(String::as_str),
            Some(sha.as_str())
        );
        assert_eq!(
            loaded.get("20261022/13").map(String::as_str),
            Some(sha.as_str())
        );
        let manifest = root.join("MANIFEST.json");
        std::fs::write(
            &manifest,
            format!(
                r#"{{"files":[{{"path":"hourly/20261023/0.lz4","sha256":"{sha}"}}],"note":"ignored"}}"#
            ),
        )
        .unwrap();
        let loaded = load_checksums(&manifest).unwrap();
        assert_eq!(
            loaded.get("20261023/0").map(String::as_str),
            Some(sha.as_str())
        );
        std::fs::write(
            &sums,
            format!("{sha}  20261022/1.lz4\n{sha}  a/20261022/1.lz4\n"),
        )
        .unwrap();
        assert!(
            load_checksums(&sums)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
