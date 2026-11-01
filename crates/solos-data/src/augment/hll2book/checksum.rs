//! Checksums for `l2Book` hours.
//!
//! A path counts when it names `YYYYMMDD/H/<COIN>.lz4` or `YYYYMMDD/H/l2Book/<COIN>.lz4`
//! (`H` unpadded, `0`..=`23`), with any prefix (`market_data/`, `l2book/`, an `s3://` key).
//! Every such entry needs a sha256 and a byte size. A size is what lets a run skip a file the
//! downloader has not finished. Two entries for one hour and coin are an error.

use crate::store::StoreError;
use serde_json::Value;
use std::path::Path;

/// One manifest row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Path as written in the manifest, with an `s3://hyperliquid-archive/` prefix removed.
    pub manifest_path: String,
    /// `YYYYMMDD`.
    pub date: String,
    /// Unpadded hour, `0`..=`23`.
    pub hour: u32,
    /// File stem, the native coin name.
    pub coin: String,
    /// Lowercase sha256 hex of the finished `.lz4` bytes.
    pub sha256: String,
    /// Finished file size. A shorter file on disk is still downloading.
    pub bytes: u64,
}

impl Entry {
    /// `YYYYMMDD/H`.
    #[must_use]
    pub fn hour_key(&self) -> String {
        format!("{}/{}", self.date, self.hour)
    }
}

/// Load book entries. Other paths are ignored.
///
/// # Errors
///
/// The file cannot be read, a book entry has no size or a bad digest, or two entries name the
/// same hour and coin.
pub fn load_entries(path: &Path) -> Result<Vec<Entry>, StoreError> {
    let text = std::fs::read_to_string(path)?;
    let trimmed = text.trim_start();
    let mut out = Vec::new();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        let value: Value = serde_json::from_str(trimmed)
            .map_err(|e| StoreError::Check(format!("checksum manifest: {e}")))?;
        ingest_json(&mut out, &value)?;
    } else {
        for (index, line) in text.lines().enumerate() {
            ingest_line(&mut out, line, index + 1)?;
        }
    }
    // One hash-set pass, not a scan per row: the full-archive manifest is millions of lines.
    let mut seen = std::collections::HashSet::with_capacity(out.len());
    for entry in &out {
        if !seen.insert((entry.date.clone(), entry.hour, entry.coin.clone())) {
            return Err(StoreError::Check(format!(
                "duplicate checksum for {}/{}/{}",
                entry.date, entry.hour, entry.coin
            )));
        }
    }
    Ok(out)
}

fn ingest_line(out: &mut Vec<Entry>, line: &str, number: usize) -> Result<(), StoreError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Ok(());
    }
    let mut parts = line.split_whitespace();
    let Some(sha) = parts.next() else {
        return Ok(());
    };
    let Some(second) = parts.next() else {
        return Err(StoreError::Check(format!(
            "checksum line {number}: expected sha256, size, and path"
        )));
    };
    let third = parts.next();
    if parts.next().is_some() {
        return Err(StoreError::Check(format!(
            "checksum line {number}: extra fields"
        )));
    }
    match third {
        Some(path) => {
            let bytes: u64 = second.parse().map_err(|_| {
                StoreError::Check(format!("checksum line {number}: size is not an integer"))
            })?;
            ingest(out, path, sha, Some(bytes))
        }
        None => ingest(out, second, sha, None),
    }
}

fn ingest_json(out: &mut Vec<Entry>, value: &Value) -> Result<(), StoreError> {
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
            for (path, body) in map {
                match body {
                    Value::String(sha) => ingest(out, path, sha, None)?,
                    Value::Object(_) => ingest_json_item(out, &{
                        let mut object = body.clone();
                        if let Some(map) = object.as_object_mut() {
                            map.entry("path".to_owned())
                                .or_insert_with(|| Value::String(path.clone()));
                        }
                        object
                    })?,
                    _ => {
                        return Err(StoreError::Check(format!(
                            "checksum for {path} is not a string or object"
                        )));
                    }
                }
            }
            Ok(())
        }
        _ => Err(StoreError::Check(
            "checksum manifest is not an object or array".into(),
        )),
    }
}

fn ingest_json_item(out: &mut Vec<Entry>, item: &Value) -> Result<(), StoreError> {
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
    let bytes = object
        .get("size")
        .or_else(|| object.get("bytes"))
        .and_then(json_u64);
    ingest(out, path, sha, bytes)
}

fn json_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|n| u64::try_from(n).ok())),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn ingest(
    out: &mut Vec<Entry>,
    path: &str,
    sha: &str,
    bytes: Option<u64>,
) -> Result<(), StoreError> {
    let Some((date, hour, coin)) = book_of_path(path) else {
        return Ok(());
    };
    let Some(bytes) = bytes else {
        return Err(StoreError::Check(format!(
            "{date}/{hour}/{coin}: checksum has no size"
        )));
    };
    let sha = sha.trim().trim_start_matches('*').to_ascii_lowercase();
    if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(StoreError::Check(format!(
            "{date}/{hour}/{coin}: checksum is not sha256"
        )));
    }
    out.push(Entry {
        manifest_path: strip_archive(path.trim().trim_start_matches('*')),
        date,
        hour,
        coin,
        sha256: sha,
        bytes,
    });
    Ok(())
}

fn strip_archive(path: &str) -> String {
    path.trim()
        .trim_start_matches("s3://hyperliquid-archive/")
        .trim_start_matches('/')
        .to_owned()
}

/// `(YYYYMMDD, hour, coin)` when `path` names an l2Book object.
#[must_use]
pub fn book_of_path(path: &str) -> Option<(String, u32, String)> {
    let path = strip_archive(path.trim().trim_start_matches('*'));
    let parts: Vec<&str> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    for index in 0..parts.len() {
        let date = parts[index];
        if calendar_date(date).is_none() {
            continue;
        }
        let hour_text = parts.get(index + 1)?;
        let hour: u32 = hour_text.parse().ok()?;
        if hour_text != &hour.to_string() || hour > 23 {
            continue;
        }
        let file = match parts.get(index + 2..)? {
            ["l2Book", file] => *file,
            [file] => *file,
            _ => continue,
        };
        let coin = file.strip_suffix(".lz4")?;
        if !valid_coin(coin) {
            continue;
        }
        return Some((date.to_owned(), hour, coin.to_owned()));
    }
    None
}

/// Eight-digit calendar date.
#[must_use]
pub fn calendar_date(value: &str) -> Option<String> {
    if value.len() != 8 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    chrono::NaiveDate::parse_from_str(value, "%Y%m%d").ok()?;
    Some(value.to_owned())
}

/// A single path component that can be a coin file stem.
#[must_use]
pub fn valid_coin(coin: &str) -> bool {
    !coin.is_empty()
        && coin != "."
        && coin != ".."
        && coin != "l2Book"
        && coin.bytes().all(|b| b.is_ascii_graphic())
        && !coin.contains(['/', '\\'])
}

/// Paths to try under `input` for one book, most specific layout last in the search only after
/// the manifest-relative path. The caller tries the manifest path first.
#[must_use]
pub fn candidate_paths(input: &Path, date: &str, hour: u32, coin: &str) -> [std::path::PathBuf; 4] {
    [
        input.join(format!("{date}/{hour}/{coin}.lz4")),
        input.join(format!("{date}/{hour}/l2Book/{coin}.lz4")),
        input.join(format!("market_data/{date}/{hour}/l2Book/{coin}.lz4")),
        input.join(format!("l2book/{date}/{hour}/{coin}.lz4")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_cover_both_layouts_and_size_is_required() {
        assert_eq!(
            book_of_path("market_data/20261022/0/l2Book/BTC.lz4"),
            Some(("20261022".into(), 0, "BTC".into()))
        );
        assert_eq!(
            book_of_path("s3://hyperliquid-archive/market_data/20261022/23/l2Book/kPEPE.lz4"),
            Some(("20261022".into(), 23, "kPEPE".into()))
        );
        assert_eq!(
            book_of_path("l2book/20261022/2/ETH.lz4"),
            Some(("20261022".into(), 2, "ETH".into()))
        );
        assert_eq!(
            book_of_path("20261022/10/SOL.lz4"),
            Some(("20261022".into(), 10, "SOL".into()))
        );
        assert!(book_of_path("20261022/00/BTC.lz4").is_none());
        assert!(book_of_path("20261301/0/BTC.lz4").is_none());
        assert!(book_of_path("20261022/24/BTC.lz4").is_none());

        let root = std::env::temp_dir().join(format!(
            "hl-l2-sums-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let sha = "ab".repeat(32);
        let manifest = root.join("MANIFEST.json");
        std::fs::write(
            &manifest,
            format!(
                r#"{{"files":[{{"path":"market_data/20261022/0/l2Book/BTC.lz4","sha256":"{sha}","size":10}}],"note":"ignored"}}"#
            ),
        )
        .unwrap();
        let loaded = load_entries(&manifest).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].coin, "BTC");
        assert_eq!(loaded[0].bytes, 10);
        std::fs::write(
            &manifest,
            format!(r#"{{"20261022/1/ETH.lz4":{{"sha256":"{sha}","bytes":4}}}}"#),
        )
        .unwrap();
        let loaded = load_entries(&manifest).unwrap();
        assert_eq!(loaded[0].hour_key(), "20261022/1");
        std::fs::write(
            &manifest,
            format!(r#"{{"files":[{{"path":"20261022/1/ETH.lz4","sha256":"{sha}"}}]}}"#),
        )
        .unwrap();
        assert!(
            load_entries(&manifest)
                .unwrap_err()
                .to_string()
                .contains("no size")
        );
        let sums = root.join("SHA256SUMS");
        std::fs::write(&sums, format!("{sha}  12  20261022/3/SOL.lz4\n")).unwrap();
        let loaded = load_entries(&sums).unwrap();
        assert_eq!(loaded[0].bytes, 12);
        assert_eq!(loaded[0].coin, "SOL");
        std::fs::write(&sums, format!("{sha}  20261022/3/SOL.lz4\n")).unwrap();
        assert!(
            load_entries(&sums)
                .unwrap_err()
                .to_string()
                .contains("no size")
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_many_row_listing_loads_and_still_names_the_duplicate() {
        let root = std::env::temp_dir().join(format!(
            "hl-l2-many-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let sha = "cd".repeat(32);
        let mut body = String::new();
        for hour in 0..24u32 {
            for coin in 0..1250usize {
                body.push_str(&format!(
                    "{sha}  {}  market_data/20261022/{hour}/l2Book/C{coin:04}.lz4\n",
                    100 + coin
                ));
            }
        }
        body.push_str(&format!(
            "{sha}  9  market_data/20261022/0/l2Book/C0007.lz4\n"
        ));
        let sums = root.join("MANIFEST");
        std::fs::write(&sums, &body).unwrap();
        let error = load_entries(&sums).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate checksum for 20261022/0/C0007"),
            "{error}"
        );
        body.pop();
        body.pop();
        std::fs::write(&sums, &body).unwrap();
        let loaded = load_entries(&sums).unwrap();
        assert_eq!(loaded.len(), 30_000);
        let _ = std::fs::remove_dir_all(&root);
    }
}
