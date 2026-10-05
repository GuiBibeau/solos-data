//! Reader-aware removal of superseded files whose verified replacement is registered. A port of
//! `garbage-collector.ts`, shared by the decoder and (later) the collector.

use crate::fsutil::{file_hash, inside, resolve};
use crate::jsonout::{Obj, now};
use crate::lease::has_readers;
use crate::store::{Store, StoreError, sql_string};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A registered replacement and a durable catalog must precede any unlink.
pub fn collect_retired(
    store: &mut Store,
    root: &Path,
    minimum_age_seconds: i64,
    publish_catalog: &dyn Fn(&mut Store) -> Result<(), StoreError>,
) -> Result<Obj, StoreError> {
    publish_catalog(store)?;
    if has_readers(root)? {
        return Ok(Obj::new()
            .with("removedFiles", 0)
            .with("removedBytes", 0)
            .with("readers", true));
    }
    let retired = store.rows("SELECT * FROM retired_files", &[])?;
    let links: HashMap<String, String> = retired
        .iter()
        .filter_map(|f| {
            Some((
                f.str("path")?.to_owned(),
                f.str("replacement_path")?.to_owned(),
            ))
        })
        .collect();
    let removed: HashSet<String> = store
        .rows("SELECT path FROM garbage_removed", &[])?
        .iter()
        .filter_map(|f| f.str("path").map(str::to_owned))
        .collect();
    let active: HashMap<String, Obj> = store
        .rows("SELECT * FROM files", &[])?
        .into_iter()
        .filter(|f| {
            f.get("status")
                .is_none_or(|s| s.is_null() || s.as_str() == Some("active"))
        })
        .filter_map(|f| Some((f.str("path")?.to_owned(), f)))
        .collect();
    let mut verified: HashSet<String> = HashSet::new();
    let (mut removed_files, mut removed_bytes) = (0u64, 0u64);
    let inside_root = |path: &str| -> Result<PathBuf, StoreError> {
        let full = resolve(root, Path::new(path));
        if !inside(root, &full) {
            return Err(StoreError::Check("retired file outside data root".into()));
        }
        Ok(full)
    };
    for file in &retired {
        let path = file.str("path").unwrap_or("").to_owned();
        if removed.contains(&path) {
            continue;
        }
        let retired_at = chrono::DateTime::parse_from_rfc3339(file.str("retired_at").unwrap_or(""))
            .map(|t| t.timestamp_millis())
            .unwrap_or(0);
        if chrono::Utc::now().timestamp_millis() - retired_at < minimum_age_seconds * 1000 {
            continue;
        }
        let mut replacement = file.str("replacement_path").unwrap_or("").to_owned();
        let mut seen: HashSet<String> = HashSet::from([path.clone()]);
        while !active.contains_key(&replacement) && links.contains_key(&replacement) {
            if seen.contains(&replacement) {
                return Err(StoreError::Check("retired replacement cycle".into()));
            }
            seen.insert(replacement.clone());
            replacement = links[&replacement].clone();
        }
        let Some(target) = active.get(&replacement) else {
            continue;
        };
        if !verified.contains(&replacement) {
            let full = inside_root(&replacement)?;
            if file_hash(&full)? != target.str("sha256").unwrap_or("") {
                return Err(StoreError::Check("replacement checksum mismatch".into()));
            }
            let count = store.rows(
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    sql_string(&full.to_string_lossy())
                ),
                &[],
            )?;
            if count.first().and_then(|r| r.int("n")) != target.int("row_count") {
                return Err(StoreError::Check("replacement row count mismatch".into()));
            }
            verified.insert(replacement.clone());
        }
        let full = inside_root(&path)?;
        let bytes = match std::fs::metadata(&full) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        if bytes == 0 {
            store.exec(
                "INSERT OR IGNORE INTO garbage_removed VALUES (?, ?)",
                &[&path, &now()],
            )?;
            continue;
        }
        std::fs::remove_file(&full)?;
        removed_files += 1;
        removed_bytes += bytes;
        store.exec(
            "INSERT OR IGNORE INTO garbage_removed VALUES (?, ?)",
            &[&path, &now()],
        )?;
        // Keep the small replacement link: descendants can still resolve through it after restart.
    }
    Ok(Obj::new()
        .with("removedFiles", removed_files)
        .with("removedBytes", removed_bytes)
        .with("readers", false))
}
