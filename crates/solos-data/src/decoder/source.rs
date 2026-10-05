//! Which raw file the decoder reads next and how it reads a bounded batch of it. A port of
//! `decode/source.ts`: newest transaction range first, legacy absolute paths rebased, payloads
//! read by physical row number so a deep resume never sorts wide rows.

use super::normalize::RawTransaction;
use crate::catalog::{CatalogFile, read_catalog};
use crate::fsutil;
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Legacy raw catalogs use absolute paths. Rebase only their known staging tree.
pub fn source_path(root: &Path, path: &str) -> Result<PathBuf, StoreError> {
    let marker = ["/staging/", "/transactions/"]
        .into_iter()
        .find(|marker| path.contains(marker));
    let suffix = match marker {
        Some(marker) if Path::new(path).is_absolute() => {
            &path[path.find(marker).unwrap_or(0) + 1..]
        }
        _ => path,
    };
    let result = fsutil::resolve(root, Path::new(suffix));
    if !fsutil::inside(root, &result) {
        return Err(StoreError::Check("source path outside raw root".into()));
    }
    Ok(result)
}

/// The next source to consume: file, row offset, and the catalog instant.
pub struct NextSource {
    /// The raw file.
    pub file: CatalogFile,
    /// Rows already consumed.
    pub offset: u64,
    /// `catalog.at` of the raw root.
    pub catalog_at: String,
}

/// Pick the unconsumed raw file with the newest transaction range.
pub fn next_source(store: &mut Store, raw_root: &Path) -> Result<Option<NextSource>, StoreError> {
    let catalog = read_catalog(raw_root).map_err(StoreError::Check)?;
    let mut progress: HashMap<String, u64> = HashMap::new();
    for row in store.rows("SELECT * FROM sources", &[])? {
        if let (Some(hash), Some(offset)) = (row.str("source_hash"), row.int("row_offset")) {
            progress.insert(hash.to_owned(), offset.max(0) as u64);
        }
    }
    let mut candidates: Vec<(CatalogFile, i64)> = Vec::new();
    let unconsumed: Vec<CatalogFile> = catalog
        .files
        .iter()
        .filter(|f| {
            f.table_name == "transactions"
                && progress.get(&f.sha256).copied().unwrap_or(0) < f.row_count
        })
        .cloned()
        .collect();
    for file in &unconsumed {
        // A merge adds no new payload when all immutable input sources were consumed.
        if !file.parents.is_empty()
            && file
                .parents
                .iter()
                .all(|p| progress.get(&p.sha256).copied().unwrap_or(0) >= p.row_count)
        {
            let count = i64::try_from(file.row_count).unwrap_or(i64::MAX);
            store.exec(
                "INSERT OR REPLACE INTO sources VALUES (?, ?, ?)",
                &[&file.sha256, &file.path, &count],
            )?;
            progress.insert(file.sha256.clone(), file.row_count);
            continue;
        }
        let slot = source_priority(store, raw_root, file)?;
        candidates.push((file.clone(), slot));
    }
    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.0.created_at.cmp(&a.0.created_at))
            .then_with(|| b.0.path.cmp(&a.0.path))
    });
    Ok(candidates.into_iter().next().map(|(file, _)| {
        let offset = progress.get(&file.sha256).copied().unwrap_or(0);
        NextSource {
            file,
            offset,
            catalog_at: catalog.at.clone(),
        }
    }))
}

fn source_priority(store: &mut Store, root: &Path, file: &CatalogFile) -> Result<i64, StoreError> {
    if let Some(slot) = range_end(&file.path) {
        return Ok(slot);
    }
    // Older compacted files have no range in their name. Cache an immutable source's actual
    // newest slot so newly written history cannot displace live data.
    let key = format!("source-priority/{}", file.sha256);
    if let Some(Value::Number(n)) = store.get(&key)? {
        return Ok(n.as_i64().unwrap_or(0));
    }
    let path = source_path(root, &file.path)?;
    let newest = store.rows(
        &format!(
            "SELECT max(slot) AS newest FROM read_parquet({})",
            sql_string(&path.to_string_lossy())
        ),
        &[],
    )?;
    let slot = newest
        .first()
        .and_then(|row| row.int("newest"))
        .unwrap_or(0);
    store.set(&key, &Value::from(slot))?;
    Ok(slot)
}

/// `<from>-<to>-<name>.parquet` → `to`.
fn range_end(path: &str) -> Option<i64> {
    let name = path.rsplit('/').next()?;
    let stem = name.strip_suffix(".parquet")?;
    let mut parts = stem.splitn(3, '-');
    let from = parts.next()?.parse::<i64>().ok()?;
    let to = parts.next()?.parse::<i64>().ok()?;
    parts.next()?;
    let _ = from;
    Some(to)
}

/// A raw row plus what the checkpoint already knows about its signature.
pub struct SourceRow {
    /// The transaction.
    pub tx: RawTransaction,
    /// Previously published content hash, if any.
    pub previous_hash: Option<String>,
    /// Publication instant of that revision, if any.
    pub previous_at: Option<String>,
}

/// Read `limit` rows after `offset` in descending (slot, signature) order, verifying the file
/// hash once per process.
pub fn source_rows(
    store: &mut Store,
    raw_root: &Path,
    file: &CatalogFile,
    offset: u64,
    limit: u64,
    verified: &mut HashSet<String>,
) -> Result<Vec<SourceRow>, StoreError> {
    let path = source_path(raw_root, &file.path)?;
    if !verified.contains(&file.sha256) {
        if fsutil::file_hash(&path)? != file.sha256 {
            return Err(StoreError::Check("raw source checksum mismatch".into()));
        }
        verified.insert(file.sha256.clone());
    }
    let quoted = sql_string(&path.to_string_lossy());
    // Keep legacy logical OFFSET ordering, but never sort wire/meta payloads for the entire file.
    let keys = store.rows(
        &format!(
            "SELECT file_row_number FROM (
    SELECT file_row_number, row_number() OVER (ORDER BY slot DESC, signature DESC) AS ordinal
    FROM read_parquet({quoted}, file_row_number=true))
    WHERE ordinal>? AND ordinal<=?"
        ),
        &[
            &i64::try_from(offset).unwrap_or(i64::MAX),
            &i64::try_from(offset + limit).unwrap_or(i64::MAX),
        ],
    )?;
    if keys.is_empty() {
        if offset < file.row_count {
            return Err(StoreError::Check("raw source row count mismatch".into()));
        }
        return Ok(Vec::new());
    }
    let numbers: Vec<String> = keys
        .iter()
        .filter_map(|row| row.int("file_row_number"))
        .map(|n| n.to_string())
        .collect();
    let raw = store.rows(
        &format!(
            "SELECT * EXCLUDE(file_row_number)
    FROM read_parquet({quoted}, file_row_number=true)
    WHERE file_row_number IN ({})
    ORDER BY slot DESC, signature DESC",
            numbers.join(",")
        ),
        &[],
    )?;
    // Point lookups avoid building a hash table for all previously decoded signatures.
    let signatures: Vec<String> = raw
        .iter()
        .filter_map(|row| row.str("signature"))
        .map(sql_string)
        .collect();
    let mut previous: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
    if !signatures.is_empty() {
        for row in store.rows(
            &format!(
                "SELECT * FROM processed WHERE signature IN ({})",
                signatures.join(",")
            ),
            &[],
        )? {
            if let Some(signature) = row.str("signature") {
                previous.insert(
                    signature.to_owned(),
                    (
                        row.str("source_hash").map(str::to_owned),
                        row.str("publication_at").map(str::to_owned),
                    ),
                );
            }
        }
    }
    let rows: Vec<SourceRow> = raw
        .into_iter()
        .map(|row| {
            let signature = row.str("signature").unwrap_or("").to_owned();
            let (previous_hash, previous_at) =
                previous.get(&signature).cloned().unwrap_or((None, None));
            SourceRow {
                tx: RawTransaction {
                    signature,
                    slot: row.get("slot").cloned().unwrap_or(Value::Null),
                    block_time: row.get("block_time").cloned().unwrap_or(Value::Null),
                    tx_index: row.get("tx_index").cloned().unwrap_or(Value::Null),
                    single_in_slot: row.get("single_in_slot").cloned().unwrap_or(Value::Null),
                    tx_b64: row.str("tx_b64").map(str::to_owned),
                    meta_json: row.str("meta_json").map(str::to_owned),
                    terminal_error: row.str("terminal_error").map(str::to_owned),
                    err: row.get("err").cloned().unwrap_or(Value::Null),
                },
                previous_hash,
                previous_at,
            }
        })
        .collect();
    if rows.is_empty() && offset < file.row_count {
        return Err(StoreError::Check("raw source row count mismatch".into()));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebases_legacy_absolute_paths_only_inside_known_trees() {
        let root = Path::new("/data/raw");
        assert_eq!(
            source_path(root, "/old/root/staging/tx.parquet").unwrap(),
            PathBuf::from("/data/raw/staging/tx.parquet")
        );
        assert_eq!(
            source_path(root, "staging/a.parquet").unwrap(),
            PathBuf::from("/data/raw/staging/a.parquet")
        );
        assert!(source_path(root, "/elsewhere/file.parquet").is_err());
        assert!(source_path(root, "../escape.parquet").is_err());
        assert_eq!(
            range_end("staging/transactions/epoch=1/100-150-abc.parquet"),
            Some(150)
        );
        assert_eq!(range_end("tables/x/compact-abc.parquet"), None);
    }
}
