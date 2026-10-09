//! The lane's checkpoint: a small DuckDB file that registers every published Parquet file with
//! its row count and hash and keeps per-series progress, plus the `catalog.json` and
//! `status.json` snapshots derived from it. Files are durable before they are registered; a
//! rerun removes anything on disk that the ledger does not know.

use crate::fsutil::{write_atomic, write_durable};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::path::Path;

/// Schema of `checkpoint.duckdb` under the augment root.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files(
  path VARCHAR PRIMARY KEY, source VARCHAR, dataset VARCHAR, symbol VARCHAR, phoenix_symbol VARCHAR,
  period VARCHAR, complete BOOLEAN, row_count BIGINT, bytes BIGINT, sha256 VARCHAR, created_at VARCHAR);
CREATE TABLE IF NOT EXISTS progress(key VARCHAR PRIMARY KEY, value JSON);
CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);
";

/// One published file.
#[derive(Clone, Debug)]
pub struct FileRecord {
    /// Root-relative path (`tables/<source>/<dataset>/<symbol>/<period>.parquet`).
    pub path: String,
    /// Source name.
    pub source: String,
    /// Dataset name.
    pub dataset: String,
    /// The venue's symbol.
    pub symbol: String,
    /// Phoenix symbol, or `None` for series that are not a Phoenix market.
    pub phoenix_symbol: Option<String>,
    /// Period label.
    pub period: String,
    /// Whether the period is closed (the file will not be rewritten).
    pub complete: bool,
    /// Rows in the file.
    pub row_count: i64,
    /// Size on disk.
    pub bytes: i64,
    /// SHA-256 of the file.
    pub sha256: String,
}

/// Open (creating) the ledger under `root`.
pub fn open(root: &Path) -> Result<Store, StoreError> {
    Store::open(root, SCHEMA)
}

/// Register a file and, when given, advance a progress record, in one transaction.
pub fn register(
    store: &mut Store,
    file: &FileRecord,
    progress: Option<(&str, &Value)>,
) -> Result<(), StoreError> {
    store.transaction(|store| {
        store.exec(
            "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                &file.path,
                &file.source,
                &file.dataset,
                &file.symbol,
                &file.phoenix_symbol,
                &file.period,
                &file.complete,
                &file.row_count,
                &file.bytes,
                &file.sha256,
                &now(),
            ],
        )?;
        if let Some((key, value)) = progress {
            set_progress(store, key, value)?;
        }
        Ok(())
    })
}

/// Upsert a progress record.
pub fn set_progress(store: &mut Store, key: &str, value: &Value) -> Result<(), StoreError> {
    let text = serde_json::to_string(value).map_err(|e| StoreError::Check(e.to_string()))?;
    store.exec(
        "INSERT OR REPLACE INTO progress VALUES (?, ?::JSON)",
        &[&key, &text],
    )?;
    Ok(())
}

/// Read a progress record.
pub fn get_progress(store: &Store, key: &str) -> Result<Option<Value>, StoreError> {
    let rows = store.rows(
        "SELECT value::VARCHAR AS value FROM progress WHERE key=?",
        &[&key],
    )?;
    match rows.first().and_then(|row| row.str("value")) {
        Some(text) => Ok(Some(
            serde_json::from_str(text).map_err(|e| StoreError::Check(e.to_string()))?,
        )),
        None => Ok(None),
    }
}

/// Period labels already registered for a series, complete ones only.
pub fn complete_periods(
    store: &Store,
    source: &str,
    dataset: &str,
    symbol: &str,
) -> Result<Vec<String>, StoreError> {
    let rows = store.rows(
        "SELECT period FROM files WHERE source=? AND dataset=? AND symbol=? AND complete ORDER BY period",
        &[&source, &dataset, &symbol],
    )?;
    Ok(rows
        .iter()
        .filter_map(|row| row.str("period").map(str::to_owned))
        .collect())
}

/// `catalog.json`: every registered file, ordered, durably written.
pub fn write_catalog(store: &mut Store) -> Result<(), StoreError> {
    let files = store.rows(
        "SELECT path, source, dataset, symbol, phoenix_symbol, period, complete, row_count, bytes, sha256, created_at
         FROM files ORDER BY source, dataset, symbol, period",
        &[],
    )?;
    let catalog = Obj::new()
        .with("at", now())
        .with("schemaVersion", 1)
        .with_rows("files", files);
    write_durable(
        &store.root.join("catalog.json"),
        format!("{}\n", catalog.to_json()).as_bytes(),
    )?;
    Ok(())
}

/// Per source and dataset: files, rows, bytes, newest period.
pub fn summary(store: &Store) -> Result<Vec<Obj>, StoreError> {
    store.rows(
        "SELECT source, dataset, count(*) AS files, count(DISTINCT symbol) AS symbols,
                sum(row_count) AS rows, sum(bytes) AS bytes, min(period) AS oldest, max(period) AS newest
         FROM files GROUP BY source, dataset ORDER BY source, dataset",
        &[],
    )
}

/// `status.json`, written without fsync as the other lanes' snapshots are.
pub fn write_status(root: &Path, status: &Obj) -> Result<(), StoreError> {
    write_atomic(
        &root.join("status.json"),
        format!("{}\n", status.to_json()).as_bytes(),
    )?;
    Ok(())
}

/// Remove `.tmp` files, the staging directory's contents and unregistered Parquet under
/// `tables/`, then rewrite the catalog. Returns the number of files removed.
pub fn recover(store: &mut Store) -> Result<u64, StoreError> {
    let registered: std::collections::HashSet<std::path::PathBuf> = store
        .rows("SELECT path FROM files", &[])?
        .iter()
        .filter_map(|row| row.str("path"))
        .map(|p| store.root.join(p))
        .collect();
    let mut removed = 0u64;
    let staging = store.root.join("staging");
    if staging.is_dir() {
        for entry in std::fs::read_dir(&staging)? {
            let path = entry?.path();
            if path.is_file() {
                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }
    }
    std::fs::create_dir_all(&staging)?;
    let tables = store.root.join("tables");
    if tables.is_dir() {
        removed += walk(&tables, &registered)?;
    }
    write_catalog(store)?;
    Ok(removed)
}

fn walk(
    dir: &Path,
    registered: &std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<u64> {
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            removed += walk(&path, registered)?;
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".tmp") || (name.ends_with(".parquet") && !registered.contains(&path)) {
            std::fs::remove_file(&path)?;
            removed += 1;
        }
    }
    Ok(removed)
}
