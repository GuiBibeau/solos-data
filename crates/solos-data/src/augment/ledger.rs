//! The lanes' checkpoints: a small DuckDB file per lane that registers every published Parquet
//! file with its row count and hash and keeps per-series progress, plus the `catalog` and
//! `status` snapshots derived from it. Files are durable before they are registered; a rerun
//! removes anything in the lane's dataset directories that the ledger does not know. The sync
//! and capture lanes run as separate processes, so each owns its ledger, staging directory and
//! snapshots; they share the `tables/` tree through distinct datasets.

use crate::fsutil::{write_atomic, write_durable};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Schema of a lane's checkpoint.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files(
  path VARCHAR PRIMARY KEY, source VARCHAR, dataset VARCHAR, symbol VARCHAR, phoenix_symbol VARCHAR,
  period VARCHAR, complete BOOLEAN, row_count BIGINT, bytes BIGINT, sha256 VARCHAR, created_at VARCHAR);
CREATE TABLE IF NOT EXISTS progress(key VARCHAR PRIMARY KEY, value JSON);
CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);
";

/// Which process owns the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// `augment sync`: `checkpoint.duckdb`, `catalog.json`, `status.json`, `staging/`.
    Sync,
    /// `augment capture`: `capture.duckdb`, `catalog-capture.json`, `status-capture.json`,
    /// `staging-capture/`.
    Capture,
}

impl Lane {
    /// Both lanes, sync first.
    pub const ALL: [Lane; 2] = [Lane::Sync, Lane::Capture];

    /// The lane's name in logs and status.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Lane::Sync => "sync",
            Lane::Capture => "capture",
        }
    }

    /// Directory the lane's checkpoint lives in (a subdirectory for the capture lane, so each
    /// `Store` keeps its own `checkpoint.duckdb`).
    #[must_use]
    pub fn ledger_dir(self, root: &Path) -> PathBuf {
        match self {
            Lane::Sync => root.to_path_buf(),
            Lane::Capture => root.join("capture"),
        }
    }

    /// `catalog.json` or `catalog-capture.json`.
    #[must_use]
    pub fn catalog_path(self, root: &Path) -> PathBuf {
        match self {
            Lane::Sync => root.join("catalog.json"),
            Lane::Capture => root.join("catalog-capture.json"),
        }
    }

    /// `status.json` or `status-capture.json`.
    #[must_use]
    pub fn status_path(self, root: &Path) -> PathBuf {
        match self {
            Lane::Sync => root.join("status.json"),
            Lane::Capture => root.join("status-capture.json"),
        }
    }

    /// The lane's staging directory.
    #[must_use]
    pub fn staging_dir(self, root: &Path) -> PathBuf {
        match self {
            Lane::Sync => root.join("staging"),
            Lane::Capture => root.join("staging-capture"),
        }
    }
}

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

/// Open (creating) a lane's ledger under `root`. The store's own root is the lane's ledger
/// directory; files are published under `root/tables/`, which callers pass explicitly.
pub fn open(root: &Path, lane: Lane) -> Result<Store, StoreError> {
    std::fs::create_dir_all(root)?;
    Store::open(&lane.ledger_dir(root), SCHEMA)
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

/// The lane's catalog: every registered file, ordered, durably written.
pub fn write_catalog(store: &mut Store, root: &Path, lane: Lane) -> Result<(), StoreError> {
    let files = store.rows(
        "SELECT path, source, dataset, symbol, phoenix_symbol, period, complete, row_count, bytes, sha256, created_at
         FROM files ORDER BY source, dataset, symbol, period",
        &[],
    )?;
    let catalog = Obj::new()
        .with("at", now())
        .with("schemaVersion", 1)
        .with("lane", lane.name())
        .with_rows("files", files);
    write_durable(
        &lane.catalog_path(root),
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

/// The lane's status snapshot, written without fsync as the other lanes' snapshots are.
pub fn write_status(root: &Path, lane: Lane, status: &Obj) -> Result<(), StoreError> {
    write_atomic(
        &lane.status_path(root),
        format!("{}\n", status.to_json()).as_bytes(),
    )?;
    Ok(())
}

/// Every lane's status that exists, keyed by lane name.
pub fn read_statuses(root: &Path) -> Obj {
    let mut out = Obj::new();
    for lane in Lane::ALL {
        if let Ok(text) = std::fs::read_to_string(lane.status_path(root))
            && let Ok(value) = serde_json::from_str::<Value>(&text)
        {
            out.set(lane.name(), value);
        }
    }
    out
}

/// Remove the lane's staging files, and `.tmp` files and unregistered Parquet in the dataset
/// directories the ledger knows, then rewrite the catalog. Returns the number of files removed.
/// Directories of datasets the ledger has never registered belong to the other lane or to a
/// first run and are left alone.
pub fn recover(store: &mut Store, root: &Path, lane: Lane) -> Result<u64, StoreError> {
    let rows = store.rows("SELECT path FROM files", &[])?;
    let registered: std::collections::HashSet<PathBuf> = rows
        .iter()
        .filter_map(|row| row.str("path"))
        .map(|p| root.join(p))
        .collect();
    let mut dirs: std::collections::BTreeSet<PathBuf> = std::collections::BTreeSet::new();
    for path in &registered {
        if let Some(dataset_dir) = path.parent().and_then(Path::parent) {
            dirs.insert(dataset_dir.to_path_buf());
        }
    }
    let mut removed = 0u64;
    let staging = lane.staging_dir(root);
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
    for dir in dirs {
        if dir.is_dir() {
            removed += walk(&dir, &registered)?;
        }
    }
    write_catalog(store, root, lane)?;
    Ok(removed)
}

fn walk(dir: &Path, registered: &std::collections::HashSet<PathBuf>) -> std::io::Result<u64> {
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
