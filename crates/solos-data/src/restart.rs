//! Restart predicate for the collector checkpoint (ADR-0016).
//!
//! Every active partition at or below the tail watermark must exist, parse as Parquet, and match
//! the `files` row count and SHA-256. A partition whose `file_bounds.slot_to` sits strictly above
//! the watermark is an orphan: this check does not read it. Parquet the `files` table does not
//! list is also an orphan; startup `recover_files` deletes those, and this check does not trust
//! them either.

use crate::fsutil::{file_hash, resolve};
use crate::jsonout::safe_error;
use crate::store::{Store, StoreError, query_rows, sql_string};
use std::path::{Path, PathBuf};

/// One partition the restart predicate trusts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoveredPartition {
    /// Absolute path of the Parquet file.
    pub path: PathBuf,
    /// Manifest `files.row_count`.
    pub row_count: u64,
    /// Manifest `files.sha256`, lowercase hex.
    pub sha256: String,
}

/// A covered partition failed the restart predicate.
#[derive(Debug, thiserror::Error)]
pub enum RestartError {
    /// Reading the manifest failed.
    #[error("{0}")]
    Store(#[from] StoreError),
    /// The file is not on disk.
    #[error("covered partition missing: {path}")]
    Missing {
        /// Manifest path.
        path: PathBuf,
    },
    /// DuckDB could not read the file as Parquet.
    #[error("covered partition does not parse: {path}: {message}")]
    Parse {
        /// Manifest path.
        path: PathBuf,
        /// DuckDB or filesystem message, URL-redacted and truncated.
        message: String,
    },
    /// The file parsed, and its row count is not the manifest's.
    #[error("covered partition row count: {path}: manifest {expected}, file {actual}")]
    RowCount {
        /// Manifest path.
        path: PathBuf,
        /// `files.row_count`.
        expected: u64,
        /// `count(*)` from `read_parquet`.
        actual: u64,
    },
    /// The file parsed with the right row count, and its hash is not the manifest's.
    #[error("covered partition hash: {path}: manifest {expected}, file {actual}")]
    Hash {
        /// Manifest path.
        path: PathBuf,
        /// `files.sha256`.
        expected: String,
        /// SHA-256 of the bytes on disk.
        actual: String,
    },
}

/// The tail watermark slot (`kv` key `W`, field `slot`), when that record exists.
pub fn tail_watermark(store: &Store) -> Result<Option<i64>, StoreError> {
    Ok(store
        .get("W")?
        .and_then(|value| value.get("slot").and_then(serde_json::Value::as_i64)))
}

/// Active `files` rows at or below `watermark`.
///
/// `watermark == None` means `W` has not been recorded: every active file is covered, because
/// nothing places it above a watermark. A row with no `file_bounds` is covered for the same
/// reason. `slot_to` equal to the watermark is covered. `slot_to` greater than the watermark is
/// omitted and not read.
pub fn covered_partitions(
    store: &Store,
    watermark: Option<i64>,
) -> Result<Vec<CoveredPartition>, StoreError> {
    let sql = match watermark {
        Some(_) => {
            "SELECT f.path, f.row_count, f.sha256, b.slot_to
             FROM files f LEFT JOIN file_bounds b ON b.path = f.path
             WHERE f.status = 'active' AND (b.slot_to IS NULL OR b.slot_to <= ?)
             ORDER BY f.path"
        }
        None => {
            "SELECT f.path, f.row_count, f.sha256
             FROM files f WHERE f.status = 'active' ORDER BY f.path"
        }
    };
    let rows = match watermark {
        Some(slot) => store.rows(sql, &[&slot])?,
        None => store.rows(sql, &[])?,
    };
    let mut covered = Vec::with_capacity(rows.len());
    for row in rows {
        let stored = row.str("path").unwrap_or("");
        let row_count = row.int("row_count").unwrap_or(0);
        covered.push(CoveredPartition {
            path: resolve(&store.root, Path::new(stored)),
            row_count: u64::try_from(row_count).unwrap_or(0),
            sha256: row.str("sha256").unwrap_or("").to_owned(),
        });
    }
    Ok(covered)
}

/// Check one covered list against the filesystem. The first failure is returned.
///
/// `conn` is only used to parse Parquet (`read_parquet`). Orphans are not in `covered` and are
/// not opened.
pub fn check_covered(
    conn: &duckdb::Connection,
    covered: &[CoveredPartition],
) -> Result<(), RestartError> {
    for partition in covered {
        if !partition.path.is_file() {
            return Err(RestartError::Missing {
                path: partition.path.clone(),
            });
        }
        let sql = format!(
            "SELECT count(*) AS n FROM read_parquet({})",
            sql_string(&partition.path.to_string_lossy())
        );
        let actual = match query_rows(conn, &sql, &[]) {
            Ok(rows) => rows.first().and_then(|row| row.int("n")).unwrap_or(0),
            Err(error) => {
                return Err(RestartError::Parse {
                    path: partition.path.clone(),
                    message: safe_error(&error.to_string()),
                });
            }
        };
        let actual = u64::try_from(actual).unwrap_or(u64::MAX);
        if actual != partition.row_count {
            return Err(RestartError::RowCount {
                path: partition.path.clone(),
                expected: partition.row_count,
                actual,
            });
        }
        let hash = file_hash(&partition.path).map_err(|error| RestartError::Parse {
            path: partition.path.clone(),
            message: safe_error(&error.to_string()),
        })?;
        if hash != partition.sha256 {
            return Err(RestartError::Hash {
                path: partition.path.clone(),
                expected: partition.sha256.clone(),
                actual: hash,
            });
        }
    }
    Ok(())
}

/// [`covered_partitions`] then [`check_covered`] on this store's connection.
pub fn restart_predicate(store: &Store, watermark: Option<i64>) -> Result<(), RestartError> {
    let covered = covered_partitions(store, watermark)?;
    check_covered(store.conn(), &covered)
}
