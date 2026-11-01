//! Restart predicate for the collector checkpoint (ADR-0016).
//!
//! Every active local partition at or below the tail watermark must exist, parse as Parquet, and
//! match the `files` row count and SHA-256. An `r2` file has no local bytes and is not checked.
//! `W` and the file rows commit in one transaction, so an active
//! file whose `file_bounds.slot_to` sits strictly above `W` is a violation. Parquet the `files`
//! table does not list is an orphan: startup `recover_files` deletes those, and this check does
//! not read them.

use crate::fsutil::{file_hash, manifest_path};
use crate::jsonout::safe_error;
use crate::store::{Store, StoreError, query_rows, sql_string};
use std::path::PathBuf;

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
    /// An active file is registered past the tail watermark. That cannot commit legitimately:
    /// `W` and the file rows are one transaction.
    #[error(
        "registered partition above the watermark: {path}: slot_to {slot_to}, watermark {watermark}"
    )]
    AboveWatermark {
        /// Manifest path.
        path: PathBuf,
        /// `file_bounds.slot_to`.
        slot_to: i64,
        /// Tail watermark slot.
        watermark: i64,
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
/// `watermark == None` means `W` has not been recorded, so every active file is covered. A row
/// with no `file_bounds` is covered. `slot_to` equal to the watermark is covered. `slot_to`
/// greater than the watermark is [`RestartError::AboveWatermark`].
pub fn covered_partitions(
    store: &Store,
    watermark: Option<i64>,
) -> Result<Vec<CoveredPartition>, RestartError> {
    let rows = store.rows(
        "SELECT f.path, f.row_count, f.sha256, f.location, b.slot_to
         FROM files f LEFT JOIN file_bounds b ON b.path = f.path
         WHERE f.status = 'active'
         ORDER BY f.path",
        &[],
    )?;
    let mut covered = Vec::with_capacity(rows.len());
    for row in rows {
        let stored = row.str("path").unwrap_or("");
        let path = manifest_path(&store.root, std::path::Path::new(stored));
        if let (Some(mark), Some(slot_to)) = (watermark, row.int("slot_to"))
            && slot_to > mark
        {
            return Err(RestartError::AboveWatermark {
                path,
                slot_to,
                watermark: mark,
            });
        }
        // r2-only bytes are not on disk. Treating them as missing would refuse every restart
        // after `--drop-local`. `local` and `both` still have to be present.
        if row.str("location") == Some("r2") {
            continue;
        }
        let row_count = row.int("row_count").unwrap_or(0);
        covered.push(CoveredPartition {
            path,
            row_count: u64::try_from(row_count).unwrap_or(0),
            sha256: row.str("sha256").unwrap_or("").to_owned(),
        });
    }
    Ok(covered)
}

/// Check one covered list against the filesystem. The first failure is returned.
///
/// The hash is compared before the file is parsed, so a same-size bit flip is
/// [`RestartError::Hash`] even when the parquet checksum would also reject the file. `conn` parses
/// Parquet (`read_parquet`) only after the hash matches. Unregistered files are not in `covered`.
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
    }
    Ok(())
}

/// [`covered_partitions`] then [`check_covered`] on this store's connection.
pub fn restart_predicate(store: &Store, watermark: Option<i64>) -> Result<(), RestartError> {
    let covered = covered_partitions(store, watermark)?;
    check_covered(store.conn(), &covered)
}
