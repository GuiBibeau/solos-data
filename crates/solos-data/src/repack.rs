//! Offline checkpoint rewrite: copy the closed database into a new file, prove every table equal
//! through whole-row digest multisets in both directions, then replace atomically. A port of
//! `repack.ts`.

use crate::fsutil::sync_path;
use crate::jsonout::Obj;
use crate::store::{DbLock, StoreError, memory_connection, query_rows, query_scalar, sql_string};
use duckdb::Connection;
use std::path::Path;

/// The previous checkpoint after a rewrite, kept until the next one.
pub const PREVIOUS: &str = "checkpoint.prev.duckdb";

/// Rewrite `<root>/checkpoint.duckdb`. `lock` is the advisory lock the caller already holds
/// (CLI `repack` takes it; [`crate::store::Store::while_closed`] passes the store's lock). The
/// database connection must be closed. This function does not lock again.
pub fn repack_checkpoint(root: &Path, lock: &DbLock) -> Result<Obj, StoreError> {
    lock.prove_held();
    let source = root.join("checkpoint.duckdb");
    let candidate = root.join("checkpoint.repack.duckdb");
    let bytes_before = std::fs::metadata(&source)?.len();
    let memory = std::env::var("SOLOS_DATA_DB_MEMORY").unwrap_or_else(|_| "4GB".into());
    {
        let conn = memory_connection("4", &memory)?;
        // A read-write attachment holds DuckDB's process lock throughout verification.
        conn.execute_batch(&format!(
            "ATTACH {} AS original",
            sql_string(&source.to_string_lossy())
        ))?;
        conn.execute_batch("CHECKPOINT original")?;
        remove_if_exists(&candidate)?;
        remove_if_exists(&crate::fsutil::tmp_path(&candidate).with_extension("duckdb.wal"))?;
        let _ = std::fs::remove_file(root.join("checkpoint.repack.duckdb.wal"));
        conn.execute_batch(&format!(
            "ATTACH {} AS packed",
            sql_string(&candidate.to_string_lossy())
        ))?;
        conn.execute_batch("COPY FROM DATABASE original TO packed")?;
        let tables = query_rows(
            &conn,
            "SELECT table_name FROM duckdb_tables() WHERE database_name='original' AND NOT temporary",
            &[],
        )?;
        for table in tables {
            let name = table.str("table_name").unwrap_or("").replace('"', "\"\"");
            let mismatch = digest_mismatch(
                &conn,
                &format!("SELECT * FROM original.\"{name}\""),
                &format!("SELECT * FROM packed.\"{name}\""),
            )?;
            if mismatch != 0 {
                return Err(StoreError::Check(
                    "repacked checkpoint data mismatch".into(),
                ));
            }
        }
        conn.execute_batch("CHECKPOINT packed")?;
        conn.close().map_err(|(_, e)| e)?;
    }
    let bytes_after = swap_in(root, &candidate)?;
    Ok(Obj::new()
        .with("ok", true)
        .with("bytesBefore", bytes_before)
        .with("bytesAfter", bytes_after)
        .with(
            "reclaimedBytes",
            i64::try_from(bytes_before).unwrap_or(i64::MAX)
                - i64::try_from(bytes_after).unwrap_or(i64::MAX),
        ))
}

/// Rows of `left` missing from `right` plus rows of `right` missing from `left`, as whole-row
/// digest multisets (`sha256(to_json(row))`); 0 means equal.
pub fn digest_mismatch(conn: &Connection, left: &str, right: &str) -> Result<i64, StoreError> {
    let mismatch = query_scalar(
        conn,
        &format!(
            "SELECT count(*) AS n FROM (
        (SELECT sha256(to_json(t)) FROM ({left}) t EXCEPT ALL SELECT sha256(to_json(t)) FROM ({right}) t) UNION ALL
        (SELECT sha256(to_json(t)) FROM ({right}) t EXCEPT ALL SELECT sha256(to_json(t)) FROM ({left}) t))"
        ),
        &[],
    )?;
    Ok(mismatch
        .as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| mismatch.as_i64())
        .unwrap_or(1))
}

/// Replace `<root>/checkpoint.duckdb` with the closed, checkpointed `candidate`; the original stays
/// reachable as [`PREVIOUS`] (a hard link, no copy) until the next rewrite replaces it or the
/// operator removes it. Refuses while the original has a write-ahead log. Returns the new size.
pub fn swap_in(root: &Path, candidate: &Path) -> Result<u64, StoreError> {
    let source = root.join("checkpoint.duckdb");
    let wal = root.join("checkpoint.duckdb.wal");
    if std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0) > 0 {
        return Err(StoreError::Check(
            "checkpoint WAL remains; refusing replacement".into(),
        ));
    }
    sync_path(candidate)?;
    let bytes_after = std::fs::metadata(candidate)?.len();
    let previous = root.join(PREVIOUS);
    remove_if_exists(&previous)?;
    std::fs::hard_link(&source, &previous)?;
    std::fs::rename(candidate, &source)?;
    sync_path(root)?;
    Ok(bytes_after)
}

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
