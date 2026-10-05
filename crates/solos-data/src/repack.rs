//! Offline checkpoint rewrite: copy the closed database into a new file, prove every table equal
//! through whole-row digest multisets in both directions, then replace atomically. A port of
//! `repack.ts`.

use crate::fsutil::sync_path;
use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, query_rows, query_scalar, sql_string};
use std::path::Path;

/// Rewrite `<root>/checkpoint.duckdb`. The writer must be closed.
pub fn repack_checkpoint(root: &Path) -> Result<Obj, StoreError> {
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
            let mismatch = query_scalar(
                &conn,
                &format!(
                    "SELECT count(*) AS n FROM (
        (SELECT sha256(to_json(t)) FROM original.\"{name}\" t EXCEPT ALL SELECT sha256(to_json(t)) FROM packed.\"{name}\" t) UNION ALL
        (SELECT sha256(to_json(t)) FROM packed.\"{name}\" t EXCEPT ALL SELECT sha256(to_json(t)) FROM original.\"{name}\" t))"
                ),
                &[],
            )?;
            if mismatch
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| mismatch.as_i64())
                .unwrap_or(1)
                != 0
            {
                return Err(StoreError::Check(
                    "repacked checkpoint data mismatch".into(),
                ));
            }
        }
        conn.execute_batch("CHECKPOINT packed")?;
        conn.close().map_err(|(_, e)| e)?;
    }
    let wal = root.join("checkpoint.duckdb.wal");
    if std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0) > 0 {
        return Err(StoreError::Check(
            "checkpoint WAL remains; refusing replacement".into(),
        ));
    }
    sync_path(&candidate)?;
    let bytes_after = std::fs::metadata(&candidate)?.len();
    std::fs::rename(&candidate, &source)?;
    sync_path(root)?;
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

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
