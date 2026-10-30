//! DuckDB storage compatibility (issue #58): a checkpoint (and its write-ahead log) written at
//! the oldest storage compatibility version the current DuckDB emits must open and migrate
//! cleanly, and a file that is not a valid database must fail with the typed DuckDB error, never
//! silently.
//!
//! Honesty note: the first test writes with the *current* DuckDB binary set to its oldest
//! supported `storage_compatibility_version`; it does not read a file produced by a genuinely
//! older DuckDB binary. It pins the reopen/migrate path and WAL replay, not cross-binary storage
//! migration.

use serde_json::json;
use solos_data::store::{Store, StoreError};
use std::path::PathBuf;

const DDL: &str = "CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);";

fn tempdir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("solos-{label}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The oldest storage version the current DuckDB binary can still emit, standing in for a
/// checkpoint written by an older DuckDB binary (ADR-0007 wrote it from the Node writer's
/// DuckDB 1.4.4). The file is still written by the current binary.
const OLDER_STORAGE_VERSION: &str = "v1.4.4";

#[test]
fn a_checkpoint_written_at_the_oldest_supported_storage_version_reopens_cleanly() {
    let root = tempdir("duckdb-old");
    let path = root.join("checkpoint.duckdb");
    {
        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "SET storage_compatibility_version='{OLDER_STORAGE_VERSION}'"
        ))
        .unwrap();
        // Leave the write in the write-ahead log so reopening must replay it.
        conn.execute_batch("PRAGMA disable_checkpoint_on_shutdown")
            .unwrap();
        conn.execute_batch("CREATE TABLE kv(name VARCHAR PRIMARY KEY, value JSON)")
            .unwrap();
        conn.execute_batch("INSERT INTO kv VALUES ('k', '{\"a\":1}'::JSON)")
            .unwrap();
    }
    assert!(
        root.join("checkpoint.duckdb.wal").exists(),
        "the older-version write left a WAL to replay"
    );
    let store = Store::open(&root, DDL).unwrap();
    assert_eq!(store.get("k").unwrap(), Some(json!({"a": 1})));
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_file_that_is_not_a_duckdb_database_is_a_typed_error_not_a_silent_open() {
    let root = tempdir("duckdb-invalid");
    std::fs::write(root.join("checkpoint.duckdb"), b"not a duckdb database").unwrap();
    match Store::open(&root, DDL) {
        Err(StoreError::Db(error)) => {
            assert!(
                error.to_string().contains("not a valid DuckDB database"),
                "the typed error names the problem: {error}"
            );
        }
        Err(other) => panic!("expected StoreError::Db, got {other}"),
        Ok(_) => panic!("a corrupt checkpoint must not open silently"),
    }
    std::fs::remove_dir_all(root).unwrap();
}
