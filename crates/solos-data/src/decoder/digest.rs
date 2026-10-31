//! Canonical content digest of a decoded root.
//!
//! `files.sha256` stays the raw file hash and is what detects bit-rot. This digest is the
//! equivalence check: two runs of the same rows produce it, including across a DuckDB build
//! change. It hashes the schema (column name, type, and order) and the reader rows in a total
//! order. Each field is tagged and length-prefixed. `decoded_at` and `source_file` keep their
//! schema slots and contribute omitted tags, not the clock or the raw-file byte hash. Footer
//! `created_by` is not read.
//!
//! All six tables are read under one lease, from one catalog snapshot. The SQL keeps the
//! canonical `sort_columns` order and adds window functions: `count(*) OVER (PARTITION BY key,
//! batch_id)` refuses a repeated primary key in one batch, and `row_number() OVER (PARTITION BY
//! key ORDER BY batch_id DESC)` keeps only the newest revision. Rows stream through duckdb-rs
//! 1.4.5's streaming iterator (one DuckDB chunk at a time, never the day as a materialized arrow
//! result); DuckDB's sort/window buffer is bounded by `memory_limit` and spills to
//! `temp_directory` (measured by `digest_rss`).

use super::canonical::{put_row, put_schema};
use super::keys::{assert_registered_keys_unique, reject};
use super::reader::{DecodedSession, with_decoded_session};
use super::schema::{TABLES, column_pairs, key_column, sort_columns};
use crate::jsonout::Obj;
use crate::store::StoreError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// Stream one table's rows in canonical order (`sort_columns`, `batch_id DESC`) and pass the
/// newest revision of each primary key to `on_new`. The newest revision and the same-batch
/// duplicate refusal are both computed in SQL (`key_rank` and `key_copies` below), not by
/// comparing one row with the previous one: the sort is by slot, not by key, so two copies of
/// one key are not adjacent. A NULL key is refused. Returns how many rows were passed (one per
/// distinct key).
fn stream_newest(
    session: &DecodedSession,
    sql: &str,
    key: &str,
    table: &str,
    mut on_new: impl FnMut(&Obj) -> Result<(), StoreError>,
) -> Result<u64, StoreError> {
    let mut count = 0u64;
    session.stream(sql, |row| {
        let value = match row.str(key) {
            Some(value) => value.to_owned(),
            None => return Err(StoreError::Check(format!("NULL {key} in {table}"))),
        };
        let copies = row.int("key_copies").ok_or_else(|| {
            StoreError::Check(format!("digest stream lost key_copies for {table}"))
        })?;
        if copies > 1 {
            // A repeated key in one batch is not a total order; the digest refuses it.
            return Err(reject(table, key, &value));
        }
        let rank = row
            .int("key_rank")
            .ok_or_else(|| StoreError::Check(format!("digest stream lost key_rank for {table}")))?;
        if rank == 1 {
            on_new(row)?;
            count += 1;
        }
        Ok(())
    })?;
    Ok(count)
}

/// SHA-256, hex, of the schema and decoded rows under `root`.
///
/// # Errors
///
/// The catalog or a Parquet file cannot be read, a primary key is repeated in one batch, the
/// DuckDB build is not 1.4.5, or a value cannot be encoded.
pub fn content_digest(root: &Path) -> Result<String, StoreError> {
    with_decoded_session(root, None, true, |session| {
        // The full registered-file duplicate check, under the read lease
        // `with_decoded_session` holds, before the reader views narrow child tables to rows
        // whose parent is the newest `decoded_transactions` revision. The session's
        // registrations come from the one catalog read taken inside that lease (issue #66).
        assert_registered_keys_unique(session.conn())?;
        let mut hasher = Sha256::new();
        hasher.update(b"SDD2");
        for table in TABLES {
            let columns = column_pairs(table);
            let key = key_column(table);
            let mut record = Vec::new();
            put_schema(&mut record, table, &columns).map_err(StoreError::Check)?;
            let projection = columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            // The window functions run over the inner block, which is ordered by the partition
            // key so DuckDB streams the window instead of building a per-key hash table. The
            // outer block re-establishes the canonical order the digest hashes. Both sorts spill
            // to `temp_directory` under `memory_limit`.
            let sql = format!(
                "SELECT * FROM ( \
                   SELECT {projection}, batch_id, \
                     count(*) OVER (PARTITION BY {key}, batch_id) AS key_copies, \
                     row_number() OVER (PARTITION BY {key} ORDER BY batch_id DESC) AS key_rank \
                   FROM {table}_raw \
                   ORDER BY {key}, batch_id DESC \
                 ) AS windowed \
                 ORDER BY {}, batch_id DESC",
                sort_columns(table)
            );
            // First pass counts the distinct keys (the rows the digest hashes) and refuses a
            // duplicate; the count must precede the rows in the record, and a cursor cannot
            // rewind, so the rows are streamed twice. Both passes use the same windowed SQL,
            // so the count and the hashed rows agree by construction.
            let count = stream_newest(session, &sql, key, table, |_| Ok(()))?;
            record.extend_from_slice(&count.to_be_bytes());
            hasher.update(&record);
            if count == 0 {
                continue;
            }
            stream_newest(session, &sql, key, table, |row| {
                let mut encoded = Vec::new();
                put_row(&mut encoded, &columns, row).map_err(StoreError::Check)?;
                hasher.update(&encoded);
                Ok(())
            })?;
        }
        Ok(hex::encode(hasher.finalize()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sql_string;

    fn transactions(path: &std::path::Path, rows: &[(&str, i64)], source_file: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let values = rows
            .iter()
            .map(|(signature, slot)| {
                format!(
                    "('{signature}', 'h', {slot}, 0, false, 1, true, 'decoded', 0, 0, '{source_file}', '2026-10-04T00:00:00.000Z', 'v')"
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES {values}) t(signature, source_hash, slot, tx_index, single_in_slot, block_time, committed, status, event_count, error_count, source_file, decoded_at, decoder_version)) TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    }

    struct Sample<'a> {
        path: &'a str,
        batch: i64,
        rows: &'a [(&'a str, i64)],
    }

    fn root_with(files: &[Sample<'_>]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("digest-rows-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut listed = Vec::new();
        for file in files {
            let path = dir.join(file.path);
            transactions(&path, file.rows, "src");
            listed.push(serde_json::json!({
                "path": file.path,
                "table_name": "decoded_transactions",
                "batch_id": file.batch
            }));
        }
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": listed }).to_string(),
        )
        .unwrap();
        dir
    }

    /// One decoded root with a single `decoded_transactions` row whose `source_file` is the
    /// given raw-file byte hash.
    fn root_with_source_file(source_file: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("digest-source-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tables/decoded_transactions/epoch=0/a.parquet");
        transactions(&path, &[("sig-a", 10)], source_file);
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [{
                "path": "tables/decoded_transactions/epoch=0/a.parquet",
                "table_name": "decoded_transactions",
                "batch_id": 1
            }] })
            .to_string(),
        )
        .unwrap();
        dir
    }

    #[test]
    fn raw_file_provenance_does_not_move_the_digest() {
        // A DuckDB build bump rewrites the raw Parquet `created_by` footer, so the raw file's
        // byte hash in `decoded_transactions.source_file` changes while the decoded rows do not.
        // The content digest must not move with that provenance.
        let before = root_with_source_file("raw-sha-before-toolchain-bump");
        let after = root_with_source_file("raw-sha-after-toolchain-bump");
        assert_eq!(
            content_digest(&before).unwrap(),
            content_digest(&after).unwrap(),
            "source_file provenance (raw Parquet byte hash) moved the content digest"
        );
        std::fs::remove_dir_all(before).unwrap();
        std::fs::remove_dir_all(after).unwrap();
    }

    #[test]
    fn the_same_rows_in_a_different_order_share_a_digest_and_one_value_changes_it() {
        let forward = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-b", 20), ("sig-a", 10)],
        }]);
        let reverse = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 10), ("sig-b", 20)],
        }]);
        let changed = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 11), ("sig-b", 20)],
        }]);
        let left = content_digest(&forward).unwrap();
        let right = content_digest(&reverse).unwrap();
        let other = content_digest(&changed).unwrap();
        assert_eq!(left, right);
        assert_ne!(left, other);
        std::fs::remove_dir_all(forward).unwrap();
        std::fs::remove_dir_all(reverse).unwrap();
        std::fs::remove_dir_all(changed).unwrap();
    }

    #[test]
    fn an_injected_duplicate_key_is_an_error() {
        let dir = root_with(&[
            Sample {
                path: "tables/decoded_transactions/epoch=0/a.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
            Sample {
                path: "tables/decoded_transactions/epoch=0/b.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
        ]);
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate signature sig-a in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_duplicate_key_split_across_files_is_refused() {
        // The two copies of `sig-a` sit in different files; the SQL window count has to see both
        // copies regardless of which file or DuckDB chunk they arrive in.
        fn write_rows(path: &std::path::Path, rows: &[(String, i64)]) {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            let values = rows
                .iter()
                .map(|(signature, slot)| {
                    format!(
                        "('{signature}', 'h', {slot}, 0, false, 1, true, 'decoded', 0, 0, 'src', '2026-10-04T00:00:00.000Z', 'v')"
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch(&format!(
                "COPY (SELECT * FROM (VALUES {values}) t(signature, source_hash, slot, tx_index, single_in_slot, block_time, committed, status, event_count, error_count, source_file, decoded_at, decoder_version)) TO {} (FORMAT PARQUET)",
                sql_string(&path.to_string_lossy())
            ))
            .unwrap();
        }

        let dir = std::env::temp_dir().join(format!("digest-cross-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut many: Vec<(String, i64)> = (0..2_000).map(|n| (format!("sig-{n:06}"), n)).collect();
        many.push(("sig-a".to_owned(), 10_000));
        write_rows(
            &dir.join("tables/decoded_transactions/epoch=0/a.parquet"),
            &many,
        );
        write_rows(
            &dir.join("tables/decoded_transactions/epoch=0/b.parquet"),
            &[("sig-a".to_owned(), 10_000)],
        );
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [
                { "path": "tables/decoded_transactions/epoch=0/a.parquet", "table_name": "decoded_transactions", "batch_id": 1 },
                { "path": "tables/decoded_transactions/epoch=0/b.parquet", "table_name": "decoded_transactions", "batch_id": 1 }
            ] })
            .to_string(),
        )
        .unwrap();
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate signature sig-a in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_duplicate_key_in_one_batch_with_rows_between_is_refused() {
        // `sig-a` appears at slots 10 and 20 with another row between them, so a previous-row
        // comparison ordered by slot would miss it; the window count must not.
        let dir = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 10), ("sig-b", 15), ("sig-a", 20)],
        }]);
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate signature sig-a in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_revision_in_a_later_batch_is_kept_and_the_older_one_is_not_hashed() {
        // The two revisions carry different content (slot 10 vs 20) so that keeping the wrong
        // revision (flipping `batch_id DESC` to `ASC`) changes the digest and fails this test.
        let latest = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 2,
            rows: &[("sig-a", 20)],
        }]);
        let revised = root_with(&[
            Sample {
                path: "tables/decoded_transactions/epoch=0/a.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
            Sample {
                path: "tables/decoded_transactions/epoch=0/b.parquet",
                batch: 2,
                rows: &[("sig-a", 20)],
            },
        ]);
        assert_eq!(
            content_digest(&latest).unwrap(),
            content_digest(&revised).unwrap(),
            "a later batch must supersede an earlier revision, not be hashed twice"
        );
        std::fs::remove_dir_all(latest).unwrap();
        std::fs::remove_dir_all(revised).unwrap();
    }

    #[test]
    fn a_null_primary_key_is_refused() {
        let dir = std::env::temp_dir().join(format!("digest-null-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tables/decoded_transactions/epoch=0/a.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT NULL::VARCHAR AS signature, 'h' AS source_hash, 10::BIGINT AS slot,
                0::INTEGER AS tx_index, false AS single_in_slot, 1::BIGINT AS block_time, true AS committed,
                'decoded' AS status, 0::INTEGER AS event_count, 0::INTEGER AS error_count, 'src' AS source_file,
                '2026-10-04T00:00:00.000Z' AS decoded_at, 'v' AS decoder_version)
             TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [{
                "path": "tables/decoded_transactions/epoch=0/a.parquet",
                "table_name": "decoded_transactions",
                "batch_id": 1
            }] })
            .to_string(),
        )
        .unwrap();
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("NULL signature in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_duplicate_child_key_under_a_superseded_parent_is_refused() {
        // `sig-a` is superseded by a newer batch with a different source_hash, and events carries
        // two `e1` rows under the old parent. The reader joins child tables to the deduplicated
        // decoded_transactions, so both rows are invisible there; the full registered-file check
        // must still refuse the duplicate.
        let dir = std::env::temp_dir().join(format!("digest-child-dup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("tables/decoded_transactions/epoch=0")).unwrap();
        std::fs::create_dir_all(dir.join("tables/events/epoch=0")).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for (name, hash) in [("old.parquet", "h1"), ("new.parquet", "h2")] {
            let path = dir.join("tables/decoded_transactions/epoch=0").join(name);
            conn.execute_batch(&format!(
                "COPY (SELECT * FROM (VALUES ('sig-a', '{hash}', 10, 0, false, 1, true, 'decoded', 1, 0, 'src', '2026-10-04T00:00:00.000Z', 'v')) t(signature, source_hash, slot, tx_index, single_in_slot, block_time, committed, status, event_count, error_count, source_file, decoded_at, decoder_version)) TO {} (FORMAT PARQUET)",
                sql_string(&path.to_string_lossy())
            ))
            .unwrap();
        }
        let events_path = dir.join("tables/events/epoch=0/day.parquet");
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES \
               ('e1', 'sig-a', 'h1', 10::BIGINT, 0::INTEGER, false, 1::BIGINT, ''::VARCHAR, 0::INTEGER, ''::VARCHAR, true, ''::VARCHAR, ''::VARCHAR, 0::UINTEGER, ''::VARCHAR, ''::VARCHAR, 0::UBIGINT, 0::UBIGINT, 0::SMALLINT, 0::SMALLINT, 'v', ''::VARCHAR), \
               ('e1', 'sig-a', 'h1', 10::BIGINT, 0::INTEGER, false, 1::BIGINT, ''::VARCHAR, 0::INTEGER, ''::VARCHAR, true, ''::VARCHAR, ''::VARCHAR, 0::UINTEGER, ''::VARCHAR, ''::VARCHAR, 0::UBIGINT, 0::UBIGINT, 0::SMALLINT, 0::SMALLINT, 'v', ''::VARCHAR)) \
             t(event_id, signature, source_hash, slot, tx_index, single_in_slot, block_time, instruction_path, event_ordinal, event_type, committed, attribution, asset_symbol, asset_id, trader, signer, sequence_number, tick_size, base_lot_decimals, quote_lot_decimals, decoder_version, event_json)) TO {} (FORMAT PARQUET)",
            sql_string(&events_path.to_string_lossy())
        ))
        .unwrap();
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [
                { "path": "tables/decoded_transactions/epoch=0/old.parquet", "table_name": "decoded_transactions", "batch_id": 1 },
                { "path": "tables/decoded_transactions/epoch=0/new.parquet", "table_name": "decoded_transactions", "batch_id": 2 },
                { "path": "tables/events/epoch=0/day.parquet", "table_name": "events", "batch_id": 1 }
            ] })
            .to_string(),
        )
        .unwrap();
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string().contains("duplicate event_id e1 in events"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn content_digest_runs_the_duplicate_key_check_under_the_read_lease() {
        let dir = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 10)],
        }]);
        // Drive the check through `content_digest`. A failpoint inside
        // `assert_registered_keys_unique` records whether the read lease is held at that
        // moment, and the session counter records how many catalog reads that took. Moving
        // the check into its own `with_decoded_session` (a second lease and a second catalog
        // read) must fail this test, as must running the check before any session.
        let held = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = std::sync::Arc::clone(&held);
        let root = dir.clone();
        crate::failpoints::arm_callback("digest/duplicate-key-check", move || {
            observed.store(
                crate::lease::has_readers(&root).unwrap_or(false),
                std::sync::atomic::Ordering::SeqCst,
            );
        });
        crate::decoder::reader::reset_decoded_session_opens();
        let digest = content_digest(&dir);
        crate::failpoints::disarm("digest/duplicate-key-check");
        digest.unwrap();
        assert!(
            held.load(std::sync::atomic::Ordering::SeqCst),
            "the duplicate-key check must run under the read lease"
        );
        assert_eq!(
            crate::decoder::reader::decoded_session_opens(),
            1,
            "content_digest must open one decoded session, so the check and the rows share one catalog read"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
