//! Ported from `tests/decode-scale.test.ts`.

mod common;

use common::*;
use serde_json::Value;
use solos_data::catalog::CatalogFile;
use solos_data::decoder::schema::schema_static;
use solos_data::decoder::source::{next_source, source_rows};
use solos_data::fsutil::file_hash;
use solos_data::store::{Store, sql_string};
use std::collections::HashSet;

#[test]
fn deep_decoder_resume_uses_bounded_payload_memory_and_preserves_descending_cursor_order() {
    let root = tempdir("decode-scale");
    let mut store = Store::open(&root, schema_static()).unwrap();
    let path = root.join("transactions.parquet");
    // Deliberately unsorted physical rows; old OFFSET cursors refer to logical order.
    store
        .exec_batch(&format!(
            "COPY (SELECT md5(i::VARCHAR)||md5(i::VARCHAR) AS signature, (i*499979)%1000000 AS slot,
      i AS block_time, 1 AS tx_index, false AS single_in_slot, 'null' AS err, 5000 AS fee,
      100 AS compute_units_consumed, repeat(md5(i::VARCHAR), 30) AS tx_b64,
      repeat(md5(i::VARCHAR), 200) AS meta_json, '{{}}' AS raw_rpc_json, 'tail' AS mode,
      'fixture' AS provider, 'fixture' AS fetched_at, NULL::VARCHAR AS terminal_error
      FROM range(1000000) r(i))
      TO {} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    let file = CatalogFile {
        path: path.to_string_lossy().into_owned(),
        table_name: "transactions".into(),
        row_count: 1_000_000,
        sha256: file_hash(&path).unwrap(),
        created_at: "fixture".into(),
        parents: vec![],
    };
    let expected: Vec<(String, String)> = store
        .rows(&format!("SELECT signature, slot FROM read_parquet({}) ORDER BY slot DESC, signature DESC LIMIT 25 OFFSET 800000", sql_string(&path.to_string_lossy())), &[])
        .unwrap()
        .iter()
        .map(|r| (r.str("signature").unwrap().to_owned(), r.str("slot").unwrap().to_owned()))
        .collect();
    store.close().unwrap();
    let mut store = Store::open(&root, schema_static()).unwrap();
    store.exec_batch("SET memory_limit='384MB'").unwrap();
    store.exec_batch("SET threads=1").unwrap();
    let rows = source_rows(&mut store, &root, &file, 800_000, 25, &mut HashSet::new()).unwrap();
    let actual: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.tx.signature.clone(), r.tx.slot_text()))
        .collect();
    assert_eq!(actual, expected);
    assert!(
        rows.iter()
            .all(|r| r.tx.meta_json.as_ref().unwrap().len() == 6400 && r.previous_hash.is_none())
    );
    assert_eq!(
        rows.iter()
            .map(|r| r.tx.signature.clone())
            .collect::<HashSet<_>>()
            .len(),
        25
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn live_transaction_ranges_take_priority_over_newly_published_history_and_legacy_compacted_files() {
    let root = tempdir("decode-priority");
    let mut store = Store::open(&root, schema_static()).unwrap();
    let legacy = root.join("compact.parquet");
    store
        .exec_batch(&format!(
            "COPY (SELECT 200 AS slot) TO {} (FORMAT PARQUET)",
            sql_string(&legacy.to_string_lossy())
        ))
        .unwrap();
    let file = |path: &str, hash: &str, at: &str| serde_json::json!({ "path": path, "sha256": hash, "created_at": at, "table_name": "transactions", "row_count": 1 });
    std::fs::write(
        root.join("catalog.json"),
        serde_json::json!({ "at": "fixture", "files": [
            file("staging/100-150-history.parquet", "history", "2026-10-04T04:00:00Z"),
            file("staging/300-350-live.parquet", "live", "2026-10-04T02:00:00Z"),
            file(&legacy.to_string_lossy(), "legacy", "2026-10-04T05:00:00Z"),
        ] })
        .to_string(),
    )
    .unwrap();
    assert_eq!(
        next_source(&mut store, &root).unwrap().unwrap().file.sha256,
        "live"
    );
    store
        .exec(
            "INSERT INTO sources VALUES (?, ?, ?)",
            &[&"live", &"live", &1i64],
        )
        .unwrap();
    assert_eq!(
        next_source(&mut store, &root).unwrap().unwrap().file.sha256,
        "legacy"
    );
    store
        .exec(
            "INSERT INTO sources VALUES (?, ?, ?)",
            &[&"legacy", &legacy.to_string_lossy().as_ref(), &1i64],
        )
        .unwrap();
    assert_eq!(
        next_source(&mut store, &root).unwrap().unwrap().file.sha256,
        "history"
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fully_consumed_compaction_parents_avoid_decoding_the_same_raw_archive_again() {
    let root = tempdir("decode-lineage");
    let mut store = Store::open(&root, schema_static()).unwrap();
    store
        .exec_batch("INSERT INTO sources VALUES ('parent-a','old-a',10),('parent-b','old-b',20)")
        .unwrap();
    std::fs::write(
        root.join("catalog.json"),
        serde_json::json!({ "at": "fixture", "files": [
            { "path": "staging/100-200-merged.parquet", "sha256": "merged", "table_name": "transactions", "row_count": 30, "created_at": "fixture",
              "parents": [{ "sha256": "parent-a", "row_count": 10 }, { "sha256": "parent-b", "row_count": 20 }] },
            { "path": "staging/50-99-pending.parquet", "sha256": "pending", "table_name": "transactions", "row_count": 1, "created_at": "fixture" },
        ] })
        .to_string(),
    )
    .unwrap();
    assert_eq!(
        next_source(&mut store, &root).unwrap().unwrap().file.sha256,
        "pending"
    );
    assert_eq!(
        store
            .rows(
                "SELECT row_offset FROM sources WHERE source_hash='merged'",
                &[]
            )
            .unwrap()[0]
            .get("row_offset"),
        Some(&Value::String("30".into()))
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
