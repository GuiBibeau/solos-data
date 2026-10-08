//! Ported from `tests/decoded-compaction.test.ts`, `tests/decoded-staging.test.ts` and the
//! offline half of `tests/repack.test.ts`, on a decoder root.

mod common;

use common::*;
use serde_json::Value;
use solos_data::decoder::compact::compact_decoded;
use solos_data::decoder::normalize::Rows;
use solos_data::decoder::publish::{SourceProgress, publish, recover, write_catalog};
use solos_data::decoder::reader::{query_decoded, query_decoded_slots};
use solos_data::decoder::schema::schema_static;
use solos_data::gc::collect_retired;
use solos_data::jsonout::Obj;
use solos_data::repack::repack_checkpoint;
use solos_data::store::Store;
use std::time::Duration;

fn fills_by_signature(root: &std::path::Path) -> Value {
    serde_json::from_str::<Value>(
        &query_decoded(
            root,
            "SELECT signature,source_hash FROM fills ORDER BY signature",
        )
        .unwrap()
        .to_json(),
    )
    .unwrap()["rows"]
        .clone()
}

fn transaction_row(signature: &str, hash: &str, slot: i64, committed: bool) -> Obj {
    Obj::new()
        .with("signature", signature)
        .with("source_hash", hash)
        .with("slot", slot)
        .with("tx_index", 0)
        .with("single_in_slot", false)
        .with("block_time", 100)
        .with("committed", committed)
        .with("status", "decoded")
        .with("event_count", i32::from(committed))
        .with("error_count", 0)
        .with("source_file", "fixture")
        .with("decoded_at", "fixture")
        .with("decoder_version", "fixture")
}

#[test]
fn decoded_compaction_and_cleanup_retain_history_and_corrections_after_restart() {
    let root = tempdir("decoded-compact");
    let mut store = Store::open(&root, schema_static()).unwrap();
    for i in 0..40 {
        let mut rows = Rows::default();
        let signature = format!("s-{}", i % 20);
        rows.get_mut("decoded_transactions").push(transaction_row(
            &signature,
            &format!("h-{i}"),
            100 + i % 20,
            true,
        ));
        rows.get_mut("fills").push(
            Obj::new()
                .with("signature", signature.clone())
                .with("source_hash", format!("h-{i}"))
                .with("slot", 100 + i % 20)
                .with("event_id", format!("e-{}", i % 20)),
        );
        publish(
            &mut store,
            &rows,
            &SourceProgress {
                hash: format!("file-{i}"),
                path: "raw".into(),
                offset: 1,
                at: Some(format!("{i:04}")),
                seen: None,
            },
        )
        .unwrap();
    }
    let before = fills_by_signature(&root);
    let originals = store.rows("SELECT path FROM files", &[]).unwrap().len();
    let spent = compact_decoded(&mut store, Duration::ZERO).unwrap();
    assert_eq!(spent.int("merges"), Some(0));
    assert!(spent.int("deferred").unwrap() > 0);
    assert_eq!(
        store.rows("SELECT path FROM files", &[]).unwrap().len(),
        originals
    );
    compact_decoded(&mut store, Duration::from_secs(60)).unwrap();
    assert!(store.rows("SELECT path FROM files", &[]).unwrap().len() < originals);
    assert_eq!(fills_by_signature(&root), before);
    let removed = collect_retired(&mut store, &root, 0, &write_catalog).unwrap();
    assert!(removed.int("removedFiles").unwrap() > 0);
    store.close().unwrap();
    let mut store = Store::open(&root, schema_static()).unwrap();
    recover(&mut store).unwrap();
    assert_eq!(fills_by_signature(&root), before);
    let mut rows = Rows::default();
    rows.get_mut("decoded_transactions")
        .push(transaction_row("s-0", "failed", 100, false));
    publish(
        &mut store,
        &rows,
        &SourceProgress {
            hash: "correction".into(),
            path: "raw".into(),
            offset: 1,
            at: Some("9999".into()),
            seen: None,
        },
    )
    .unwrap();
    assert_eq!(
        count(
            &root,
            "SELECT count(*) AS n FROM fills WHERE signature='s-0'"
        ),
        0
    );
    assert_eq!(
        count(&root, "SELECT count(*) AS n FROM decoded_transactions"),
        20
    );
    for file in store.rows("SELECT path FROM files", &[]).unwrap() {
        assert!(root.join(file.str("path").unwrap()).exists());
    }
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn repeated_decoded_batches_release_previous_temporary_payload_storage() {
    let root = tempdir("decoded-staging");
    let mut store = Store::open(&root, schema_static()).unwrap();
    for batch in 0..40 {
        let mut rows = Rows::default();
        for i in 0..500 {
            rows.get_mut("events").push(
                Obj::new()
                    .with("signature", format!("s-{batch}-{i}"))
                    .with("source_hash", "fixture")
                    .with("slot", 100)
                    .with("event_id", format!("e-{i}"))
                    .with("event_json", "payload-".repeat(1024)),
            );
        }
        publish(
            &mut store,
            &rows,
            &SourceProgress {
                hash: format!("file-{batch}"),
                path: "raw".into(),
                offset: 500,
                at: None,
                seen: None,
            },
        )
        .unwrap();
    }
    let memory = store.rows("SELECT sum(memory_usage_bytes+temporary_storage_bytes) AS bytes FROM duckdb_memory() WHERE tag='IN_MEMORY_TABLE'", &[]).unwrap();
    let bytes = memory[0].int("bytes").unwrap_or(0);
    assert!(bytes < 16 * 1024 * 1024, "staging retained {bytes} bytes");
    assert_eq!(
        store.rows("SELECT count(*) AS n FROM events", &[]).unwrap()[0].get("n"),
        Some(&Value::String("500".into()))
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn offline_checkpoint_repack_preserves_every_row_cursor_and_primary_key() {
    let root = tempdir("decoded-repack");
    let mut store = Store::open(&root, schema_static()).unwrap();
    let mut rows = Rows::default();
    rows.get_mut("decoded_transactions")
        .push(transaction_row("kept", "h", 10, true));
    publish(
        &mut store,
        &rows,
        &SourceProgress {
            hash: "src".into(),
            path: "raw".into(),
            offset: 1,
            at: None,
            seen: None,
        },
    )
    .unwrap();
    store
        .set("cursor", &serde_json::json!({ "next": 9 }))
        .unwrap();
    store.close().unwrap();
    let report = repack_checkpoint(&root).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(true)));
    let mut store = Store::open(&root, schema_static()).unwrap();
    assert_eq!(
        store.get("cursor").unwrap(),
        Some(serde_json::json!({ "next": 9 }))
    );
    assert_eq!(
        store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].get("n"),
        Some(&Value::String("1".into()))
    );
    assert!(
        store
            .exec("INSERT INTO sources VALUES ('src', 'dup', 2)", &[])
            .is_err()
    );
    assert_eq!(
        count(&root, "SELECT count(*) AS n FROM decoded_transactions"),
        1
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// One batch holding a transaction revision and its fill at `slot`.
fn publish_revision(store: &mut Store, i: i64, keys: i64, slot: i64) {
    let mut rows = Rows::default();
    let signature = format!("s-{}", i % keys);
    rows.get_mut("decoded_transactions").push(transaction_row(
        &signature,
        &format!("h-{i}"),
        slot,
        true,
    ));
    rows.get_mut("fills").push(
        Obj::new()
            .with("signature", signature)
            .with("source_hash", format!("h-{i}"))
            .with("slot", slot)
            .with("event_id", format!("e-{}", i % keys)),
    );
    publish(
        store,
        &rows,
        &SourceProgress {
            hash: format!("file-{slot}-{i}"),
            path: "raw".into(),
            offset: 1,
            at: Some(format!("{i:04}")),
            seen: None,
        },
    )
    .unwrap();
}

fn active_files(store: &Store, table: &str) -> Vec<Obj> {
    store
        .rows(
            "SELECT path,batch_id,row_count FROM files WHERE table_name=? ORDER BY batch_id DESC",
            &[&table],
        )
        .unwrap()
}

#[test]
fn compaction_merges_a_stranded_run_below_an_oversized_head_file_and_keeps_precedence() {
    let root = tempdir("decoded-stranded");
    let mut store = Store::open(&root, schema_static()).unwrap();
    for i in 0..12 {
        publish_revision(&mut store, i, 4, 100 + i % 4);
    }
    // The newest file of each table is registered at the merge bound, as a file an earlier pass
    // produced would be: the head run cannot absorb it and is far too short to merge on its own.
    for table in ["fills", "decoded_transactions"] {
        let head = active_files(&store, table)[0]
            .str("path")
            .unwrap()
            .to_owned();
        store
            .exec("UPDATE files SET row_count=4000000 WHERE path=?", &[&head])
            .unwrap();
    }
    let before = fills_by_signature(&root);
    let report = compact_decoded(&mut store, Duration::from_secs(60)).unwrap();
    assert_eq!(report.int("merges"), Some(2));
    assert_eq!(report.int("mergedFiles"), Some(22));
    for table in ["fills", "decoded_transactions"] {
        let files = active_files(&store, table);
        assert_eq!(files.len(), 2, "{table}: head plus one merged file");
        assert_eq!(files[0].int("row_count"), Some(4_000_000));
        assert!(files[1].str("path").unwrap().contains("/compact-"));
        assert!(files[1].int("batch_id") < files[0].int("batch_id"));
    }
    assert_eq!(fills_by_signature(&root), before);
    let again = compact_decoded(&mut store, Duration::from_secs(60)).unwrap();
    assert_eq!(again.int("merges"), Some(0));
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn the_head_run_waits_for_thirty_two_files() {
    let root = tempdir("decoded-head");
    let mut store = Store::open(&root, schema_static()).unwrap();
    for i in 0..31 {
        publish_revision(&mut store, i, 31, 100);
    }
    let report = compact_decoded(&mut store, Duration::from_secs(60)).unwrap();
    assert_eq!(report.int("merges"), Some(0));
    publish_revision(&mut store, 31, 32, 100);
    let report = compact_decoded(&mut store, Duration::from_secs(60)).unwrap();
    assert_eq!(report.int("merges"), Some(2));
    assert_eq!(active_files(&store, "fills").len(), 1);
    assert_eq!(count(&root, "SELECT count(*) AS n FROM fills"), 32);
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_slot_scoped_query_reads_only_the_covering_epochs() {
    let root = tempdir("decoded-scoped");
    let mut store = Store::open(&root, schema_static()).unwrap();
    for i in 0..3 {
        publish_revision(&mut store, i, 3, 100 + i);
    }
    for i in 1010..1015 {
        publish_revision(&mut store, i, 1000, 432_000 + i);
    }
    store.close().unwrap();
    let scoped = |slots: Option<(i64, i64)>| -> Value {
        serde_json::from_str(
            &query_decoded_slots(&root, "SELECT count(*) AS n FROM fills", slots)
                .unwrap()
                .to_json(),
        )
        .unwrap()
    };
    let all = scoped(None);
    assert_eq!(all["rows"][0]["n"], Value::String("8".into()));
    assert_eq!(all["epochs"], Value::Null);
    let second = scoped(Some((432_000, 432_999)));
    assert_eq!(second["rows"][0]["n"], Value::String("5".into()));
    assert_eq!(second["epochs"], serde_json::json!([1, 1]));
    let first = scoped(Some((0, 431_999)));
    assert_eq!(first["rows"][0]["n"], Value::String("3".into()));
    assert!(query_decoded_slots(&root, "SELECT 1 AS n", Some((5, 1))).is_err());
    std::fs::remove_dir_all(root).unwrap();
}
