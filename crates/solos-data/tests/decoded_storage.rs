//! Ported from `tests/decoded-compaction.test.ts`, `tests/decoded-staging.test.ts` and the
//! offline half of `tests/repack.test.ts`, on a decoder root.

mod common;

use common::*;
use serde_json::Value;
use solos_data::decoder::compact::compact_decoded;
use solos_data::decoder::normalize::Rows;
use solos_data::decoder::publish::{SourceProgress, publish, recover, write_catalog};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::schema::schema_static;
use solos_data::gc::collect_retired;
use solos_data::jsonout::Obj;
use solos_data::repack::repack_checkpoint;
use solos_data::store::Store;

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
    compact_decoded(&mut store).unwrap();
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
