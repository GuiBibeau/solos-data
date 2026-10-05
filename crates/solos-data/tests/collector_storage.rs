//! Ported from `tests/retention.test.ts`, `tests/storage-scale.test.ts` (insert and compaction),
//! `tests/gc.test.ts`, `tests/repack.test.ts`, `tests/relocate.test.ts` and
//! `tests/crash-recovery.test.ts`.

mod collector_common;

use collector_common::*;
use serde_json::{Value, json};
use solos_data::collector::catalog::write_catalog;
use solos_data::collector::compactor::compact;
use solos_data::collector::config::Lane;
use solos_data::collector::fetcher::{RawRow, insert_raw};
use solos_data::collector::maintenance::{reclaim_checkpoint, relocate};
use solos_data::collector::reader::query_dataset;
use solos_data::collector::retention::{collect_legacy, prune_published};
use solos_data::collector::walker::{make_walk, walk_page};
use solos_data::collector::writer::publish_range;
use solos_data::gc::collect_retired;
use solos_data::lease::with_read_lease;
use solos_data::repack::repack_checkpoint;
use solos_data::store::Store;
use std::collections::HashMap;

fn publish(f: &Fixture, from: i64, to: i64) {
    let root = f.root.clone();
    f.db.run_blocking(move |store| publish_range(store, &root, from, to, None))
        .unwrap();
}

fn signatures(root: &std::path::Path) -> Vec<String> {
    let value: Value = serde_json::from_str(
        &query_dataset(
            root,
            "SELECT signature FROM transactions ORDER BY signature",
        )
        .unwrap()
        .to_json(),
    )
    .unwrap();
    value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["signature"].as_str().unwrap().to_owned())
        .collect()
}

fn catalog_writer(
    root: std::path::PathBuf,
) -> impl Fn(&mut Store) -> Result<(), solos_data::store::StoreError> {
    move |store| write_catalog(store, &root).map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_trimming_retains_full_archive_unpublished_rows_tail_overlap_and_walk_cursor() {
    let f = Fixture::new();
    walk_page(
        &FixtureRpc::new(
            vec![vec![sig("live", 1000), sig("pending", 90), sig("old", 80)]],
            HashMap::new(),
        ),
        &f.db,
        "walk/backfill",
        make_walk("p", Lane::Backfill, "c", 0, 1000),
    )
    .await
    .unwrap();
    for (id, slot) in [("old", 80), ("pending", 90), ("live", 1000)] {
        insert_tx(&f, id, slot);
    }
    publish(&f, 80, 80);
    publish(&f, 1000, 1000);
    f.set("W", json!({ "slot": 1000 }));
    let cursor = f.get("walk/backfill");
    let root = f.root.clone();
    f.db.run_blocking(move |store| prune_published(store, &root, 200, 16_000))
        .unwrap();
    let hot: Vec<String> = f
        .rows("SELECT signature FROM transactions ORDER BY signature")
        .iter()
        .map(|r| r.str("signature").unwrap().to_owned())
        .collect();
    assert_eq!(hot, vec!["live", "pending"]);
    assert_eq!(f.get("walk/backfill"), cursor);
    assert_eq!(signatures(&f.root), vec!["live", "old"]);
    assert_eq!(
        f.scalar("SELECT count(*) AS n FROM signatures WHERE signature='pending'"),
        "1"
    );
    let root = f.root.clone();
    f.db.run_blocking(move |store| prune_published(store, &root, 200, 16_000))
        .unwrap();
    assert_eq!(signatures(&f.root), vec!["live", "old"]);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn large_archive_trimming_compares_payload_hashes_within_a_bounded_memory_budget() {
    let mut f = Fixture::new();
    f.exec(
        "INSERT INTO transactions SELECT md5(i::VARCHAR)||md5(i::VARCHAR),i+100,i,NULL,NULL,'null',5000,10,
      repeat(md5(i::VARCHAR),2),repeat(md5(i::VARCHAR),50),'{}','backfill','fixture','fixture',NULL FROM range(300000) r(i)",
    );
    publish(&f, 100, 300_099);
    f.set("W", json!({ "slot": 400_000 }));
    f.reopen();
    f.exec("SET memory_limit='384MB'");
    f.exec("SET threads=1");
    let root = f.root.clone();
    let result =
        f.db.run_blocking(move |store| prune_published(store, &root, 200, 1_000_000_000))
            .unwrap();
    assert_eq!(result.int("removedRows"), Some(300_000));
    let value: Value = serde_json::from_str(
        &query_dataset(&f.root, "SELECT count(*) AS n FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(value["rows"][0]["n"], "300000");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "0");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_trimming_rejects_corrupt_archives_and_changed_unpublished_payloads() {
    let f = Fixture::new();
    insert_tx(&f, "old", 80);
    publish(&f, 80, 80);
    f.set("W", json!({ "slot": 1000 }));
    f.exec("UPDATE transactions SET meta_json='changed' WHERE signature='old'");
    let root = f.root.clone();
    let error =
        f.db.run_blocking(move |store| prune_published(store, &root, 200, 16_000))
            .unwrap_err()
            .to_string()
            .to_lowercase();
    assert!(error.contains("match") || error.contains("unarchived"));
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    f.exec("UPDATE transactions SET meta_json='{}'");
    let file = f.rows("SELECT path FROM files WHERE table_name='transactions'")[0]
        .str("path")
        .unwrap()
        .to_owned();
    let bytes = std::fs::read(&file).unwrap();
    std::fs::write(&file, "corrupt").unwrap();
    let root = f.root.clone();
    assert!(
        f.db.run_blocking(move |store| prune_published(store, &root, 200, 16_000))
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains("checksum")
    );
    std::fs::write(&file, bytes).unwrap();
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    let root = f.root.clone();
    f.db.run_blocking(move |store| prune_published(store, &root, 200, 16_000))
        .unwrap();
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "0");
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(f.root.join("catalog.json")).unwrap())
            .unwrap();
    assert!(
        catalog["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["from"] == 80 && r["to"] == 80)
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_older_backfill_arrivals_do_not_starve_other_cold_checkpoint_ranges() {
    let f = Fixture::new();
    f.set("W", json!({ "slot": 1_000_000 }));
    for slot in [10, 50_000, 100_000] {
        insert_tx(&f, &format!("initial-{slot}"), slot);
        publish(&f, slot, slot);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| prune_published(store, &root, 10_000, 10))
        .unwrap();
    for slot in [9, 8] {
        insert_tx(&f, &format!("older-{slot}"), slot);
        publish(&f, slot, slot);
        let root = f.root.clone();
        f.db.run_blocking(move |store| prune_published(store, &root, 10_000, 10))
            .unwrap();
    }
    assert_eq!(
        f.scalar("SELECT count(*) AS n FROM transactions WHERE signature LIKE 'initial-%'"),
        "0"
    );
    let value: Value = serde_json::from_str(
        &query_dataset(&f.root, "SELECT count(*) AS n FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(value["rows"][0]["n"], "5");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_raw_insert_jobs_dedupe_atomically_and_reject_an_identity_in_another_slot() {
    let f = Fixture::new();
    let row = RawRow {
        signature: "a".into(),
        slot: 100,
        block_time: Value::from(100),
        err: Value::Null,
        fee: Value::from(5000),
        compute_units_consumed: Value::from(10),
        tx_b64: "AA==".into(),
        meta_json: "{}".into(),
        raw_rpc_json: "{}".into(),
        mode: "tail".into(),
        provider: "fixture".into(),
        fetched_at: "fixture".into(),
    };
    let (r1, r2) = (row.clone(), row.clone());
    let a = f.db.run(move |store| {
        store.transaction(|store| insert_raw(store, &[r1.clone(), r1], 100, 100))
    });
    let b =
        f.db.run(move |store| store.transaction(|store| insert_raw(store, &[r2], 100, 100)));
    let (a, b) = tokio::join!(a, b);
    a.unwrap();
    b.unwrap();
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    let mut moved = row.clone();
    moved.slot = 101;
    let error = f
        .db
        .run_blocking(move |store| store.transaction(|store| insert_raw(store, &[moved], 101, 101)))
        .unwrap_err()
        .to_string()
        .to_lowercase();
    assert!(error.contains("key") || error.contains("constraint"));
    assert_eq!(f.scalar("SELECT slot FROM transactions"), "100");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_compaction_leaves_an_oversized_base_intact_and_compacts_the_newest_prefix() {
    let f = Fixture::new();
    insert_tx(&f, "a", 100);
    publish(&f, 100, 100);
    let base = f.rows("SELECT path FROM files WHERE table_name='transactions'")[0]
        .str("path")
        .unwrap()
        .to_owned();
    // Catalog size is deliberately inflated to exercise the scheduling boundary.
    f.exec(&format!(
        "UPDATE files SET row_count=300000 WHERE path='{base}'"
    ));
    for i in 0..10 {
        f.exec(&format!("UPDATE transactions SET tx_index={i}"));
        publish(&f, 100, 100);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    let files =
        f.rows("SELECT path FROM files WHERE status='active' AND table_name='transactions'");
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|file| file.str("path") == Some(base.as_str()))
    );
    let merged = files
        .iter()
        .find(|file| file.str("path") != Some(base.as_str()))
        .unwrap()
        .str("path")
        .unwrap()
        .to_owned();
    assert_eq!(
        f.rows(&format!("SELECT tx_index FROM read_parquet('{merged}')"))[0].int("tx_index"),
        Some(9)
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_never_promotes_older_files_above_an_excluded_newest_revision() {
    let f = Fixture::new();
    insert_tx(&f, "a", 100);
    for i in 0..11 {
        f.exec(&format!("UPDATE transactions SET tx_index={i}"));
        publish(&f, 100, 100);
    }
    let latest = f.rows("SELECT path FROM files WHERE table_name='transactions' ORDER BY created_at DESC, path DESC LIMIT 1")[0].str("path").unwrap().to_owned();
    f.exec(&format!(
        "UPDATE files SET row_count=300000 WHERE path='{latest}'"
    ));
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    assert_eq!(
        f.scalar(
            "SELECT count(*) AS n FROM files WHERE status='active' AND table_name='transactions'"
        ),
        "11"
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn retired_file_cleanup_preserves_an_active_reader_and_checks_replacement_integrity() {
    let f = Fixture::new();
    insert_tx(&f, "old", 80);
    for _ in 0..10 {
        publish(&f, 80, 80);
    }
    let originals: Vec<String> = f
        .rows("SELECT path FROM files WHERE table_name='transactions'")
        .iter()
        .map(|r| r.str("path").unwrap().to_owned())
        .collect();
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    let root = f.root.clone();
    with_read_lease::<_, std::io::Error>(&f.root, || {
        let root2 = root.clone();
        let result =
            f.db.run_blocking(move |store| {
                collect_retired(store, &root2, 0, &catalog_writer(root2.clone()))
            })
            .unwrap();
        assert_eq!(result.int("removedFiles"), Some(0));
        Ok(())
    })
    .unwrap();
    for path in &originals {
        assert!(std::path::Path::new(path).exists());
    }
    let replacement = f
        .rows("SELECT path FROM files WHERE status='active' AND table_name='transactions'")[0]
        .str("path")
        .unwrap()
        .to_owned();
    let bytes = std::fs::read(&replacement).unwrap();
    std::fs::write(&replacement, "corrupt").unwrap();
    let root = f.root.clone();
    assert!(
        f.db.run_blocking(move |store| collect_retired(
            store,
            &root,
            0,
            &catalog_writer(root.clone())
        ))
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("checksum")
    );
    for path in &originals {
        assert!(std::path::Path::new(path).exists());
    }
    std::fs::write(&replacement, bytes).unwrap();
    let root = f.root.clone();
    assert!(
        f.db.run_blocking(move |store| collect_retired(
            store,
            &root,
            0,
            &catalog_writer(root.clone())
        ))
        .unwrap()
        .int("removedFiles")
        .unwrap()
            >= 10
    );
    for path in &originals {
        assert!(!std::path::Path::new(path).exists());
    }
    assert_eq!(signatures(&f.root), vec!["old"]);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_cleanup_deletes_only_files_whose_complete_payloads_still_exist_in_the_canonical_archive()
 {
    let f = Fixture::new();
    insert_tx(&f, "old", 80);
    for _ in 0..10 {
        publish(&f, 80, 80);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    f.exec("DELETE FROM retired_files");
    let originals: Vec<String> = f
        .rows("SELECT path FROM files WHERE status='superseded'")
        .iter()
        .map(|r| r.str("path").unwrap().to_owned())
        .collect();
    f.exec("UPDATE transactions SET meta_json='changed'");
    for _ in 0..10 {
        publish(&f, 80, 80);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    let root = f.root.clone();
    assert!(
        f.db.run_blocking(move |store| collect_legacy(store, &root))
            .unwrap()
            .int("retainedFiles")
            .unwrap()
            > 0
    );
    for path in &originals {
        assert!(std::path::Path::new(path).exists());
    }
    f.exec("UPDATE transactions SET meta_json='{}'");
    for _ in 0..10 {
        publish(&f, 80, 80);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    let root = f.root.clone();
    assert!(
        f.db.run_blocking(move |store| collect_legacy(store, &root))
            .unwrap()
            .int("removedFiles")
            .unwrap()
            > 0
    );
    for path in &originals {
        assert!(!std::path::Path::new(path).exists());
    }
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_checkpoint_repack_preserves_every_row_cursor_view_and_primary_key() {
    let mut f = Fixture::new();
    insert_tx(&f, "kept", 10);
    f.set("backfill", json!({ "next": 9 }));
    f.exec("INSERT INTO published_ranges VALUES (10,10)");
    let store = f.thread.take().unwrap().join(f.db.clone());
    store.close().unwrap();
    assert_eq!(
        repack_checkpoint(&f.root).unwrap().get("ok"),
        Some(&Value::Bool(true))
    );
    let store = Store::open(&f.root, solos_data::collector::schema::SCHEMA).unwrap();
    let (db, thread) = solos_data::db::Db::spawn(store);
    f.db = db;
    f.thread = Some(thread);
    assert_eq!(f.get("backfill").unwrap(), json!({ "next": 9 }));
    assert_eq!(f.scalar("SELECT count(*) AS n FROM dataset_watermark"), "1");
    let error = f.db.run_blocking(|store| store.exec_batch("INSERT INTO transactions VALUES ('kept', 20, 20, NULL, NULL, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture', 'x', NULL)")).unwrap_err().to_string().to_lowercase();
    assert!(error.contains("key") || error.contains("constraint"));
    assert_eq!(f.scalar("SELECT meta_json FROM transactions"), "{}");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn writer_rewrite_queues_concurrent_database_users_and_reopens_its_connection() {
    let f = Fixture::new();
    insert_tx(&f, "before", 10);
    let rewrite = f.db.run(|store| store.while_closed(repack_checkpoint));
    let queued = f.db.run(|store| store.exec_batch("INSERT INTO transactions VALUES ('after', 11, 11, NULL, NULL, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture', 'x', NULL)"));
    let (rewrite, queued) = tokio::join!(rewrite, queued);
    assert_eq!(rewrite.unwrap().get("ok"), Some(&Value::Bool(true)));
    queued.unwrap();
    let order: Vec<String> = f
        .rows("SELECT signature FROM transactions ORDER BY slot")
        .iter()
        .map(|r| r.str("signature").unwrap().to_owned())
        .collect();
    assert_eq!(order, vec!["before", "after"]);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_checkpoint_reclamation_keeps_data_and_records_a_durable_size_baseline() {
    let f = Fixture::new();
    insert_tx(&f, "kept", 10);
    assert_eq!(
        f.db.run_blocking(|store| reclaim_checkpoint(store, 1))
            .unwrap()
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(true))
    );
    assert!(
        f.get("checkpoint-repack").unwrap()["bytesAfter"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    assert!(
        f.db.run_blocking(|store| reclaim_checkpoint(store, 16 * 1024u64.pow(3)))
            .unwrap()
            .is_none()
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_collector_file_registrations_rebase_after_a_move_before_resuming_or_querying() {
    let mut f = Fixture::new();
    insert_tx(&f, "sig", 100);
    publish(&f, 100, 100);
    let store = f.thread.take().unwrap().join(f.db.clone());
    store.close().unwrap();
    let moved = f.root.with_file_name(format!(
        "{}-moved",
        f.root.file_name().unwrap().to_string_lossy()
    ));
    std::fs::rename(&f.root, &moved).unwrap();
    let mut store = Store::open(&moved, solos_data::collector::schema::SCHEMA).unwrap();
    assert_eq!(relocate(&mut store).unwrap().int("relocated"), Some(1));
    store.close().unwrap();
    let value: Value = serde_json::from_str(
        &query_dataset(&moved, "SELECT count(*) AS n FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(value["rows"][0]["n"], "1");
    std::fs::remove_dir_all(&moved).unwrap();
    let store = Store::open(&f.root, solos_data::collector::schema::SCHEMA).unwrap();
    let (db, thread) = solos_data::db::Db::spawn(store);
    f.db = db;
    f.thread = Some(thread);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn relocation_rebases_archive_bounds_and_removed_file_lineage_after_cleanup() {
    let mut f = Fixture::new();
    insert_tx(&f, "sig", 100);
    for _ in 0..10 {
        publish(&f, 100, 100);
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| {
        compact(store, &root)?;
        collect_retired(store, &root, 0, &catalog_writer(root.clone())).map(|_| ())
    })
    .unwrap();
    let store = f.thread.take().unwrap().join(f.db.clone());
    store.close().unwrap();
    let moved = f.root.with_file_name(format!(
        "{}-moved",
        f.root.file_name().unwrap().to_string_lossy()
    ));
    std::fs::rename(&f.root, &moved).unwrap();
    let mut store = Store::open(&moved, solos_data::collector::schema::SCHEMA).unwrap();
    relocate(&mut store).unwrap();
    for row in store
        .rows(
            "SELECT path FROM file_bounds UNION SELECT path FROM retired_files",
            &[],
        )
        .unwrap()
    {
        assert!(
            row.str("path")
                .unwrap()
                .starts_with(&format!("{}/", moved.display()))
        );
    }
    assert_eq!(
        collect_retired(&mut store, &moved, 0, &catalog_writer(moved.clone()))
            .unwrap()
            .int("removedFiles"),
        Some(0)
    );
    store.close().unwrap();
    let value: Value = serde_json::from_str(
        &query_dataset(&moved, "SELECT count(*) AS n FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(value["rows"][0]["n"], "1");
    std::fs::remove_dir_all(&moved).unwrap();
    let store = Store::open(&f.root, solos_data::collector::schema::SCHEMA).unwrap();
    let (db, thread) = solos_data::db::Db::spawn(store);
    f.db = db;
    f.thread = Some(thread);
    f.close();
}

#[test]
fn sigkill_before_a_checkpoint_recovers_committed_manifest_progress_and_rolls_back_unfinished_work()
{
    let root = std::env::temp_dir().join(format!("collector-crash-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_solos-data"))
        .args(["dev", "crash-writer", "--root", root.to_str().unwrap()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    {
        use std::io::Read;
        let mut stdout = child.stdout.take().unwrap();
        let mut buffer = [0u8; 16];
        let read = stdout.read(&mut buffer).unwrap();
        assert!(
            std::str::from_utf8(&buffer[..read])
                .unwrap()
                .starts_with("ready"),
            "fixture writer did not become ready"
        );
    }
    assert!(
        std::fs::metadata(root.join("checkpoint.duckdb.wal"))
            .unwrap()
            .len()
            > 0
    );
    child.kill().unwrap();
    child.wait().unwrap();
    let store = Store::open(&root, solos_data::collector::schema::SCHEMA).unwrap();
    assert_eq!(store.get("durable").unwrap(), Some(Value::Bool(true)));
    assert_eq!(store.get("unfinished").unwrap(), None);
    assert_eq!(
        store.rows("SELECT signature FROM signatures", &[]).unwrap()[0].str("signature"),
        Some("durable")
    );
    store.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
