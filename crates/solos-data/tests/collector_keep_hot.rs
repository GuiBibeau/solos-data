//! `collector repack --keep-hot`: the rewrite keeps exactly the rows trimming would keep, proves
//! dropped rows against the archive, keeps chunks that fail the proof, and swaps atomically with
//! `checkpoint.prev.duckdb` as the way back.

mod collector_common;

use collector_common::*;
use serde_json::{Value, json};
use solos_data::collector::keep_hot::repack_keep_hot;
use solos_data::collector::reader::query_dataset;
use solos_data::collector::writer::publish_range;

fn publish(f: &Fixture, from: i64, to: i64) {
    let root = f.root.clone();
    f.db.run_blocking(move |store| publish_range(store, &root, from, to, None))
        .unwrap();
}

fn column(f: &Fixture, sql: &str) -> Vec<String> {
    f.rows(sql)
        .iter()
        .map(|r| r.iter().next().unwrap().1.as_str().unwrap().to_owned())
        .collect()
}

fn published(f: &Fixture) -> Vec<String> {
    let value: Value = serde_json::from_str(
        &query_dataset(
            &f.root,
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

fn signature_row(f: &Fixture, signature: &str, slot: i64) {
    f.exec(&format!(
        "INSERT INTO signatures VALUES ('{signature}', {slot}, {slot}, 'null', ['p'], 'tail', 'c', 'now')"
    ));
}

/// Published at 80 and 1000, unpublished at 90, watermark 1000, 200 hot slots: cutoff 800.
fn seeded() -> Fixture {
    let f = Fixture::new();
    for (id, slot) in [("old", 80), ("pending", 90), ("live", 1000)] {
        insert_tx(&f, id, slot);
        signature_row(&f, id, slot);
    }
    publish(&f, 80, 80);
    publish(&f, 1000, 1000);
    f.set("W", json!({ "slot": 1000 }));
    f.set("walk/backfill", json!({ "before": "x", "lastSlot": 90 }));
    f.exec(
        "INSERT INTO rpc_pages VALUES ('legacy-page', 'getBlock', 'fixture', '{}', 'now', NULL)",
    );
    f.exec("INSERT INTO addresses VALUES ('m', 'market', 'SOL', 'active', 1, 2)");
    f
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_hot_rewrite_drops_only_archived_copies_and_keeps_every_other_row() {
    let f = seeded();
    let kv_before = f.rows("SELECT name, value::VARCHAR AS v FROM kv WHERE name NOT IN ('checkpoint-repack') ORDER BY name");
    let files_before = f.scalar("SELECT count(*) AS n FROM files");
    let report =
        f.db.run_blocking(|store| repack_keep_hot(store, 200, 50, false))
            .unwrap();
    assert_eq!(report.get("ok"), Some(&json!(true)));
    assert_eq!(report.get("cutoff"), Some(&json!(800)));
    assert_eq!(
        report.get("drops"),
        Some(&json!([{ "from": 80, "to": 80 }]))
    );
    assert_eq!(report.get("dropped").unwrap()["transactions"], json!(1));
    assert_eq!(report.get("dropped").unwrap()["signatures"], json!(1));
    assert_eq!(report.get("unverified"), Some(&json!([])));
    assert_eq!(
        column(&f, "SELECT signature FROM transactions ORDER BY signature"),
        vec!["live", "pending"]
    );
    assert_eq!(
        column(&f, "SELECT signature FROM signatures ORDER BY signature"),
        vec!["live", "pending"]
    );
    assert_eq!(
        column(&f, "SELECT page_id FROM rpc_pages WHERE slot IS NULL"),
        vec!["legacy-page"]
    );
    assert_eq!(f.scalar("SELECT count(*) AS n FROM addresses"), "1");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM files"), files_before);
    let kv_after = f.rows("SELECT name, value::VARCHAR AS v FROM kv WHERE name NOT IN ('checkpoint-repack') ORDER BY name");
    assert_eq!(kv_after, kv_before);
    assert_eq!(
        f.get("checkpoint-repack").unwrap()["mode"],
        json!("keep-hot")
    );
    // Primary keys survive the rewrite.
    let duplicate = f.db.run_blocking(|store| {
        store.exec_batch(
            "INSERT INTO transactions SELECT * FROM transactions WHERE signature='live'",
        )
    });
    assert!(duplicate.is_err());
    // Published data unchanged, and the original kept as the way back.
    assert_eq!(published(&f), vec!["live", "old"]);
    assert!(f.root.join("checkpoint.prev.duckdb").exists());
    assert!(!f.root.join("checkpoint.keep.duckdb").exists());
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_that_fails_the_archive_check_is_kept_whole() {
    let f = seeded();
    insert_tx(&f, "other", 300);
    publish(&f, 300, 300);
    // The checkpoint copy at 80 no longer matches its published row: it must stay.
    f.exec("UPDATE transactions SET meta_json='changed' WHERE signature='old'");
    let report =
        f.db.run_blocking(|store| repack_keep_hot(store, 200, 50, false))
            .unwrap();
    let unverified = report
        .get("unverified")
        .unwrap()
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(unverified.len(), 1);
    assert_eq!(unverified[0]["from"], json!(80));
    assert_eq!(
        report.get("drops"),
        Some(&json!([{ "from": 300, "to": 300 }]))
    );
    assert_eq!(
        column(&f, "SELECT signature FROM transactions ORDER BY signature"),
        vec!["live", "old", "pending"]
    );
    assert_eq!(
        f.scalar("SELECT meta_json FROM transactions WHERE signature='old'"),
        "changed"
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_verifies_and_changes_nothing() {
    let f = seeded();
    let report =
        f.db.run_blocking(|store| repack_keep_hot(store, 200, 50, true))
            .unwrap();
    assert_eq!(report.get("dryRun"), Some(&json!(true)));
    assert_eq!(report.get("dropped").unwrap()["transactions"], json!(1));
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "3");
    assert!(!f.root.join("checkpoint.prev.duckdb").exists());
    f.close();
}
