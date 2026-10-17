//! The 2026-10-17 checkpoint growth and the poisoned-connection exit (ADR-0008,
//! `docs/issues/002-poisoned-connection.md`): trimming follows the watermark through a tail cycle
//! that catches up for days, and an error that leaves DuckDB unusable ends the collector instead
//! of retrying on the same connection.

mod collector_common;

use collector_common::*;
use serde_json::json;
use solos_data::collector::lanes::Lanes;
use solos_data::collector::retention::prune_published;
use solos_data::collector::service::{FatalHook, lane_error};
use solos_data::collector::writer::publish_range;
use solos_data::store::StoreError;
use std::sync::{Arc, Mutex};

fn publish(f: &Fixture, from: i64, to: i64) {
    let root = f.root.clone();
    f.db.run_blocking(move |store| publish_range(store, &root, from, to, None))
        .unwrap();
}

fn trim(f: &Fixture, hot: i64) -> i64 {
    let root = f.root.clone();
    f.db.run_blocking(move |store| prune_published(store, &root, hot, 16_000))
        .unwrap()
        .int("removedRows")
        .unwrap()
}

fn hot(f: &Fixture) -> Vec<String> {
    f.rows("SELECT signature FROM transactions ORDER BY slot")
        .iter()
        .map(|r| r.str("signature").unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catch_up_tail_cycle_lets_its_published_chunks_be_trimmed_behind_the_watermark() {
    let f = Fixture::new();
    // A cycle from floor 100: chunks 100, 200 and 300 published, 400 listed but not yet fetched.
    for (id, slot) in [("a", 100), ("b", 200), ("c", 300), ("d", 400)] {
        insert_tx(&f, id, slot);
    }
    // While the cycle walks, nothing from its floor up is trimmed.
    publish(&f, 100, 100);
    f.set("W", json!({ "slot": 300 }));
    f.set(
        "tail-active",
        json!({ "id": "c1", "floor": 100, "next": 100, "ceiling": 900 }),
    );
    assert_eq!(trim(&f, 50), 0);
    // Publishing: the chunks below W minus the hot window go, the unpublished one stays.
    publish(&f, 200, 200);
    publish(&f, 300, 300);
    f.set(
        "tail-active",
        json!({ "id": "c1", "floor": 100, "next": 301, "ceiling": 900 }),
    );
    assert_eq!(trim(&f, 50), 1);
    assert_eq!(trim(&f, 50), 1);
    assert_eq!(trim(&f, 50), 0);
    assert_eq!(hot(&f), vec!["c", "d"]);
    // The hot window still holds the newest published chunk; the next unpublished never goes.
    f.set(
        "tail-active",
        json!({ "id": "c1", "floor": 100, "next": 301, "ceiling": 900 }),
    );
    assert_eq!(trim(&f, 0), 1);
    assert_eq!(trim(&f, -1000), 0);
    assert_eq!(hot(&f), vec!["d"]);
    f.close();
}

type Calls = Arc<Mutex<Vec<(String, &'static str)>>>;

fn recorder() -> (FatalHook, Calls) {
    let calls: Calls = Arc::default();
    let sink = Arc::clone(&calls);
    let hook: FatalHook = Arc::new(move |lane, class| {
        sink.lock().unwrap().push((lane.to_owned(), class));
    });
    (hook, calls)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_rollback_ends_the_collector_before_any_more_database_work() {
    let f = Fixture::new();
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    let poisoned = StoreError::Check(
        "TransactionContext Error: Failed to commit: Failed to rollback transaction. Cannot continue operation".into(),
    );
    let ranges = f.scalar("SELECT count(*) AS n FROM published_ranges");
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &poisoned, &hook).await);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![("tail".to_owned(), "rollback_failed")]
    );
    // The poisoned connection is not touched: no error row, no published range, and the streak is
    // still counted in memory for status.json.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(
        f.scalar("SELECT count(*) AS n FROM published_ranges"),
        ranges
    );
    assert_eq!(
        lanes.to_value()["tail"]["lastErrorClass"],
        "rollback_failed"
    );

    // The 2026-10-16 shape: an out-of-memory commit, then out-of-memory on every statement.
    let oom = StoreError::Check("TransactionContext Error: Failed to commit: Out of Memory Error: could not allocate block of size 256.0 KiB (59.6 GiB/59.6 GiB used)".into());
    assert!(lane_error(&f.db, "fixture", &lanes, "maintenance", &oom, &hook).await);
    let pinned =
        StoreError::Check("Out of Memory Error: failed to pin block of size 256.0 KiB".into());
    assert!(lane_error(&f.db, "fixture", &lanes, "backfill", &pinned, &hook).await);
    assert_eq!(
        calls.lock().unwrap()[1..],
        [
            ("maintenance".to_owned(), "commit_failed"),
            ("backfill".to_owned(), "oom")
        ]
    );

    // Anything else is retried on the same connection and recorded in the checkpoint.
    let rpc = StoreError::Check("getBlock failed (code 429)".into());
    assert!(!lane_error(&f.db, "fixture", &lanes, "tail", &rpc, &hook).await);
    assert_eq!(calls.lock().unwrap().len(), 3);
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "1");
    f.close();
}
