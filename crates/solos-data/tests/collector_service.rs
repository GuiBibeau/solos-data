//! The 2026-10-17 checkpoint growth and the poisoned-connection exit (ADR-0008,
//! `docs/issues/002-poisoned-connection.md`): trimming follows the watermark through a tail cycle
//! that catches up for days, and an error that leaves DuckDB unusable ends the collector instead
//! of retrying on the same connection.

mod collector_common;

use collector_common::*;
use serde_json::json;
use solos_data::collector::lanes::Lanes;
use solos_data::collector::retention::prune_published;
use solos_data::collector::service::{FatalHook, fatal_class, lane_error};
use solos_data::collector::writer::{Checkpoint, publish_range};
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

#[tokio::test(flavor = "multi_thread")]
async fn a_guarded_writer_runs_nothing_after_a_failed_rollback_queued_jobs_included() {
    let mut f = Fixture::new();
    f.db.guard_fatal();
    insert_tx(&f, "before", 10);
    // The failing job and a publication-like write queued right behind it, before any lane can
    // react to the failure.
    let failing = f.db.run(|_store| -> Result<(), StoreError> {
        std::thread::sleep(std::time::Duration::from_millis(200));
        Err(StoreError::Check(
            "TransactionContext Error: Failed to commit: Failed to rollback transaction. Cannot continue operation".into(),
        ))
    });
    let queued = f.db.run(|store| {
        store.exec_batch("INSERT INTO published_ranges VALUES (10, 10)")?;
        Ok(())
    });
    let (failing, queued) = tokio::join!(failing, queued);
    assert!(failing.is_err());
    let refused = queued.unwrap_err().to_string();
    assert!(
        refused.starts_with("checkpoint connection poisoned"),
        "{refused}"
    );
    assert_eq!(
        solos_data::collector::service::fatal_class(&refused),
        Some("rollback_failed")
    );
    assert!(f.db.poisoned().unwrap().contains("Failed to rollback"));
    // Read the store directly: the queued write never ran.
    let store = f.thread.take().unwrap().join(f.db.clone());
    let ranges = store
        .rows("SELECT count(*) AS n FROM published_ranges", &[])
        .unwrap();
    assert_eq!(ranges[0].int("n"), Some(0));
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&f.root);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_refused_after_an_out_of_memory_commit_reports_the_original_class() {
    let mut f = Fixture::new();
    f.db.guard_fatal();
    let failing = f.db.run(|_store| -> Result<(), StoreError> {
        std::thread::sleep(std::time::Duration::from_millis(100));
        Err(StoreError::Check(
            "TransactionContext Error: Failed to commit: Out of Memory Error: could not allocate block".into(),
        ))
    });
    let queued = f.db.run(|store| store.exec_batch("SELECT 1"));
    let (failing, queued) = tokio::join!(failing, queued);
    assert!(failing.is_err());
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    // The queued job's lane may react first: it still reports the commit failure's class.
    let refused = queued.unwrap_err();
    assert!(lane_error(&f.db, "fixture", &lanes, "backfill", &refused, &hook).await);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![("backfill".to_owned(), "commit_failed")]
    );
    let store = f.thread.take().unwrap().join(f.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&f.root);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unguarded_writer_keeps_running_after_errors() {
    let f = Fixture::new();
    let oom =
        f.db.run(|_store| -> Result<(), StoreError> {
            Err(StoreError::Check(
                "Out of Memory Error: failed to pin block".into(),
            ))
        })
        .await;
    assert!(oom.is_err());
    insert_tx(&f, "after", 10);
    assert!(f.db.poisoned().is_none());
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_fsync_failure_ends_the_lane_and_does_not_retry() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error =
        f.db.run_blocking(move |store| {
            solos_data::fsutil::inject_fsync_failure(solos_data::fsutil::FsyncStep::Any);
            struct Clear;
            impl Drop for Clear {
                fn drop(&mut self) {
                    solos_data::fsutil::clear_fsync_failure();
                }
            }
            let _clear = Clear;
            publish_range(
                store,
                &root,
                100,
                100,
                Some(Checkpoint {
                    key: "W".into(),
                    value: json!({
                        "signature": "sig",
                        "slot": 100,
                        "completedAt": "2026-10-29T00:00:00Z"
                    }),
                    cycle_id: None,
                }),
            )
        })
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    assert_eq!(fatal_class(&message), Some("fsync"));
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &error, &hook).await);
    assert_eq!(*calls.lock().unwrap(), vec![("tail".to_owned(), "fsync")]);
    // Fatal: nothing is written for a retry, and the watermark stays where it was.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_fsync_failure_on_the_temp_file_ends_the_lane_and_does_not_retry() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error = fsync_failure(&f, &root, solos_data::fsutil::FsyncStep::TempFile);
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    assert_eq!(fatal_class(&message), Some("fsync"));
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &error, &hook).await);
    assert_eq!(*calls.lock().unwrap(), vec![("tail".to_owned(), "fsync")]);
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_fsync_failure_on_the_epoch_directory_after_rename_ends_the_lane_and_does_not_retry() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error = fsync_failure(&f, &root, solos_data::fsutil::FsyncStep::DirAfterRename);
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    assert_eq!(fatal_class(&message), Some("fsync"));
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &error, &hook).await);
    assert_eq!(*calls.lock().unwrap(), vec![("tail".to_owned(), "fsync")]);
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
    f.close();
}

/// Run `publish_range` once with an injected fsync failure at `step`, on the writer thread.
fn fsync_failure(f: &Fixture, root: &std::path::Path, step: solos_data::fsutil::FsyncStep) -> solos_data::store::StoreError {
    let root = root.to_path_buf();
    f.db
        .run_blocking(move |store| {
            solos_data::fsutil::inject_fsync_failure(step);
            struct Clear;
            impl Drop for Clear {
                fn drop(&mut self) {
                    solos_data::fsutil::clear_fsync_failure();
                }
            }
            let _clear = Clear;
            publish_range(
                store,
                &root,
                100,
                100,
                Some(Checkpoint {
                    key: "W".into(),
                    value: json!({
                        "signature": "sig",
                        "slot": 100,
                        "completedAt": "2026-10-29T00:00:00Z"
                    }),
                    cycle_id: None,
                }),
            )
        })
        .unwrap_err()
}
