//! Offline robustness checks from issue #60: range-checker proptests, typed write/fsync ENOSPC
//! failpoints, fatal fsync errors (EIO included), a clock-skew fixture through decode and the
//! range check, and a guard that fails a test whose fsutil helper writes leave its temp dir.
//!
//! Everything runs on temporary directories with scripted inputs; nothing touches a provider URL.

mod collector_common;
mod common;

use collector_common::{Fixture, insert_tx};
use common::{count, golden_fixtures, raw_fixture, tempdir};
use proptest::prelude::*;
use serde_json::{Value, json};
use solos_data::collector::lanes::Lanes;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::service::{FatalHook, fatal_class, lane_error};
use solos_data::collector::validation::{assert_publishable, validate_range};
use solos_data::collector::writer::{Checkpoint, publish_range, recover_files};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::{self, file_hash};
use solos_data::store::{Store, StoreError};
use std::path::Path;
use std::sync::{Arc, Mutex};

fn open_collector(root: &Path) -> Store {
    Store::open(root, SCHEMA).unwrap()
}

fn insert_manifest(store: &mut Store, signature: &str, slot: i64, block_time: i64) {
    store
        .exec(
            "INSERT INTO signatures VALUES (?, ?, ?, 'null', [], 'tail', 'c', 'fixture')",
            &[&signature.to_owned(), &slot, &block_time],
        )
        .unwrap();
}

fn insert_collected(
    store: &mut Store,
    signature: &str,
    slot: i64,
    block_time: i64,
    tx_index: Option<i64>,
    single: bool,
) {
    store
        .exec(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture', 'fixture', NULL)",
            &[&signature.to_owned(), &slot, &block_time, &tx_index, &single],
        )
        .unwrap();
}

/// A complete, valid range: slot 1000 holds one single transaction, slot 1001 holds two indexed
/// transactions. Every manifest row has its fetched transaction and the V6 ordering is correct.
fn insert_complete_valid_range(store: &mut Store) {
    insert_manifest(store, "a", 1000, 1000);
    insert_collected(store, "a", 1000, 1000, None, true);
    insert_manifest(store, "b", 1001, 1001);
    insert_collected(store, "b", 1001, 1001, Some(0), false);
    insert_manifest(store, "c", 1001, 1001);
    insert_collected(store, "c", 1001, 1001, Some(1), false);
}

fn assert_ok(store: &mut Store, from: i64, to: i64) {
    let report = validate_range(store, from, to).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
    assert!(assert_publishable(&report).is_ok());
}

/// Random complete ranges: every signature is fetched, every fetched row is listed, and the V6
/// ordering is valid, so each one must publish.
fn complete_valid_range() -> impl Strategy<Value = (i64, i64, Vec<(i64, usize)>)> {
    (1usize..=8).prop_flat_map(|len| {
        let counts = proptest::collection::vec(1usize..=3usize, len);
        (Just(1000i64), counts).prop_map(|(from, counts)| {
            let slots: Vec<(i64, usize)> = counts
                .into_iter()
                .enumerate()
                .map(|(i, n)| (from + i as i64, n))
                .collect();
            (from, from + slots.len() as i64 - 1, slots)
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn every_complete_valid_range_publishes((from, to, slots) in complete_valid_range()) {
        let root = tempdir("prop-valid");
        let mut store = open_collector(&root);
        for (slot, n) in &slots {
            for i in 0..*n {
                let signature = format!("{slot}-{i}");
                insert_manifest(&mut store, &signature, *slot, *slot);
                let (tx_index, single) = if *n == 1 {
                    (None, true)
                } else {
                    (Some(i as i64), false)
                };
                insert_collected(&mut store, &signature, *slot, *slot, tx_index, single);
            }
        }
        let report = validate_range(&mut store, from, to).unwrap();
        prop_assert!(
            assert_publishable(&report).is_ok(),
            "a complete range must publish: {report:?}"
        );
        prop_assert_eq!(report.get("ok"), Some(&Value::Bool(true)));
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// The range check's `ok` is the conjunction of the four counters. Each mutation below breaks
/// exactly one of them in an otherwise valid range, so removing that counter's term from `ok`
/// (or from `assert_publishable`) must turn the case green again.
#[test]
fn each_range_check_mutation_fails_with_its_counter() {
    let from = 1000i64;
    let to = 1001i64;

    // A listed signature whose transaction never arrived.
    {
        let root = tempdir("mut-missing-fetch");
        let mut store = open_collector(&root);
        insert_complete_valid_range(&mut store);
        assert_ok(&mut store, from, to);
        store
            .exec_batch("DELETE FROM transactions WHERE signature='b'")
            .unwrap();
        let report = validate_range(&mut store, from, to).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(false)), "{report:?}");
        assert!(report.int("missing").unwrap_or(0) > 0, "{report:?}");
        assert_eq!(report.int("unlisted").unwrap_or(0), 0, "{report:?}");
        assert!(assert_publishable(&report).is_err());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    // A fetched transaction whose tx_b64 is NULL: `missing` is the only failing counter, so this
    // is the case that catches a `missing == 0` term dropped from `ok`.
    {
        let root = tempdir("mut-null-b64");
        let mut store = open_collector(&root);
        insert_complete_valid_range(&mut store);
        assert_ok(&mut store, from, to);
        store
            .exec_batch("UPDATE transactions SET tx_b64=NULL WHERE signature='a'")
            .unwrap();
        let report = validate_range(&mut store, from, to).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(false)), "{report:?}");
        assert_eq!(report.int("missing"), Some(1), "{report:?}");
        assert_eq!(report.int("unlisted"), Some(0), "{report:?}");
        assert_eq!(report.int("unordered"), Some(0), "{report:?}");
        assert_eq!(report.int("badOrderingSlots"), Some(0), "{report:?}");
        assert!(assert_publishable(&report).is_err());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    // A collected transaction the manifest does not list.
    {
        let root = tempdir("mut-unlisted");
        let mut store = open_collector(&root);
        insert_complete_valid_range(&mut store);
        assert_ok(&mut store, from, to);
        insert_collected(&mut store, "u", 1001, 1001, None, true);
        let report = validate_range(&mut store, from, to).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(false)), "{report:?}");
        assert_eq!(report.int("unlisted"), Some(1), "{report:?}");
        assert_eq!(report.int("missing"), Some(0), "{report:?}");
        assert_eq!(report.int("unordered"), Some(0), "{report:?}");
        assert_eq!(report.int("badOrderingSlots"), Some(0), "{report:?}");
        assert!(assert_publishable(&report).is_err());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    // A broken V6 ordering: a row with a null `single_in_slot` while the slot stays indexed.
    {
        let root = tempdir("mut-unordered");
        let mut store = open_collector(&root);
        insert_complete_valid_range(&mut store);
        assert_ok(&mut store, from, to);
        store
            .exec_batch("UPDATE transactions SET single_in_slot=NULL WHERE signature='b'")
            .unwrap();
        let report = validate_range(&mut store, from, to).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(false)), "{report:?}");
        assert_eq!(report.int("unordered"), Some(1), "{report:?}");
        assert_eq!(report.int("missing"), Some(0), "{report:?}");
        assert_eq!(report.int("unlisted"), Some(0), "{report:?}");
        assert_eq!(report.int("badOrderingSlots"), Some(0), "{report:?}");
        assert!(assert_publishable(&report).is_err());
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    // A collected row one slot past `to` is outside the range: the boundary must exclude it, so
    // the check stays green and neither `unlisted` nor `fetched` changes.
    {
        let root = tempdir("mut-outside");
        let mut store = open_collector(&root);
        insert_complete_valid_range(&mut store);
        assert_ok(&mut store, from, to);
        insert_collected(&mut store, "out", to + 1, to + 1, None, true);
        let report = validate_range(&mut store, from, to).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
        assert_eq!(report.int("unlisted"), Some(0), "{report:?}");
        assert_eq!(report.int("missing"), Some(0), "{report:?}");
        assert_eq!(report.int("fetched"), Some(3), "{report:?}");
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A shrunk regression case for a gap: one listed signature whose transaction never arrived.
#[test]
fn a_single_missing_transaction_keeps_the_watermark() {
    let root = tempdir("gap");
    let mut store = open_collector(&root);
    insert_manifest(&mut store, "m0", 1000, 1000);
    let report = validate_range(&mut store, 1000, 1000).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
    assert_eq!(report.int("missing"), Some(1));
    assert_eq!(report.int("unlisted"), Some(0));
    assert!(assert_publishable(&report).is_err());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

/// A shrunk regression case for an overlap: one collected transaction the manifest does not list.
#[test]
fn a_single_unlisted_transaction_keeps_the_watermark() {
    let root = tempdir("overlap");
    let mut store = open_collector(&root);
    insert_collected(&mut store, "u0", 1000, 1000, None, true);
    let report = validate_range(&mut store, 1000, 1000).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
    assert_eq!(report.int("unlisted"), Some(1));
    assert_eq!(report.int("missing"), Some(0));
    assert!(assert_publishable(&report).is_err());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

fn watermark(slot: i64) -> Value {
    json!({
        "signature": "sig",
        "slot": slot,
        "completedAt": "2026-10-29T00:00:00Z"
    })
}

/// No `.tmp` or `.parquet` files remain under `root/staging`.
fn assert_no_staging_files(root: &Path) {
    let staging = root.join("staging");
    if !staging.exists() {
        return;
    }
    let mut files = Vec::new();
    fn walk(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, files);
            } else {
                files.push(path);
            }
        }
    }
    walk(&staging, &mut files);
    assert!(files.is_empty(), "orphan staging files: {files:?}");
}

/// A disk full during a durable write (before the body is written) is a typed, retryable error:
/// it keeps its OS error code and is not wrapped as `fsync failed`, so `fatal_class` does not
/// treat it as fatal.
#[test]
fn a_write_enospc_is_typed_and_retryable() {
    let root = tempdir("write-enospc");
    let path = root.join("file.txt");
    fsutil::inject_write_enospc(true);
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            fsutil::inject_write_enospc(false);
        }
    }
    let _clear = Clear;
    let error = fsutil::write_durable(&path, b"body").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::StorageFull, "{error}");
    assert_eq!(error.raw_os_error(), Some(nix::libc::ENOSPC), "{error}");
    assert_eq!(fatal_class(&error.to_string()), None, "{error}");
    let _ = std::fs::remove_dir_all(&root);
}

/// A disk full during publication (ENOSPC on the durable fsync) is fatal like production: the
/// error is wrapped as `fsync failed: ...`, the lane exits, and the watermark never moves.
#[tokio::test(flavor = "multi_thread")]
async fn an_enospc_fsync_during_publication_is_fatal_and_keeps_the_watermark() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error =
        f.db.run_blocking({
            let root = root.clone();
            move |store| {
                fsutil::inject_fsync_enospc(true);
                struct Clear;
                impl Drop for Clear {
                    fn drop(&mut self) {
                        fsutil::inject_fsync_enospc(false);
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
                        value: watermark(100),
                        cycle_id: None,
                    }),
                )
            }
        })
        .unwrap_err();
    match &error {
        StoreError::Io(io) => {
            assert_eq!(io.kind(), std::io::ErrorKind::StorageFull, "{io}");
        }
        other => panic!("expected a typed StoreError::Io, got {other}"),
    }
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    assert!(message.contains("No space left on device"), "{message}");
    assert_eq!(fatal_class(&message), Some("fsync"));
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &error, &hook).await);
    assert_eq!(*calls.lock().unwrap(), vec![("tail".to_owned(), "fsync")]);
    // Fatal: nothing is recorded for a retry, and the watermark stays where it was.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
    // The failed publication registered no files, and recovery on restart removes the orphan
    // `.tmp` the failed COPY left behind, leaving no Parquet under staging.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM files"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM file_bounds"), "0");
    f.db.run_blocking(move |store| recover_files(store, &root))
        .unwrap();
    assert_no_staging_files(&f.root);
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

/// An fsync that returns EIO is fatal and is never retried: the lane exits and the process
/// restarts from the watermark (ADR-0016).
#[tokio::test(flavor = "multi_thread")]
async fn an_eio_fsync_ends_the_lane_and_does_not_retry() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error =
        f.db.run_blocking({
            let root = root.clone();
            move |store| {
                fsutil::inject_fsync_eio(true);
                struct Clear;
                impl Drop for Clear {
                    fn drop(&mut self) {
                        fsutil::inject_fsync_eio(false);
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
                        value: watermark(100),
                        cycle_id: None,
                    }),
                )
            }
        })
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    assert!(message.contains("Input/output error"), "{message}");
    assert_eq!(fatal_class(&message), Some("fsync"));
    let lanes = Lanes::new(true, true);
    let (hook, calls) = recorder();
    assert!(lane_error(&f.db, "fixture", &lanes, "tail", &error, &hook).await);
    assert_eq!(*calls.lock().unwrap(), vec![("tail".to_owned(), "fsync")]);
    // Fatal: nothing is recorded for a retry, and the watermark stays where it was.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM errors"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
    // The failed publication registered no files, and recovery on restart removes the orphan
    // `.tmp` the failed COPY left behind, leaving no Parquet under staging.
    assert_eq!(f.scalar("SELECT count(*) AS n FROM files"), "0");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM file_bounds"), "0");
    f.db.run_blocking(move |store| recover_files(store, &root))
        .unwrap();
    assert_no_staging_files(&f.root);
    f.close();
}

/// A raw transactions Parquet file with explicit ordering columns, for the clock-skew fixture.
fn write_raw_parquet(conn: &duckdb::Connection, path: &Path, rows: &[Value]) {
    let data = Value::Array(rows.to_vec()).to_string();
    conn.execute(
        &format!(
            "COPY (SELECT value->>'signature' AS signature, (value->>'slot')::BIGINT AS slot,
      (value->>'block_time')::BIGINT AS block_time, (value->>'tx_index')::INTEGER AS tx_index,
      (value->>'single_in_slot')::BOOLEAN AS single_in_slot, value->>'tx_b64' AS tx_b64,
      value->>'meta_json' AS meta_json, NULL::VARCHAR AS terminal_error, 'null' AS err
      FROM json_each(?::JSON)) TO '{}' (FORMAT PARQUET)",
            path.display()
        ),
        [data],
    )
    .unwrap();
}

/// Out-of-order block times and duplicate slots flow through decode and the range check without
/// corruption or a false watermark advance.
#[test]
fn clock_skew_decodes_and_passes_the_range_check() {
    // The golden wire carries a SlotContext event for this slot, so every row must keep it or the
    // decoder quarantines the row as "event context validation failed".
    let golden = golden_fixtures();
    let slot = golden[0]["slot"].as_i64().unwrap();
    // Three transactions share the real slot, two of them indexed as a duplicate slot, with their
    // block times out of order (newest first).
    let rows = [
        ("sig-a", slot, 900i64, Some(0i64), false),
        ("sig-b", slot, 300, Some(1i64), false),
        ("sig-c", slot, 300, Some(2i64), false),
    ];

    // Range check: the manifest lists every transaction; out-of-order block times are not part of
    // V1/V6 and must not fail the check.
    let collector = tempdir("skew-collector");
    let mut store = open_collector(&collector);
    for (signature, s, block_time, _, _) in &rows {
        insert_manifest(&mut store, signature, *s, *block_time);
    }
    for (signature, s, block_time, tx_index, single) in &rows {
        insert_collected(&mut store, signature, *s, *block_time, *tx_index, *single);
    }
    let report = validate_range(&mut store, slot, slot).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
    assert!(assert_publishable(&report).is_ok());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&collector);

    // Decode: the same rows as a raw Parquet source, all using one valid Phoenix wire.
    let fixture = raw_fixture(&golden[0]);
    let raw = rows
        .iter()
        .map(|(signature, s, block_time, tx_index, single)| {
            json!({
                "signature": signature,
                "slot": s,
                "block_time": block_time,
                "tx_index": tx_index,
                "single_in_slot": single,
                "tx_b64": fixture.tx_b64.clone(),
                "meta_json": fixture.meta_json.clone(),
            })
        })
        .collect::<Vec<Value>>();
    let root = tempdir("skew-decode");
    let raw_dir = root.join("raw");
    let data_dir = root.join("decoded");
    std::fs::create_dir_all(raw_dir.join("staging")).unwrap();
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path = raw_dir.join("staging/skew.parquet");
    write_raw_parquet(&conn, &path, &raw);
    let catalog = json!({
        "at": "fixture",
        "files": [{
            "path": path.to_string_lossy(),
            "table_name": "transactions",
            "row_count": raw.len(),
            "sha256": file_hash(&path).unwrap(),
            "created_at": "2026-10-04T03:00:00Z"
        }]
    });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
    let config = DecoderConfig {
        batch_size: 100,
        poll_ms: 100,
        checkpoint_interval_seconds: 0,
        ..DecoderConfig::new(raw_dir.clone(), data_dir.clone())
    };
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    run_decoder(&config, true, stop).unwrap();

    // Every row decoded (not quarantined) with its signature, slot and out-of-order block time
    // preserved.
    let transactions = query_decoded(
        &data_dir,
        "SELECT signature, slot::INTEGER AS slot, block_time::INTEGER AS block_time, status, error_count
         FROM decoded_transactions ORDER BY signature",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&transactions.to_json()).unwrap();
    let got = value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["signature"].as_str().unwrap().to_owned(),
                row["slot"].as_i64().unwrap(),
                row["block_time"].as_i64().unwrap(),
                row["status"].as_str().unwrap().to_owned(),
                row["error_count"].as_i64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        got,
        vec![
            ("sig-a".to_owned(), slot, 900, "decoded".to_owned(), 0),
            ("sig-b".to_owned(), slot, 300, "decoded".to_owned(), 0),
            ("sig-c".to_owned(), slot, 300, "decoded".to_owned(), 0),
        ],
        "decode must preserve out-of-order block times and duplicate slots"
    );

    // Nothing is quarantined, and the wire's twelve events decode for each signature.
    assert_eq!(
        count(&data_dir, "SELECT count(*) AS n FROM decode_errors"),
        0,
        "decode_errors must be empty"
    );
    let events = query_decoded(
        &data_dir,
        "SELECT signature, count(*) AS n FROM events GROUP BY signature ORDER BY signature",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&events.to_json()).unwrap();
    let by_signature = value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let n = &row["n"];
            let n = n
                .as_i64()
                .unwrap_or_else(|| n.as_str().unwrap().parse().unwrap());
            (row["signature"].as_str().unwrap().to_owned(), n)
        })
        .collect::<Vec<(String, i64)>>();
    assert_eq!(
        by_signature,
        vec![
            ("sig-a".to_owned(), 12),
            ("sig-b".to_owned(), 12),
            ("sig-c".to_owned(), 12),
        ],
        "each signature must decode the golden fixture's twelve events"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The fsutil write guard fails a test whose fsutil helper writes leave its temp dir. It is a
/// narrow assertion on the fsutil helpers only; it does not cover `sync_path`, plain `std::fs`,
/// renames, or DuckDB COPY/temp writes.
#[test]
fn the_write_guard_fails_a_write_outside_its_temp_dir() {
    let root = tempdir("guard");
    let inside = root.join("inside.txt");
    fsutil::write_durable(&inside, b"ok").unwrap();
    assert!(inside.exists());

    fsutil::install_fsutil_write_guard(&root);
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            fsutil::clear_fsutil_write_guard();
        }
    }
    let _clear = Clear;

    fsutil::write_durable(&root.join("still-inside.txt"), b"ok").unwrap();
    let outside = std::env::temp_dir().join(format!("solos-guard-out-{}", uuid::Uuid::new_v4()));
    let panic = std::panic::catch_unwind(|| {
        let _ = fsutil::write_durable(&outside, b"bad");
    });
    assert!(
        panic.is_err(),
        "a write outside the temp dir must fail the test"
    );
    let _ = std::fs::remove_dir_all(&root);
}
