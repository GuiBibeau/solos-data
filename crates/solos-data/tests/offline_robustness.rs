//! Offline robustness checks from issue #60: range-checker proptests, a typed ENOSPC failpoint,
//! fatal fsync errors (EIO included), a clock-skew fixture through decode and the range check, and
//! a guard that fails a test whose fsutil writes leave its temp dir.
//!
//! Everything runs on temporary directories with scripted inputs; nothing touches a provider URL.

mod collector_common;
mod common;

use collector_common::{Fixture, insert_tx};
use common::{golden_fixtures, raw_fixture, tempdir};
use proptest::prelude::*;
use serde_json::{Value, json};
use solos_data::collector::lanes::Lanes;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::service::{FatalHook, fatal_class, lane_error};
use solos_data::collector::validation::{assert_publishable, validate_range};
use solos_data::collector::writer::{Checkpoint, publish_range};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::{self, file_hash};
use solos_data::store::{Store, StoreError};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn open_collector(root: &Path) -> Store {
    Store::open(root, SCHEMA).unwrap()
}

fn insert_manifest(store: &mut Store, signature: &str, slot: i64) {
    store
        .exec(
            "INSERT INTO signatures VALUES (?, ?, ?, 'null', [], 'tail', 'c', 'fixture')",
            &[&signature.to_owned(), &slot, &slot],
        )
        .unwrap();
}

fn insert_collected(
    store: &mut Store,
    signature: &str,
    slot: i64,
    tx_index: Option<i64>,
    single: bool,
) {
    store
        .exec(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture', 'fixture', NULL)",
            &[&signature.to_owned(), &slot, &slot, &tx_index, &single],
        )
        .unwrap();
}

/// Random manifest entries (`slot` in `[0, len)`, `present` says the transaction was fetched) and
/// random unlisted transactions. The range is `[1000, 1000 + len)`.
fn manifest_and_extras() -> impl Strategy<Value = (usize, Vec<(u32, bool)>, Vec<u32>)> {
    (1usize..=24).prop_flat_map(|len| {
        (
            Just(len),
            proptest::collection::vec((0u32..len as u32, any::<bool>()), 0..40),
            proptest::collection::vec(0u32..len as u32, 0..16),
        )
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn random_gap_and_overlap_sets_never_let_the_watermark_advance(
        (len, manifest, extras) in manifest_and_extras(),
    ) {
        let from: i64 = 1000;
        let to = from + len as i64 - 1;
        let root = tempdir("prop");
        let mut store = open_collector(&root);

        // The manifest: every entry is listed, fetched or not. Duplicate slots are fine (one
        // signature per entry). `present == false` is a gap in the fetched set.
        let mut counts: HashMap<i64, usize> = HashMap::new();
        let mut by_slot: HashMap<i64, Vec<String>> = HashMap::new();
        for (i, (slot_index, present)) in manifest.iter().enumerate() {
            let slot = from + i64::from(*slot_index);
            *counts.entry(slot).or_default() += 1;
            insert_manifest(&mut store, &format!("m{i}"), slot);
            if *present {
                by_slot.entry(slot).or_default().push(format!("m{i}"));
            }
        }
        // Fetched transactions carry a valid V6 ordering when the manifest is complete, so a
        // complete set is the only case that may publish.
        for (slot, names) in &by_slot {
            let total = counts[slot];
            for (index, name) in names.iter().enumerate() {
                let (tx_index, single) = if total == 1 {
                    (None, true)
                } else {
                    (Some(index as i64), false)
                };
                insert_collected(&mut store, name, *slot, tx_index, single);
            }
        }
        // Unlisted transactions: collected rows the manifest does not list.
        for (i, slot_index) in extras.iter().enumerate() {
            insert_collected(&mut store, &format!("u{i}"), from + i64::from(*slot_index), None, true);
        }

        let report = validate_range(&mut store, from, to).unwrap();
        let publishable = assert_publishable(&report).is_ok();
        let has_gap = manifest.iter().any(|(_, present)| !present);
        let has_overlap = !extras.is_empty();

        if has_gap || has_overlap {
            prop_assert!(!publishable, "watermark advanced over a gap or overlap: {report:?}");
            prop_assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
            if has_gap {
                prop_assert!(report.int("missing").unwrap_or(0) > 0, "{report:?}");
            }
            if has_overlap {
                prop_assert!(report.int("unlisted").unwrap_or(0) > 0, "{report:?}");
            }
        } else {
            prop_assert!(publishable, "a complete range must publish: {report:?}");
            prop_assert_eq!(report.get("ok"), Some(&Value::Bool(true)));
        }
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A shrunk regression case for a gap: one listed signature whose transaction never arrived.
#[test]
fn a_single_missing_transaction_keeps_the_watermark() {
    let root = tempdir("gap");
    let mut store = open_collector(&root);
    insert_manifest(&mut store, "m0", 1000);
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
    insert_collected(&mut store, "u0", 1000, None, true);
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

/// A disk full during publication (ENOSPC on the first durable fsync) is a typed `StoreError::Io`
/// and never moves the watermark.
#[tokio::test(flavor = "multi_thread")]
async fn an_enospc_publication_keeps_the_watermark_and_is_typed() {
    let f = Fixture::new();
    insert_tx(&f, "sig", 100);
    let root = f.root.clone();
    let error =
        f.db.run_blocking(move |store| {
            fsutil::inject_enospc(true);
            struct Clear;
            impl Drop for Clear {
                fn drop(&mut self) {
                    fsutil::inject_enospc(false);
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
        })
        .unwrap_err();
    match &error {
        StoreError::Io(io) => {
            assert_eq!(io.kind(), std::io::ErrorKind::StorageFull, "{io}");
            assert_eq!(io.raw_os_error(), Some(nix::libc::ENOSPC), "{io}");
        }
        other => panic!("expected a typed StoreError::Io, got {other}"),
    }
    assert_eq!(f.scalar("SELECT count(*) AS n FROM published_ranges"), "0");
    assert!(f.get("W").is_none());
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
        f.db.run_blocking(move |store| {
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
    // Three transactions: slot 199 has the newest block time, slot 200 holds two transactions
    // (a duplicate slot) with an older block time.
    let rows = [
        ("sig-a", 199i64, 900i64, None, true),
        ("sig-b", 200, 300, Some(0i64), false),
        ("sig-c", 200, 300, Some(1i64), false),
    ];

    // Range check: the manifest lists every transaction; out-of-order block times are not part of
    // V1/V6 and must not fail the check.
    let collector = tempdir("skew-collector");
    let mut store = open_collector(&collector);
    for (signature, slot, _, _, _) in &rows {
        insert_manifest(&mut store, signature, *slot);
    }
    for (signature, slot, _, tx_index, single) in &rows {
        insert_collected(&mut store, signature, *slot, *tx_index, *single);
    }
    let report = validate_range(&mut store, 199, 200).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
    assert!(assert_publishable(&report).is_ok());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&collector);

    // Decode: the same rows as a raw Parquet source, all using one valid Phoenix wire.
    let fixture = raw_fixture(&golden_fixtures()[0]);
    let raw = rows
        .iter()
        .map(|(signature, slot, block_time, tx_index, single)| {
            json!({
                "signature": signature,
                "slot": slot,
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

    let decoded = query_decoded(
        &data_dir,
        "SELECT signature, slot::INTEGER AS slot, block_time::INTEGER AS block_time
         FROM decoded_transactions ORDER BY slot, signature",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&decoded.to_json()).unwrap();
    let got = value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["signature"].as_str().unwrap().to_owned(),
                row["slot"].as_i64().unwrap(),
                row["block_time"].as_i64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        got,
        vec![
            ("sig-a".to_owned(), 199, 900),
            ("sig-b".to_owned(), 200, 300),
            ("sig-c".to_owned(), 200, 300),
        ],
        "decode must preserve out-of-order block times and duplicate slots"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The write guard fails a test whose fsutil writes leave its temp dir.
#[test]
fn the_write_guard_fails_a_write_outside_its_temp_dir() {
    let root = tempdir("guard");
    let inside = root.join("inside.txt");
    fsutil::write_durable(&inside, b"ok").unwrap();
    assert!(inside.exists());

    fsutil::install_write_guard(&root);
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            fsutil::clear_write_guard();
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
