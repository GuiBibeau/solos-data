//! The spot lane (ADR-0014) offline: a batch's files land in the lane's own tree with its
//! cursor, the augment views read them, a redone batch under another range retires the old
//! files, and `spot check` passes on consistent batches and fails on a hole or a block mismatch.

use serde_json::{Value, json};
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::query::query_augment;
use solos_data::spot::archive::{Block, Stream};
use solos_data::spot::check::check;
use solos_data::spot::dex::{Dex, Programs};
use solos_data::spot::rows::{Rows, named};
use solos_data::spot::run::{Blocks, CURSOR_KEY, Done, Manifest, register_batch, write_batch};
use solos_data::spot::tx::from_rpc;

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("spot-lane-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The fixtures' rows whose slots fall in `[lo, hi]`, with one block per such slot.
fn batch(lo: u64, hi: u64, listed_extra: u64) -> Done {
    let doc: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/data/spot-fixtures.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let programs = Programs::all();
    let mut stream = Stream {
        rows: Rows::new(),
        ..Stream::default()
    };
    for t in doc["transactions"].as_array().unwrap() {
        let slot = t["slot"].as_u64().unwrap();
        if !(lo..=hi).contains(&slot) {
            continue;
        }
        let tx = from_rpc(t["tx_b64"].as_str().unwrap(), &t["meta"], slot, 0).unwrap();
        stream
            .rows
            .add_tx(&tx, &programs, &named(&tx.keys, &programs));
        stream.blocks.insert(
            slot,
            Block {
                time: Some(1_790_000_000),
                executed: 1,
            },
        );
        stream.delivered.insert(slot, 1);
    }
    let streamed = stream.blocks.len() as u64;
    Done {
        stream,
        blocks: Blocks {
            streamed,
            listed: streamed + listed_extra,
            transactions: streamed,
        },
        window: (lo, lo),
        manifests: vec![(
            Dex::PumpSwap,
            Manifest {
                listed: 3,
                missing: 0,
                unlisted: 1,
            },
        )],
        seconds: 1.0,
    }
}

fn commit(
    store: &mut solos_data::store::Store,
    root: &std::path::Path,
    lo: u64,
    hi: u64,
    done: &Done,
) -> u64 {
    let files = write_batch("1GB", root, lo, hi, done).unwrap();
    let cursor = json!({ "next": lo - 1, "done": false });
    let retired = register_batch(store, root, lo, hi, &files, &cursor).unwrap();
    ledger::write_catalog(store, root, Lane::Spot).unwrap();
    retired
}

#[test]
fn spot_files_have_their_own_tree_and_the_check_compares_them_with_the_batches() {
    let root = tempdir();
    let mut store = ledger::open(&root, Lane::Spot).unwrap();
    ledger::recover(&mut store, &root, Lane::Spot).unwrap();
    // The fixtures span slots 371M to 459M: two adjacent batches that tile it.
    let (top, mid, low) = (459_999_999u64, 430_000_000u64, 370_000_000u64);
    let upper = batch(mid, top, 0);
    let lower = batch(low, mid - 1, 0);
    let swaps = upper.stream.rows.swaps.len() + lower.stream.rows.swaps.len();
    assert!(upper.stream.rows.swaps.len() > 10 && lower.stream.rows.swaps.len() > 10);
    commit(&mut store, &root, mid, top, &upper);
    commit(&mut store, &root, low, mid - 1, &lower);
    assert!(
        root.join(format!(
            "spot-history/tables/spot/swaps/ALL/{:010}-{:010}.parquet",
            mid, top
        ))
        .is_file()
    );
    assert_eq!(
        ledger::get_progress(&store, CURSOR_KEY).unwrap().unwrap()["next"],
        low - 1
    );
    let report = check(&root).unwrap();
    assert_eq!(
        report.get("ok"),
        Some(&Value::Bool(true)),
        "{}",
        report.to_json()
    );
    assert_eq!(report.get("swaps"), Some(&json!(swaps)));
    assert_eq!(report.get("unlisted"), Some(&json!(2)));
    // The augment views read the lane's catalog.
    let out = query_augment(
        &root,
        "SELECT count(*) AS n, count(DISTINCT program) AS p FROM spot_swaps",
    )
    .unwrap();
    assert_eq!(out.to_value()["rows"][0]["n"], json!(swaps.to_string()));

    // A batch redone under another range retires the overlapping files; leaving its old lower
    // half uncovered is a hole the check reports.
    let redone = batch(440_000_000, top, 0);
    assert_eq!(commit(&mut store, &root, 440_000_000, top, &redone), 3);
    let report = check(&root).unwrap();
    assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
    assert_eq!(
        report.get("holesOrOverlaps"),
        Some(&json!([[low, mid - 1]]))
    );

    // A batch whose archive blocks differ from the RPC list fails the check.
    let root2 = tempdir();
    let mut store2 = ledger::open(&root2, Lane::Spot).unwrap();
    commit(&mut store2, &root2, mid, top, &batch(mid, top, 1));
    let report = check(&root2).unwrap();
    assert_eq!(report.get("blockMismatches"), Some(&json!([mid])));
    store.close().unwrap();
    store2.close().unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::remove_dir_all(&root2).unwrap();
}
