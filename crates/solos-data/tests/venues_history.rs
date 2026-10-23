//! The venues history lane (ADR-0013) offline: its files land in their own tree under the augment
//! root, the live venues lane's recovery leaves them alone, the augment views read both lanes,
//! and `venues history check` passes on consistent batches and fails on a hole or a batch whose
//! stored rows differ from its counts.

use serde_json::{Value, json};
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::series::Ctx;
use solos_data::db::Db;
use solos_data::venues::history::{check::check, config, run::cursor_value};
use solos_data::venues::store::{self, Buffer};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("venues-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn batch(from: i64, to: i64, txs: i64, prices: i64) -> Value {
    json!({ "from_slot": from, "to_slot": to, "manifest": txs, "failed": 0, "bulk": txs,
            "single": 0, "unlisted": 0, "txs": txs, "events": 0, "undecoded": 0, "unknown": 0,
            "instructions": 0, "unparsed": 0, "prices": prices, "pyth_applied": prices,
            "oldest_block_time": 1_775_000_000, "at_ms": 1_792_310_000_000_i64 + from })
}

fn tx(signature: &str, slot: i64) -> Value {
    json!({ "signature": signature, "slot": slot, "block_time": 1_775_000_000 + slot,
            "tx_b64": "AA==", "meta_json": "{}", "fetched_via": "archive" })
}

fn price(signature: &str, slot: i64, publish: i64) -> Value {
    json!({ "at_ms": (1_775_000_000 + slot) * 1000, "context_slot": slot, "symbol": "SOL",
            "feed_id": "ef", "account": "A", "price": 1, "conf": 1, "exponent": -8,
            "publish_time": publish, "prev_publish_time": publish - 1, "ema_price": 1,
            "ema_conf": 1, "posted_slot": slot, "verification": "Full", "price_f": 1.0,
            "conf_f": 1.0, "signature": signature, "shard": 0 })
}

#[test]
fn history_files_have_their_own_tree_and_the_check_compares_them_with_the_batches() {
    let root = tempdir();
    let mut store = ledger::open(&root, Lane::History).unwrap();
    ledger::recover(&mut store, &root, Lane::History).unwrap();
    let (db, thread) = Db::spawn(store);
    let ctx = Ctx {
        http: Http::new(1.0).unwrap(),
        db: db.clone(),
        root: root.clone(),
        lane: Lane::History,
        start: chrono::NaiveDate::from_ymd_opt(2026, 10, 23).unwrap(),
        now_ms: 0,
        stop: Arc::new(AtomicBool::new(false)),
        disk_budget_bytes: 0,
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let buffer = Buffer::default();
    let cursor = |next: i64| cursor_value(next, &["P".to_owned()], &[Default::default()], false);
    buffer.push_cycle(
        "jupiter",
        vec![
            ("batches", vec![batch(900, 1_000, 2, 0)]),
            ("transactions", vec![tx("a", 950), tx("b", 999)]),
        ],
        "history/jupiter/cursor",
        cursor(899),
    );
    buffer.push_cycle(
        "flash",
        vec![("batches", vec![batch(900, 1_000, 0, 0)])],
        "history/flash/cursor",
        cursor(899),
    );
    buffer.push_cycle(
        "pyth",
        vec![
            ("batches", vec![batch(900, 1_000, 5, 2)]),
            (
                "prices",
                vec![
                    price("p", 950, 1_775_000_900),
                    price("q", 951, 1_775_000_960),
                ],
            ),
        ],
        "history/pyth/cursor",
        cursor(899),
    );
    let flushed = runtime.block_on(store::flush(&ctx, &buffer, 1_792_310_000_000));
    assert_eq!(flushed.errors, 0);
    let root_c = root.clone();
    ctx.db
        .run_blocking(move |s| ledger::write_catalog(s, &root_c, Lane::History))
        .unwrap();
    // The files are under venues-history/tables, and the live lane's recovery never sees them.
    let tree = root.join("venues-history/tables/jupiter/transactions/ALL");
    assert_eq!(std::fs::read_dir(&tree).unwrap().count(), 1);
    let mut live = ledger::open(&root, Lane::Venues).unwrap();
    ledger::recover(&mut live, &root, Lane::Venues).unwrap();
    live.close().unwrap();
    assert_eq!(std::fs::read_dir(&tree).unwrap().count(), 1);
    // The augment views read the history lane too.
    let rows = solos_data::augment::query::query_augment(
        &root,
        "SELECT count(*) AS n FROM jupiter_transactions",
    )
    .unwrap()
    .to_value();
    assert_eq!(rows["rows"][0]["n"], "2");

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/venues-history.json"
    );
    let cfg = config::load(Some(path)).unwrap();
    let sources = cfg.resolve(&cfg.venues().unwrap()).unwrap();
    let report = check(&root, &sources).unwrap().to_value();
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["sources"]["pyth"]["stored"]["stored"], "2");

    // A batch below with a one-slot hole and a stored count short of its own: both found.
    buffer.push_cycle(
        "jupiter",
        vec![
            ("batches", vec![batch(800, 898, 3, 0)]),
            ("transactions", vec![tx("c", 850), tx("d", 851)]),
        ],
        "history/jupiter/cursor",
        cursor(799),
    );
    runtime.block_on(store::flush(&ctx, &buffer, 1_792_310_000_000));
    let root_c = root.clone();
    ctx.db
        .run_blocking(move |s| ledger::write_catalog(s, &root_c, Lane::History))
        .unwrap();
    let report = check(&root, &sources).unwrap().to_value();
    assert_eq!(report["ok"], false);
    let jupiter = &report["sources"]["jupiter"];
    assert_eq!(jupiter["coverage"]["gaps_or_overlaps"], "1");
    assert_eq!(jupiter["stored"]["mismatched"], "1");
    assert_eq!(report["sources"]["flash"]["ok"], true);

    drop(ctx);
    let store = thread.join(db);
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_flash_v1_transaction_decodes_through_the_fallback_idls() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/data/venues-history-fixtures.json"
    ))
    .unwrap();
    let fx: Value = serde_json::from_str(&text).unwrap();
    let tx = &fx["transactions"][0];
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/venues-history.json"
    );
    let cfg = config::load(Some(path)).unwrap();
    let sources = cfg.resolve(&cfg.venues().unwrap()).unwrap();
    let flash = sources.iter().find(|s| s.name == "flash").unwrap();
    let meta: Value = serde_json::from_str(tx["meta_json"].as_str().unwrap()).unwrap();
    let at = solos_data::venues::events::TxMeta {
        signature: tx["signature"].as_str().unwrap(),
        slot: tx["slot"].as_i64().unwrap(),
        block_time: tx["block_time"].as_i64(),
    };
    let extract = |idl: &solos_data::venues::idl::Idl| {
        solos_data::venues::events::extract(
            tx["tx_b64"].as_str().unwrap(),
            &meta,
            &flash.addresses[0],
            idl,
            &at,
        )
        .unwrap()
    };
    // The program's current on-chain IDL alone does not decode the v1 events.
    let newest =
        solos_data::venues::idl::Idl::parse(&std::fs::read_to_string(&flash.idls[0]).unwrap())
            .unwrap();
    let alone = extract(&newest);
    assert!(alone.undecoded + alone.unknown > 0);
    // With the fallbacks every event decodes, with the names the history run recorded.
    let rows = extract(&flash.idl().unwrap());
    assert_eq!(rows.undecoded + rows.unknown, 0);
    let names: Vec<&str> = rows
        .events
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    let expected: Vec<&str> = tx["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, expected);
    assert!(rows.events.iter().all(|e| e["fields"].is_object()));
}
