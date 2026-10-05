//! Ported from `tests/bulk.test.ts` (the fetch tests; the envelope test lives in `solana-wire`).

mod collector_common;

use collector_common::*;
use serde_json::{Value, json};
use solos_data::collector::bulk::bulk_fetch_range;
use solos_data::collector::config::Lane;
use solos_data::collector::fetcher::fetch_range;
use solos_data::collector::rpc::Rpc;
use solos_data::collector::walker::{make_walk, walk_page};
use solos_data::collector::writer::{publish_range, verify_files};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn tx(slot: i64, byte: u8) -> Value {
    json!({ "slot": slot, "blockTime": slot, "transaction": [wire(1, byte), "base64"], "meta": { "err": null, "fee": 5000 } })
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_fetch_skips_hydrated_overlap_slots_while_validation_retains_the_full_manifest() {
    let f = Fixture::new();
    let old = wire_signature(&wire(1, 7));
    let recent = wire_signature(&wire(1, 8));
    walk_page(
        &FixtureRpc::new(vec![vec![sig(&recent, 100), sig(&old, 90)]], HashMap::new()),
        &f.db,
        "manifest",
        make_walk(&f.config.program_id, Lane::Tail, "cycle", 0, 100),
    )
    .await
    .unwrap();
    insert_tx(&f, &old, 90);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = Arc::clone(&calls);
    let provider: Arc<dyn Rpc> = Arc::new(ClosureRpc::new(move |method, params| {
        calls2.fetch_add(1, Ordering::Relaxed);
        assert_eq!(method, "getTransactionsForAddress");
        assert_eq!(
            params[1]["filters"]["slot"],
            json!({ "gte": 100, "lte": 100 })
        );
        Ok(json!({ "data": [tx(100, 8)], "paginationToken": null }))
    }));
    fetch_range(provider.clone(), &f.db, &f.config, Lane::Tail, 90, 100)
        .await
        .unwrap();
    fetch_range(provider, &f.db, &f.config, Lane::Tail, 90, 100)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(f.rows("SELECT * FROM transactions").len(), 2);
    assert_eq!(f.rows("SELECT * FROM signatures").len(), 2);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_pages_checkpoint_raw_response_and_cursor_with_deduped_transactions_restart_resumes_token()
 {
    let f = Fixture::new();
    let signature = wire_signature(&wire(1, 7));
    walk_page(
        &FixtureRpc::new(vec![vec![sig(&signature, 100)]], HashMap::new()),
        &f.db,
        "manifest",
        make_walk(&f.config.program_id, Lane::Tail, "cycle", 0, 100),
    )
    .await
    .unwrap();
    let first = json!({ "data": [{ "slot": 100, "blockTime": 100, "transaction": [wire(1, 7), "base64"], "version": 1, "meta": { "err": { "InstructionError": [0, "Custom"] }, "fee": 5000, "computeUnitsConsumed": 10 } }], "paginationToken": "next" });
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts2 = Arc::clone(&attempts);
    let provider = Arc::new(ClosureRpc::new(move |_method, params| {
        let n = attempts2.fetch_add(1, Ordering::Relaxed) + 1;
        assert_eq!(params[1]["maxSupportedTransactionVersion"], 1);
        if n == 1 {
            return Ok(first.clone());
        }
        if n == 2 {
            return Err("crash after first durable page".into());
        }
        assert_eq!(params[1]["paginationToken"], "next");
        Ok(json!({ "data": [], "paginationToken": null }))
    }));
    assert!(
        bulk_fetch_range(provider.as_ref(), &f.db, &f.config, Lane::Tail, 100, 100)
            .await
            .unwrap_err()
            .to_string()
            .contains("crash")
    );
    let rows = f.rows("SELECT * FROM transactions");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("err").is_some_and(|e| !e.is_null()));
    assert_eq!(f.get("bulk/tail/100/100").unwrap()["token"], "next");
    bulk_fetch_range(provider.as_ref(), &f.db, &f.config, Lane::Tail, 100, 100)
        .await
        .unwrap();
    assert_eq!(f.rows("SELECT * FROM transactions").len(), 1);
    let root = f.root.clone();
    f.db.run_blocking(move |store| publish_range(store, &root, 100, 100, None))
        .unwrap();
    assert_eq!(
        f.db.run_blocking(verify_files).unwrap().int("files"),
        Some(3)
    );
    assert_eq!(f.rows("SELECT * FROM rpc_pages").len(), 2);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_bulk_windows_drain_on_failure_and_a_narrower_retry_fills_every_manifest_gap() {
    let mut f = Fixture::new();
    f.config.bulk_fetch_window_slots = 2;
    let signatures: Vec<Value> = [103, 102, 101, 100]
        .iter()
        .map(|slot| sig(&wire_signature(&wire(1, *slot as u8)), *slot))
        .collect();
    walk_page(
        &FixtureRpc::new(vec![signatures], HashMap::new()),
        &f.db,
        "manifest",
        make_walk(&f.config.program_id, Lane::Backfill, "cycle", 0, 103),
    )
    .await
    .unwrap();
    let fail = Arc::new(AtomicBool::new(true));
    let other_finished = Arc::new(AtomicBool::new(false));
    let (fail2, other2) = (Arc::clone(&fail), Arc::clone(&other_finished));
    let provider: Arc<dyn Rpc> = Arc::new(ClosureRpc::new(move |method, params| {
        assert_eq!(method, "getTransactionsForAddress");
        let gte = params[1]["filters"]["slot"]["gte"].as_i64().unwrap();
        let lte = params[1]["filters"]["slot"]["lte"].as_i64().unwrap();
        if !params[1]["paginationToken"].is_null() {
            return Err("interrupted first window".into());
        }
        if fail2.load(Ordering::Relaxed) && gte == 100 {
            return Ok(json!({ "data": [tx(100, 100)], "paginationToken": "next" }));
        }
        if gte == 102 {
            std::thread::sleep(std::time::Duration::from_millis(20));
            other2.store(true, Ordering::Relaxed);
        }
        Ok(
            json!({ "data": (gte..=lte).map(|s| tx(s, s as u8)).collect::<Vec<_>>(), "paginationToken": null }),
        )
    }));
    assert!(
        fetch_range(provider.clone(), &f.db, &f.config, Lane::Backfill, 100, 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("interrupted")
    );
    assert!(other_finished.load(Ordering::Relaxed));
    assert_eq!(f.rows("SELECT * FROM transactions").len(), 3);
    assert_eq!(f.get("bulk/backfill/102/103").unwrap()["done"], true);
    fail.store(false, Ordering::Relaxed);
    fetch_range(provider, &f.db, &f.config, Lane::Backfill, 100, 103)
        .await
        .unwrap();
    assert_eq!(f.rows("SELECT * FROM transactions").len(), 4);
    assert_eq!(f.rows("SELECT * FROM signatures s LEFT JOIN transactions t USING(signature) WHERE t.signature IS NULL").len(), 0);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_bulk_pages_never_advance_the_durable_cursor_past_committed_raw_responses() {
    let f = Fixture::new();
    let mut signatures: Vec<Value> = [100, 101, 102, 103]
        .iter()
        .map(|slot| sig(&wire_signature(&wire(1, *slot as u8)), *slot))
        .collect();
    signatures.reverse();
    walk_page(
        &FixtureRpc::new(vec![signatures], HashMap::new()),
        &f.db,
        "manifest",
        make_walk(&f.config.program_id, Lane::Tail, "c", 0, 103),
    )
    .await
    .unwrap();
    let fail = Arc::new(AtomicBool::new(true));
    let fail2 = Arc::clone(&fail);
    let provider = ClosureRpc::new(move |_method, params| {
        let index: i64 = params[1]["paginationToken"]
            .as_str()
            .map_or(0, |t| t.parse().unwrap());
        if fail2.load(Ordering::Relaxed) && index == 3 {
            return Err("interrupted buffered pages".into());
        }
        let slot = 100 + index;
        Ok(
            json!({ "data": [tx(slot, slot as u8)], "paginationToken": if index == 3 { Value::Null } else { Value::String((index + 1).to_string()) } }),
        )
    });
    assert!(
        bulk_fetch_range(&provider, &f.db, &f.config, Lane::Tail, 100, 103)
            .await
            .unwrap_err()
            .to_string()
            .contains("interrupted")
    );
    assert_eq!(f.get("bulk/tail/100/103").unwrap()["token"], "1");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "1");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM rpc_pages"), "1");
    fail.store(false, Ordering::Relaxed);
    bulk_fetch_range(&provider, &f.db, &f.config, Lane::Tail, 100, 103)
        .await
        .unwrap();
    assert_eq!(f.scalar("SELECT count(*) AS n FROM transactions"), "4");
    assert_eq!(f.scalar("SELECT count(*) AS n FROM rpc_pages"), "4");
    assert_eq!(f.get("bulk/tail/100/103").unwrap()["done"], true);
    f.close();
}
