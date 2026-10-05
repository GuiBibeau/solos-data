//! Ported from `tests/pipeline.test.ts`, `tests/history.test.ts`, `tests/ordering-batches.test.ts`,
//! `tests/ordering-throughput.test.ts` and the ordering half of `tests/storage-scale.test.ts`.

mod collector_common;

use collector_common::*;
use serde_json::{Value, json};
use solos_data::collector::catalog::{merge_coverage, write_catalog};
use solos_data::collector::compactor::compact;
use solos_data::collector::config::Lane;
use solos_data::collector::history::backfill_step;
use solos_data::collector::ordering::{join_ordering, order_range};
use solos_data::collector::pipeline::tail_cycle;
use solos_data::collector::reader::query_dataset;
use solos_data::collector::rpc::Rpc;
use solos_data::collector::validation::{assert_publishable, chunk_end, validate_range};
use solos_data::collector::walker::{check_page, make_walk, walk_page, walk_to_end};
use solos_data::collector::writer::{publish_range, recover_files, verify_files};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn blocks(entries: &[(i64, &[&str])]) -> HashMap<i64, Vec<&'static str>> {
    entries
        .iter()
        .map(|(slot, signatures)| {
            (
                *slot,
                signatures
                    .iter()
                    .map(|s| Box::leak(s.to_string().into_boxed_str()) as &str)
                    .collect(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn cursor_and_manifest_commit_together_restart_resumes_before_cursor_overlap_dedupes_sources()
{
    let f = Fixture::new();
    let rpc = FixtureRpc::new(
        vec![
            vec![sig("a", 102), sig("b", 102)],
            vec![sig("c", 101)],
            vec![],
        ],
        HashMap::new(),
    );
    walk_page(
        &rpc,
        &f.db,
        "walk",
        make_walk("program", Lane::Tail, "cycle", 0, 102),
    )
    .await
    .unwrap();
    let stored = f.get("walk").unwrap();
    assert_eq!(stored["before"], "b");
    walk_to_end(
        &rpc,
        &f.db,
        "walk",
        make_walk("program", Lane::Tail, "ignored", 0, 102),
    )
    .await
    .unwrap();
    assert_eq!(rpc.calls_of("getSignaturesForAddress")[1][1]["before"], "b");
    let other = FixtureRpc::new(vec![vec![sig("a", 102)]], HashMap::new());
    walk_page(
        &other,
        &f.db,
        "other",
        make_walk("market", Lane::Backfill, "overlap", 0, 102),
    )
    .await
    .unwrap();
    let rows = f.rows("SELECT signature, source_addresses FROM signatures ORDER BY signature");
    assert_eq!(rows.len(), 3);
    let addresses = rows[0].get("source_addresses").unwrap().as_array().unwrap();
    assert!(addresses.contains(&json!("program")) && addresses.contains(&json!("market")));
    let bad = FixtureRpc::new(vec![vec![sig("bad", 103)]], HashMap::new());
    let mut resumed: solos_data::collector::walker::Walk = serde_json::from_value(stored).unwrap();
    resumed.done = false;
    assert!(
        walk_page(&bad, &f.db, "walk", resumed)
            .await
            .unwrap_err()
            .to_string()
            .contains("slots increased")
    );
    assert_eq!(f.get("walk").unwrap()["before"], "c");
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_slot_boundary_fully_crosses_floor_block_indexes_control_ordering() {
    let f = Fixture::new();
    let rpc = Arc::new(FixtureRpc::new(
        vec![vec![sig("b", 100)], vec![sig("a", 100), sig("old", 99)]],
        blocks(&[(100, &["other", "a", "b"])]),
    ));
    walk_to_end(
        rpc.as_ref(),
        &f.db,
        "boundary",
        make_walk("program", Lane::Tail, "boundary", 100, 100),
    )
    .await
    .unwrap();
    assert_eq!(f.rows("SELECT * FROM signatures").len(), 2);
    insert_tx(&f, "a", 100);
    insert_tx(&f, "b", 100);
    let dyn_rpc: Arc<dyn Rpc> = rpc.clone();
    order_range(dyn_rpc, &f.db, Lane::Tail, 100, 100, 256)
        .await
        .unwrap();
    let rows = f.rows("SELECT signature, tx_index FROM transactions ORDER BY tx_index");
    assert_eq!(
        rows.iter()
            .map(|r| (
                r.str("signature").unwrap().to_owned(),
                r.int("tx_index").unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)]
    );
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 100, 100))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(true))
    );
    assert!(
        join_ordering(100, &["missing".into()], &["other".into()])
            .unwrap_err()
            .to_string()
            .contains("missing")
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_cycle_holds_watermark_retry_publishes_files_before_advancing_restart_removes_orphans()
 {
    let f = Fixture::new();
    f.set("H0", json!({ "signature": "a", "slot": 100 }));
    let rpc = Arc::new(FixtureRpc::new(
        vec![vec![sig("b", 102), sig("a", 102)], vec![], vec![]],
        blocks(&[(102, &["a", "b"])]),
    ));
    insert_tx(&f, "a", 102);
    insert_tx(&f, "b", 102);
    rpc.fail_block.store(true, Ordering::Relaxed);
    let dyn_rpc: Arc<dyn Rpc> = rpc.clone();
    assert!(
        tail_cycle(dyn_rpc.clone(), &f.db, &f.config, "programdata")
            .await
            .is_err()
    );
    assert!(f.get("W").is_none());
    rpc.fail_block.store(false, Ordering::Relaxed);
    tail_cycle(dyn_rpc, &f.db, &f.config, "programdata")
        .await
        .unwrap();
    assert_eq!(f.get("W").unwrap()["slot"], 102);
    assert!(
        f.db.run_blocking(verify_files)
            .unwrap()
            .int("files")
            .unwrap()
            >= 3
    );
    let orphan = f.root.join("orphan.parquet");
    std::fs::write(&orphan, "not registered").unwrap();
    std::fs::write(f.root.join("interrupted.tmp"), "incomplete").unwrap();
    let root = f.root.clone();
    f.db.run_blocking(move |store| recover_files(store, &root))
        .unwrap();
    assert!(!orphan.exists());
    assert_eq!(
        f.db.run_blocking(verify_files).unwrap().get("ok"),
        Some(&Value::Bool(true))
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_fetch_cannot_be_published_catch_up_chunks_are_bounded() {
    let f = Fixture::new();
    let rpc = FixtureRpc::new(vec![vec![sig("a", 100)]], HashMap::new());
    walk_page(
        &rpc,
        &f.db,
        "one",
        make_walk("program", Lane::Tail, "one", 0, 100),
    )
    .await
    .unwrap();
    assert!(
        assert_publishable(&solos_data::jsonout::Obj::new().with("ok", false))
            .unwrap_err()
            .to_string()
            .contains("watermark")
    );
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 0, 100))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(false))
    );
    insert_tx(&f, "a", 101);
    f.exec("UPDATE transactions SET single_in_slot=true");
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 0, 100))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(false)),
        "a matching signature in the wrong slot cannot satisfy coverage"
    );
    assert_eq!(chunk_end(100, 50_000, 20_000), 20_099);
    assert_eq!(chunk_end(49_999, 50_000, 20_000), 50_000);
    let page: Vec<solos_data::collector::walker::Signature> = vec![
        serde_json::from_value(sig("a", 100)).unwrap(),
        serde_json::from_value(sig("a", 100)).unwrap(),
    ];
    assert!(
        check_page(&page, &make_walk("p", Lane::Tail, "c", 0, 100))
            .unwrap_err()
            .to_string()
            .contains("repeated")
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_provider_node_cannot_move_an_existing_watermark_backward() {
    let f = Fixture::new();
    f.set("W", json!({ "signature": "existing", "slot": 200 }));
    let rpc: Arc<dyn Rpc> = Arc::new(FixtureRpc::new(vec![], HashMap::new()));
    tail_cycle(rpc, &f.db, &f.config, "programdata")
        .await
        .unwrap();
    assert_eq!(f.get("W").unwrap()["slot"], 200);
    assert!(f.get("tail-active").is_none());
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_dedupes_publications_and_picks_corrected_ordering_hash_corruption_fails_v7() {
    let f = Fixture::new();
    let rpc = FixtureRpc::new(vec![vec![sig("a", 100)]], HashMap::new());
    walk_page(
        &rpc,
        &f.db,
        "one",
        make_walk("program", Lane::Tail, "one", 0, 100),
    )
    .await
    .unwrap();
    insert_tx(&f, "a", 100);
    for index in 0..10 {
        f.exec(&format!(
            "UPDATE transactions SET tx_index={index}, single_in_slot=false"
        ));
        let root = f.root.clone();
        f.db.run_blocking(move |store| publish_range(store, &root, 100, 100, None))
            .unwrap();
    }
    let root = f.root.clone();
    f.db.run_blocking(move |store| compact(store, &root))
        .unwrap();
    let files = f.rows("SELECT * FROM files WHERE status='active' AND table_name='transactions'");
    assert_eq!(files.len(), 1);
    let path = files[0].str("path").unwrap().to_owned();
    let rows = f.rows(&format!(
        "SELECT signature, tx_index FROM read_parquet('{path}')"
    ));
    assert_eq!(
        (
            rows[0].str("signature").unwrap(),
            rows[0].int("tx_index").unwrap()
        ),
        ("a", 9)
    );
    std::fs::write(&path, "corrupt").unwrap();
    assert!(
        f.db.run_blocking(verify_files)
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn recent_batches_reuse_a_partial_manifest_and_publish_before_eof_failed_restarted_batches_hold_coverage()
 {
    let mut f = Fixture::new();
    f.config.backfill_chunk_slots = 2;
    f.set("H0", json!({ "signature": "a", "slot": 100 }));
    f.set("W", json!({ "signature": "live", "slot": 200 }));
    f.set(
        "backfill",
        json!({ "phase": "fetching", "next": 1, "ceiling": 100 }),
    );
    let rows = vec![
        sig("b", 100),
        sig("a", 100),
        sig("c", 99),
        sig("e", 98),
        sig("d", 98),
        sig("f", 97),
        sig("old", 90),
    ];
    walk_page(
        &FixtureRpc::new(vec![rows.clone()], HashMap::new()),
        &f.db,
        "walk/backfill",
        make_walk(&f.config.program_id, Lane::Backfill, "old", 0, 100),
    )
    .await
    .unwrap();
    let mut done =
        serde_json::to_value(make_walk("programdata", Lane::Backfill, "data", 0, 100)).unwrap();
    done["done"] = Value::Bool(true);
    f.set("walk/programdata", done);
    for row in &rows {
        insert_tx(
            &f,
            row["signature"].as_str().unwrap(),
            row["slot"].as_i64().unwrap(),
        );
    }
    let rpc = Arc::new(FixtureRpc::new(
        vec![],
        blocks(&[(100, &["other", "a", "b"]), (98, &["d", "e"])]),
    ));
    let dyn_rpc: Arc<dyn Rpc> = rpc.clone();
    let first = backfill_step(dyn_rpc.clone(), &f.db, &f.config, "programdata", None)
        .await
        .unwrap();
    assert_eq!(first.oldest_published_slot, Some(99));
    assert_eq!(first.next, 98);
    assert_eq!(first.direction, "newest-first");
    assert_eq!(first.published_transactions, 3);
    assert_eq!(f.get("walk/backfill").unwrap()["done"], false);
    assert_eq!(rpc.calls_of("getSignaturesForAddress").len(), 0);
    assert_eq!(f.get("backfill/oldest-first").unwrap()["next"], 1);
    assert_eq!(f.get("W").unwrap()["slot"], 200);
    assert_eq!(f.scalar("SELECT count(*) AS n FROM dataset_watermark"), "3");
    let root = f.root.clone();
    f.db.run_blocking(move |store| write_catalog(store, &root))
        .unwrap();
    let query: Value = serde_json::from_str(
        &query_dataset(
            &f.root,
            "SELECT count(*) AS n, min(slot) AS oldest FROM transactions",
        )
        .unwrap()
        .to_json(),
    )
    .unwrap();
    assert_eq!(query["rows"], json!([{ "n": "3", "oldest": "99" }]));
    assert_eq!(query["coverage"], json!([{ "from": 99, "to": 100 }]));
    rpc.fail_block.store(true, Ordering::Relaxed);
    assert!(
        backfill_step(dyn_rpc.clone(), &f.db, &f.config, "programdata", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("block failure")
    );
    assert_eq!(f.get("backfill").unwrap()["next"], 98);
    // An old deployment has file registrations but no published-range registry.
    f.exec("DELETE FROM published_ranges");
    f.reopen();
    let root = f.root.clone();
    f.db.run_blocking(move |store| recover_files(store, &root))
        .unwrap();
    assert_eq!(f.scalar("SELECT count(*) AS n FROM dataset_watermark"), "3");
    rpc.fail_block.store(false, Ordering::Relaxed);
    let second = backfill_step(dyn_rpc, &f.db, &f.config, "programdata", None)
        .await
        .unwrap();
    assert_eq!(second.oldest_published_slot, Some(97));
    assert_eq!(second.next, 96);
    let root = f.root.clone();
    let catalog =
        f.db.run_blocking(move |store| write_catalog(store, &root))
            .unwrap();
    assert_eq!(
        catalog.get("coverage"),
        Some(&json!([{ "from": 97, "to": 100 }]))
    );
    let query: Value = serde_json::from_str(
        &query_dataset(&f.root, "SELECT count(*) AS n FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(query["rows"], json!([{ "n": "6" }]));
    assert_eq!(
        f.db.run_blocking(verify_files).unwrap().get("ok"),
        Some(&Value::Bool(true))
    );
    assert_eq!(f.get("W").unwrap()["slot"], 200);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn reverse_batches_strictly_cross_split_slot_pages_and_stop_at_the_launch_boundary() {
    let mut f = Fixture::new();
    f.config.backfill_chunk_slots = 2;
    f.set("H0", json!({ "signature": "a", "slot": 100 }));
    for (signature, slot) in [("a", 100), ("b", 99), ("c", 99), ("old", 98)] {
        insert_tx(&f, signature, slot);
    }
    let rpc = Arc::new(FixtureRpc::new(
        vec![
            vec![sig("a", 100), sig("b", 99)],
            vec![sig("c", 99), sig("old", 98)],
            vec![],
            vec![],
        ],
        blocks(&[(99, &["b", "c"])]),
    ));
    let dyn_rpc: Arc<dyn Rpc> = rpc.clone();
    let first = backfill_step(dyn_rpc.clone(), &f.db, &f.config, "programdata", None)
        .await
        .unwrap();
    assert_eq!(first.oldest_published_slot, Some(99));
    assert_eq!(first.phase, "fetching");
    assert_eq!(
        f.scalar("SELECT count(*) AS n FROM slot_order WHERE slot=99"),
        "2"
    );
    let second = backfill_step(dyn_rpc.clone(), &f.db, &f.config, "programdata", None)
        .await
        .unwrap();
    assert_eq!(second.phase, "complete");
    assert_eq!(second.oldest_published_slot, Some(98));
    assert_eq!(f.get("S_start").unwrap(), 98);
    let requests = rpc.call_count();
    backfill_step(dyn_rpc, &f.db, &f.config, "programdata", None)
        .await
        .unwrap();
    assert_eq!(rpc.call_count(), requests);
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn published_reader_dedupes_overlap_revisions_and_leaves_disjoint_coverage_explicit() {
    let f = Fixture::new();
    walk_page(
        &FixtureRpc::new(vec![vec![sig("a", 100)]], HashMap::new()),
        &f.db,
        "one",
        make_walk("program", Lane::Tail, "one", 0, 100),
    )
    .await
    .unwrap();
    insert_tx(&f, "a", 100);
    let root = f.root.clone();
    f.db.run_blocking(move |store| publish_range(store, &root, 100, 100, None))
        .unwrap();
    f.exec("UPDATE transactions SET tx_index=7, single_in_slot=false");
    let root = f.root.clone();
    f.db.run_blocking(move |store| {
        publish_range(store, &root, 100, 100, None)?;
        publish_range(store, &root, 200, 200, None)?;
        write_catalog(store, &root).map(|_| ())
    })
    .unwrap();
    let query: Value = serde_json::from_str(
        &query_dataset(&f.root, "SELECT signature, tx_index FROM transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(query["rows"], json!([{ "signature": "a", "tx_index": 7 }]));
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(f.root.join("catalog.json")).unwrap())
            .unwrap();
    assert_eq!(
        catalog["coverage"],
        json!([{ "from": 100, "to": 100 }, { "from": 200, "to": 200 }])
    );
    assert_eq!(
        merge_coverage(vec![(3, 4), (1, 3), (10, 12)]),
        vec![(1, 4), (10, 12)]
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn batched_ordering_preserves_finalized_indexes_bounded_restart_and_cached_repairs() {
    let f = Fixture::new();
    let mut signatures = Vec::new();
    let mut block_map: HashMap<i64, Vec<&'static str>> = HashMap::new();
    for slot in (100..=165).rev() {
        let a: &'static str = Box::leak(format!("a-{slot}").into_boxed_str());
        signatures.push(sig(a, slot));
        insert_tx(&f, a, slot);
        if slot != 101 {
            let b: &'static str = Box::leak(format!("b-{slot}").into_boxed_str());
            signatures.push(sig(b, slot));
            insert_tx(&f, b, slot);
            block_map.insert(slot, vec!["unrelated", b, a]);
        }
    }
    walk_page(
        &FixtureRpc::new(vec![signatures], HashMap::new()),
        &f.db,
        "manifest",
        make_walk("program", Lane::Backfill, "batch", 0, 165),
    )
    .await
    .unwrap();
    let mut failing_blocks = block_map.clone();
    failing_blocks.insert(164, vec![]);
    let failing: Arc<dyn Rpc> = Arc::new(FixtureRpc::new(vec![], failing_blocks));
    assert!(
        order_range(failing, &f.db, Lane::Backfill, 100, 165, 64)
            .await
            .unwrap_err()
            .to_string()
            .contains("missing")
    );
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 100, 163))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 164, 165))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(false))
    );
    // Cached block data must repair transactions whose ordering write was not retained.
    f.exec("UPDATE transactions SET tx_index=NULL WHERE slot=100");
    let retry = Arc::new(FixtureRpc::new(vec![], block_map));
    let dyn_retry: Arc<dyn Rpc> = retry.clone();
    order_range(dyn_retry, &f.db, Lane::Backfill, 100, 165, 256)
        .await
        .unwrap();
    let mut asked: Vec<i64> = retry
        .calls_of("getBlock")
        .iter()
        .map(|p| p[0].as_i64().unwrap())
        .collect();
    asked.sort_unstable();
    assert_eq!(asked, vec![164, 165]);
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 100, 165))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(true))
    );
    let rows =
        f.rows("SELECT signature, tx_index FROM transactions WHERE slot=100 ORDER BY tx_index");
    assert_eq!(
        rows.iter()
            .map(|r| (
                r.str("signature").unwrap().to_owned(),
                r.int("tx_index").unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("b-100".to_owned(), 1), ("a-100".to_owned(), 2)]
    );
    let single = f.rows("SELECT tx_index, single_in_slot FROM transactions WHERE slot=101");
    assert_eq!(single[0].get("tx_index"), Some(&Value::Null));
    assert_eq!(single[0].get("single_in_slot"), Some(&Value::Bool(true)));
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_a_256_slot_range_uses_one_durable_commit_and_retains_block_index_checks() {
    let f = Fixture::new();
    f.exec("INSERT INTO signatures SELECT 'a-'||i,i,i,'null',['p'],'backfill','c','fixture' FROM range(256) r(i)");
    f.exec("INSERT INTO signatures SELECT 'b-'||i,i,i,'null',['p'],'backfill','c','fixture' FROM range(256) r(i)");
    f.exec("INSERT INTO transactions SELECT signature,slot,slot,NULL,NULL,'null',5000,10,'AA==','{}','{}','backfill','fixture','fixture',NULL FROM signatures");
    let block_map: HashMap<i64, Vec<&'static str>> = (0..256)
        .map(|i| {
            (
                i,
                vec![
                    "unrelated",
                    Box::leak(format!("b-{i}").into_boxed_str()) as &str,
                    Box::leak(format!("a-{i}").into_boxed_str()) as &str,
                ],
            )
        })
        .collect();
    let before =
        f.db.run_blocking(|store| {
            Ok(store
                .timings()
                .get("COMMIT:")
                .and_then(|t| t.get("calls"))
                .and_then(Value::as_u64)
                .unwrap_or(0))
        })
        .unwrap();
    let rpc: Arc<dyn Rpc> = Arc::new(FixtureRpc::new(vec![], block_map));
    order_range(rpc, &f.db, Lane::Backfill, 0, 255, 256)
        .await
        .unwrap();
    let after =
        f.db.run_blocking(|store| {
            Ok(store
                .timings()
                .get("COMMIT:")
                .and_then(|t| t.get("calls"))
                .and_then(Value::as_u64)
                .unwrap_or(0))
        })
        .unwrap();
    assert_eq!(after - before, 1);
    assert_eq!(
        f.db.run_blocking(|store| validate_range(store, 0, 255))
            .unwrap()
            .get("ok"),
        Some(&Value::Bool(true))
    );
    f.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_repairs_only_transactions_in_the_validated_slot_range() {
    let f = Fixture::new();
    walk_page(
        &FixtureRpc::new(vec![vec![sig("a", 100), sig("b", 100)]], HashMap::new()),
        &f.db,
        "manifest",
        make_walk("p", Lane::Tail, "c", 0, 100),
    )
    .await
    .unwrap();
    insert_tx(&f, "a", 100);
    insert_tx(&f, "b", 101);
    let rpc: Arc<dyn Rpc> = Arc::new(FixtureRpc::new(vec![], blocks(&[(100, &["a", "b"])])));
    order_range(rpc, &f.db, Lane::Tail, 100, 100, 256)
        .await
        .unwrap();
    assert_eq!(
        f.rows("SELECT tx_index FROM transactions WHERE signature='b'")[0].get("tx_index"),
        Some(&Value::Null)
    );
    f.close();
}
