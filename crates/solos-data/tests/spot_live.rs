//! The forward spot lane (ADR-0015) offline: a batch fetched from `getBlock` answers built from
//! real archive transactions gives the rows the archive gave, passes the history's block and
//! manifest checks, lands in the lane's own tree, and `spot live check` checks it and the seam
//! with the history; a block the provider does not serve or a manifest signature the blocks lack
//! fails the batch.

use serde_json::{Value, json};
use solos_data::augment::ledger::{self, Lane};
use solos_data::collector::config::Lane as RpcLane;
use solos_data::collector::rpc::{Rpc, RpcFailure, RpcFuture, RpcResponse};
use solos_data::spot::check::check_live;
use solos_data::spot::config::LiveConfig;
use solos_data::spot::dex::{ALL, Programs};
use solos_data::spot::live::{LIVE, batch};
use solos_data::spot::rows::{liquidity_json, named, swap_json};
use solos_data::spot::run::{register_batch_to, write_batch_to};
use solos_data::spot::tx::{b58, from_rpc};
use std::collections::BTreeMap;

const LO: u64 = 430_000_000;
const HI: u64 = 430_008_999;

fn fixtures() -> Vec<Value> {
    let doc: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/data/spot-fixtures.json"
        ))
        .unwrap(),
    )
    .unwrap();
    doc["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| (LO..=HI).contains(&t["slot"].as_u64().unwrap()))
        .cloned()
        .collect()
}

/// A provider serving one block per fixture slot, each holding that slot's fixture
/// transactions, and each program's manifest of them.
struct Blocks {
    blocks: BTreeMap<u64, Vec<Value>>,
    /// Program address to its manifest rows, newest first.
    manifests: BTreeMap<String, Vec<Value>>,
    /// A listed slot whose block the provider does not serve.
    unserved: Option<u64>,
}

impl Blocks {
    fn new(txs: &[Value]) -> Blocks {
        let programs = Programs::all();
        let mut blocks: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        let mut manifests: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for t in txs {
            let slot = t["slot"].as_u64().unwrap();
            blocks.entry(slot).or_default().push(t.clone());
            let tx = from_rpc(t["tx_b64"].as_str().unwrap(), &t["meta"], slot, 0).unwrap();
            for dex in named(&tx.keys, &programs) {
                manifests.entry(b58(&dex.key())).or_default().push(json!({
                    "signature": tx.signature_b58(), "slot": slot, "err": t["meta"]["err"],
                }));
            }
        }
        for rows in manifests.values_mut() {
            rows.sort_by_key(|r| std::cmp::Reverse(r["slot"].as_u64().unwrap()));
        }
        Blocks {
            blocks,
            manifests,
            unserved: None,
        }
    }

    fn answer(&self, method: &str, params: &Value) -> Option<Value> {
        Some(match method {
            "getBlocks" => {
                let (a, b) = (params[0].as_u64()?, params[1].as_u64()?);
                json!(
                    self.blocks
                        .range(a..=b)
                        .map(|(s, _)| *s)
                        .collect::<Vec<_>>()
                )
            }
            "getBlock" => {
                let slot = params[0].as_u64()?;
                if Some(slot) == self.unserved {
                    return Some(Value::Null);
                }
                let txs: Vec<Value> = self.blocks.get(&slot)?.iter().map(|t| {
                    json!({ "transaction": [t["tx_b64"], "base64"], "meta": t["meta"], "version": 0 })
                }).collect();
                json!({ "blockTime": 1_790_000_000 + slot as i64 - LO as i64, "transactions": txs })
            }
            // The seed finds nothing above the window: the walk starts at the tip.
            "getTransactionsForAddress" => json!({ "data": [] }),
            "getSignaturesForAddress" => {
                if params[1].get("before").is_some() {
                    return Some(json!([]));
                }
                let mut rows = self
                    .manifests
                    .get(params[0].as_str()?)
                    .cloned()
                    .unwrap_or_default();
                // One older row below the window ends every walk there.
                rows.push(json!({ "signature": "1111", "slot": 1, "err": null }));
                json!(rows)
            }
            _ => return None,
        })
    }
}

impl Rpc for Blocks {
    fn call<'a>(&'a self, method: &'a str, params: Value, _lane: RpcLane) -> RpcFuture<'a> {
        let answer = self.answer(method, &params);
        Box::pin(async move {
            let result = answer.ok_or_else(|| RpcFailure {
                method: method.to_owned(),
                code: -32601,
                throttled: false,
            })?;
            let raw = json!({ "jsonrpc": "2.0", "id": 1, "result": result }).to_string();
            Ok(RpcResponse { result, raw })
        })
    }

    fn provider_name(&self) -> &str {
        "fixture"
    }
}

fn config(root: &std::path::Path) -> LiveConfig {
    let mut config = LiveConfig::parse(
        r#"{"dataDir": "x", "start": 430000000, "batchSlots": 9000, "manifestSlots": 9000,
            "creditsPerSecond": 10, "concurrency": 4}"#,
    )
    .unwrap();
    config.data_dir = root.to_path_buf();
    config
}

fn without_index(mut row: Value) -> Value {
    row.as_object_mut().unwrap().remove("tx_index");
    row
}

fn sorted(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(Value::to_string);
    rows
}

#[test]
fn a_batch_from_blocks_gives_the_archive_rows_and_checks_like_the_history() {
    let root = std::env::temp_dir().join(format!("spot-live-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut txs = fixtures();
    assert!(txs.len() >= 10);
    // One more transaction, failed: counted, no rows, listed as failed by the manifest.
    let mut failed = txs[0].clone();
    failed["meta"]["err"] = json!({ "InstructionError": [0, "Custom"] });
    failed["slot"] = json!(LO + 7);
    let rpc = Blocks::new(&{
        let mut all = txs.clone();
        all.push(failed);
        all
    });
    let programs = Programs::all();
    let config = config(&root);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (done, fetched) = runtime
        .block_on(batch(&rpc, &programs, &config, (LO, HI)))
        .unwrap();
    assert_eq!(done.blocks.streamed, rpc.blocks.len() as u64);
    assert_eq!(done.blocks.listed, done.blocks.streamed);
    assert_eq!(fetched.blocks, done.blocks.streamed);
    assert!(fetched.bytes > 0);
    assert_eq!(done.window, (LO, HI));
    assert!(done.manifests.iter().all(|(_, m)| m.missing == 0));
    let failed_count: u64 = done.stream.rows.counts.iter().map(|c| c.failed).sum();
    assert!(failed_count >= 1);
    // The same rows as the archive decoded, but for the transaction index (a fixture block
    // holds only the captured transactions).
    txs.sort_by_key(|t| t["slot"].as_u64().unwrap());
    let expected_swaps: Vec<Value> = txs
        .iter()
        .flat_map(|t| t["swaps"].as_array().unwrap().clone())
        .map(without_index)
        .collect();
    let expected_liquidity: Vec<Value> = txs
        .iter()
        .flat_map(|t| t["liquidity"].as_array().unwrap().clone())
        .map(without_index)
        .collect();
    let swaps: Vec<Value> = done
        .stream
        .rows
        .swaps
        .iter()
        .map(swap_json)
        .map(without_index)
        .collect();
    let liquidity: Vec<Value> = done
        .stream
        .rows
        .liquidity
        .iter()
        .map(liquidity_json)
        .map(without_index)
        .collect();
    assert!(!expected_swaps.is_empty());
    assert_eq!(sorted(swaps), sorted(expected_swaps));
    assert_eq!(sorted(liquidity), sorted(expected_liquidity));
    assert_eq!(done.stream.rows.counts.len(), ALL.len());

    // Files in the lane's own tree, cursor at the next slot, and the check passes.
    let mut store = ledger::open(&root, Lane::SpotLive).unwrap();
    ledger::recover(&mut store, &root, Lane::SpotLive).unwrap();
    let files = write_batch_to(Lane::SpotLive, "1GB", &root, (LO, HI), &done).unwrap();
    let cursor = json!({ "next": HI + 1 });
    register_batch_to(LIVE, &mut store, &root, (LO, HI), &files, &cursor).unwrap();
    ledger::write_catalog(&mut store, &root, Lane::SpotLive).unwrap();
    assert!(
        root.join(format!(
            "spot/tables/spot/swaps/ALL/{LO:010}-{HI:010}.parquet"
        ))
        .is_file()
    );
    assert_eq!(
        ledger::get_progress(&store, LIVE.cursor_key)
            .unwrap()
            .unwrap()["next"],
        HI + 1
    );
    let report = check_live(&root, LO).unwrap().to_value();
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["seam"]["meets"], Value::Null);
    // The wrong start is a seam failure.
    assert_eq!(check_live(&root, LO + 1).unwrap().to_value()["ok"], false);
    // A history whose newest batch ends on the slot before: the lanes meet.
    let history = |to: u64| {
        std::fs::write(
            Lane::Spot.catalog_path(&root),
            json!({ "files": [{ "dataset": "batches", "period": format!("{:010}-{:010}", 420_000_000, to) }] })
                .to_string(),
        )
        .unwrap();
    };
    history(LO - 1);
    let report = check_live(&root, LO).unwrap().to_value();
    assert_eq!(report["seam"]["meets"], true, "{report}");
    assert_eq!(report["ok"], true);
    // A gap of one slot fails.
    history(LO - 2);
    let report = check_live(&root, LO).unwrap().to_value();
    assert_eq!(report["seam"]["meets"], false);
    assert_eq!(report["ok"], false);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn an_unserved_block_or_an_unfetched_manifest_signature_fails_the_batch() {
    let root = std::env::temp_dir();
    let txs = fixtures();
    let programs = Programs::all();
    let config = config(&root);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut rpc = Blocks::new(&txs);
    rpc.unserved = rpc.blocks.keys().next().copied();
    let error = runtime
        .block_on(batch(&rpc, &programs, &config, (LO, HI)))
        .err()
        .unwrap();
    assert!(error.contains("answered no block"), "{error}");
    let mut rpc = Blocks::new(&txs);
    let (program, rows) = rpc.manifests.iter_mut().next().unwrap();
    let program = program.clone();
    rows.insert(
        0,
        json!({ "signature": "5".repeat(88), "slot": LO + 8_000, "err": null }),
    );
    let error = runtime
        .block_on(batch(&rpc, &programs, &config, (LO, HI)))
        .err()
        .unwrap();
    assert!(error.contains("were not streamed"), "{program}: {error}");
}
