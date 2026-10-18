//! The venues lane (ADR-0012) offline: real Jupiter Perps transactions and Jupiter and Flash
//! Trade accounts captured from mainnet decode through the committed on-chain IDLs; a cycle over a
//! fixture provider assembles the manifest, the bulk pages and the single fetches and refuses a
//! window it cannot complete; the hour files merge and the cursor follows the files.

use serde_json::{Value, json};
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane, get_progress};
use solos_data::augment::series::Ctx;
use solos_data::collector::config::Lane as RpcLane;
use solos_data::collector::rpc::{Rpc, RpcFailure, RpcFuture, RpcResponse};
use solos_data::db::Db;
use solos_data::venues::events::{TxMeta, extract};
use solos_data::venues::idl::Idl;
use solos_data::venues::store::{self, Buffer, cursor_key, cursor_value};
use solos_data::venues::walk;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

const PROGRAM: &str = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu";

fn repo(path: &str) -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path),
    )
    .unwrap()
}

fn fixtures() -> Value {
    serde_json::from_str(&repo("tests/data/venues-fixtures.json")).unwrap()
}

fn idl(file: &str) -> Idl {
    Idl::parse(&repo(&format!("config/idl/{file}"))).unwrap()
}

fn names(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn captured_jupiter_transactions_decode_every_event_and_instruction() {
    let idl = idl("jupiter-perpetuals.json");
    let fx = fixtures();
    let txs = fx["transactions"].as_array().unwrap();
    assert_eq!(txs.len(), 4);
    for tx in txs {
        let signature = tx["signature"].as_str().unwrap();
        let meta: Value = serde_json::from_str(tx["meta_json"].as_str().unwrap()).unwrap();
        let at = TxMeta {
            signature,
            slot: tx["slot"].as_i64().unwrap(),
            block_time: tx["block_time"].as_i64(),
        };
        let rows = extract(tx["tx_b64"].as_str().unwrap(), &meta, PROGRAM, &idl, &at).unwrap();
        let events: Vec<String> = rows
            .events
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(events, names(&tx["events"]), "{signature}");
        assert_eq!(rows.undecoded + rows.unknown, 0, "{signature}");
        assert!(
            rows.events
                .iter()
                .all(|e| e["fields"].is_object() && e["decode_error"].is_null())
        );
        let mut instructions: Vec<(String, String)> = rows
            .instructions
            .iter()
            .map(|i| {
                (
                    i["path"].as_str().unwrap().to_owned(),
                    i["name"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        instructions.sort();
        let listed: Vec<String> = instructions.into_iter().map(|(_, n)| n).collect();
        assert_eq!(listed, names(&tx["instructions"]), "{signature}");
    }
    // The liquidation: CPI event, every field of the IDL, wide integers as decimal strings.
    let liquidation = txs
        .iter()
        .find(|t| names(&t["events"]) == ["LiquidateFullPositionEvent"])
        .unwrap();
    let meta: Value = serde_json::from_str(liquidation["meta_json"].as_str().unwrap()).unwrap();
    let at = TxMeta {
        signature: liquidation["signature"].as_str().unwrap(),
        slot: 0,
        block_time: None,
    };
    let rows = extract(
        liquidation["tx_b64"].as_str().unwrap(),
        &meta,
        PROGRAM,
        &idl,
        &at,
    )
    .unwrap();
    let event = &rows.events[0];
    assert_eq!(event["via"], "cpi");
    assert_eq!(event["discriminator"], "806547a880485654");
    let fields = &event["fields"];
    for key in [
        "positionKey",
        "positionSide",
        "positionSizeUsd",
        "pnlDelta",
        "price",
        "liquidationFeeUsd",
        "owner",
    ] {
        assert!(!fields[key].is_null(), "{key}");
    }
    assert!(
        fields["positionSizeUsd"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .is_ok()
    );
    // The fee withdrawals are emitted with `emit!`, as log lines.
    let fees = txs
        .iter()
        .find(|t| names(&t["events"])[0] == "WithdrawFeesEvent")
        .unwrap();
    let meta: Value = serde_json::from_str(fees["meta_json"].as_str().unwrap()).unwrap();
    let rows = extract(fees["tx_b64"].as_str().unwrap(), &meta, PROGRAM, &idl, &at).unwrap();
    assert!(rows.events.iter().all(|e| e["via"] == "log"));
}

#[test]
fn captured_accounts_decode_through_the_onchain_idls() {
    let jupiter = idl("jupiter-perpetuals.json");
    let flash = idl("flash-perpetuals.json");
    for account in fixtures()["accounts"].as_array().unwrap() {
        let data = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            account["data_b64"].as_str().unwrap(),
        )
        .unwrap();
        let idl = if account["venue"] == "jupiter" {
            &jupiter
        } else {
            &flash
        };
        let (name, decoded) = idl.decode_account(&data).unwrap();
        assert_eq!(name, account["type"].as_str().unwrap());
        let (fields, used) = decoded.unwrap();
        assert!(used <= data.len());
        if account["venue"] == "jupiter" && name == "Custody" {
            assert!(
                fields["assets"]["guaranteedUsd"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .is_ok()
            );
            assert!(fields["fundingRateState"]["cumulativeInterestRate"].is_string());
        }
        if account["venue"] == "jupiter" && name == "Pool" {
            assert!(fields["aumUsd"].as_str().unwrap().parse::<u128>().is_ok());
        }
    }
}

/// A provider that answers from fixtures and records what it was asked.
struct Fixture {
    signatures: Vec<Value>,
    bulk: Vec<Value>,
    single: BTreeMap<String, Value>,
    calls: Mutex<Vec<String>>,
    /// Answer every bulk page with the same token.
    stuck: bool,
}

impl Rpc for Fixture {
    fn call<'a>(&'a self, method: &'a str, params: Value, _lane: RpcLane) -> RpcFuture<'a> {
        self.calls.lock().unwrap().push(method.to_owned());
        let result = match method {
            "getSlot" => json!(1_000),
            "getSignaturesForAddress" => Value::Array(self.signatures.clone()),
            "getTransactionsForAddress" => {
                let token = params[1]["paginationToken"].as_str();
                // Two pages: the first carries a token, the second none.
                match token {
                    _ if self.stuck => {
                        json!({ "data": self.bulk[..1].to_vec(), "paginationToken": "p2" })
                    }
                    None => json!({ "data": self.bulk[..1].to_vec(), "paginationToken": "p2" }),
                    Some(_) => json!({ "data": self.bulk[1..].to_vec(), "paginationToken": null }),
                }
            }
            "getTransaction" => self
                .single
                .get(params[0].as_str().unwrap())
                .cloned()
                .unwrap_or(Value::Null),
            _ => {
                return Box::pin(async move {
                    Err(RpcFailure {
                        method: method.to_owned(),
                        code: -1,
                        throttled: false,
                    })
                });
            }
        };
        let raw = json!({ "jsonrpc": "2.0", "id": 1, "result": result }).to_string();
        Box::pin(async move { Ok(RpcResponse { result, raw }) })
    }

    fn provider_name(&self) -> &str {
        "fixture"
    }
}

fn entry(tx: &Value, slot: i64) -> Value {
    json!({
        "slot": slot, "blockTime": tx["block_time"],
        "transaction": [tx["tx_b64"], "base64"],
        "meta": serde_json::from_str::<Value>(tx["meta_json"].as_str().unwrap()).unwrap(),
    })
}

fn fixture_rpc(serve_last: bool) -> Fixture {
    let fx = fixtures();
    let txs = fx["transactions"].as_array().unwrap().clone();
    // The window is [900, 1000]; the manifest page also lists a newer transaction above it, a
    // failed one inside it and an older one below it, which ends the walk.
    let mut signatures = vec![json!({ "signature": "newer", "slot": 1_001, "err": null })];
    for (i, tx) in txs.iter().enumerate() {
        signatures
            .push(json!({ "signature": tx["signature"], "slot": 990 - i as i64, "err": null }));
    }
    signatures.push(
        json!({ "signature": "failed", "slot": 950, "err": { "InstructionError": [0, "Custom"] } }),
    );
    signatures.push(json!({ "signature": "older", "slot": 899, "err": null }));
    let bulk: Vec<Value> = txs[..3]
        .iter()
        .enumerate()
        .map(|(i, tx)| entry(tx, 990 - i as i64))
        .collect();
    let mut single = BTreeMap::new();
    if serve_last {
        single.insert(
            txs[3]["signature"].as_str().unwrap().to_owned(),
            entry(&txs[3], 987),
        );
    }
    Fixture {
        signatures,
        bulk,
        single,
        calls: Mutex::new(Vec::new()),
        stuck: false,
    }
}

#[test]
fn a_cycle_joins_the_manifest_the_bulk_pages_and_single_fetches() {
    let rpc = fixture_rpc(true);
    let idl = idl("jupiter-perpetuals.json");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (cycle, rows) = runtime
        .block_on(walk::cycle(&rpc, PROGRAM, &idl, (900, 1_000), true))
        .unwrap();
    assert_eq!(
        (
            cycle.manifest,
            cycle.failed,
            cycle.bulk,
            cycle.single,
            cycle.unlisted,
            cycle.missing
        ),
        (4, 1, 3, 1, 0, 0)
    );
    assert_eq!(rows.transactions.len(), 4);
    assert_eq!(cycle.undecoded + cycle.unknown, 0);
    assert_eq!(cycle.events, rows.events.len() as u64);
    assert_eq!(
        rows.transactions
            .iter()
            .filter(|t| t["fetched_via"] == "single")
            .count(),
        1
    );
    let calls = rpc.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        [
            "getSignaturesForAddress",
            "getTransactionsForAddress",
            "getTransactionsForAddress",
            "getTransaction"
        ]
    );
}

#[test]
fn a_window_with_an_unserved_signature_or_a_short_manifest_is_refused() {
    let idl = idl("jupiter-perpetuals.json");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let rpc = fixture_rpc(false);
    let error = runtime
        .block_on(walk::cycle(&rpc, PROGRAM, &idl, (900, 1_000), true))
        .unwrap_err();
    assert!(
        error.contains("1 manifest signatures not served"),
        "{error}"
    );
    // A bulk provider that repeats its pagination token.
    let mut rpc = fixture_rpc(true);
    rpc.stuck = true;
    let error = runtime
        .block_on(walk::cycle(&rpc, PROGRAM, &idl, (900, 1_000), true))
        .unwrap_err();
    assert!(error.contains("did not advance"), "{error}");
    // A page that ends before reaching the window start and is followed by an empty page.
    let mut rpc = fixture_rpc(true);
    rpc.signatures.truncate(2);
    let short = runtime
        .block_on(walk::manifest(&rpc, PROGRAM, 900, 1_000))
        .unwrap();
    assert_eq!(
        short.ok.len(),
        1,
        "a page shorter than the limit is the end of history"
    );
    rpc.signatures.clear();
    assert!(
        runtime
            .block_on(walk::manifest(&rpc, PROGRAM, 900, 1_000))
            .is_err()
    );
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("venues-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn flushes_merge_into_hour_files_and_the_cursor_follows_them() {
    let root = tempdir();
    let mut store = ledger::open(&root, Lane::Venues).unwrap();
    ledger::recover(&mut store, &root, Lane::Venues).unwrap();
    let (db, thread) = Db::spawn(store);
    let ctx = Ctx {
        http: Http::new(1.0).unwrap(),
        db: db.clone(),
        root: root.clone(),
        lane: Lane::Venues,
        start: chrono::NaiveDate::from_ymd_opt(2026, 10, 18).unwrap(),
        now_ms: 0,
        stop: Arc::new(AtomicBool::new(false)),
        disk_budget_bytes: 0,
    };
    let rpc = fixture_rpc(true);
    let idl = idl("jupiter-perpetuals.json");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (_, rows) = runtime
        .block_on(walk::cycle(&rpc, PROGRAM, &idl, (900, 1_000), true))
        .unwrap();
    let events = rows.events.len();
    let buffer = Buffer::default();
    let push = |rows: &walk::CycleRows| {
        let sets = vec![
            ("transactions", rows.transactions.clone()),
            ("events", rows.events.clone()),
            ("instructions", rows.instructions.clone()),
        ];
        buffer.push_cycle("jupiter", sets, &cursor_key("jupiter"), cursor_value(1_001));
    };
    push(&rows);
    let now_ms = 1_792_310_000_000;
    let flushed = runtime.block_on(store::flush(&ctx, &buffer, now_ms));
    assert_eq!(flushed.errors, 0);
    assert!(flushed.files >= 3);
    // The same window again (a restart before the cursor was saved): the merge keeps one copy.
    push(&rows);
    runtime.block_on(store::flush(&ctx, &buffer, now_ms));
    assert!(buffer.is_empty());
    let progress = ctx
        .db
        .run_blocking(|s| get_progress(s, &cursor_key("jupiter")))
        .unwrap()
        .unwrap();
    assert_eq!(progress["next"], 1_001);
    let root_c = root.clone();
    ctx.db
        .run_blocking(move |s| ledger::write_catalog(s, &root_c, Lane::Venues))
        .unwrap();
    let query = |sql: &str| {
        solos_data::augment::query::query_augment(&root, sql)
            .unwrap()
            .to_value()["rows"]
            .clone()
    };
    let counts = query(
        "SELECT (SELECT count(*) FROM jupiter_transactions) AS t, (SELECT count(*) FROM jupiter_events) AS e, (SELECT count(*) FROM jupiter_events WHERE name = 'LiquidateFullPositionEvent' AND json_extract_string(fields, '$.pnlDelta') IS NOT NULL) AS l",
    );
    assert_eq!(counts[0]["t"], "4");
    assert_eq!(counts[0]["e"], events.to_string());
    assert_eq!(counts[0]["l"], "1");
    let files = query("SELECT count(DISTINCT ts::DATE) AS days FROM jupiter_transactions");
    assert_eq!(files[0]["days"], "1");
    drop(ctx);
    let store = thread.join(db);
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}
