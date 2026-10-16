//! The state snapshot lane end to end over the captured mainnet accounts
//! (`tests/data/phoenix-state/`): a fixture RPC serves them, the lane reads the asset map, the
//! books with the trader index and the active trader buffer (twice: the second, unchanged read
//! writes summaries only) and the trader scan, then publishes Parquet that `snapshot query` reads.

use serde_json::{Value, json};
use solos_data::jsonout::Obj;
use solos_data::snapshot::config::{load_snapshot_config, repository_config_path};
use solos_data::snapshot::rpc::{Account, Accounts, Fut, ProgramPage, StateRpc};
use solos_data::snapshot::{query, service};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const PROGRAM: &str = "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih";
const TRADER: &str = "GvXxLxJL5FoJrLUciJ5U1BjjfiWtJvQifiCwf1MzRM7A";

fn dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/phoenix-state")
}

struct FixtureRpc {
    slot: u64,
}

impl FixtureRpc {
    fn account(pubkey: &str) -> Option<Account> {
        let zstd = std::fs::read(dir().join(format!("{pubkey}.zst"))).ok()?;
        let data = zstd::decode_all(&zstd[..]).ok()?;
        Some(Account {
            pubkey: pubkey.to_owned(),
            owner: PROGRAM.to_owned(),
            lamports: 1,
            zstd,
            data,
        })
    }
}

impl StateRpc for FixtureRpc {
    fn multiple<'a>(&'a self, keys: &'a [String]) -> Fut<'a, Accounts> {
        Box::pin(async move {
            Ok(Accounts {
                slot: self.slot,
                accounts: keys.iter().map(|k| FixtureRpc::account(k)).collect(),
            })
        })
    }

    fn program_page<'a>(
        &'a self,
        _program: &'a str,
        _prefix: &'a str,
        _limit: u32,
        cursor: Option<&'a str>,
    ) -> Fut<'a, ProgramPage> {
        Box::pin(async move {
            let accounts = if cursor.is_none() {
                FixtureRpc::account(TRADER).into_iter().collect()
            } else {
                Vec::new()
            };
            Ok(ProgramPage {
                slot: self.slot,
                accounts,
                next: Some("end".into()),
            })
        })
    }

    fn meter(&self) -> Obj {
        Obj::new().with("credits", 0).with("bytes", 0)
    }
}

fn exchange() -> service::Exchange {
    let market = |symbol: &str, asset: u64, book: &str| {
        json!({ "symbol": symbol, "assetId": asset, "marketStatus": "active",
            "marketPubkey": book, "splinePubkey": format!("{symbol}-spline-not-captured") })
    };
    service::parse_exchange(json!({
        "keys": {
            "perpAssetMap": "2nHGAaEw3D5dd4hVueaUNoygkQFmoeKqRQWnSPqSMFUC",
            "globalTraderIndex": ["HCrPXLByGqRh2szQi3gj7oRdRVBNi1gccAyn4CQCT3HK"],
            "activeTraderBuffer": ["2U32rSzzrQS3eVmGHsnbw5kcqKF3wQXpHGd3hMq5YJok"],
        },
        "markets": [
            market("SOL", 0, "71Si24E4uc3oCaPbPZTozC1ptSNNqygjjebxSmErSsC2"),
            market("BTC", 1, "AXFz1MuzMUBHi5UKJuK3FDCQ73o3rSzubGU2mPr4LLU7"),
            market("ETH", 2, "9u7aqptdRFbsnnoHtjK13E5JkeM14EW5fAKTRPidVF88"),
            market("SPCX", 41, "HCPybc8PFzS3gL2AHWXtT7jT1WDYSva7A6qf9aVGiLMf"),
        ],
    }))
    .unwrap()
}

fn count(root: &Path, sql: &str) -> i64 {
    let out = query::query_state(root, sql).unwrap();
    let value: Value = serde_json::from_str(&out.to_json()).unwrap();
    let n = &value["rows"][0]["n"];
    n.as_i64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("{sql}: {value}"))
}

#[test]
fn the_lane_publishes_every_dataset_from_captured_accounts() {
    let root = std::env::temp_dir().join(format!("snap-lane-{}", uuid::Uuid::new_v4()));
    let mut config =
        load_snapshot_config(Some(&repository_config_path().to_string_lossy())).unwrap();
    config.data_dir = root.clone();
    let rpc = Arc::new(FixtureRpc { slot: 457_591_011 });
    let (lane, thread) = service::start(config, rpc, Arc::new(AtomicBool::new(false))).unwrap();
    lane.set_exchange(exchange());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (first, second) = runtime.block_on(async {
        lane.market_tick().await.unwrap();
        let first = lane.book_tick().await.unwrap();
        let second = lane.book_tick().await.unwrap();
        lane.trader_tick().await.unwrap();
        lane.flush().await.unwrap();
        (first.1, second.1)
    });
    let first: Value = serde_json::from_str(&first.to_json()).unwrap();
    let second: Value = serde_json::from_str(&second.to_json()).unwrap();
    assert_eq!(first["markets"], 4);
    assert!(first["activePositions"].as_u64().unwrap() > 100);
    assert_eq!(first["activeRowsWritten"], first["activePositions"]);
    assert_eq!(first["ordersWithTraderId"], first["ordersResolved"]);
    // Nothing changed: summaries only, no orders, no buffer rows, no raw copies.
    assert_eq!(second["ordersWritten"], 0);
    assert_eq!(second["activeRowsWritten"], 0);
    assert_eq!(second["rawWritten"], 0);
    service::finish(lane, thread).unwrap();
    let slot = "slot = 457591011";
    assert_eq!(
        count(
            &root,
            &format!("SELECT count(*) AS n FROM book_snapshots WHERE {slot}")
        ),
        8
    );
    assert_eq!(
        count(&root, "SELECT count(*) AS n FROM book_orders"),
        first["ordersWritten"].as_i64().unwrap()
    );
    assert_eq!(
        count(
            &root,
            "SELECT count(*) AS n FROM active_positions WHERE trader IS NOT NULL AND symbol IS NOT NULL AND asset_id < 3"
        ),
        count(
            &root,
            "SELECT count(*) AS n FROM active_positions WHERE asset_id < 3"
        )
    );
    assert!(count(&root, "SELECT count(*) AS n FROM market_state") >= 50);
    assert_eq!(count(&root, "SELECT count(*) AS n FROM traders"), 1);
    assert!(count(&root, "SELECT count(*) AS n FROM trader_index") > 100);
    // One raw copy per account read (asset map, index, buffer, four books, the trader), each
    // decompressing back to the captured bytes.
    assert_eq!(count(&root, "SELECT count(*) AS n FROM raw_accounts"), 8);
    assert_eq!(
        count(&root, "SELECT count(DISTINCT kind) AS n FROM raw_accounts"),
        5
    );
    let _ = std::fs::remove_dir_all(&root);
}
