//! The state snapshot decoders against Phoenix accounts captured from mainnet in one
//! `getMultipleAccounts` call (`tests/data/phoenix-state/`, written by `snapshot account`): the
//! active trader buffer, the global trader index, the perp asset map, the SOL, BTC, ETH and SPCX
//! order books and one hot trader account, all at the same slot. Offline; public chain data only.

use phoenix_rise_accounts::owned::Orderbook;
use serde_json::Value;
use solos_data::snapshot::decode::{self, At, TraderIndex};
use solos_data::snapshot::validate::{compare_active, summarize_book};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const BUFFER: &str = "2U32rSzzrQS3eVmGHsnbw5kcqKF3wQXpHGd3hMq5YJok";
const INDEX: &str = "HCrPXLByGqRh2szQi3gj7oRdRVBNi1gccAyn4CQCT3HK";
const ASSET_MAP: &str = "2nHGAaEw3D5dd4hVueaUNoygkQFmoeKqRQWnSPqSMFUC";
const TRADER: &str = "GvXxLxJL5FoJrLUciJ5U1BjjfiWtJvQifiCwf1MzRM7A";
const BOOKS: [(&str, &str, u32); 4] = [
    ("SOL", "71Si24E4uc3oCaPbPZTozC1ptSNNqygjjebxSmErSsC2", 0),
    ("BTC", "AXFz1MuzMUBHi5UKJuK3FDCQ73o3rSzubGU2mPr4LLU7", 1),
    ("ETH", "9u7aqptdRFbsnnoHtjK13E5JkeM14EW5fAKTRPidVF88", 2),
    ("SPCX", "HCPybc8PFzS3gL2AHWXtT7jT1WDYSva7A6qf9aVGiLMf", 41),
];

fn dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/phoenix-state")
}

fn account(pubkey: &str) -> Vec<u8> {
    let frame = std::fs::read(dir().join(format!("{pubkey}.zst"))).unwrap();
    zstd::decode_all(&frame[..]).unwrap()
}

fn at() -> At {
    let meta: Value =
        serde_json::from_str(&std::fs::read_to_string(dir().join("accounts.json")).unwrap())
            .unwrap();
    At {
        captured_ms: 1_792_137_600_000,
        slot: meta["slot"].as_u64().unwrap(),
    }
}

#[test]
fn every_captured_account_has_its_kind() {
    assert_eq!(decode::kind_of(&account(BUFFER)), "active_trader_buffer");
    assert_eq!(decode::kind_of(&account(INDEX)), "global_trader_index");
    assert_eq!(decode::kind_of(&account(ASSET_MAP)), "perp_asset_map");
    assert_eq!(decode::kind_of(&account(TRADER)), "trader");
    for (_, book, _) in BOOKS {
        assert_eq!(decode::kind_of(&account(book)), "orderbook");
    }
}

#[test]
fn the_active_trader_buffer_matches_the_books_at_the_same_slot() {
    let index = TraderIndex::decode(&account(INDEX), &[]).unwrap();
    let active = decode::decode_active(&account(BUFFER), &[]).unwrap();
    assert!(active.len() > 100, "{}", active.len());
    // Keyed by (trader id, asset id), unique, and every trader is in the index.
    let keys: HashSet<(u32, u32)> = active.iter().map(|p| (p.trader_id, p.asset_id)).collect();
    assert_eq!(keys.len(), active.len());
    assert!(
        active
            .iter()
            .all(|p| index.by_id.contains_key(&p.trader_id))
    );
    let mut resting = HashMap::new();
    let mut assets = HashSet::new();
    let mut orders = 0;
    for (symbol, book, asset) in BOOKS {
        let b = Orderbook::try_from_account_bytes(&account(book)).unwrap();
        assert_eq!(b.header.asset_symbol, symbol);
        assert_eq!(b.header.asset_id, asset);
        orders += b.bids.len() + b.asks.len();
        summarize_book(&b, &mut resting);
        assets.insert(asset);
    }
    assert!(orders > 50, "{orders}");
    let (compared, missing, mismatched) = compare_active(&active, &assets, &resting);
    assert!(compared as usize >= resting.len());
    assert_eq!(missing, 0);
    assert!(mismatched.is_empty(), "{mismatched:?}");
    // Some trader has reduce-only lots and some has both sides: the fields are not all zero.
    assert!(
        active
            .iter()
            .any(|p| p.ask_reduce_only_lots + p.bid_reduce_only_lots > 0)
    );
    assert!(
        active
            .iter()
            .any(|p| p.best_ask_ticks > 0 && p.best_bid_ticks > 0)
    );
    let symbols: HashMap<u64, String> = BOOKS
        .iter()
        .map(|(s, _, a)| (u64::from(*a), (*s).to_owned()))
        .collect();
    let rows = decode::active_rows(&active, at(), &index, &symbols);
    assert_eq!(rows.len(), active.len());
    assert!(rows.iter().any(|r| r["trader"] == TRADER));
}

#[test]
fn books_resolve_their_traders_and_sort_by_price() {
    let index = TraderIndex::decode(&account(INDEX), &[]).unwrap();
    assert_eq!(index.by_id.len(), index.size as usize);
    for (symbol, book, _) in BOOKS {
        let decoded = decode::decode_book(&account(book), at(), book, Some(&index)).unwrap();
        assert_eq!(decoded.symbol, symbol);
        assert_eq!(decoded.summary["slot"], at().slot);
        for order in &decoded.orders {
            if !order["trader_id"].is_null() {
                assert!(order["trader"].is_string(), "{order}");
            }
        }
        let prices = |side: &str| -> Vec<u64> {
            decoded
                .orders
                .iter()
                .filter(|o| o["side"] == side)
                .map(|o| o["price_ticks"].as_u64().unwrap())
                .collect()
        };
        assert!(prices("bid").windows(2).all(|w| w[0] >= w[1]));
        assert!(prices("ask").windows(2).all(|w| w[0] <= w[1]));
        if let (Some(bid), Some(ask)) = (
            decoded.summary["best_bid_ticks"].as_u64(),
            decoded.summary["best_ask_ticks"].as_u64(),
        ) {
            assert!(bid < ask, "{symbol} crossed: {bid} >= {ask}");
        }
    }
}

#[test]
fn the_asset_map_and_a_trader_decode() {
    let rows = decode::decode_markets(&account(ASSET_MAP), at()).unwrap();
    assert!(rows.len() >= 50, "{}", rows.len());
    for (symbol, book, asset) in BOOKS {
        let row = rows.iter().find(|r| r["symbol"] == symbol).unwrap();
        assert_eq!(row["asset_id"], asset);
        assert_eq!(row["market"], book);
        assert!(row["mark_price_ticks"].as_u64().unwrap() > 0);
        assert!(row["funding_interval_seconds"].as_u64().unwrap() > 0);
    }
    let index = TraderIndex::decode(&account(INDEX), &[]).unwrap();
    let ids = index.ids();
    let symbols = HashMap::new();
    let trader = decode::decode_trader(&account(TRADER), at(), TRADER, &symbols, &ids).unwrap();
    assert_eq!(trader.trader["key"], TRADER);
    assert_eq!(trader.trader["trader_id"], ids[TRADER]);
    assert_eq!(
        trader.trader["flags"].as_u64().unwrap() & 1,
        1,
        "a hot trader"
    );
}
