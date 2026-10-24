//! The spot decoders (ADR-0014) on real mainnet transactions: every fixture, read through the RPC
//! shape, gives exactly the rows the archive stream decoded from the native types when it was
//! captured; every program has swaps; a swap's amounts are what the trader's own token accounts
//! paid and received; and a batch's rows survive the Parquet writer.
//!
//! After a decoder change, `SOLOS_DATA_REFRESH_SPOT_FIXTURES=1 cargo test --test spot` rewrites
//! the expected rows; `solos-data spot trial` then checks the archive path agrees again.

use serde_json::{Value, json};
use solos_data::spot::dex::{ALL, Programs};
use solos_data::spot::rows::{Rows, liquidity_json, named, swap_json};
use solos_data::spot::tx::from_rpc;
use solos_data::spot::write::{Names, connection, copy_to, load_liquidity, load_swaps};
use std::collections::HashMap;

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/data/spot-fixtures.json"
);

fn fixtures() -> Value {
    serde_json::from_str(&std::fs::read_to_string(FIXTURES).unwrap()).unwrap()
}

fn decode(t: &Value) -> Rows {
    let tx = from_rpc(
        t["tx_b64"].as_str().unwrap(),
        &t["meta"],
        t["slot"].as_u64().unwrap(),
        u32::try_from(t["index"].as_u64().unwrap()).unwrap(),
    )
    .unwrap();
    let programs = Programs::all();
    let mut rows = Rows::new();
    rows.add_tx(&tx, &programs, &named(&tx.keys, &programs));
    rows
}

#[test]
fn every_fixture_decodes_to_its_recorded_rows() {
    let mut doc = fixtures();
    let refresh = std::env::var("SOLOS_DATA_REFRESH_SPOT_FIXTURES").is_ok();
    let mut differ = Vec::new();
    for t in doc["transactions"].as_array_mut().unwrap() {
        let rows = decode(t);
        let swaps: Vec<Value> = rows.swaps.iter().map(swap_json).collect();
        let liquidity: Vec<Value> = rows.liquidity.iter().map(liquidity_json).collect();
        if t["swaps"] != json!(swaps) || t["liquidity"] != json!(liquidity) {
            differ.push(t["signature"].as_str().unwrap().to_owned());
            t["swaps"] = json!(swaps);
            t["liquidity"] = json!(liquidity);
        }
    }
    if refresh {
        std::fs::write(FIXTURES, serde_json::to_string(&doc).unwrap()).unwrap();
        return;
    }
    assert!(differ.is_empty(), "rows differ for {differ:?}");
}

#[test]
fn every_program_has_swaps_and_the_counts_add_up() {
    let doc = fixtures();
    let mut swaps: HashMap<String, u64> = HashMap::new();
    let mut liquidity = 0;
    for t in doc["transactions"].as_array().unwrap() {
        let rows = decode(t);
        for s in &rows.swaps {
            *swaps.entry(s.dex.name().to_owned()).or_default() += 1;
            assert!(s.amount_in > 0 || s.amount_out > 0, "an empty trade");
            assert_ne!(s.mint_a, s.mint_b, "a pool trades two tokens");
        }
        liquidity += rows.liquidity.len();
        let counted: u64 = rows.counts.iter().map(|c| c.swaps).sum();
        assert_eq!(counted, rows.swaps.len() as u64);
        let seq: Vec<u16> = {
            let mut s: Vec<u16> = rows
                .swaps
                .iter()
                .map(|s| s.seq)
                .chain(rows.liquidity.iter().map(|l| l.seq))
                .collect();
            s.sort_unstable();
            s
        };
        assert_eq!(
            seq,
            (0..seq.len() as u16).collect::<Vec<_>>(),
            "seq numbers the rows"
        );
    }
    for dex in ALL {
        assert!(
            swaps.get(dex.name()).copied().unwrap_or(0) > 0,
            "{} has no swap in the fixtures",
            dex.name()
        );
    }
    assert!(liquidity > 20);
}

/// The trader's (fee payer's) token balance change per mint, over SPL Token accounts only (a
/// Token-2022 transfer fee makes a trader's change differ from the pool's amounts by design).
fn trader_deltas(t: &Value, signer: &str) -> Option<HashMap<String, i128>> {
    let mut pre: HashMap<(u64, String), i128> = HashMap::new();
    let mut post: HashMap<(u64, String), i128> = HashMap::new();
    for (key, map) in [
        ("preTokenBalances", &mut pre),
        ("postTokenBalances", &mut post),
    ] {
        for b in t["meta"][key].as_array()? {
            if b["owner"].as_str() != Some(signer) {
                continue;
            }
            if b["programId"].as_str() != Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA") {
                return None;
            }
            let amount: i128 = b["uiTokenAmount"]["amount"].as_str()?.parse().ok()?;
            map.insert(
                (b["accountIndex"].as_u64()?, b["mint"].as_str()?.to_owned()),
                amount,
            );
        }
    }
    let accounts: std::collections::HashSet<&(u64, String)> =
        pre.keys().chain(post.keys()).collect();
    let mut out: HashMap<String, i128> = HashMap::new();
    for key in accounts {
        let d = post.get(key).copied().unwrap_or(0) - pre.get(key).copied().unwrap_or(0);
        *out.entry(key.1.clone()).or_default() += d;
    }
    Some(out)
}

#[test]
fn a_lone_swap_moves_the_traders_own_tokens_by_its_amounts() {
    const WSOL: &str = "So11111111111111111111111111111111111111112";
    let doc = fixtures();
    let (mut checked, mut exact) = (0, 0);
    for t in doc["transactions"].as_array().unwrap() {
        let rows = decode(t);
        let [s] = rows.swaps.as_slice() else {
            continue;
        };
        // pump.fun curves report the curve's amounts, before their fees (ADR-0014).
        if !rows.liquidity.is_empty() || s.dex.name() == "pump" {
            continue;
        }
        let row = swap_json(s);
        let (mint_in, mint_out) = if s.a_to_b {
            (
                row["mint_a"].as_str().unwrap(),
                row["mint_b"].as_str().unwrap(),
            )
        } else {
            (
                row["mint_b"].as_str().unwrap(),
                row["mint_a"].as_str().unwrap(),
            )
        };
        if mint_in == WSOL || mint_out == WSOL {
            continue;
        }
        let Some(d) = trader_deltas(t, row["signer"].as_str().unwrap()) else {
            continue;
        };
        let (Some(&paid), Some(&got)) = (d.get(mint_in), d.get(mint_out)) else {
            continue;
        };
        if paid == 0 || got == 0 {
            continue;
        }
        // The output is exactly what the trader got; the input is what it paid, less a
        // referral or host fee the trader's own account took back (self-referral).
        let (paid, amount_in) = (-paid, i128::from(s.amount_in));
        let rebate = i128::from(s.fee.unwrap_or(0));
        assert_eq!(
            got,
            i128::from(s.amount_out),
            "{} {}",
            s.dex.name(),
            t["signature"]
        );
        assert!(
            paid == amount_in || (paid < amount_in && amount_in - paid <= rebate),
            "{} {}: paid {paid}, amount_in {amount_in}",
            s.dex.name(),
            t["signature"]
        );
        exact += i64::from(paid == amount_in);
        checked += 1;
    }
    assert!(checked >= 10, "only {checked} lone swaps reconciled");
    assert!(exact * 10 >= checked * 9, "{exact} of {checked} exact");
}

#[test]
fn a_batch_survives_the_parquet_writer() {
    let doc = fixtures();
    let mut rows = Rows::new();
    for t in doc["transactions"].as_array().unwrap() {
        rows.append(decode(t));
    }
    let dir = std::env::temp_dir().join(format!("spot-write-{}", uuid::Uuid::new_v4()));
    let conn = connection("2", "1GB").unwrap();
    let time = |slot: u64| Some(i64::try_from(slot).unwrap() / 2);
    let mut names = Names::default();
    load_swaps(&conn, &rows.swaps, &time, &mut names).unwrap();
    load_liquidity(&conn, &rows.liquidity, &time, &mut names).unwrap();
    let swaps = copy_to(
        &conn,
        "SELECT * FROM swaps ORDER BY pool, slot, tx_index, seq",
        &dir,
        "tables/spot/swaps/ALL/x.parquet",
        ("spot", "swaps", "x"),
    )
    .unwrap();
    assert_eq!(swaps.row_count, rows.swaps.len() as i64);
    assert_eq!(swaps.symbol, "ALL");
    let path = dir.join("tables/spot/swaps/ALL/x.parquet");
    let (programs, max_in, nulls): (i64, String, i64) = conn
        .query_row(
            &format!(
                "SELECT count(DISTINCT program), max(amount_in)::VARCHAR, count(*) FILTER (WHERE block_time IS NULL OR pool IS NULL OR signer IS NULL) FROM read_parquet('{}')",
                path.display()
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(programs, ALL.len() as i64);
    assert_eq!(
        max_in,
        rows.swaps
            .iter()
            .map(|s| s.amount_in)
            .max()
            .unwrap()
            .to_string(),
        "u64 amounts keep their full range"
    );
    assert_eq!(nulls, 0);
    std::fs::remove_dir_all(&dir).unwrap();
}
