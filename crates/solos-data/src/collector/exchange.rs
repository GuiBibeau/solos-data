//! Exchange snapshot and program identity: the market list comes from the exchange endpoint, the
//! program's identity from finalized chain data (globalConfig owner, executable program, its
//! ProgramData account). A port of `exchange.ts`.

use super::config::{Config, Lane};
use super::rpc::{Rpc, call_value};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::{Param, StoreError};
use serde_json::{Value, json};

/// What a refresh learned.
#[derive(Clone, Debug)]
pub struct Exchange {
    /// ProgramData address.
    pub program_data: String,
    /// Slot the global config was observed at.
    pub slot: i64,
    /// Markets in the snapshot.
    pub markets: usize,
}

/// Fetch the exchange snapshot, confirm identity against the chain, upsert addresses.
pub async fn refresh_exchange(
    rpc: &dyn Rpc,
    db: &Db,
    config: &Config,
    http: &reqwest::Client,
) -> Result<Exchange, StoreError> {
    let response = http
        .get(&config.exchange_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| StoreError::Check(crate::jsonout::safe_error(&e.to_string())))?;
    if !response.status().is_success() {
        return Err(StoreError::Check(format!(
            "Exchange HTTP {}",
            response.status().as_u16()
        )));
    }
    let snapshot: Value = response
        .json()
        .await
        .map_err(|_| StoreError::Check("Invalid exchange snapshot".into()))?;
    let global = snapshot
        .get("keys")
        .and_then(|k| k.get("globalConfig"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let markets = snapshot.get("markets").and_then(Value::as_array).cloned();
    let (Some(global), Some(markets)) = (global, markets) else {
        return Err(StoreError::Check("Invalid exchange snapshot".into()));
    };
    let account = call_value(
        rpc,
        "getAccountInfo",
        json!([global, { "encoding": "base64", "commitment": "finalized" }]),
        Lane::Tail,
    )
    .await
    .map_err(|e| StoreError::Check(e.to_string()))?;
    let owner = account
        .get("value")
        .and_then(|v| v.get("owner"))
        .and_then(Value::as_str);
    let snapshot_program = snapshot.get("programId").and_then(Value::as_str);
    if owner != Some(config.program_id.as_str())
        || snapshot_program.is_some_and(|p| p != config.program_id)
    {
        return Err(StoreError::Check(
            "Exchange program identity mismatch".into(),
        ));
    }
    let program = call_value(
        rpc,
        "getAccountInfo",
        json!([config.program_id, { "encoding": "jsonParsed", "commitment": "finalized" }]),
        Lane::Tail,
    )
    .await
    .map_err(|e| StoreError::Check(e.to_string()))?;
    if program
        .get("value")
        .and_then(|v| v.get("executable"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(StoreError::Check("Program is not executable".into()));
    }
    let program_data = program
        .get("value")
        .and_then(|v| v.get("data"))
        .and_then(|d| d.get("parsed"))
        .and_then(|p| p.get("info"))
        .and_then(|i| i.get("programData"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| StoreError::Check("ProgramData address is missing".into()))?;
    let slot = account
        .get("context")
        .and_then(|c| c.get("slot"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut entries: Vec<(String, &str, String, String)> = vec![
        (
            config.program_id.clone(),
            "program",
            String::new(),
            "active".into(),
        ),
        (
            program_data.clone(),
            "programdata",
            String::new(),
            "active".into(),
        ),
    ];
    for market in &markets {
        for (field, kind) in [("marketPubkey", "market"), ("splinePubkey", "spline")] {
            let Some(address) = market.get(field).and_then(Value::as_str) else {
                return Err(StoreError::Check(
                    "Exchange market address is missing".into(),
                ));
            };
            entries.push((
                address.to_owned(),
                kind,
                market
                    .get("symbol")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                market
                    .get("marketStatus")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
            ));
        }
    }
    let count = markets.len();
    let added = db
        .run(move |store| {
            let mut added = 0u64;
            for (address, kind, symbol, status) in &entries {
                let exists = store.rows("SELECT address FROM addresses WHERE address=?", &[address])?;
                if exists.is_empty() {
                    added += 1;
                }
                store.exec(
                    "INSERT INTO addresses VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(address) DO UPDATE SET status=excluded.status, last_seen_slot=excluded.last_seen_slot",
                    &[address, kind, symbol, status, &slot, &slot],
                )?;
            }
            store.set("exchange", &Obj::new().with("fetchedAt", now()).with("observedSlot", slot).with("markets", count).with("programData", program_data.clone()).to_value())?;
            Ok((added, program_data))
        })
        .await?;
    if added.0 > 0 {
        log(
            "addresses_added",
            Obj::new().with("count", added.0).with("observedSlot", slot),
        );
    }
    Ok(Exchange {
        program_data: added.1,
        slot,
        markets: count,
    })
}

/// Mainnet genesis hash check.
pub async fn confirm_mainnet(rpc: &dyn Rpc) -> Result<String, StoreError> {
    let genesis = call_value(rpc, "getGenesisHash", json!([]), Lane::Tail)
        .await
        .map_err(|e| StoreError::Check(e.to_string()))?;
    if genesis.as_str() != Some("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d") {
        return Err(StoreError::Check("RPC is not Solana mainnet".into()));
    }
    Ok(genesis.as_str().unwrap_or("").to_owned())
}

/// Params helper for SQL.
#[must_use]
pub fn text(value: &str) -> Param {
    Param::Text(value.to_owned())
}
