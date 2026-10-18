//! One transaction cycle of one venue over a finalized slot window `[from, to]`: the manifest is
//! the program's successful signatures from `getSignaturesForAddress` (newest first, down past
//! `from`); the transactions come in bulk from `getTransactionsForAddress` (slot-filtered,
//! successful only, 100 a page) and any manifest signature the bulk pages missed is fetched alone
//! with `getTransaction`. A bulk transaction the manifest does not list is kept and counted as
//! `unlisted`. Failed transactions are not collected (ADR-0012).

use super::events::{TxMeta, extract};
use super::idl::Idl;
use super::rpc::call;
use crate::collector::fetcher::{raw_meta, raw_metas};
use crate::collector::rpc::Rpc;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};

/// Signatures per `getSignaturesForAddress` page.
pub const PAGE: usize = 1000;
/// Most manifest pages one cycle walks (a catch-up after days of downtime is refused instead).
pub const MAX_MANIFEST_PAGES: usize = 2000;

/// A fetched transaction.
#[derive(Clone, Debug)]
pub struct Tx {
    /// Signature.
    pub signature: String,
    /// Slot.
    pub slot: i64,
    /// Block time, seconds.
    pub block_time: Option<i64>,
    /// Base64 wire.
    pub tx_b64: String,
    /// The provider's exact `meta` text.
    pub meta_json: String,
    /// `bulk` or `single`.
    pub via: &'static str,
}

/// The manifest of a window.
#[derive(Debug, Default)]
pub struct Manifest {
    /// Successful signatures and their slots.
    pub ok: BTreeMap<String, i64>,
    /// Failed signatures in the window (not collected).
    pub failed: u64,
    /// Pages walked.
    pub pages: u64,
}

/// What a cycle did.
#[derive(Debug, Default, Clone)]
pub struct Cycle {
    /// First slot.
    pub from: i64,
    /// Last slot.
    pub to: i64,
    /// Manifest signatures (successful).
    pub manifest: u64,
    /// Failed signatures skipped.
    pub failed: u64,
    /// Transactions from bulk pages.
    pub bulk: u64,
    /// Transactions fetched one by one.
    pub single: u64,
    /// Bulk transactions the manifest does not list.
    pub unlisted: u64,
    /// Manifest signatures neither path returned.
    pub missing: u64,
    /// Events.
    pub events: u64,
    /// Events the IDL names but could not decode.
    pub undecoded: u64,
    /// Event payloads of unknown discriminators.
    pub unknown: u64,
    /// Program instructions.
    pub instructions: u64,
    /// Transactions whose wire or meta did not parse.
    pub unparsed: u64,
}

/// Rows a cycle produced, by dataset.
#[derive(Debug, Default)]
pub struct CycleRows {
    /// `transactions`.
    pub transactions: Vec<Value>,
    /// `events`.
    pub events: Vec<Value>,
    /// `instructions`.
    pub instructions: Vec<Value>,
}

fn rpc_error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// The finalized slot.
///
/// # Errors
///
/// RPC failure or a non-numeric answer.
pub async fn finalized_slot(rpc: &dyn Rpc) -> Result<i64, String> {
    let reply = call(rpc, "getSlot", json!([{ "commitment": "finalized" }]))
        .await
        .map_err(rpc_error)?;
    reply
        .result
        .as_i64()
        .ok_or_else(|| "getSlot answered no slot".into())
}

/// Walk the program's signatures from the newest down past `from`.
///
/// # Errors
///
/// RPC failure, an empty page before `from` was reached (the provider's index lost the cursor;
/// ADR-0008 saw this), or more than [`MAX_MANIFEST_PAGES`] pages.
pub async fn manifest(
    rpc: &dyn Rpc,
    program: &str,
    from: i64,
    to: i64,
) -> Result<Manifest, String> {
    let mut out = Manifest::default();
    let mut before: Option<String> = None;
    loop {
        if out.pages as usize >= MAX_MANIFEST_PAGES {
            return Err("manifest walk exceeded its page limit".into());
        }
        let mut options = json!({ "commitment": "finalized", "limit": PAGE });
        if let Some(cursor) = &before {
            options["before"] = Value::String(cursor.clone());
        }
        let reply = call(rpc, "getSignaturesForAddress", json!([program, options]))
            .await
            .map_err(rpc_error)?;
        out.pages += 1;
        let Some(page) = reply.result.as_array() else {
            return Err("getSignaturesForAddress answered no list".into());
        };
        if page.is_empty() {
            return Err("manifest page empty before the window start".into());
        }
        let mut oldest = i64::MAX;
        for entry in page {
            let (Some(signature), Some(slot)) = (
                entry.get("signature").and_then(Value::as_str),
                entry.get("slot").and_then(Value::as_i64),
            ) else {
                return Err("manifest entry without signature or slot".into());
            };
            oldest = oldest.min(slot);
            if slot < from || slot > to {
                continue;
            }
            if entry.get("err").is_none_or(Value::is_null) {
                out.ok.insert(signature.to_owned(), slot);
            } else {
                out.failed += 1;
            }
        }
        if oldest < from || page.len() < PAGE {
            return Ok(out);
        }
        before = page
            .last()
            .and_then(|e| e.get("signature"))
            .and_then(Value::as_str)
            .map(str::to_owned);
    }
}

fn tx_of(entry: &Value, meta_json: Option<String>, via: &'static str) -> Result<Tx, String> {
    let wire = entry.get("transaction").and_then(Value::as_array);
    let b64 = wire.and_then(|w| w.first()).and_then(Value::as_str);
    let encoding = wire.and_then(|w| w.get(1)).and_then(Value::as_str);
    let (Some(b64), Some("base64"), Some(meta_json)) = (b64, encoding, meta_json) else {
        return Err("transaction without base64 wire or meta".into());
    };
    Ok(Tx {
        signature: solana_wire::signature_of_base64(b64).map_err(rpc_error)?,
        slot: entry
            .get("slot")
            .and_then(Value::as_i64)
            .ok_or("transaction without slot")?,
        block_time: entry.get("blockTime").and_then(Value::as_i64),
        tx_b64: b64.to_owned(),
        meta_json,
        via,
    })
}

/// Every successful transaction of the program in `[from, to]` from the bulk method.
///
/// # Errors
///
/// RPC failure, a page outside the window, or an entry without wire or meta.
pub async fn bulk(rpc: &dyn Rpc, program: &str, from: i64, to: i64) -> Result<Vec<Tx>, String> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let options = json!({
            "commitment": "finalized", "transactionDetails": "full", "sortOrder": "asc", "limit": 100,
            "encoding": "base64", "maxSupportedTransactionVersion": 1, "paginationToken": token,
            "filters": { "slot": { "gte": from, "lte": to }, "status": "succeeded", "tokenAccounts": "none" }
        });
        let reply = call(rpc, "getTransactionsForAddress", json!([program, options]))
            .await
            .map_err(rpc_error)?;
        let metas = raw_metas(&reply.raw);
        let Some(entries) = reply.result.get("data").and_then(Value::as_array) else {
            return Err("invalid bulk page".into());
        };
        for (index, entry) in entries.iter().enumerate() {
            let tx = tx_of(entry, metas.get(index).cloned().flatten(), "bulk")?;
            if tx.slot < from || tx.slot > to {
                return Err("bulk page outside the slot window".into());
            }
            out.push(tx);
        }
        token = reply
            .result
            .get("paginationToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if token.is_none() || entries.is_empty() {
            return Ok(out);
        }
    }
}

/// One transaction by signature, or `None` when the provider has none.
///
/// # Errors
///
/// RPC failure or an answer without wire or meta.
pub async fn single(rpc: &dyn Rpc, signature: &str) -> Result<Option<Tx>, String> {
    let options = json!({ "commitment": "finalized", "encoding": "base64", "maxSupportedTransactionVersion": 1 });
    let reply = call(rpc, "getTransaction", json!([signature, options]))
        .await
        .map_err(rpc_error)?;
    if reply.result.is_null() {
        return Ok(None);
    }
    tx_of(&reply.result, raw_meta(&reply.raw), "single").map(Some)
}

/// Run one cycle over `[from, to]` and turn the transactions into rows.
///
/// # Errors
///
/// Any RPC or completeness failure; nothing of the window is kept then, and the next cycle
/// retries it whole.
pub async fn cycle(
    rpc: &dyn Rpc,
    program: &str,
    idl: &Idl,
    window: (i64, i64),
    store_transactions: bool,
) -> Result<(Cycle, CycleRows), String> {
    let (from, to) = window;
    let manifest = manifest(rpc, program, from, to).await?;
    let mut txs = bulk(rpc, program, from, to).await?;
    let mut stats = Cycle {
        from,
        to,
        manifest: manifest.ok.len() as u64,
        failed: manifest.failed,
        ..Cycle::default()
    };
    stats.bulk = txs.len() as u64;
    let have: HashSet<String> = txs.iter().map(|t| t.signature.clone()).collect();
    stats.unlisted = have
        .iter()
        .filter(|s| !manifest.ok.contains_key(*s))
        .count() as u64;
    for signature in manifest.ok.keys().filter(|s| !have.contains(*s)) {
        match single(rpc, signature).await? {
            Some(tx) => {
                stats.single += 1;
                txs.push(tx);
            }
            None => stats.missing += 1,
        }
    }
    if stats.missing > 0 {
        return Err(format!("{} manifest signatures not served", stats.missing));
    }
    let mut rows = CycleRows::default();
    for tx in &txs {
        to_rows(tx, program, idl, store_transactions, &mut stats, &mut rows);
    }
    Ok((stats, rows))
}

/// The rows of one transaction.
pub fn to_rows(
    tx: &Tx,
    program: &str,
    idl: &Idl,
    store_transactions: bool,
    stats: &mut Cycle,
    rows: &mut CycleRows,
) {
    if store_transactions {
        rows.transactions.push(json!({
            "signature": tx.signature, "slot": tx.slot, "block_time": tx.block_time,
            "tx_b64": tx.tx_b64, "meta_json": tx.meta_json, "fetched_via": tx.via,
        }));
    }
    let meta: Value = serde_json::from_str(&tx.meta_json).unwrap_or(Value::Null);
    let at = TxMeta {
        signature: &tx.signature,
        slot: tx.slot,
        block_time: tx.block_time,
    };
    match extract(&tx.tx_b64, &meta, program, idl, &at) {
        Ok(found) => {
            stats.events += found.events.len() as u64;
            stats.undecoded += found.undecoded;
            stats.unknown += found.unknown;
            stats.instructions += found.instructions.len() as u64;
            rows.events.extend(found.events);
            rows.instructions.extend(found.instructions);
        }
        Err(_) => stats.unparsed += 1,
    }
}
