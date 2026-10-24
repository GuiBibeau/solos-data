//! The forward spot lane's source (ADR-0015): every produced block of a slot range from the
//! RPC's `getBlock` (full transactions, base64, no rewards, finalized), decoded as it arrives
//! into the rows, block counts and manifest samples the archive stream gives (ADR-0014), so the
//! checks, files and tables are the same. A transaction's index is its position in the block,
//! as the archive's is; its rows come from the RPC-shape decoder the fixtures prove equal to the
//! archive's.

use super::archive::{Block, Stream};
use super::dex::Programs;
use super::rows::{Rows, named};
use super::tx::{Key, from_rpc, key_of};
use crate::collector::rpc::Rpc;
use crate::venues::rpc::call;
use futures_util::StreamExt;
use serde_json::{Value, json};

/// What a fetch read besides rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fetched {
    /// Blocks fetched.
    pub blocks: u64,
    /// Bytes of the answers (the JSON bodies as received).
    pub bytes: u64,
}

/// The parameters of one block's `getBlock`.
#[must_use]
pub fn block_params(slot: u64) -> Value {
    json!([slot, {
        "commitment": "finalized", "encoding": "base64", "transactionDetails": "full",
        "rewards": false, "maxSupportedTransactionVersion": 1
    }])
}

/// The account keys a transaction names: its static keys, then those it loaded.
fn keys_of(tx_b64: &str, meta: &Value) -> Result<Vec<Key>, String> {
    let wire = solana_wire::parse_base64(tx_b64).map_err(|e| e.to_string())?;
    let mut keys = wire.static_account_bytes();
    for list in ["writable", "readonly"] {
        keys.extend(
            meta.get("loadedAddresses")
                .and_then(|l| l.get(list))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter_map(key_of),
        );
    }
    Ok(keys)
}

/// Add one block (a `getBlock` result) to `stream`: its time and transaction count, the rows of
/// its successful transactions that name a program, the failed ones counted per program, and
/// the signatures of those inside a manifest window.
///
/// # Errors
///
/// A block without its transaction list, or a transaction without its wire or meta, or whose
/// wire does not parse.
pub fn add_block(
    stream: &mut Stream,
    slot: u64,
    block: &Value,
    programs: &Programs,
    windows: &[(u64, u64)],
) -> Result<(), String> {
    let txs = block
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("block {slot} without transactions"))?;
    let count = txs.len() as u64;
    stream.blocks.insert(
        slot,
        Block {
            time: block.get("blockTime").and_then(Value::as_i64),
            executed: count,
        },
    );
    *stream.delivered.entry(slot).or_default() += count;
    let sampled = windows.iter().any(|(a, b)| (*a..=*b).contains(&slot));
    for (index, entry) in txs.iter().enumerate() {
        let wire = entry
            .get("transaction")
            .and_then(Value::as_array)
            .and_then(|w| w.first())
            .and_then(Value::as_str);
        let (Some(tx_b64), Some(meta)) = (wire, entry.get("meta").filter(|m| m.is_object())) else {
            return Err(format!(
                "block {slot}: transaction {index} without wire or meta"
            ));
        };
        let dexes = named(
            &keys_of(tx_b64, meta).map_err(|e| format!("block {slot}: {e}"))?,
            programs,
        );
        if dexes.is_empty() {
            continue;
        }
        if meta.get("err").is_some_and(|e| !e.is_null()) {
            for dex in dexes {
                stream.rows.failed(dex);
            }
            continue;
        }
        let index = u32::try_from(index).map_err(|_| format!("block {slot}: too many txs"))?;
        let tx = from_rpc(tx_b64, meta, slot, index).map_err(|e| format!("block {slot}: {e}"))?;
        if sampled {
            let signature = tx.signature_b58();
            for dex in &dexes {
                stream.sampled.push((*dex, slot, signature.clone()));
            }
        }
        stream.rows.add_tx(&tx, programs, &dexes);
    }
    Ok(())
}

/// Fetch every slot of `listed` with `getBlock`, `concurrency` at a time, into one stream.
///
/// # Errors
///
/// An RPC failure after the provider's retries, a listed block the provider does not serve, or
/// a block [`add_block`] rejects: the caller retries the batch whole.
pub async fn fetch(
    rpc: &dyn Rpc,
    listed: &[u64],
    concurrency: usize,
    programs: &Programs,
    windows: &[(u64, u64)],
) -> Result<(Stream, Fetched), String> {
    let mut stream = Stream {
        rows: Rows::new(),
        ..Stream::default()
    };
    let mut fetched = Fetched::default();
    let mut replies = futures_util::stream::iter(listed.iter().copied())
        .map(|slot| async move { (slot, call(rpc, "getBlock", block_params(slot)).await) })
        .buffer_unordered(concurrency.max(1));
    while let Some((slot, reply)) = replies.next().await {
        let reply = reply.map_err(|e| format!("getBlock {slot}: {e}"))?;
        if !reply.result.is_object() {
            return Err(format!("getBlock {slot} answered no block"));
        }
        fetched.blocks += 1;
        fetched.bytes += reply.raw.len() as u64;
        add_block(&mut stream, slot, &reply.result, programs, windows)?;
    }
    Ok((stream, fetched))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_needs_its_transactions_and_their_meta() {
        let mut stream = Stream {
            rows: Rows::new(),
            ..Stream::default()
        };
        let programs = Programs::all();
        assert!(add_block(&mut stream, 5, &json!({}), &programs, &[]).is_err());
        let no_meta =
            json!({ "blockTime": 7, "transactions": [{ "transaction": ["AA==", "base64"] }] });
        assert!(add_block(&mut stream, 5, &no_meta, &programs, &[]).is_err());
        let empty = json!({ "blockTime": 7, "transactions": [] });
        add_block(&mut stream, 6, &empty, &programs, &[]).unwrap();
        assert_eq!(
            stream.blocks[&6],
            Block {
                time: Some(7),
                executed: 0
            }
        );
        assert_eq!(stream.delivered[&6], 0);
    }
}
