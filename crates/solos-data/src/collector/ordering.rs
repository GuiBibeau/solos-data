//! Block ordering (V6): every multi-transaction slot's finalized block supplies the transaction
//! index; batches of slots commit together. A port of `ordering.ts`.

use super::config::Lane;
use super::rpc::Rpc;
use crate::db::Db;
use crate::jsonout::Obj;
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

/// One ordered signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ordered {
    /// Signature.
    pub signature: String,
    /// Slot.
    pub slot: i64,
    /// Index in the block.
    pub tx_index: i64,
    /// Transactions in the block.
    pub block_signature_count: i64,
}

/// Join manifest signatures to the block's signature list.
pub fn join_ordering(
    slot: i64,
    signatures: &[String],
    block: &[String],
) -> Result<Vec<Ordered>, StoreError> {
    let indexes: HashMap<&str, usize> = block
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    signatures
        .iter()
        .map(|signature| {
            let index = indexes.get(signature.as_str()).ok_or_else(|| {
                StoreError::Check(format!("V6: signature missing from finalized block {slot}"))
            })?;
            Ok(Ordered {
                signature: signature.clone(),
                slot,
                tx_index: *index as i64,
                block_signature_count: block.len() as i64,
            })
        })
        .collect()
}

/// Order every slot of `[from, to]` in batches of `batch_slots`, reusing cached block indexes.
pub async fn order_range(
    rpc: Arc<dyn Rpc>,
    db: &Db,
    lane: Lane,
    from: i64,
    to: i64,
    batch_slots: usize,
) -> Result<(), StoreError> {
    let slots = db
        .rows(
            "SELECT slot, list(signature ORDER BY signature) AS signatures
    FROM signatures WHERE slot BETWEEN ? AND ? GROUP BY slot ORDER BY slot",
            vec![from.into(), to.into()],
        )
        .await?;
    let mut cache: HashMap<i64, Vec<Obj>> = HashMap::new();
    for item in db
        .rows(
            "SELECT * FROM slot_order WHERE slot BETWEEN ? AND ?",
            vec![from.into(), to.into()],
        )
        .await?
    {
        cache
            .entry(item.int("slot").unwrap_or(0))
            .or_default()
            .push(item);
    }
    let cache = Arc::new(cache);
    // Bound memory and replay on failure; avoid one read/commit/fsync for every slot.
    for batch in slots.chunks(batch_slots.max(1)) {
        let ordered = Arc::new(std::sync::Mutex::new(Vec::<Ordered>::new()));
        let singles = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let items: Vec<(i64, Vec<String>)> = batch
            .iter()
            .map(|row| {
                let signatures = row
                    .get("signatures")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                (row.int("slot").unwrap_or(0), signatures)
            })
            .collect();
        let (rpc2, cache2, ordered2, singles2) = (
            Arc::clone(&rpc),
            Arc::clone(&cache),
            Arc::clone(&ordered),
            Arc::clone(&singles),
        );
        super::fetcher::parallel(items, 32, move |(slot, signatures)| {
            let (rpc, cache, ordered, singles) = (Arc::clone(&rpc2), Arc::clone(&cache2), Arc::clone(&ordered2), Arc::clone(&singles2));
            async move {
                if signatures.len() == 1 {
                    singles.lock().expect("singles").push(slot);
                    return Ok(());
                }
                let cached = cache.get(&slot).cloned().unwrap_or_default();
                if cached.len() == signatures.len() && signatures.iter().all(|s| cached.iter().any(|c| c.str("signature") == Some(s.as_str()))) {
                    let mut out = ordered.lock().expect("ordered");
                    for item in cached {
                        out.push(Ordered { signature: item.str("signature").unwrap_or("").to_owned(), slot, tx_index: item.int("tx_index").unwrap_or(0), block_signature_count: item.int("block_signature_count").unwrap_or(0) });
                    }
                    return Ok(());
                }
                let block = rpc
                    .call("getBlock", json!([slot, { "transactionDetails": "signatures", "rewards": false, "maxSupportedTransactionVersion": 1, "commitment": "finalized" }]), lane)
                    .await
                    .map_err(|e| StoreError::Check(e.to_string()))?;
                let Some(block_signatures) = block.result.get("signatures").and_then(Value::as_array) else {
                    return Err(StoreError::Check(format!("V6: finalized block {slot} unavailable")));
                };
                let block_signatures: Vec<String> = block_signatures.iter().filter_map(Value::as_str).map(str::to_owned).collect();
                let joined = join_ordering(slot, &signatures, &block_signatures)?;
                ordered.lock().expect("ordered").extend(joined);
                Ok(())
            }
        })
        .await?;
        let first = batch.first().and_then(|r| r.int("slot")).unwrap_or(from);
        let last = batch.last().and_then(|r| r.int("slot")).unwrap_or(to);
        let ordered: Vec<Ordered> = std::mem::take(&mut *ordered.lock().expect("ordered"));
        let singles: Vec<i64> = std::mem::take(&mut *singles.lock().expect("singles"));
        db.run(move |store| {
            store.transaction(|store| {
                store.exec("DELETE FROM slot_order WHERE slot BETWEEN ? AND ?", &[&first, &last])?;
                if !ordered.is_empty() {
                    let payload = Value::Array(
                        ordered.iter().map(|o| json!({ "signature": o.signature, "slot": o.slot, "tx_index": o.tx_index, "block_signature_count": o.block_signature_count })).collect(),
                    )
                    .to_string();
                    store.exec(
                        "INSERT INTO slot_order
        SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'tx_index')::INTEGER,
        (value->>'block_signature_count')::INTEGER FROM json_each(?::JSON)",
                        &[&payload],
                    )?;
                }
                store.exec(
                    "UPDATE transactions SET tx_index=o.tx_index, single_in_slot=false
        FROM slot_order o WHERE transactions.signature=o.signature AND o.slot BETWEEN ? AND ?
        AND transactions.slot BETWEEN ? AND ?
        AND (transactions.tx_index IS DISTINCT FROM o.tx_index OR transactions.single_in_slot IS DISTINCT FROM false)",
                    &[&first, &last, &first, &last],
                )?;
                if !singles.is_empty() {
                    let payload = Value::Array(singles.iter().map(|s| Value::from(*s)).collect()).to_string();
                    store.exec("UPDATE transactions SET tx_index=NULL, single_in_slot=true WHERE slot IN (SELECT value::BIGINT FROM json_each(?::JSON))", &[&payload])?;
                }
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}
