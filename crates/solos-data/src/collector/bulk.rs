//! Bulk acceleration over `getTransactionsForAddress`: the standard manifest still defines fetch
//! completeness; pages and their cursor commit together. A port of `bulk-fetcher.ts`.

use super::config::{Config, Lane};
use super::fetcher::{RawRow, insert_raw, raw_metas};
use super::rpc::Rpc;
use crate::db::Db;
use crate::jsonout::now;
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::HashMap;

struct Page {
    id: String,
    raw: String,
    at: String,
    slot: i64,
    rows: Vec<RawRow>,
}

/// Fetch every bulk page of `[from, to]`, resuming from the durable pagination token.
pub async fn bulk_fetch_range(
    rpc: &dyn Rpc,
    db: &Db,
    config: &Config,
    lane: Lane,
    from: i64,
    to: i64,
) -> Result<(), StoreError> {
    let key = format!("bulk/{}/{from}/{to}", lane.as_str());
    let mut state = db
        .get(&key)
        .await?
        .unwrap_or_else(|| json!({ "done": false }));
    if state.get("done").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    // This range's standard manifest is already complete. Load it once, not once per page.
    let manifest_rows = db
        .rows(
            "SELECT signature, slot, mode FROM signatures WHERE slot BETWEEN ? AND ?",
            vec![from.into(), to.into()],
        )
        .await?;
    let manifests: HashMap<String, (i64, String)> = manifest_rows
        .iter()
        .filter_map(|r| {
            Some((
                r.str("signature")?.to_owned(),
                (r.int("slot")?, r.str("mode").unwrap_or("").to_owned()),
            ))
        })
        .collect();
    let mut first = true;
    let mut pending: Vec<Page> = Vec::new();
    while state.get("done").and_then(Value::as_bool) != Some(true) {
        let mut options = json!({
            "commitment": "finalized", "transactionDetails": "full", "sortOrder": "asc", "limit": 100,
            "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version,
            "filters": { "slot": { "gte": from, "lte": to }, "status": "any", "tokenAccounts": "none" }
        });
        if let Some(token) = state.get("token").and_then(Value::as_str) {
            options["paginationToken"] = Value::String(token.to_owned());
        } else {
            options["paginationToken"] = Value::Null;
        }
        let reply = rpc
            .call(
                "getTransactionsForAddress",
                json!([config.program_id, options]),
                lane,
            )
            .await
            .map_err(|e| StoreError::Check(e.to_string()))?;
        let metas = raw_metas(&reply.raw);
        let result = reply.result;
        let Some(entries) = result.get("data").and_then(Value::as_array) else {
            return Err(StoreError::Check("Invalid bulk transaction page".into()));
        };
        let page_id = uuid::Uuid::new_v4().to_string();
        let mut rows = Vec::new();
        for (index, tx) in entries.iter().enumerate() {
            let slot = tx.get("slot").and_then(Value::as_i64).unwrap_or(-1);
            if slot < from || slot > to {
                return Err(StoreError::Check(
                    "Bulk provider ignored slot bounds".into(),
                ));
            }
            let wire = tx.get("transaction").and_then(Value::as_array);
            let meta = tx.get("meta");
            let (Some(wire), Some(meta)) = (wire, meta) else {
                return Err(StoreError::Check("Invalid bulk wire transaction".into()));
            };
            if wire.get(1).and_then(Value::as_str) != Some("base64") || meta.is_null() {
                return Err(StoreError::Check("Invalid bulk wire transaction".into()));
            }
            let tx_b64 = wire
                .first()
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            let signature = solana_wire::signature_of_base64(&tx_b64)
                .map_err(|e| StoreError::Check(e.to_string()))?;
            let Some((manifest_slot, mode)) = manifests.get(&signature) else {
                // Do not silently treat a discrepancy between two provider indexes as independent proof.
                let provider = rpc.provider_name().to_owned();
                let sig = signature.clone();
                db.run(move |store| {
                    store.exec(
                        "INSERT INTO errors VALUES (?, ?, ?, ?, ?)",
                        &[
                            &"index_gap",
                            &sig,
                            &provider,
                            &"bulk_signature_absent_from_manifest",
                            &now(),
                        ],
                    )?;
                    Ok(())
                })
                .await?;
                return Err(StoreError::Check(
                    "Bulk signature is absent from the completed standard manifest".into(),
                ));
            };
            if *manifest_slot != slot {
                return Err(StoreError::Check(
                    "Bulk slot does not match manifest".into(),
                ));
            }
            rows.push(RawRow {
                signature,
                slot,
                block_time: tx.get("blockTime").cloned().unwrap_or(Value::Null),
                err: meta.get("err").cloned().unwrap_or(Value::Null),
                fee: meta.get("fee").cloned().unwrap_or(Value::Null),
                compute_units_consumed: meta
                    .get("computeUnitsConsumed")
                    .cloned()
                    .unwrap_or(Value::Null),
                tx_b64,
                meta_json: metas
                    .get(index)
                    .cloned()
                    .flatten()
                    .unwrap_or_else(|| meta.to_string()),
                mode: mode.clone(),
                raw_rpc_json: json!({ "pageId": page_id, "arrayIndex": index }).to_string(),
                provider: rpc.provider_name().to_owned(),
                fetched_at: now(),
            });
        }
        let next_token = result
            .get("paginationToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if next_token.is_some()
            && next_token.as_deref() == state.get("token").and_then(Value::as_str)
        {
            return Err(StoreError::Check("Bulk cursor did not advance".into()));
        }
        state = match &next_token {
            Some(token) => json!({ "token": token, "done": false }),
            None => json!({ "done": true }),
        };
        pending.push(Page {
            id: page_id,
            raw: reply.raw,
            at: now(),
            slot: entries
                .first()
                .and_then(|e| e.get("slot"))
                .and_then(Value::as_i64)
                .unwrap_or(from),
            rows,
        });
        // Establish an immediate durable anchor; subsequent bounded batches amortize commits. On
        // interruption, uncommitted pages replay from the stored token.
        let done = state.get("done").and_then(Value::as_bool) == Some(true);
        if !first && !done && pending.len() < config.bulk_commit_pages as usize {
            continue;
        }
        let pages = std::mem::take(&mut pending);
        let provider = rpc.provider_name().to_owned();
        let key2 = key.clone();
        let state2 = state.clone();
        db.run(move |store| {
            store.transaction(|store| {
                for page in &pages {
                    store.exec(
                        "INSERT INTO rpc_pages VALUES (?, ?, ?, ?, ?, ?)",
                        &[
                            &page.id,
                            &"getTransactionsForAddress",
                            &provider,
                            &page.raw,
                            &page.at,
                            &page.slot,
                        ],
                    )?;
                }
                let rows: Vec<RawRow> = pages
                    .iter()
                    .flat_map(|page| page.rows.iter().cloned())
                    .collect();
                insert_raw(store, &rows, from, to)?;
                store.exec(
                    "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
                    &[&key2, &state2.to_string()],
                )?;
                Ok(())
            })
        })
        .await?;
        first = false;
    }
    Ok(())
}
