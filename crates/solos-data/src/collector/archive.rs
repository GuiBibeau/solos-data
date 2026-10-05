//! The archive lane (ADR-0008): transactions and block indexes for a slot range come from the Old
//! Faithful archive through Jetstreamer; the RPC manifest stays the independent completeness
//! check and `getBlock` becomes a sampled ordering cross-check. Without the `archive` feature
//! this module only reports that the lane is unavailable.

#[cfg(not(feature = "archive"))]
use super::config::Config;
#[cfg(not(feature = "archive"))]
use super::rpc::Rpc;
#[cfg(not(feature = "archive"))]
use crate::db::Db;
#[cfg(not(feature = "archive"))]
use crate::store::StoreError;
#[cfg(not(feature = "archive"))]
use std::sync::Arc;

/// Collect `[from, to]` from the archive into the checkpoint (transactions and `slot_order`).
#[cfg(not(feature = "archive"))]
pub async fn collect_range(
    _rpc: Arc<dyn Rpc>,
    _db: &Db,
    _config: &Config,
    _program_data: &str,
    _from: i64,
    _to: i64,
) -> Result<(), StoreError> {
    Err(StoreError::Check(
        "archive lane unavailable: this binary was built without the archive feature".into(),
    ))
}

/// The newest slot the archive covers, or `None` when the lane is unavailable or disabled.
#[cfg(not(feature = "archive"))]
pub async fn archive_tip(_config: &Config, _current_epoch_hint: Option<u64>) -> Option<i64> {
    None
}

#[cfg(feature = "archive")]
pub use enabled::{archive_tip, check_range, collect_range};

/// Re-collect `[from, to]` from the archive into a scratch checkpoint and compare it with what
/// the raw archive already published for the same slots.
#[cfg(not(feature = "archive"))]
pub async fn check_range(
    _rpc: Arc<dyn Rpc>,
    _config: &Config,
    _program_data: &str,
    _from: i64,
    _to: i64,
) -> Result<crate::jsonout::Obj, StoreError> {
    Err(StoreError::Check(
        "archive lane unavailable: this binary was built without the archive feature".into(),
    ))
}

#[cfg(feature = "archive")]
mod enabled {
    use super::super::config::{Config, Lane};
    use super::super::fetcher::{RawRow, insert_raw};
    use super::super::ordering::Ordered;
    use super::super::rpc::Rpc;
    use crate::db::Db;
    use crate::jsonout::{Obj, log, now};
    use crate::store::StoreError;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use futures_util::FutureExt;
    use jetstreamer_firehose::firehose::{BlockData, TransactionData, firehose};
    use serde_json::{Value, json};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    /// Epoch length on mainnet.
    const EPOCH_SLOTS: u64 = 432_000;

    /// The last slot of the newest published epoch at or below `hint`, probing downward.
    pub async fn archive_tip(config: &Config, current_epoch_hint: Option<u64>) -> Option<i64> {
        if !config.archive_backfill_enabled {
            return None;
        }
        let client = reqwest::Client::new();
        let mut epoch = current_epoch_hint?;
        for _ in 0..8 {
            if jetstreamer_firehose::epochs::epoch_exists(epoch, &client).await {
                return i64::try_from(epoch * EPOCH_SLOTS + EPOCH_SLOTS - 1).ok();
            }
            epoch = epoch.checked_sub(1)?;
        }
        None
    }

    struct Collected {
        rows: Vec<RawRow>,
        blocks: HashMap<u64, (Option<i64>, u64)>,
        slots_seen: HashSet<u64>,
    }

    /// Stream the range, keep the program's transactions, then write transactions and ordering.
    pub async fn collect_range(
        rpc: Arc<dyn Rpc>,
        db: &Db,
        config: &Config,
        program_data: &str,
        from: i64,
        to: i64,
    ) -> Result<(), StoreError> {
        let started = std::time::Instant::now();
        let watched: Arc<HashSet<String>> = Arc::new(
            [config.program_id.clone(), program_data.to_owned()]
                .into_iter()
                .collect(),
        );
        let collected = Arc::new(Mutex::new(Collected {
            rows: Vec::new(),
            blocks: HashMap::new(),
            slots_seen: HashSet::new(),
        }));
        let (c_tx, c_block) = (Arc::clone(&collected), Arc::clone(&collected));
        let watched_tx = Arc::clone(&watched);
        let on_tx = move |_thread: usize, tx: TransactionData| {
            let collected = Arc::clone(&c_tx);
            let watched = Arc::clone(&watched_tx);
            async move {
                let keys: Vec<String> = tx
                    .transaction
                    .message
                    .static_account_keys()
                    .iter()
                    .map(ToString::to_string)
                    .chain(tx.transaction_status_meta.loaded_addresses.writable.iter().map(ToString::to_string))
                    .chain(tx.transaction_status_meta.loaded_addresses.readonly.iter().map(ToString::to_string))
                    .collect();
                if !keys.iter().any(|k| watched.contains(k)) {
                    return Ok(());
                }
                let wire = wincode::serialize(&tx.transaction).map_err(|e| -> jetstreamer_firehose::SharedError { Box::new(std::io::Error::other(e.to_string())) })?;
                let ui = solana_transaction_status::UiTransactionStatusMeta::from(tx.transaction_status_meta.clone());
                let meta = serde_json::to_value(&ui).map_err(|e| -> jetstreamer_firehose::SharedError { Box::new(e) })?;
                let row = RawRow {
                    signature: tx.signature.to_string(),
                    slot: i64::try_from(tx.slot).unwrap_or(i64::MAX),
                    block_time: Value::Null,
                    err: meta.get("err").cloned().unwrap_or(Value::Null),
                    fee: meta.get("fee").cloned().unwrap_or(Value::Null),
                    compute_units_consumed: meta.get("computeUnitsConsumed").cloned().unwrap_or(Value::Null),
                    tx_b64: STANDARD.encode(&wire),
                    meta_json: meta.to_string(),
                    raw_rpc_json: json!({ "source": "old-faithful", "epoch": tx.slot / EPOCH_SLOTS, "transactionSlotIndex": tx.transaction_slot_index }).to_string(),
                    mode: "backfill".into(),
                    provider: "old-faithful".into(),
                    fetched_at: now(),
                };
                let mut guard = collected.lock().expect("collected");
                guard.rows.push(row);
                Ok(())
            }
            .boxed()
        };
        let on_block = move |_thread: usize, block: BlockData| {
            let collected = Arc::clone(&c_block);
            async move {
                let mut guard = collected.lock().expect("collected");
                match block {
                    BlockData::Block {
                        slot,
                        block_time,
                        executed_transaction_count,
                        ..
                    } => {
                        guard
                            .blocks
                            .insert(slot, (block_time, executed_transaction_count));
                        guard.slots_seen.insert(slot);
                    }
                    BlockData::PossibleLeaderSkipped { slot } => {
                        guard.slots_seen.insert(slot);
                    }
                }
                Ok(())
            }
            .boxed()
        };
        let threads = std::env::var("JETSTREAMER_THREADS")
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or_else(|| {
                jetstreamer_firehose::system::optimal_firehose_thread_count() as u64
            });
        let range = u64::try_from(from).unwrap_or(0)..u64::try_from(to + 1).unwrap_or(0);
        let outcome = firehose(
            threads,
            false,
            true,
            None,
            range,
            Some(on_block),
            Some(on_tx),
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::EntryData,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::RewardsData,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::FirehoseErrorContext,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::StatsTracking<
                    jetstreamer_firehose::firehose::HandlerFn<
                        jetstreamer_firehose::firehose::Stats,
                    >,
                >,
            >,
            None,
        )
        .await;
        if let Err((error, slot)) = outcome {
            return Err(StoreError::Check(crate::jsonout::safe_error(&format!(
                "archive stream failed at slot {slot}: {error:?}"
            ))));
        }
        let Collected {
            mut rows, blocks, ..
        } = std::mem::replace(
            &mut *collected.lock().expect("collected"),
            Collected {
                rows: Vec::new(),
                blocks: HashMap::new(),
                slots_seen: HashSet::new(),
            },
        );
        for row in &mut rows {
            if let Some((time, _)) = blocks.get(&u64::try_from(row.slot).unwrap_or(0)) {
                row.block_time = time.map_or(Value::Null, Value::from);
            }
        }
        // Block order from the archive: index within the block and the block's transaction count.
        let mut per_slot: HashMap<i64, Vec<&RawRow>> = HashMap::new();
        for row in &rows {
            per_slot.entry(row.slot).or_default().push(row);
        }
        let mut ordered: Vec<Ordered> = Vec::new();
        let mut singles: Vec<i64> = Vec::new();
        for (slot, slot_rows) in &per_slot {
            if slot_rows.len() == 1 {
                singles.push(*slot);
                continue;
            }
            let count = blocks
                .get(&u64::try_from(*slot).unwrap_or(0))
                .map_or(0, |b| i64::try_from(b.1).unwrap_or(0));
            for row in slot_rows {
                let index: Value = serde_json::from_str::<Value>(&row.raw_rpc_json)
                    .ok()
                    .and_then(|v| v.get("transactionSlotIndex").cloned())
                    .unwrap_or(Value::Null);
                ordered.push(Ordered {
                    signature: row.signature.clone(),
                    slot: *slot,
                    tx_index: index.as_i64().unwrap_or(0),
                    block_signature_count: count,
                });
            }
        }
        let fetched = rows.len();
        db.run(move |store| {
            store.transaction(|store| {
                insert_raw(store, &rows, from, to)?;
                store.exec("DELETE FROM slot_order WHERE slot BETWEEN ? AND ?", &[&from, &to])?;
                if !ordered.is_empty() {
                    let payload = Value::Array(ordered.iter().map(|o| json!({ "signature": o.signature, "slot": o.slot, "tx_index": o.tx_index, "block_signature_count": o.block_signature_count })).collect()).to_string();
                    store.exec(
                        "INSERT INTO slot_order SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'tx_index')::INTEGER, (value->>'block_signature_count')::INTEGER FROM json_each(?::JSON)",
                        &[&payload],
                    )?;
                }
                store.exec(
                    "UPDATE transactions SET tx_index=o.tx_index, single_in_slot=false FROM slot_order o WHERE transactions.signature=o.signature AND o.slot BETWEEN ? AND ? AND transactions.slot BETWEEN ? AND ?",
                    &[&from, &to, &from, &to],
                )?;
                if !singles.is_empty() {
                    let payload = Value::Array(singles.iter().map(|s| Value::from(*s)).collect()).to_string();
                    store.exec("UPDATE transactions SET tx_index=NULL, single_in_slot=true WHERE slot IN (SELECT value::BIGINT FROM json_each(?::JSON)) AND slot BETWEEN ? AND ?", &[&payload, &from, &to])?;
                }
                Ok(())
            })
        })
        .await?;
        cross_check(rpc, db, config, from, to).await?;
        log(
            "archive_range",
            Obj::new()
                .with("from", from)
                .with("to", to)
                .with("transactions", fetched)
                .with("seconds", started.elapsed().as_secs_f64()),
        );
        Ok(())
    }

    /// The archive-lane proof (ADR-0008): collect `[from, to]` into a scratch root under the data
    /// directory and compare signatures, block indexes, wire bytes and the decisive meta fields with
    /// the rows the raw archive already holds for those slots. Nothing in the live root changes.
    pub async fn check_range(
        rpc: Arc<dyn Rpc>,
        config: &Config,
        program_data: &str,
        from: i64,
        to: i64,
    ) -> Result<Obj, StoreError> {
        let scratch = config
            .data_dir
            .join(format!(".archive-check-{}", uuid::Uuid::new_v4()));
        let store = crate::store::Store::open(&scratch, super::super::schema::SCHEMA)?;
        let (db, thread) = crate::db::Db::spawn(store);
        let mut check_config = config.clone();
        check_config.archive_ordering_sample = 0.0;
        let collected =
            collect_range(Arc::clone(&rpc), &db, &check_config, program_data, from, to).await;
        let scratch_rows = db.rows("SELECT signature, slot, tx_index, single_in_slot, tx_b64, meta_json FROM transactions ORDER BY slot, signature", vec![]).await?;
        let store = thread.join(db);
        store.close()?;
        let _ = std::fs::remove_dir_all(&scratch);
        collected?;
        let published = crate::collector::reader::query_dataset(
            &config.data_dir,
            &format!(
                "SELECT signature, slot, tx_index, single_in_slot, tx_b64, meta_json FROM transactions WHERE slot BETWEEN {from} AND {to} ORDER BY slot, signature"
            ),
        )?;
        let published_text = published.to_json();
        let published_rows: Vec<Value> = serde_json::from_str::<Value>(&published_text)
            .ok()
            .and_then(|v| v.get("rows").and_then(Value::as_array).cloned())
            .unwrap_or_default();
        let key = |v: &Value, k: &str| {
            v.get(k)
                .map(|x| x.as_str().map_or_else(|| x.to_string(), str::to_owned))
        };
        let published_by_signature: HashMap<String, Value> = published_rows
            .iter()
            .filter_map(|r| Some((r.get("signature")?.as_str()?.to_owned(), r.clone())))
            .collect();
        let mut only_archive = Vec::new();
        let mut index_mismatch = Vec::new();
        let mut wire_mismatch = Vec::new();
        let mut meta_mismatch = Vec::new();
        for row in &scratch_rows {
            let signature = row.str("signature").unwrap_or("").to_owned();
            let Some(theirs) = published_by_signature.get(&signature) else {
                only_archive.push(Value::String(signature));
                continue;
            };
            let mine = row.to_value();
            if key(&mine, "tx_index") != key(theirs, "tx_index")
                || key(&mine, "single_in_slot") != key(theirs, "single_in_slot")
            {
                index_mismatch.push(Value::String(signature.clone()));
            }
            if key(&mine, "tx_b64") != key(theirs, "tx_b64") {
                wire_mismatch.push(Value::String(signature.clone()));
            }
            let decisive = |text: Option<String>| -> Value {
                let parsed: Value = text
                    .and_then(|t| serde_json::from_str(&t).ok())
                    .unwrap_or(Value::Null);
                json!({
                    "err": parsed.get("err"), "fee": parsed.get("fee"), "computeUnitsConsumed": parsed.get("computeUnitsConsumed"),
                    "innerInstructions": parsed.get("innerInstructions"), "loadedAddresses": parsed.get("loadedAddresses"),
                    "preBalances": parsed.get("preBalances"), "postBalances": parsed.get("postBalances"),
                })
            };
            if decisive(key(&mine, "meta_json")) != decisive(key(theirs, "meta_json")) {
                meta_mismatch.push(Value::String(signature));
            }
        }
        let archive_signatures: HashSet<String> = scratch_rows
            .iter()
            .filter_map(|r| r.str("signature").map(str::to_owned))
            .collect();
        let only_published: Vec<Value> = published_rows
            .iter()
            .filter_map(|r| r.get("signature").and_then(Value::as_str))
            .filter(|s| !archive_signatures.contains(*s))
            .map(|s| Value::String(s.to_owned()))
            .collect();
        let identical = only_archive.is_empty()
            && only_published.is_empty()
            && index_mismatch.is_empty()
            && wire_mismatch.is_empty()
            && meta_mismatch.is_empty();
        Ok(Obj::new()
            .with("from", from)
            .with("to", to)
            .with("archiveRows", scratch_rows.len())
            .with("publishedRows", published_rows.len())
            .with("identical", identical)
            .with(
                "onlyArchive",
                Value::Array(only_archive.into_iter().take(10).collect()),
            )
            .with(
                "onlyPublished",
                Value::Array(only_published.into_iter().take(10).collect()),
            )
            .with(
                "indexMismatch",
                Value::Array(index_mismatch.into_iter().take(10).collect()),
            )
            .with(
                "wireMismatch",
                Value::Array(wire_mismatch.into_iter().take(10).collect()),
            )
            .with(
                "metaMismatch",
                Value::Array(meta_mismatch.into_iter().take(10).collect()),
            ))
    }

    /// Sampled V6: a share of multi-transaction slots is compared with the finalized block from RPC.
    async fn cross_check(
        rpc: Arc<dyn Rpc>,
        db: &Db,
        config: &Config,
        from: i64,
        to: i64,
    ) -> Result<(), StoreError> {
        if config.archive_ordering_sample <= 0.0 {
            return Ok(());
        }
        let slots = db.rows("SELECT slot, list(signature ORDER BY tx_index) AS signatures, list(tx_index ORDER BY tx_index) AS indexes FROM slot_order WHERE slot BETWEEN ? AND ? GROUP BY slot ORDER BY slot", vec![from.into(), to.into()]).await?;
        let every = (1.0 / config.archive_ordering_sample).round().max(1.0) as usize;
        for row in slots.iter().step_by(every) {
            let slot = row.int("slot").unwrap_or(0);
            let block = rpc.call("getBlock", json!([slot, { "transactionDetails": "signatures", "rewards": false, "maxSupportedTransactionVersion": 1, "commitment": "finalized" }]), Lane::Backfill).await.map_err(|e| StoreError::Check(e.to_string()))?;
            let Some(block_signatures) = block.result.get("signatures").and_then(Value::as_array)
            else {
                return Err(StoreError::Check(format!(
                    "V6: finalized block {slot} unavailable"
                )));
            };
            let signatures: Vec<&str> = row
                .get("signatures")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let indexes: Vec<i64> = row
                .get("indexes")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| {
                            v.as_str()
                                .and_then(|s| s.parse().ok())
                                .or_else(|| v.as_i64())
                        })
                        .collect()
                })
                .unwrap_or_default();
            for (signature, index) in signatures.iter().zip(indexes) {
                if block_signatures
                    .get(usize::try_from(index).unwrap_or(usize::MAX))
                    .and_then(Value::as_str)
                    != Some(signature)
                {
                    return Err(StoreError::Check(format!(
                        "V6: archive ordering disagrees with finalized block {slot}"
                    )));
                }
            }
        }
        Ok(())
    }
}
