//! Transaction fetching: the bulk endpoint fills a range, `getTransaction` fills whatever the
//! manifest still misses. A port of `fetcher.ts`, `bulk-fetcher.ts` and `insert-raw.ts`.

use super::config::{Config, Lane};
use super::rpc::{Rpc, call_value};
use crate::db::Db;
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError};
use serde_json::{Value, json};
use std::future::Future;
use std::sync::Arc;

/// Run `job` over `items` with at most `concurrency` in flight; every job finishes even when one
/// fails, and the first failure is returned.
pub async fn parallel<T, F, Fut>(
    items: Vec<T>,
    concurrency: usize,
    job: F,
) -> Result<(), StoreError>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), StoreError>> + Send + 'static,
{
    let queue = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
        items,
    )));
    let job = Arc::new(job);
    let workers = concurrency
        .max(1)
        .min(queue.lock().expect("queue").len().max(1));
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..workers {
        let queue = Arc::clone(&queue);
        let job = Arc::clone(&job);
        set.spawn(async move {
            loop {
                let next = queue.lock().expect("queue").pop_front();
                let Some(item) = next else { return Ok(()) };
                job(item).await?;
            }
        });
    }
    let mut failure: Option<StoreError> = None;
    while let Some(result) = set.join_next().await {
        let outcome = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(StoreError::Check(error.to_string())),
        };
        if let Some(error) = outcome
            && failure.is_none()
        {
            failure = Some(error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// One raw row to insert.
#[derive(Clone, Debug)]
pub struct RawRow {
    /// Base58 signature.
    pub signature: String,
    /// Slot.
    pub slot: i64,
    /// Block time.
    pub block_time: Value,
    /// `meta.err`.
    pub err: Value,
    /// Fee.
    pub fee: Value,
    /// Compute units consumed.
    pub compute_units_consumed: Value,
    /// Base64 wire.
    pub tx_b64: String,
    /// Meta JSON text.
    pub meta_json: String,
    /// Raw response text or provenance.
    pub raw_rpc_json: String,
    /// Lane that collected it.
    pub mode: String,
    /// Provider name.
    pub provider: String,
    /// Fetch instant.
    pub fetched_at: String,
}

impl RawRow {
    fn to_json(&self) -> Value {
        Obj::new()
            .with("signature", self.signature.clone())
            .with("slot", self.slot)
            .with("block_time", self.block_time.clone())
            .with("err", self.err.clone())
            .with("fee", self.fee.clone())
            .with(
                "compute_units_consumed",
                self.compute_units_consumed.clone(),
            )
            .with("tx_b64", self.tx_b64.clone())
            .with("meta_json", self.meta_json.clone())
            .with("raw_rpc_json", self.raw_rpc_json.clone())
            .with("mode", self.mode.clone())
            .with("provider", self.provider.clone())
            .with("fetched_at", self.fetched_at.clone())
            .to_value()
    }
}

/// Insert rows, deduplicated within the manifest window; the primary key is the final gate.
pub fn insert_raw(
    store: &mut Store,
    rows: &[RawRow],
    from: i64,
    to: i64,
) -> Result<(), StoreError> {
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<Value> = rows
        .iter()
        .filter(|row| seen.insert(row.signature.clone()))
        .map(RawRow::to_json)
        .collect();
    if unique.is_empty() {
        return Ok(());
    }
    // DuckDB's ON CONFLICT builds a join over the entire existing table. Limit the anti-join to
    // this manifest window, then use a normal constraint-checked insert.
    store.exec(
        "INSERT INTO transactions
    SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'block_time')::BIGINT,
    NULL, NULL, value->'err', (value->>'fee')::BIGINT, (value->>'compute_units_consumed')::BIGINT,
    value->>'tx_b64', value->>'meta_json', value->>'raw_rpc_json', value->>'mode',
    value->>'provider', value->>'fetched_at', NULL FROM json_each(?::JSON)
    WHERE value->>'signature' NOT IN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?)",
        &[&Value::Array(unique).to_string(), &from, &to],
    )?;
    Ok(())
}

#[derive(serde::Deserialize)]
struct RawEnvelope<'a> {
    #[serde(borrow)]
    result: Option<RawResult<'a>>,
}

#[derive(serde::Deserialize)]
struct RawResult<'a> {
    #[serde(borrow)]
    meta: Option<&'a serde_json::value::RawValue>,
}

#[derive(serde::Deserialize)]
struct RawPage<'a> {
    #[serde(borrow)]
    result: Option<RawPageResult<'a>>,
}

#[derive(serde::Deserialize)]
struct RawPageResult<'a> {
    #[serde(borrow, default)]
    data: Vec<RawResult<'a>>,
}

/// The exact `result.meta` text of a `getTransaction` body, as the provider sent it.
#[must_use]
pub fn raw_meta(body: &str) -> Option<String> {
    serde_json::from_str::<RawEnvelope>(body)
        .ok()?
        .result?
        .meta
        .map(|m| m.get().to_owned())
}

/// The exact `meta` text of every entry of a `getTransactionsForAddress` page body.
#[must_use]
pub fn raw_metas(body: &str) -> Vec<Option<String>> {
    serde_json::from_str::<RawPage>(body)
        .ok()
        .and_then(|page| page.result)
        .map(|result| {
            result
                .data
                .into_iter()
                .map(|entry| entry.meta.map(|m| m.get().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

/// Fetch one transaction by signature and store it.
pub async fn fetch_transaction(
    rpc: &dyn Rpc,
    db: &Db,
    row: &Obj,
    config: &Config,
    lane: Lane,
) -> Result<(), StoreError> {
    let signature = row.str("signature").unwrap_or("").to_owned();
    let slot = row.int("slot").unwrap_or(0);
    let mode = row.str("mode").unwrap_or(lane.as_str()).to_owned();
    let mut response = None;
    for attempt in 0..config.max_retries {
        let reply = rpc
            .call("getTransaction", json!([signature, { "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version, "commitment": "finalized" }]), lane)
            .await
            .map_err(|e| StoreError::Check(e.to_string()))?;
        if !reply.result.is_null() {
            response = Some(reply);
            break;
        }
        if attempt + 1 < config.max_retries {
            tokio::time::sleep(std::time::Duration::from_millis(jitter(
                (500.0 * 2f64.powi(attempt as i32)).min(32_000.0),
            )))
            .await;
        }
    }
    let Some(reply) = response else {
        let provider = rpc.provider_name().to_owned();
        let sig = signature.clone();
        db.run(move |store| {
            store.exec(
                "INSERT INTO errors VALUES (?, ?, ?, ?, ?)",
                &[
                    &"transaction",
                    &sig,
                    &provider,
                    &"null_after_retries",
                    &now(),
                ],
            )?;
            Ok(())
        })
        .await?;
        return Err(StoreError::Check(
            "Transaction unavailable after retries; range remains unpublished".into(),
        ));
    };
    let result = reply.result;
    if result.get("slot").and_then(Value::as_i64) != Some(slot) {
        return Err(StoreError::Check(
            "Transaction slot does not match manifest".into(),
        ));
    }
    let wire = result.get("transaction").and_then(Value::as_array);
    let (Some(wire), Some(meta)) = (wire, result.get("meta")) else {
        return Err(StoreError::Check("Invalid raw transaction response".into()));
    };
    if wire.get(1).and_then(Value::as_str) != Some("base64") || meta.is_null() {
        return Err(StoreError::Check("Invalid raw transaction response".into()));
    }
    let tx_b64 = wire
        .first()
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    if solana_wire::signature_of_base64(&tx_b64).map_err(|e| StoreError::Check(e.to_string()))?
        != signature
    {
        return Err(StoreError::Check(
            "Wire signature does not match manifest".into(),
        ));
    }
    let row = RawRow {
        signature,
        slot,
        block_time: result.get("blockTime").cloned().unwrap_or(Value::Null),
        err: meta.get("err").cloned().unwrap_or(Value::Null),
        fee: meta.get("fee").cloned().unwrap_or(Value::Null),
        compute_units_consumed: meta
            .get("computeUnitsConsumed")
            .cloned()
            .unwrap_or(Value::Null),
        tx_b64,
        meta_json: raw_meta(&reply.raw).unwrap_or_else(|| meta.to_string()),
        raw_rpc_json: reply.raw,
        mode,
        provider: rpc.provider_name().to_owned(),
        fetched_at: now(),
    };
    db.run(move |store| insert_raw(store, &[row], slot, slot))
        .await
}

/// Fill every manifest signature of `[from, to]`: bulk windows first, then standard fetches.
pub async fn fetch_range(
    rpc: Arc<dyn Rpc>,
    db: &Db,
    config: &Config,
    lane: Lane,
    from: i64,
    to: i64,
) -> Result<(), StoreError> {
    let missing = db
        .rows(
            "SELECT count(*) AS n, min(s.slot) AS first_slot, max(s.slot) AS last_slot
    FROM signatures s LEFT JOIN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?) t USING(signature)
    WHERE t.signature IS NULL AND s.slot BETWEEN ? AND ?",
            vec![from.into(), to.into(), from.into(), to.into()],
        )
        .await?;
    let missing = missing.into_iter().next().unwrap_or_default();
    // Tail overlap stays in the manifest and validation, but fetched slots need no replay.
    if config.bulk_fetch_enabled && missing.int("n").unwrap_or(0) > 0 {
        let first = missing.int("first_slot").unwrap_or(from);
        let last = missing.int("last_slot").unwrap_or(to);
        let mut windows = Vec::new();
        let mut start = first;
        while start <= last {
            windows.push((
                start,
                (start + config.bulk_fetch_window_slots - 1).min(last),
            ));
            start += config.bulk_fetch_window_slots;
        }
        // Tokens remain sequential inside each window; independent slot bounds may overlap in flight.
        let concurrency = config
            .bulk_fetch_concurrency
            .min(config.concurrency.get(lane)) as usize;
        let (rpc2, db2, config2) = (Arc::clone(&rpc), db.clone(), config.clone());
        parallel(windows, concurrency, move |(wf, wt)| {
            let (rpc, db, config) = (Arc::clone(&rpc2), db2.clone(), config2.clone());
            async move { super::bulk::bulk_fetch_range(rpc.as_ref(), &db, &config, lane, wf, wt).await }
        })
        .await?;
    }
    loop {
        let pending = db
            .rows(
                "SELECT s.* FROM signatures s
      LEFT JOIN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?) t USING(signature)
      WHERE t.signature IS NULL AND s.slot BETWEEN ? AND ? ORDER BY s.slot ASC, s.signature LIMIT ?",
                vec![from.into(), to.into(), from.into(), to.into(), config.fetch_batch_size.into()],
            )
            .await?;
        if pending.is_empty() {
            return Ok(());
        }
        let (rpc2, db2, config2) = (Arc::clone(&rpc), db.clone(), config.clone());
        parallel(pending, config.concurrency.get(lane) as usize, move |row| {
            let (rpc, db, config) = (Arc::clone(&rpc2), db2.clone(), config2.clone());
            async move { fetch_transaction(rpc.as_ref(), &db, &row, &config, lane).await }
        })
        .await?;
    }
}

fn jitter(max: f64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    ((f64::from(nanos % 1_000_000) / 1_000_000.0) * max) as u64
}

/// Lane for `call_value` users that only need a value.
pub async fn value(
    rpc: &dyn Rpc,
    method: &str,
    params: Value,
    lane: Lane,
) -> Result<Value, StoreError> {
    call_value(rpc, method, params, lane)
        .await
        .map_err(|e| StoreError::Check(e.to_string()))
}
