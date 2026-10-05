//! Bounded live sizing probe and the Alchemy capability probe. Ports of `probe.ts` and
//! `capabilities.ts`.

use super::config::{Config, Lane};
use super::exchange::{confirm_mainnet, refresh_exchange};
use super::fetcher::{fetch_transaction, parallel, value};
use super::rpc::Rpc;
use super::walker::{make_walk, walk_page};
use crate::db::Db;
use crate::jsonout::{Obj, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::sync::Arc;

/// Walk a recent day of signatures, fetch a sample, extrapolate daily volume.
pub async fn probe(rpc: Arc<dyn Rpc>, db: &Db, config: &Config) -> Result<Obj, StoreError> {
    let mut config = config.clone();
    config.max_retries = 2;
    confirm_mainnet(rpc.as_ref()).await?;
    let http = reqwest::Client::new();
    let exchange = refresh_exchange(rpc.as_ref(), db, &config, &http).await?;
    let head = value(
        rpc.as_ref(),
        "getSlot",
        json!([{ "commitment": "finalized" }]),
        Lane::Tail,
    )
    .await?
    .as_i64()
    .unwrap_or(0);
    let cutoff = chrono::Utc::now().timestamp() - 86_400;
    let mut walk = make_walk(&config.program_id, Lane::Backfill, "probe", 0, head);
    let mut bounded = false;
    while !walk.done && walk.pages < config.probe_page_limit {
        walk = walk_page(rpc.as_ref(), db, "walk/probe", walk).await?;
        let oldest = db
            .rows("SELECT min(block_time) AS time FROM signatures", vec![])
            .await?;
        if let Some(time) = oldest.first().and_then(|r| r.int("time"))
            && time <= cutoff
        {
            bounded = true;
            break;
        }
    }
    let sample = db
        .rows(
            &format!(
                "SELECT * FROM signatures USING SAMPLE reservoir({} ROWS) REPEATABLE(42)",
                config.probe_transaction_limit
            ),
            vec![],
        )
        .await?;
    let unavailable = Arc::new(std::sync::Mutex::new(0u64));
    let failures = Arc::new(std::sync::Mutex::new(Obj::new()));
    let (rpc2, db2, config2, unavailable2, failures2) = (
        Arc::clone(&rpc),
        db.clone(),
        config.clone(),
        Arc::clone(&unavailable),
        Arc::clone(&failures),
    );
    parallel(sample, config.concurrency.backfill as usize, move |row| {
        let (rpc, db, config, unavailable, failures) = (
            Arc::clone(&rpc2),
            db2.clone(),
            config2.clone(),
            Arc::clone(&unavailable2),
            Arc::clone(&failures2),
        );
        async move {
            if let Err(error) =
                fetch_transaction(rpc.as_ref(), &db, &row, &config, Lane::Backfill).await
            {
                *unavailable.lock().expect("unavailable") += 1;
                let mut f = failures.lock().expect("failures");
                let reason = error.to_string();
                let count = f.int(&reason).unwrap_or(0) + 1;
                f.set(&reason, count);
            }
            Ok(())
        }
    })
    .await?;
    let stats = db.rows("SELECT count(*) AS signatures, min(slot) AS from_slot, max(slot) AS to_slot, min(block_time) AS from_time, max(block_time) AS to_time FROM signatures", vec![]).await?.into_iter().next().unwrap_or_default();
    let tx = db
        .rows("SELECT count(*) AS sampled, avg(length(tx_b64)*0.75+length(meta_json)) AS mean_bytes, count(*) FILTER (WHERE err::VARCHAR<>'null') AS failed FROM transactions", vec![])
        .await?
        .into_iter()
        .next()
        .unwrap_or_default();
    let multi = db.rows("SELECT count(*) AS n FROM (SELECT slot FROM signatures GROUP BY slot HAVING count(*)>1)", vec![]).await?.into_iter().next().unwrap_or_default();
    let span =
        (stats.int("to_time").unwrap_or(0) - stats.int("from_time").unwrap_or(0)).max(1) as f64;
    let per_day = stats.int("signatures").unwrap_or(0) as f64 / span * 86_400.0;
    let unavailable = *unavailable.lock().expect("unavailable");
    let failures = failures.lock().expect("failures").clone();
    let report = Obj::new()
        .with("at", now())
        .with("exchange", json!({ "programData": exchange.program_data, "slot": exchange.slot, "markets": exchange.markets, "identityConfirmed": true }))
        .with("head", head)
        .with("pages", walk.pages)
        .with("coveredOneDay", bounded)
        .with("pageLimited", !bounded && !walk.done)
        .with_obj("stats", stats)
        .with_obj("sample", tx)
        .with("multiSlots", multi.int("n").unwrap_or(0))
        .with("unavailableSamples", unavailable)
        .with_obj("failures", failures)
        .with("complete", unavailable == 0)
        .with("estimatedTxPerDay", per_day.round() as i64)
        .with("estimatedTransactionCuPerDay", (per_day * 40.0).round() as i64)
        .with("estimatedMeanCuPerSecond", per_day * 40.0 / 86_400.0)
        .with("D1", "pending; block ordering retained")
        .with("missing", json!(["same-slot until probe", "CPI/lookup-table cross-check", "authenticated fills", "cross-provider", "WebSocket deltas"]))
        .with("note", "Sizing extrapolates a bounded recent sample; it is not a complete M0 day.");
    std::fs::write(
        config.data_dir.join("probe.json"),
        format!("{}\n", report.to_json()),
    )?;
    Ok(report)
}

/// Small, repeatable probe of current Alchemy extensions and the launch boundary.
pub async fn capabilities(rpc: Arc<dyn Rpc>, config: &Config) -> Result<Obj, StoreError> {
    confirm_mainnet(rpc.as_ref()).await?;
    let mut report = Obj::new().with("at", now());
    for details in ["signatures", "full"] {
        let outcome: Result<Obj, StoreError> = async {
            let result = value(
                rpc.as_ref(),
                "getTransactionsForAddress",
                json!([config.program_id, { "commitment": "finalized", "transactionDetails": details, "sortOrder": "asc", "limit": 3, "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version }]),
                Lane::Tail,
            )
            .await?;
            let entries = result.get("data").or_else(|| result.get("transactions")).and_then(Value::as_array).cloned().unwrap_or_default();
            let mut section = Obj::new()
                .with("keys", Value::Array(result.as_object().map(|o| o.keys().map(|k| Value::String(k.clone())).collect()).unwrap_or_default()))
                .with("count", entries.len())
                .with("pagination", result.get("paginationToken").is_some_and(|t| !t.is_null()))
                .with(
                    "entries",
                    Value::Array(
                        entries
                            .iter()
                            .map(|e| {
                                json!({
                                    "keys": e.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),
                                    "signature": e.get("signature"), "slot": e.get("slot"), "blockTime": e.get("blockTime"),
                                    "transactionIndex": e.get("transactionIndex"),
                                    "transactionEncoding": e.get("transaction").and_then(Value::as_array).and_then(|a| a.get(1).cloned()).unwrap_or_else(|| Value::String(e.get("transaction").map_or("undefined", |t| if t.is_string() { "string" } else { "object" }).into())),
                                    "hasMeta": e.get("meta").is_some_and(|m| !m.is_null()),
                                })
                            })
                            .collect(),
                    ),
                );
            if details == "signatures"
                && let Some(signature) = entries.first().and_then(|e| e.get("signature")).and_then(Value::as_str)
            {
                let transaction = value(rpc.as_ref(), "getTransaction", json!([signature, { "commitment": "finalized", "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version }]), Lane::Tail).await?;
                report.set("oldestStandardFetch", json!({ "available": !transaction.is_null(), "slot": transaction.get("slot"), "blockTime": transaction.get("blockTime"), "hasMeta": transaction.get("meta").is_some_and(|m| !m.is_null()) }));
                let parsed = value(rpc.as_ref(), "getTransaction", json!([signature, { "commitment": "finalized", "encoding": "jsonParsed", "maxSupportedTransactionVersion": config.max_supported_transaction_version }]), Lane::Tail).await?;
                let instructions = parsed.get("transaction").and_then(|t| t.get("message")).and_then(|m| m.get("instructions")).and_then(Value::as_array).cloned().unwrap_or_default();
                report.set(
                    "launchBoundary",
                    json!({
                        "programAccountCreated": instructions.iter().any(|i| i.get("parsed").and_then(|p| p.get("type")).and_then(Value::as_str) == Some("createAccount") && i.get("parsed").and_then(|p| p.get("info")).and_then(|f| f.get("newAccount")).and_then(Value::as_str) == Some(config.program_id.as_str())),
                        "instructions": instructions.iter().map(|i| json!({ "program": i.get("program"), "type": i.get("parsed").and_then(|p| p.get("type")) })).collect::<Vec<_>>(),
                    }),
                );
            }
            if details == "full" {
                let signatures: Vec<Value> = entries.iter().filter_map(|e| e.get("transaction").and_then(Value::as_array).and_then(|a| a.first()).and_then(Value::as_str)).filter_map(|b64| solana_wire::signature_of_base64(b64).ok()).map(Value::String).collect();
                report.set("bulkWireSignatures", Value::Array(signatures));
            }
            section.set("ok", true);
            Ok(section)
        }
        .await;
        match outcome {
            Ok(section) => report.set_obj(details, section),
            Err(error) => report.set(details, json!({ "error": safe_error(&error.to_string()) })),
        }
    }
    let recent: Result<Obj, StoreError> = async {
        let recent = value(
            rpc.as_ref(),
            "getTransactionsForAddress",
            json!([config.program_id, { "commitment": "finalized", "transactionDetails": "full", "sortOrder": "desc", "limit": 10, "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version, "filters": { "status": "any", "tokenAccounts": "none" } }]),
            Lane::Tail,
        )
        .await?;
        let data = recent.get("data").and_then(Value::as_array).cloned().unwrap_or_default();
        let first = data.first().cloned();
        let bounded = match first.as_ref().and_then(|f| f.get("slot")).and_then(Value::as_i64) {
            Some(slot) => Some(
                value(
                    rpc.as_ref(),
                    "getTransactionsForAddress",
                    json!([config.program_id, { "commitment": "finalized", "transactionDetails": "full", "sortOrder": "asc", "limit": 100, "encoding": "base64", "maxSupportedTransactionVersion": config.max_supported_transaction_version, "filters": { "slot": { "gte": slot, "lte": slot }, "status": "any", "tokenAccounts": "none" } }]),
                    Lane::Tail,
                )
                .await?,
            ),
            None => None,
        };
        let first_slot = first.as_ref().and_then(|f| f.get("slot")).cloned().unwrap_or(Value::Null);
        let bounded_data = bounded.as_ref().and_then(|b| b.get("data")).and_then(Value::as_array).cloned();
        let mut versions: Vec<Value> = Vec::new();
        for tx in &data {
            let version = tx.get("version").cloned().unwrap_or(Value::Null);
            if !versions.contains(&version) {
                versions.push(version);
            }
        }
        let checked = data.iter().filter_map(|t| t.get("transaction").and_then(Value::as_array).and_then(|a| a.first()).and_then(Value::as_str)).filter(|b64| solana_wire::signature_of_base64(b64).is_ok()).count();
        Ok(Obj::new()
            .with("count", data.len())
            .with("slot", first_slot.clone())
            .with("hasMeta", first.as_ref().and_then(|f| f.get("meta")).is_some_and(|m| !m.is_null()))
            .with("boundedCount", bounded_data.as_ref().map_or(Value::Null, |d| Value::from(d.len())))
            .with("boundsHonored", bounded_data.as_ref().map_or(Value::Null, |d| Value::Bool(d.iter().all(|t| t.get("slot") == Some(&first_slot)))))
            .with("versions", Value::Array(versions))
            .with("wireSignaturesChecked", checked))
    }
    .await;
    match recent {
        Ok(section) => report.set_obj("recentBulk", section),
        Err(error) => report.set(
            "recentBulk",
            json!({ "error": safe_error(&error.to_string()) }),
        ),
    }
    std::fs::write(
        config.data_dir.join("capabilities.json"),
        format!("{}\n", report.to_json()),
    )?;
    Ok(report)
}
