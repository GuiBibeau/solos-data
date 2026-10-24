//! The lane's RPC: the collector's provider (retries, throttle detection, exact response text)
//! behind a limiter of its own, so the venues lane never draws on the Phoenix collector's
//! compute-unit bucket (ADR-0012). Every method weighs the same; the rate is requests a second.
//! The provider is the same account as the collector's (`SOLANA_RPC_URL`); the requests and the
//! credits they cost by Helius's published prices are counted for the status.

use crate::collector::config::{Config, Lane, Lanes};
use crate::collector::limiter::Limiter;
use crate::collector::rpc::{Provider, Rpc, RpcFailure, RpcResponse};
use crate::store::StoreError;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The methods the RPC lanes call (venues, their history, and the spot lanes).
pub const METHODS: [&str; 10] = [
    "getSlot",
    "getBlock",
    "getBlocks",
    "getBlockTime",
    "getAccountInfo",
    "getSignaturesForAddress",
    "getTransactionsForAddress",
    "getTransaction",
    "getMultipleAccounts",
    "getProgramAccounts",
];

/// Uniform weight per request in the borrowed limiter.
const WEIGHT: u32 = 10;

/// A provider limited to `rps` requests a second, on the collector's provider code, stopped by
/// `stop`.
///
/// # Errors
///
/// When `SOLANA_RPC_URL` is unset or the embedded collector defaults do not parse.
pub fn provider(
    rps: f64,
    concurrency: u32,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Result<Arc<Provider>, StoreError> {
    let mut config: Config = serde_json::from_str(include_str!("../../../../config/phoenix.json"))
        .map_err(|e| StoreError::Check(e.to_string()))?;
    config.cu_weights = METHODS.iter().map(|m| ((*m).to_owned(), WEIGHT)).collect();
    config.cu_per_second = rps * f64::from(WEIGHT);
    config.utilization = 1.0;
    let limiter = Arc::new(Limiter::new(
        config.cu_per_second,
        0.0,
        Lanes {
            tail: 1,
            backfill: concurrency.max(1),
        },
    ));
    let url = crate::collector::config::provider_url()?;
    let mut provider = Provider::new(url, config, limiter);
    // SIGTERM ends waits and retries at once; the interrupted cycle is retried after a restart.
    provider.shutdown = stop;
    Ok(Arc::new(provider))
}

/// A provider whose limiter spends credits rather than requests: each method weighs its price in
/// `costs` (1 when unpriced), and `credits_per_second` is the budget. The history backfill
/// (ADR-0013) runs on it, so its bulk pages (10 credits) and manifest pages (1) share one budget.
///
/// # Errors
///
/// When `SOLANA_RPC_URL` is unset or the embedded collector defaults do not parse.
pub fn credit_provider(
    credits_per_second: f64,
    costs: &BTreeMap<String, f64>,
    concurrency: u32,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Result<Arc<Provider>, StoreError> {
    let mut config: Config = serde_json::from_str(include_str!("../../../../config/phoenix.json"))
        .map_err(|e| StoreError::Check(e.to_string()))?;
    config.cu_weights = weights(costs);
    config.cu_per_second = credits_per_second * f64::from(WEIGHT);
    config.utilization = 1.0;
    let limiter = Arc::new(Limiter::new(
        config.cu_per_second,
        0.0,
        Lanes {
            tail: 1,
            backfill: concurrency.max(1),
        },
    ));
    let url = crate::collector::config::provider_url()?;
    let mut provider = Provider::new(url, config, limiter);
    provider.shutdown = stop;
    Ok(Arc::new(provider))
}

/// [`credit_provider`] over `transport` (the spot lane's gzip transport, ADR-0015).
///
/// # Errors
///
/// When `SOLANA_RPC_URL` is unset or the embedded collector defaults do not parse.
pub fn credit_provider_over(
    credits_per_second: f64,
    costs: &BTreeMap<String, f64>,
    concurrency: u32,
    stop: Arc<std::sync::atomic::AtomicBool>,
    transport: Box<dyn crate::collector::rpc::Transport>,
) -> Result<Arc<Provider>, StoreError> {
    let mut config: Config = serde_json::from_str(include_str!("../../../../config/phoenix.json"))
        .map_err(|e| StoreError::Check(e.to_string()))?;
    config.cu_weights = weights(costs);
    config.cu_per_second = credits_per_second * f64::from(WEIGHT);
    config.utilization = 1.0;
    let limiter = Arc::new(Limiter::new(
        config.cu_per_second,
        0.0,
        Lanes {
            tail: 1,
            backfill: concurrency.max(1),
        },
    ));
    let url = crate::collector::config::provider_url()?;
    let mut provider = Provider::with_transport(url, config, limiter, transport);
    provider.shutdown = stop;
    Ok(Arc::new(provider))
}

/// Limiter weights: a method's credits times [`WEIGHT`], at least 1.
#[must_use]
pub fn weights(costs: &BTreeMap<String, f64>) -> BTreeMap<String, u32> {
    METHODS
        .iter()
        .map(|m| {
            let cost = costs.get(*m).copied().unwrap_or(1.0);
            let weight = (cost * f64::from(WEIGHT))
                .round()
                .clamp(1.0, f64::from(u32::MAX));
            ((*m).to_owned(), weight as u32)
        })
        .collect()
}

/// Call `method` on the lane's (backfill) limiter lane; the answer counts as progress.
///
/// # Errors
///
/// The provider's failure after its retries.
pub async fn call(rpc: &dyn Rpc, method: &str, params: Value) -> Result<RpcResponse, RpcFailure> {
    let reply = rpc.call(method, params, Lane::Backfill).await;
    // An answer, good or bad, is progress for the stall watchdog (`crate::stall`).
    crate::stall::bump();
    reply
}

/// Credits a set of request counters cost: `backfill_<method>_requests` counters times the
/// configured price per method (Helius: 1 a call, 10 for `getProgramAccounts`, 10 for a
/// `getTransactionsForAddress` page of up to 100 full transactions); unpriced methods count 1.
#[must_use]
pub fn credits(counters: &Value, costs: &BTreeMap<String, f64>) -> f64 {
    METHODS
        .iter()
        .map(|method| {
            let requests = counters
                .get(format!("backfill_{method}_requests"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            requests * costs.get(*method).copied().unwrap_or(1.0)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_follow_the_price_list() {
        let counters = serde_json::json!({
            "backfill_getSlot_requests": 10, "backfill_getProgramAccounts_requests": 2,
            "backfill_getTransactionsForAddress_requests": 3
        });
        let costs: BTreeMap<String, f64> = [
            ("getSlot".to_owned(), 1.0),
            ("getProgramAccounts".to_owned(), 10.0),
            ("getTransactionsForAddress".to_owned(), 10.0),
        ]
        .into();
        assert!((credits(&counters, &costs) - (10.0 + 20.0 + 30.0)).abs() < 1e-9);
        let w = weights(&costs);
        assert_eq!(
            (w["getSlot"], w["getTransactionsForAddress"], w["getBlocks"]),
            (10, 100, 10)
        );
    }
}
