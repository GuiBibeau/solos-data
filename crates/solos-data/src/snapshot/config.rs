//! `config/snapshot.json`: where the state snapshots go, how often each kind of account is read,
//! how often its raw bytes are kept, and the lane's own request budget. `SOLOS_DATA_STATE_DIR`
//! overrides `dataDir`; the RPC URL comes from `SOLANA_RPC_URL` and is never logged.

use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The lane's settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotConfig {
    /// Phoenix program id; every account read must be owned by it.
    pub program_id: String,
    /// Phoenix exchange view (market list and the program's well-known accounts).
    pub exchange_url: String,
    /// State root.
    pub data_dir: PathBuf,
    /// Seconds between order book, spline and trader-index reads.
    pub book_interval_seconds: u64,
    /// Seconds between perp asset map (market state) reads.
    pub market_interval_seconds: u64,
    /// Seconds between full trader account scans.
    pub trader_interval_seconds: u64,
    /// Seconds between exchange view refreshes (new or delisted markets).
    pub exchange_refresh_seconds: u64,
    /// Seconds between flushes of the staged rows into Parquet.
    pub flush_seconds: u64,
    /// Staged bytes above which a flush starts early.
    pub flush_bytes: u64,
    /// Minimum seconds between two raw copies of the same account, per kind; an unchanged
    /// account is never copied again, and every account is copied once after each flush so
    /// each file set holds a baseline. `0` keeps every change.
    pub raw_every_seconds: BTreeMap<String, u64>,
    /// Accounts per `getMultipleAccounts` call (the RPC accepts at most 100).
    pub accounts_per_call: usize,
    /// Accounts per `getProgramAccountsV2` page.
    pub trader_page_limit: u32,
    /// The lane's own request ceiling, requests per second (the collector keeps its budget).
    pub requests_per_second: f64,
    /// Provider credits per call, per method (Helius: 1 for standard calls and the paginated
    /// `getProgramAccountsV2`, 10 for `getProgramAccounts`).
    pub credits: BTreeMap<String, u64>,
    /// Attempts per RPC call.
    pub max_retries: u32,
}

impl SnapshotConfig {
    /// Raw-copy spacing for a kind of account; kinds not listed keep every change.
    #[must_use]
    pub fn raw_every(&self, kind: &str) -> u64 {
        self.raw_every_seconds.get(kind).copied().unwrap_or(0)
    }

    /// Credits a call to `method` costs.
    #[must_use]
    pub fn credits_of(&self, method: &str) -> u64 {
        self.credits.get(method).copied().unwrap_or(1)
    }
}

fn validate(config: &SnapshotConfig) -> Result<(), StoreError> {
    let bad = |what: &str| {
        Err(StoreError::Check(format!(
            "Invalid snapshot config: {what}"
        )))
    };
    if config.book_interval_seconds == 0
        || config.market_interval_seconds == 0
        || config.trader_interval_seconds == 0
        || config.exchange_refresh_seconds == 0
        || config.flush_seconds == 0
    {
        return bad("intervals must be positive");
    }
    if config.accounts_per_call == 0 || config.accounts_per_call > 100 {
        return bad("accountsPerCall must be 1..100");
    }
    if config.trader_page_limit == 0 || config.trader_page_limit > 10_000 {
        return bad("traderPageLimit must be 1..10000");
    }
    if config.requests_per_second.is_nan()
        || config.requests_per_second <= 0.0
        || config.max_retries == 0
    {
        return bad("requestsPerSecond and maxRetries must be positive");
    }
    Ok(())
}

/// Load the config; `SOLOS_DATA_STATE_DIR` overrides `dataDir`, resolved against the cwd.
pub fn load_snapshot_config(path: Option<&str>) -> Result<SnapshotConfig, StoreError> {
    let path = path.unwrap_or("config/snapshot.json");
    let text = std::fs::read_to_string(path)?;
    let mut config: SnapshotConfig =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let cwd = std::env::current_dir()?;
    let data_dir = std::env::var("SOLOS_DATA_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| config.data_dir.clone());
    config.data_dir = crate::fsutil::resolve(&cwd, &data_dir);
    validate(&config)?;
    Ok(config)
}

/// The repository's `config/snapshot.json`, for tests.
#[must_use]
pub fn repository_config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/snapshot.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_repository_config_loads_and_validates() {
        let path = repository_config_path();
        let config = load_snapshot_config(Some(&path.to_string_lossy())).unwrap();
        assert_eq!(config.book_interval_seconds, 10);
        assert!(config.accounts_per_call <= 100);
        assert_eq!(config.credits_of("getProgramAccounts"), 10);
        assert_eq!(config.credits_of("getMultipleAccounts"), 1);
        assert_eq!(config.raw_every("unknown_kind"), 0);
        let mut bad = config.clone();
        bad.accounts_per_call = 101;
        assert!(validate(&bad).is_err());
    }
}
