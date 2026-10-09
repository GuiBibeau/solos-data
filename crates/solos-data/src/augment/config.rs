//! `config/augment.json`: the data root, the start date, the per-source settings and the symbol
//! map that ties every Phoenix market to its name on each venue. The map is generated once from
//! the venues' listings and committed; nothing in it is fetched at run time.

use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// One Phoenix market and its names elsewhere. `None` means the venue has no such market.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    /// Phoenix symbol (`SOL`, `kBONK`, `NVDA`).
    pub phoenix: String,
    /// Phoenix asset id.
    pub asset_id: i64,
    /// `crypto`, `equity` or `commodity`.
    pub kind: String,
    /// CoinGecko id when Phoenix publishes one.
    #[serde(default)]
    pub coin_gecko_id: Option<String>,
    /// Binance USD-M futures symbol (`SOLUSDT`, `1000BONKUSDT`).
    #[serde(default)]
    pub binance: Option<String>,
    /// Hyperliquid coin, with the HIP-3 dex prefix for equities and commodities (`xyz:NVDA`).
    #[serde(default)]
    pub hyperliquid: Option<String>,
    /// Bybit linear perpetual symbol.
    #[serde(default)]
    pub bybit: Option<String>,
    /// Elfa entity id.
    #[serde(default)]
    pub elfa_entity_id: Option<String>,
    /// SEC EDGAR Central Index Key of the issuer (equities with filings).
    #[serde(default)]
    pub sec_cik: Option<String>,
}

/// Binance public dumps.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Binance {
    /// Whether the source runs.
    pub enabled: bool,
    /// Base of the file URLs.
    pub files_url: String,
    /// Base of the S3 listing URL.
    pub list_url: String,
    /// Phase 1 datasets.
    pub datasets: Vec<String>,
    /// Phase 2 datasets (large; opt-in).
    #[serde(default)]
    pub large_datasets: Vec<String>,
    /// Compare each file with its published `.CHECKSUM`.
    #[serde(default = "d_true")]
    pub verify_checksums: bool,
}

/// Hyperliquid info API.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hyperliquid {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Requests per second to this host: the info API weighs most requests 20 against a budget
    /// of 1,200 a minute per address, so one a second.
    #[serde(default = "d_hl_rps")]
    pub requests_per_second: f64,
    /// Hourly funding history (sync).
    #[serde(default = "d_true")]
    pub funding_history: bool,
    /// 1-minute candles (capture).
    #[serde(default = "d_true")]
    pub candles: bool,
    /// Seconds between candle snapshots.
    #[serde(default = "d_candle_interval")]
    pub candle_interval_seconds: u64,
    /// Asset contexts: open interest, funding, mark and oracle prices (capture).
    #[serde(default = "d_true")]
    pub asset_contexts: bool,
    /// Seconds between asset-context snapshots.
    #[serde(default = "d_context_interval")]
    pub asset_context_interval_seconds: u64,
}

/// Deribit volatility index.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deribit {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Index currencies (`BTC`, `ETH`).
    pub currencies: Vec<String>,
    /// Resolutions in seconds (60 and 3600).
    pub resolutions: Vec<u64>,
}

/// One DefiLlama stablecoin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stablecoin {
    /// DefiLlama id.
    pub id: String,
    /// Symbol used as the directory name.
    pub symbol: String,
}

/// DefiLlama stablecoin charts.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Defillama {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Per-coin charts in addition to the total.
    #[serde(default)]
    pub stablecoins: Vec<Stablecoin>,
}

/// Bybit public API and dumps.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bybit {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Base of the public dump URLs.
    pub files_url: String,
    /// Funding history (sync).
    #[serde(default = "d_true")]
    pub funding_history: bool,
    /// Tick trades dumps (large; opt-in).
    #[serde(default)]
    pub trades: bool,
}

/// Elfa v3.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elfa {
    /// Whether the source runs (it also needs `ELFA_API_KEY`).
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Seconds between capture cycles.
    #[serde(default = "d_elfa_interval")]
    pub interval_seconds: u64,
    /// The key's request allowance per minute.
    #[serde(default = "d_elfa_rpm")]
    pub requests_per_minute: u64,
}

/// SEC EDGAR: the submissions JSON of every mapped issuer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sec {
    /// Whether the source runs.
    pub enabled: bool,
    /// `data.sec.gov` base.
    pub base_url: String,
    /// `company_tickers.json`, for `augment sec-map`.
    pub tickers_url: String,
    /// Base of filing document URLs (`/Archives/edgar/data`).
    pub archive_url: String,
    /// The User-Agent EDGAR sees. EDGAR's fair-access policy asks automated tools to declare
    /// a contact in it; the operator decides what to declare.
    #[serde(default = "d_user_agent")]
    pub user_agent: String,
    /// Requests per second to EDGAR (its ceiling is ten).
    #[serde(default = "d_sec_rps")]
    pub requests_per_second: f64,
}

/// alternative.me Fear and Greed index.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alternative {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base.
    pub base_url: String,
}

/// One curated Polymarket event; its markets are resolved at run time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolymarketEvent {
    /// Gamma event id.
    pub id: String,
    /// Event slug, for the reader.
    pub slug: String,
}

/// Polymarket: Gamma for the catalogue, the CLOB for price history.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Polymarket {
    /// Whether the source runs.
    pub enabled: bool,
    /// Gamma API base.
    pub gamma_url: String,
    /// CLOB API base.
    pub clob_url: String,
    /// The curated events.
    #[serde(default)]
    pub events: Vec<PolymarketEvent>,
}

/// Kalshi public read endpoints.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kalshi {
    /// Whether the source runs.
    pub enabled: bool,
    /// API base (`/trade-api/v2`).
    pub base_url: String,
    /// The curated series tickers.
    #[serde(default)]
    pub series: Vec<String>,
}

/// Phoenix's own market list, for the earnings dates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Phoenix {
    /// Whether the source runs.
    pub enabled: bool,
    /// The markets endpoint.
    pub markets_url: String,
}

/// All sources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sources {
    /// Binance public dumps.
    pub binance: Binance,
    /// Hyperliquid info API.
    pub hyperliquid: Hyperliquid,
    /// Deribit DVOL.
    pub deribit: Deribit,
    /// DefiLlama stablecoins.
    pub defillama: Defillama,
    /// Bybit.
    pub bybit: Bybit,
    /// Elfa.
    pub elfa: Elfa,
    /// SEC EDGAR.
    pub sec: Sec,
    /// Fear and Greed.
    pub alternative: Alternative,
    /// Polymarket.
    pub polymarket: Polymarket,
    /// Kalshi.
    pub kalshi: Kalshi,
    /// Phoenix markets metadata.
    pub phoenix: Phoenix,
}

/// `config/augment.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AugmentConfig {
    /// Data root (`SOLOS_DATA_AUGMENT_DIR` overrides it).
    pub data_dir: PathBuf,
    /// First UTC day stored, `YYYY-MM-DD`.
    pub start_date: String,
    /// Requests per second per host.
    #[serde(default = "d_rps")]
    pub requests_per_second: f64,
    /// Bytes the large datasets may bring the root to, in GB; 0 means no large dataset runs.
    #[serde(default)]
    pub disk_budget_gb: f64,
    /// Per-source settings.
    pub sources: Sources,
    /// The symbol map.
    pub symbols: Vec<Symbol>,
}

fn d_true() -> bool {
    true
}
fn d_rps() -> f64 {
    2.0
}
fn d_hl_rps() -> f64 {
    1.0
}
fn d_candle_interval() -> u64 {
    1800
}
fn d_context_interval() -> u64 {
    60
}
fn d_elfa_interval() -> u64 {
    3600
}
fn d_elfa_rpm() -> u64 {
    60
}
fn d_user_agent() -> String {
    super::http::USER_AGENT.to_owned()
}
fn d_sec_rps() -> f64 {
    4.0
}

/// Known Binance datasets and whether each is a large one.
pub const BINANCE_DATASETS: [(&str, bool); 8] = [
    ("fundingRate", false),
    ("klines", false),
    ("premiumIndexKlines", false),
    ("markPriceKlines", false),
    ("indexPriceKlines", false),
    ("metrics", false),
    ("aggTrades", true),
    ("bookDepth", true),
];

/// Load `config/augment.json` (or `path`), apply `SOLOS_DATA_AUGMENT_DIR`, validate.
pub fn load_augment_config(path: Option<&str>) -> Result<AugmentConfig, StoreError> {
    let path = path.unwrap_or("config/augment.json");
    let text = std::fs::read_to_string(path)?;
    let mut config: AugmentConfig =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let cwd = std::env::current_dir()?;
    let data_dir = std::env::var("SOLOS_DATA_AUGMENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| config.data_dir.clone());
    config.data_dir = crate::fsutil::resolve(&cwd, &data_dir);
    validate(&config)?;
    Ok(config)
}

/// Rewrite a config file the way the repository keeps it: two-space JSON, struct field order,
/// trailing newline. `augment sec-map --write` uses it, so the rewrite must be byte-stable.
pub fn write_augment_config(path: &Path, config: &AugmentConfig) -> Result<(), StoreError> {
    let text =
        serde_json::to_string_pretty(config).map_err(|e| StoreError::Check(e.to_string()))?;
    std::fs::write(path, format!("{text}\n"))?;
    Ok(())
}

/// The repository's `config/augment.json`, for tests.
#[must_use]
pub fn repository_config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/augment.json")
}

fn validate(config: &AugmentConfig) -> Result<(), StoreError> {
    if super::periods::parse_date(&config.start_date).is_none() {
        return Err(StoreError::Check("Invalid startDate".into()));
    }
    if !(config.requests_per_second > 0.0 && config.requests_per_second <= 50.0) {
        return Err(StoreError::Check("Invalid requestsPerSecond".into()));
    }
    if !(config.disk_budget_gb >= 0.0 && config.disk_budget_gb.is_finite()) {
        return Err(StoreError::Check("Invalid diskBudgetGb".into()));
    }
    let hl_rps = config.sources.hyperliquid.requests_per_second;
    if !(hl_rps > 0.0 && hl_rps <= 50.0) {
        return Err(StoreError::Check(
            "Invalid hyperliquid.requestsPerSecond".into(),
        ));
    }
    let sec_rps = config.sources.sec.requests_per_second;
    if !(sec_rps > 0.0 && sec_rps <= 10.0) {
        return Err(StoreError::Check("Invalid sec.requestsPerSecond".into()));
    }
    if config.sources.sec.user_agent.trim().is_empty() {
        return Err(StoreError::Check("sec.userAgent must not be empty".into()));
    }
    if config.symbols.is_empty() {
        return Err(StoreError::Check("symbols must not be empty".into()));
    }
    let mut seen = std::collections::HashSet::new();
    for symbol in &config.symbols {
        if !seen.insert(symbol.phoenix.as_str()) {
            return Err(StoreError::Check(format!(
                "duplicate symbol {}",
                symbol.phoenix
            )));
        }
        if !["crypto", "equity", "commodity"].contains(&symbol.kind.as_str()) {
            return Err(StoreError::Check(format!(
                "invalid kind for {}",
                symbol.phoenix
            )));
        }
        if symbol.sec_cik.as_deref().is_some_and(|cik| {
            cik.is_empty() || cik.len() > 10 || !cik.bytes().all(|b| b.is_ascii_digit())
        }) {
            return Err(StoreError::Check(format!(
                "invalid secCik for {}",
                symbol.phoenix
            )));
        }
    }
    let binance = &config.sources.binance;
    for dataset in binance.datasets.iter().chain(&binance.large_datasets) {
        if !BINANCE_DATASETS.iter().any(|(name, _)| name == dataset) {
            return Err(StoreError::Check(format!(
                "unknown Binance dataset {dataset}"
            )));
        }
    }
    for resolution in &config.sources.deribit.resolutions {
        if ![60, 3600].contains(resolution) {
            return Err(StoreError::Check("Invalid Deribit resolution".into()));
        }
    }
    for url in [
        &binance.files_url,
        &binance.list_url,
        &config.sources.hyperliquid.base_url,
        &config.sources.deribit.base_url,
        &config.sources.defillama.base_url,
        &config.sources.bybit.base_url,
        &config.sources.bybit.files_url,
        &config.sources.elfa.base_url,
        &config.sources.sec.base_url,
        &config.sources.sec.tickers_url,
        &config.sources.sec.archive_url,
        &config.sources.alternative.base_url,
        &config.sources.polymarket.gamma_url,
        &config.sources.polymarket.clob_url,
        &config.sources.kalshi.base_url,
        &config.sources.phoenix.markets_url,
    ] {
        check_url(url)?;
    }
    Ok(())
}

/// Source URLs are https, or http on the loopback address (tests).
fn check_url(url: &str) -> Result<(), StoreError> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(StoreError::Check("source URL is not a URL".into()));
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");
    if scheme == "https" || (scheme == "http" && (host == "127.0.0.1" || host == "localhost")) {
        Ok(())
    } else {
        Err(StoreError::Check("source URLs require HTTPS".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_config_is_valid_and_complete() {
        let path = repository_config_path();
        let config = load_augment_config(path.to_str()).unwrap();
        assert_eq!(config.symbols.len(), 94);
        assert_eq!(config.start_date, "2026-01-01");
        let bonk = config
            .symbols
            .iter()
            .find(|s| s.phoenix == "kBONK")
            .unwrap();
        assert_eq!(bonk.binance.as_deref(), Some("1000BONKUSDT"));
        assert_eq!(bonk.hyperliquid.as_deref(), Some("kBONK"));
        let nvda = config.symbols.iter().find(|s| s.phoenix == "NVDA").unwrap();
        assert_eq!(nvda.kind, "equity");
        assert!(nvda.binance.is_none());
        assert_eq!(nvda.hyperliquid.as_deref(), Some("xyz:NVDA"));
        assert!(config.symbols.iter().all(|s| s.elfa_entity_id.is_some()));
        assert_eq!(nvda.sec_cik.as_deref(), Some("1045810"));
        assert!(
            config
                .symbols
                .iter()
                .filter(|s| s.sec_cik.is_some())
                .count()
                >= 30
        );
        assert!(
            config
                .symbols
                .iter()
                .all(|s| s.sec_cik.is_none() || s.kind == "equity")
        );
        assert!(config.sources.polymarket.events.len() >= 10);
        assert!(config.sources.kalshi.series.contains(&"KXFED".to_owned()));
        assert!(check_url("http://127.0.0.1:8080").is_ok());
        assert!(check_url("http://example.com").is_err());
    }

    #[test]
    fn repository_config_round_trips_byte_for_byte() {
        let path = repository_config_path();
        let text = std::fs::read_to_string(&path).unwrap();
        let raw: AugmentConfig = serde_json::from_str(&text).unwrap();
        let again = format!("{}\n", serde_json::to_string_pretty(&raw).unwrap());
        assert_eq!(
            again, text,
            "config/augment.json must be the serializer's own output"
        );
    }
}
