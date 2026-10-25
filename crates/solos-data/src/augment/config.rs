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
    /// OKX USDT-margined perpetual swap (`SOL-USDT-SWAP`), as Tardis.dev's `okex-swap` names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub okx: Option<String>,
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
    /// How `largeDatasets` are walked: newest day first, from `walk.startDate`.
    #[serde(default)]
    pub walk: DumpWalk,
}

/// A walk over an exchange's dated dump files (ADR-0009, exchange dumps 2026-10-19): newest
/// period first down to `startDate`, across every series day by day, bounded per run in time
/// and per source in bytes, so the hourly sync stays short and a stopped run resumes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DumpWalk {
    /// First UTC day (or month) walked, `YYYY-MM-DD`.
    pub start_date: String,
    /// Symbol kinds covered (`crypto`).
    pub kinds: Vec<String>,
    /// Size, in GB, the walk's datasets may reach in the ledger before it stops adding files.
    pub budget_gb: f64,
    /// Minutes one sync run may spend on the walk before yielding to the next run.
    pub run_minutes: u64,
}

impl Default for DumpWalk {
    fn default() -> Self {
        DumpWalk {
            start_date: "2025-10-01".into(),
            kinds: vec!["crypto".into()],
            budget_gb: 0.0,
            run_minutes: 45,
        }
    }
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
    /// How the tick dumps are walked.
    #[serde(default)]
    pub walk: DumpWalk,
}

/// OKX historical market data: the official static files the
/// `/api/v5/public/market-data-history` endpoint links to (trades daily, funding monthly).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Okx {
    /// Whether the source runs.
    pub enabled: bool,
    /// Base of the static files (`https://static.okx.com/cdn/okex/traderecords`).
    pub files_url: String,
    /// Daily swap trades.
    pub trades: bool,
    /// Monthly swap funding rates.
    pub funding: bool,
    /// The walk.
    pub walk: DumpWalk,
}

impl Default for Okx {
    fn default() -> Self {
        Okx {
            enabled: false,
            files_url: "https://static.okx.com/cdn/okex/traderecords".into(),
            trades: false,
            funding: false,
            walk: DumpWalk::default(),
        }
    }
}

/// AWS credentials for the requester-pays Hyperliquid buckets, kept out of `Debug` output.
#[derive(Clone)]
pub struct AwsKeys {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Session token of temporary credentials.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AwsKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AwsKeys(<{} chars>, <{} chars>)",
            self.access_key_id.len(),
            self.secret_access_key.len()
        )
    }
}

/// The variables the Hyperliquid S3 credentials are read from (the sync unit's `augment.env`).
pub const HL_AWS_ENV: [&str; 3] = [
    "HL_AWS_ACCESS_KEY_ID",
    "HL_AWS_SECRET_ACCESS_KEY",
    "HL_AWS_SESSION_TOKEN",
];

/// The Hyperliquid credentials, when both required variables are set and not empty.
#[must_use]
pub fn hl_aws_keys_from_env() -> Option<AwsKeys> {
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    Some(AwsKeys {
        access_key_id: read(HL_AWS_ENV[0])?,
        secret_access_key: read(HL_AWS_ENV[1])?,
        session_token: read(HL_AWS_ENV[2]),
    })
}

/// Hyperliquid's requester-pays S3 archive (ADR-0009, exchange dumps): hourly `l2Book`
/// snapshots, daily asset contexts and hourly node fills. Off without `HL_AWS_*` credentials;
/// with them, `estimateOnly` lists and prices the window before anything is downloaded.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidS3 {
    /// Whether the source runs (it also needs the credentials).
    pub enabled: bool,
    /// `hyperliquid-archive` endpoint (`market_data/`, `asset_ctxs/`).
    pub archive_url: String,
    /// Its region, for signing.
    pub archive_region: String,
    /// `hl-mainnet-node-data` endpoint (`node_fills_by_block/`).
    pub node_url: String,
    /// Its region.
    pub node_region: String,
    /// List and price the window only; download nothing.
    pub estimate_only: bool,
    /// Hourly `l2Book` snapshots of `bookSymbols`.
    pub l2_book: bool,
    /// Daily asset contexts of every coin.
    pub asset_ctxs: bool,
    /// Hourly node fills, kept for the mapped coins.
    pub fills: bool,
    /// Phoenix symbols whose books are fetched.
    pub book_symbols: Vec<String>,
    /// Transfer, in GB, one run may download (requester pays it).
    pub transfer_gb_per_run: f64,
    /// The walk (start, kinds, total budget, minutes per run).
    pub walk: DumpWalk,
    /// From `HL_AWS_*`, never from the file.
    #[serde(skip)]
    pub keys: Option<AwsKeys>,
}

impl Default for HyperliquidS3 {
    fn default() -> Self {
        HyperliquidS3 {
            enabled: false,
            archive_url: "https://hyperliquid-archive.s3.us-east-1.amazonaws.com".into(),
            archive_region: "us-east-1".into(),
            node_url: "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com".into(),
            node_region: "ap-northeast-1".into(),
            estimate_only: true,
            l2_book: false,
            asset_ctxs: false,
            fills: false,
            book_symbols: Vec::new(),
            transfer_gb_per_run: 0.0,
            walk: DumpWalk::default(),
            keys: None,
        }
    }
}

/// One exchange of the Tardis.dev source: its id, where each Phoenix market's symbol comes
/// from, and the data types fetched for it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TardisExchange {
    /// Tardis.dev exchange id (`binance-futures`, `bybit`, `okex-swap`, `hyperliquid`, `deribit`).
    pub id: String,
    /// The symbol-map field that names the market on this exchange (`binance`, `bybit`, `okx`,
    /// `hyperliquid`); absent when `symbols` lists them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_field: Option<String>,
    /// Explicit Phoenix symbol to exchange symbol pairs (Deribit's `BTC-PERPETUAL`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub symbols: std::collections::BTreeMap<String, String>,
    /// Data types fetched (`trades`, `derivative_ticker`, `liquidations`, `book_snapshot_25`).
    pub data_types: Vec<String>,
}

/// The API key, kept out of `Debug` output and never serialized.
#[derive(Clone, Default)]
pub struct ApiKey(pub String);

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ApiKey(<{} chars>)", self.0.len())
    }
}

/// Tardis.dev downloadable CSV datasets (ADR-0009, Tardis section): the free first-of-month
/// samples always, every other day only with `TARDIS_API_KEY`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tardis {
    /// Whether the source runs.
    pub enabled: bool,
    /// Base of the dataset URLs (`https://datasets.tardis.dev/v1`).
    pub datasets_url: String,
    /// First first-of-month sample day fetched without a key.
    pub sample_start: String,
    /// First day fetched with a key; the plan's history decides how far back it may reach.
    pub keyed_start: String,
    /// Symbol kinds covered (`crypto`).
    pub kinds: Vec<String>,
    /// Phoenix symbols whose order book snapshots are fetched; the others get no book type.
    pub book_symbols: Vec<String>,
    /// Size, in GB, the source's registered files may reach before keyed days stop.
    pub budget_gb: f64,
    /// The exchanges.
    pub exchanges: Vec<TardisExchange>,
    /// `TARDIS_API_KEY`, from the environment only.
    #[serde(skip)]
    pub api_key: Option<ApiKey>,
}

impl Default for Tardis {
    fn default() -> Self {
        Tardis {
            enabled: false,
            datasets_url: "https://datasets.tardis.dev/v1".into(),
            sample_start: "2025-10-01".into(),
            keyed_start: "2025-10-01".into(),
            kinds: vec!["crypto".into()],
            book_symbols: Vec::new(),
            budget_gb: 0.0,
            exchanges: Vec::new(),
            api_key: None,
        }
    }
}

/// The variable `TARDIS_API_KEY` is read from (the augment units' `augment.env`).
pub const TARDIS_API_KEY_ENV: &str = "TARDIS_API_KEY";

/// `TARDIS_API_KEY`, trimmed, when set and not empty.
#[must_use]
pub fn tardis_key_from_env() -> Option<ApiKey> {
    std::env::var(TARDIS_API_KEY_ENV)
        .ok()
        .map(|key| key.trim().to_owned())
        .filter(|key| !key.is_empty())
        .map(ApiKey)
}

/// One Elfa Auto alert definition: the EQL conditions and the repeat rule. The lane adds the
/// `notify` action and the expiry; no other action type can be configured.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertDef {
    /// Title, the idempotency key against the account's active queries.
    pub title: String,
    /// Description shown with the notification.
    pub description: String,
    /// EQL condition tree (`{"AND": [...]}` or `{"OR": [...]}`).
    pub conditions: serde_json::Value,
    /// `{"cooldown": "15m", "maxTriggers": 500}`; absent for a one-shot alert.
    #[serde(default)]
    pub repeat: Option<serde_json::Value>,
}

/// Elfa Auto: a small set of alerts whose firings are recorded as they arrive.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElfaAuto {
    /// Whether alerts are created and streamed.
    #[serde(default)]
    pub enabled: bool,
    /// Ceiling on alerts the lane may hold active.
    #[serde(default = "d_max_alerts")]
    pub max_alerts: usize,
    /// Credits the lane may spend creating alerts in one calendar month.
    #[serde(default = "d_credit_budget")]
    pub credit_budget_per_month: i64,
    /// `expiresIn` of every alert (`720h` is the API's maximum).
    #[serde(default = "d_expires_in")]
    pub expires_in: String,
    /// An alert expiring within this many hours is recreated and the old one cancelled.
    #[serde(default = "d_renew_hours")]
    pub renew_within_hours: i64,
    /// Hours between listings of the account's queries (one credit each); a restart within
    /// the interval reuses the alerts stored in the ledger.
    #[serde(default = "d_reconcile_hours")]
    pub reconcile_interval_hours: i64,
    /// The alert definitions.
    #[serde(default)]
    pub alerts: Vec<AlertDef>,
}

impl Default for ElfaAuto {
    fn default() -> Self {
        ElfaAuto {
            enabled: false,
            max_alerts: d_max_alerts(),
            credit_budget_per_month: d_credit_budget(),
            expires_in: d_expires_in(),
            renew_within_hours: d_renew_hours(),
            reconcile_interval_hours: d_reconcile_hours(),
            alerts: Vec::new(),
        }
    }
}

/// Elfa v3 and Auto.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elfa {
    /// Whether the source runs (it also needs `ELFA_API_KEY`).
    pub enabled: bool,
    /// API base.
    pub base_url: String,
    /// Seconds between capture cycles of the calls, episodes and call-book streams.
    #[serde(default = "d_elfa_interval")]
    pub interval_seconds: u64,
    /// The key's request allowance per minute.
    #[serde(default = "d_elfa_rpm")]
    pub requests_per_minute: u64,
    /// Seconds between incremental pulls of the events stream; 0 keeps events in the hourly
    /// cycle.
    #[serde(default = "d_events_interval")]
    pub events_interval_seconds: u64,
    /// Pages one events pull may request (30 events each): the poll's request budget.
    #[serde(default = "d_events_pages")]
    pub events_pages_per_poll: usize,
    /// Hard monthly cap on the credits the whole Elfa client may spend (v3 and Auto), as the
    /// answers' `x-elfa-credits` declare them; past it every Elfa request is refused.
    #[serde(default = "d_elfa_credit_cap")]
    pub credit_cap_per_month: i64,
    /// Auto alerts.
    #[serde(default)]
    pub auto: ElfaAuto,
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
    /// The User-Agent EDGAR sees, from `SEC_USER_AGENT` (never the config file). EDGAR's
    /// fair-access policy answers `403` to a User-Agent without a contact email; the operator
    /// declares `<name> <contact email>` in the environment, and without it the source is off.
    #[serde(skip)]
    pub user_agent: Option<String>,
    /// Requests per second to EDGAR (its ceiling is ten).
    #[serde(default = "d_sec_rps")]
    pub requests_per_second: f64,
}

/// The environment variable that carries EDGAR's User-Agent.
pub const SEC_USER_AGENT_ENV: &str = "SEC_USER_AGENT";

impl Sec {
    /// Why the source cannot run with this User-Agent, if it cannot.
    #[must_use]
    pub fn disabled_reason(&self) -> Option<&'static str> {
        match self.user_agent.as_deref() {
            None => Some("SEC_USER_AGENT not set"),
            Some(agent) if !agent.contains('@') => Some("SEC_USER_AGENT has no contact email"),
            Some(_) => None,
        }
    }
}

/// `SEC_USER_AGENT`, trimmed, when set and not empty.
#[must_use]
pub fn sec_user_agent_from_env() -> Option<String> {
    std::env::var(SEC_USER_AGENT_ENV)
        .ok()
        .map(|agent| agent.trim().to_owned())
        .filter(|agent| !agent.is_empty())
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
    /// Request pace on Kalshi's host. The Basic tier's published read budget is 200 tokens a
    /// second at 10 tokens a request (20 requests a second); the default is half of that.
    #[serde(default = "d_kalshi_rps")]
    pub requests_per_second: f64,
    /// First close day stored; the lane's `startDate` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    /// Minutes one sync run may spend on Kalshi (0: no time bound).
    #[serde(default = "d_kalshi_run_minutes")]
    pub run_minutes: u64,
    /// Requests one sync run may send to Kalshi (0: no request bound).
    #[serde(default = "d_kalshi_max_requests")]
    pub max_requests: u64,
    /// The curated series and how much of each ladder is kept.
    #[serde(default)]
    pub series: Vec<KalshiSeries>,
}

/// One curated Kalshi series.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KalshiSeries {
    /// Series ticker (`KXBTCD`).
    pub ticker: String,
    /// What the ladder is on (`BTC`, `GOLD`, `SPX`, `FED`); informational, kept on every row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
    /// `crypto`, `commodity`, `equity` or `macro`.
    pub kind: String,
    /// Candle interval in minutes: 1 or 60.
    #[serde(default = "d_kalshi_interval")]
    pub interval: u32,
    /// Strike window around the causal centre, percent of the centre for a one-hour life
    /// (scaled by the square root of the stored life in hours); 0 keeps every strike.
    #[serde(default)]
    pub window_pct: f64,
    /// At most this many strikes per event, the nearest the centre (0: no cap).
    #[serde(default)]
    pub max_strikes: usize,
    /// Hours of candles stored before each market's close.
    #[serde(default = "d_kalshi_life")]
    pub max_life_hours: i64,
    /// Collect the trades of the kept markets that traded.
    #[serde(default)]
    pub trades: bool,
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
    /// Tardis.dev datasets (off unless configured).
    #[serde(default)]
    pub tardis: Tardis,
    /// OKX historical market data files.
    #[serde(default)]
    pub okx: Okx,
    /// Hyperliquid's requester-pays S3 archive.
    #[serde(default, rename = "hyperliquidS3")]
    pub hyperliquid_s3: HyperliquidS3,
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
fn d_events_interval() -> u64 {
    60
}
fn d_events_pages() -> usize {
    2
}
fn d_max_alerts() -> usize {
    8
}
fn d_credit_budget() -> i64 {
    60
}
fn d_expires_in() -> String {
    "720h".into()
}
fn d_renew_hours() -> i64 {
    48
}
fn d_reconcile_hours() -> i64 {
    12
}
fn d_elfa_credit_cap() -> i64 {
    100
}
fn d_kalshi_rps() -> f64 {
    10.0
}
fn d_kalshi_run_minutes() -> u64 {
    40
}
fn d_kalshi_max_requests() -> u64 {
    24_000
}
fn d_kalshi_interval() -> u32 {
    1
}
fn d_kalshi_life() -> i64 {
    26
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
    config.sources.sec.user_agent = sec_user_agent_from_env();
    config.sources.tardis.api_key = tardis_key_from_env();
    config.sources.hyperliquid_s3.keys = hl_aws_keys_from_env();
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
    validate_kalshi(&config.sources.kalshi)?;
    let auto = &config.sources.elfa.auto;
    if auto.alerts.len() > auto.max_alerts {
        return Err(StoreError::Check(format!(
            "elfa.auto has {} alerts, more than maxAlerts {}",
            auto.alerts.len(),
            auto.max_alerts
        )));
    }
    let mut titles = std::collections::HashSet::new();
    for alert in &auto.alerts {
        if alert.title.trim().is_empty() || !titles.insert(alert.title.as_str()) {
            return Err(StoreError::Check(
                "elfa.auto alert titles must be unique".into(),
            ));
        }
        if !alert.conditions.is_object() {
            return Err(StoreError::Check(format!(
                "elfa.auto alert {} has no conditions",
                alert.title
            )));
        }
    }
    if !auto.expires_in.ends_with('h')
        || auto.credit_budget_per_month < 0
        || auto.reconcile_interval_hours < 1
        || config.sources.elfa.credit_cap_per_month < 0
    {
        return Err(StoreError::Check("invalid elfa.auto settings".into()));
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
    // Phase-1 datasets are listed and walked oldest first; the large ones are dump walks.
    for (datasets, large) in [(&binance.datasets, false), (&binance.large_datasets, true)] {
        for dataset in datasets {
            if !BINANCE_DATASETS
                .iter()
                .any(|(name, is_large)| name == dataset && *is_large == large)
            {
                return Err(StoreError::Check(format!(
                    "Binance dataset {dataset} is not a {} dataset",
                    if large { "large" } else { "phase-1" }
                )));
            }
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
        &config.sources.tardis.datasets_url,
        &config.sources.okx.files_url,
        &config.sources.hyperliquid_s3.archive_url,
        &config.sources.hyperliquid_s3.node_url,
    ] {
        check_url(url)?;
    }
    for (name, walk) in [
        ("binance.walk", &binance.walk),
        ("bybit.walk", &config.sources.bybit.walk),
        ("okx.walk", &config.sources.okx.walk),
        ("hyperliquidS3.walk", &config.sources.hyperliquid_s3.walk),
    ] {
        validate_walk(name, walk)?;
    }
    let hl = &config.sources.hyperliquid_s3;
    if !(hl.transfer_gb_per_run >= 0.0 && hl.transfer_gb_per_run.is_finite())
        || [&hl.archive_region, &hl.node_region]
            .iter()
            .any(|r| r.is_empty() || !r.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
    {
        return Err(StoreError::Check("invalid hyperliquidS3 settings".into()));
    }
    super::tardis::validate(&config.sources.tardis)
}

fn validate_kalshi(kalshi: &Kalshi) -> Result<(), StoreError> {
    if !(kalshi.requests_per_second > 0.0 && kalshi.requests_per_second <= 20.0) {
        return Err(StoreError::Check(
            "Invalid kalshi.requestsPerSecond (Basic tier: at most 20)".into(),
        ));
    }
    if kalshi
        .start_date
        .as_deref()
        .is_some_and(|d| super::periods::parse_date(d).is_none())
    {
        return Err(StoreError::Check("Invalid kalshi.startDate".into()));
    }
    let mut tickers = std::collections::HashSet::new();
    for series in &kalshi.series {
        let ok = !series.ticker.trim().is_empty()
            && tickers.insert(series.ticker.as_str())
            && ["crypto", "commodity", "equity", "macro"].contains(&series.kind.as_str())
            && [1, 60].contains(&series.interval)
            && series.window_pct >= 0.0
            && series.window_pct.is_finite()
            && (1..=24 * 400).contains(&series.max_life_hours);
        if !ok {
            return Err(StoreError::Check(format!(
                "Invalid kalshi series {}",
                series.ticker
            )));
        }
    }
    Ok(())
}

fn validate_walk(name: &str, walk: &DumpWalk) -> Result<(), StoreError> {
    let kinds_ok = walk
        .kinds
        .iter()
        .all(|k| ["crypto", "equity", "commodity"].contains(&k.as_str()));
    if super::periods::parse_date(&walk.start_date).is_none()
        || !(walk.budget_gb >= 0.0 && walk.budget_gb.is_finite())
        || !kinds_ok
    {
        return Err(StoreError::Check(format!("invalid {name}")));
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
        // Elfa is switched off at the operator's request (2026-10-25); the alert definitions stay.
        assert!(!config.sources.elfa.enabled);
        let auto = &config.sources.elfa.auto;
        assert!(!auto.enabled);
        assert_eq!(auto.alerts.len(), 7);
        assert!(auto.alerts.len() <= auto.max_alerts);
        assert_eq!(auto.expires_in, "720h");
        assert_eq!(config.sources.elfa.events_interval_seconds, 60);
        let kalshi = &config.sources.kalshi;
        assert!(
            kalshi
                .series
                .iter()
                .any(|s| s.ticker == "KXBTCD" && s.interval == 1)
        );
        assert!(
            kalshi
                .series
                .iter()
                .any(|s| s.ticker == "KXFED" && s.kind == "macro")
        );
        assert!(check_url("http://127.0.0.1:8080").is_ok());
        assert!(check_url("http://example.com").is_err());
    }

    #[test]
    fn binance_datasets_stay_in_their_walk() {
        let path = repository_config_path();
        let mut config = load_augment_config(path.to_str()).unwrap();
        config.sources.binance.large_datasets = vec!["metrics".into()];
        assert!(validate(&config).is_err());
        config.sources.binance.large_datasets = vec!["bookDepth".into()];
        config.sources.binance.datasets = vec!["aggTrades".into()];
        assert!(validate(&config).is_err());
        config.sources.binance.datasets = vec!["klines".into()];
        assert!(validate(&config).is_ok());
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
