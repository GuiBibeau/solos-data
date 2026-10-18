//! `config/venues.json`: the other Solana perp venues and the Pyth feeds the `venues` lane reads
//! (ADR-0012). The data root is the augment root (`dataDir`, or `SOLOS_DATA_AUGMENT_DIR`), so
//! the files join the augment catalogs, query views and backup.

use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One account the lane polls.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    /// Address.
    pub address: String,
    /// What it is (`pool`, `custody`, `market`, …); free text for readers.
    pub kind: String,
    /// The market or asset it belongs to (`SOL`, `JLP`, …).
    #[serde(default)]
    pub label: Option<String>,
}

/// One venue program.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Venue {
    /// Source name in the tables (`jupiter`, `flash`, `zeta`).
    pub name: String,
    /// Program address.
    pub program_id: String,
    /// Whether the lane reads it.
    #[serde(default = "yes")]
    pub enabled: bool,
    /// The IDL file, relative to the config file's directory.
    pub idl: String,
    /// Where the IDL came from (recorded in the status, not fetched).
    #[serde(default)]
    pub idl_source: Option<String>,
    /// Whether whole transactions (wire and meta) are stored beside the decoded rows.
    #[serde(default = "yes")]
    pub store_transactions: bool,
    /// Accounts to poll every `accountsIntervalSeconds`.
    #[serde(default)]
    pub accounts: Vec<Account>,
    /// Account types to list with `getProgramAccounts` every `discoverIntervalSeconds` and then
    /// poll like `accounts` (only types with few instances: pools, custodies, markets).
    #[serde(default)]
    pub discover: Vec<String>,
}

/// One Pyth price feed.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Feed {
    /// Phoenix symbol.
    pub symbol: String,
    /// Pyth feed id, hex without `0x`.
    pub feed_id: String,
    /// The price update account (Pyth push oracle, shard 0).
    pub account: String,
    /// Pyth's name for the feed (`BONK/USD` for `kBONK`: a Pyth price is per unit of the base).
    #[serde(default)]
    pub pyth: Option<String>,
    /// Whether Pyth lists the account among its sponsored Solana feeds (kept fresh by Pyth);
    /// other shard-0 accounts exist only while someone pushes them.
    #[serde(default)]
    pub sponsored: bool,
}

/// Pyth settings.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Pyth {
    /// Whether the lane polls the feeds.
    #[serde(default)]
    pub enabled: bool,
    /// Seconds between polls.
    #[serde(default = "five")]
    pub interval_seconds: u64,
    /// The feeds.
    #[serde(default)]
    pub feeds: Vec<Feed>,
}

/// The whole file.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VenuesConfig {
    /// The augment root.
    pub data_dir: PathBuf,
    /// Requests a second through the lane's own limiter (the collector keeps its own).
    pub requests_per_second: f64,
    /// Seconds between transaction cycles.
    pub cycle_seconds: u64,
    /// Seconds between flushes of the buffered rows into the hour files.
    pub flush_seconds: u64,
    /// Seconds between account polls.
    pub accounts_interval_seconds: u64,
    /// Seconds between `getProgramAccounts` discoveries.
    pub discover_interval_seconds: u64,
    /// Largest slot window one transaction cycle covers (a catch-up after downtime is cut into
    /// windows of this size).
    pub max_slots_per_cycle: i64,
    /// Credits per request by method, for the status estimate (Helius's published prices).
    #[serde(default)]
    pub credit_costs: BTreeMap<String, f64>,
    /// The venues.
    pub venues: Vec<Venue>,
    /// Pyth.
    #[serde(default)]
    pub pyth: Pyth,
    /// The directory relative IDL paths resolve against (the config file's directory).
    #[serde(skip)]
    pub config_dir: PathBuf,
}

fn yes() -> bool {
    true
}

fn five() -> u64 {
    5
}

impl VenuesConfig {
    /// Parse a document; `config_dir` resolves IDL paths.
    ///
    /// # Errors
    ///
    /// Invalid JSON or values out of range.
    pub fn parse(text: &str, config_dir: &Path) -> Result<VenuesConfig, StoreError> {
        let mut config: VenuesConfig = serde_json::from_str(text)
            .map_err(|e| StoreError::Check(format!("venues config: {e}")))?;
        config.config_dir = config_dir.to_path_buf();
        if !(0.1..=500.0).contains(&config.requests_per_second) {
            return Err(StoreError::Check(
                "requestsPerSecond must be within 0.1..500".into(),
            ));
        }
        if config.cycle_seconds == 0 || config.flush_seconds == 0 || config.max_slots_per_cycle <= 0
        {
            return Err(StoreError::Check(
                "cycle, flush and window sizes must be positive".into(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for venue in &config.venues {
            if !names.insert(venue.name.as_str()) || venue.name == "pyth" {
                return Err(StoreError::Check(format!(
                    "duplicate venue name {}",
                    venue.name
                )));
            }
        }
        for feed in &config.pyth.feeds {
            if feed.feed_id.len() != 64 || hex::decode(&feed.feed_id).is_err() {
                return Err(StoreError::Check(format!(
                    "feed {} has an invalid id",
                    feed.symbol
                )));
            }
        }
        Ok(config)
    }

    /// The IDL file of a venue.
    #[must_use]
    pub fn idl_path(&self, venue: &Venue) -> PathBuf {
        self.config_dir.join(&venue.idl)
    }
}

/// Load `path` (default `config/venues.json`); `SOLOS_DATA_AUGMENT_DIR` overrides `dataDir`.
///
/// # Errors
///
/// Unreadable or invalid file.
pub fn load(path: Option<&str>) -> Result<VenuesConfig, StoreError> {
    let path = Path::new(path.unwrap_or("config/venues.json"));
    let text = std::fs::read_to_string(path)?;
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut config = VenuesConfig::parse(&text, dir)?;
    if let Ok(dir) = std::env::var("SOLOS_DATA_AUGMENT_DIR")
        && !dir.is_empty()
    {
        config.data_dir = PathBuf::from(dir);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::idl::Idl;

    #[test]
    fn the_repository_config_and_its_idls_load_and_every_layout_is_readable() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/venues.json");
        let config = load(Some(path)).unwrap();
        assert!(
            config
                .pyth
                .feeds
                .iter()
                .any(|f| f.symbol == "SOL" && f.sponsored)
        );
        for venue in &config.venues {
            let idl =
                Idl::parse(&std::fs::read_to_string(config.idl_path(venue)).unwrap()).unwrap();
            idl.check()
                .unwrap_or_else(|e| panic!("{}: {e}", venue.name));
            let (events, accounts, instructions) = idl.counts();
            assert!(
                events > 0 && accounts > 0 && instructions > 0,
                "{}",
                venue.name
            );
            for type_name in &venue.discover {
                assert!(
                    idl.account_discriminator(type_name).is_some(),
                    "{type_name}"
                );
            }
        }
        let jupiter = &config.venues[0];
        let idl = Idl::parse(&std::fs::read_to_string(config.idl_path(jupiter)).unwrap()).unwrap();
        // Discriminators recomputed from the IDL names match the published ones.
        assert_eq!(
            idl.event_name(&hex::decode("f5715534d6bb9984").unwrap()),
            Some("IncreasePositionEvent")
        );
        assert_eq!(
            idl.event_name(&hex::decode("806547a880485654").unwrap()),
            Some("LiquidateFullPositionEvent")
        );
        assert_eq!(
            idl.account_name(&hex::decode("01b830515d833f91").unwrap()),
            Some("Custody")
        );
    }

    #[test]
    fn invalid_values_are_refused() {
        let base = r#"{"dataDir":"d","requestsPerSecond":20,"cycleSeconds":30,"flushSeconds":300,
            "accountsIntervalSeconds":60,"discoverIntervalSeconds":3600,"maxSlotsPerCycle":2000,"venues":[]}"#;
        assert!(VenuesConfig::parse(base, Path::new(".")).is_ok());
        let fast = base.replace("\"requestsPerSecond\":20", "\"requestsPerSecond\":5000");
        assert!(VenuesConfig::parse(&fast, Path::new(".")).is_err());
        let bad_feed = base.replace("\"venues\":[]", "\"venues\":[],\"pyth\":{\"feeds\":[{\"symbol\":\"S\",\"feedId\":\"zz\",\"account\":\"a\"}]}");
        assert!(VenuesConfig::parse(&bad_feed, Path::new(".")).is_err());
    }
}
