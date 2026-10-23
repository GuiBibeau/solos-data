//! `config/venues-history.json`: the history backfill of the venues lane (ADR-0013). The venue
//! programs, IDLs and Pyth feeds come from the venues config it names; this file adds the
//! history window, older IDLs, and the budget.

use crate::store::StoreError;
use crate::venues::config::{self as venues_config, Feed, VenuesConfig};
use crate::venues::idl::Idl;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One history source as configured.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceConfig {
    /// `jupiter`, `flash` (a venue of the venues config) or `pyth` (the push oracle).
    pub name: String,
    /// Whether the backfill runs it.
    #[serde(default = "yes")]
    pub enabled: bool,
    /// The newest slot to collect: the slot before the live lane's first window.
    pub top: i64,
    /// Older IDLs of the program, newest first, relative to the config file's directory.
    #[serde(default)]
    pub idl_fallbacks: Vec<String>,
    /// Slots per window (one bulk walk per address each); sparse sources take larger windows so
    /// a bulk page carries close to its 100 transactions.
    pub window_slots: i64,
    /// Pyth only: the feed symbols to walk (every feed of the venues config when empty).
    #[serde(default)]
    pub feeds: Vec<String>,
    /// Where the transactions come from: `archive` (Old Faithful through Jetstreamer, one
    /// stream for every archive source) or `rpc` (`getTransactionsForAddress` pages).
    #[serde(default)]
    pub via: Via,
}

/// Where a source's transactions come from; the manifest is always the RPC signature walk.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Via {
    /// `getTransactionsForAddress` bulk pages per window.
    #[default]
    Rpc,
    /// The Old Faithful archive (ADR-0008), streamed per range for all archive sources at once.
    Archive,
}

/// The archive stream's settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveConfig {
    /// Slots one stream covers.
    pub chunk_slots: i64,
    /// Streams in flight at once (consecutive chunks of one batch).
    pub parallel: usize,
}

/// The whole file.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryConfig {
    /// The venues config, relative to this file's directory.
    pub venues_config: String,
    /// The oldest instant collected (RFC 3339); resolved once to a slot and recorded.
    pub floor_time: String,
    /// Helius credits a second the backfill may spend (its limiter weighs each method by
    /// `creditCosts` of the venues config).
    pub credits_per_second: f64,
    /// Requests in flight at once.
    pub concurrency: u32,
    /// Windows fetched together; their manifest is one stretch of the signature walk.
    pub batch_windows: usize,
    /// Seconds between flushes of the buffered rows into the hour files.
    pub flush_seconds: u64,
    /// Rows that trigger an early flush; sources pause at twice this many.
    pub flush_rows: usize,
    /// The archive stream, for sources with `via: archive`.
    #[serde(default)]
    pub archive: Option<ArchiveConfig>,
    /// The sources.
    pub sources: Vec<SourceConfig>,
    /// The directory relative paths resolve against.
    #[serde(skip)]
    pub config_dir: PathBuf,
}

fn yes() -> bool {
    true
}

/// What a source collects.
#[derive(Clone, Debug)]
pub enum Kind {
    /// An Anchor venue: transactions, instructions and events through its IDLs.
    Venue {
        /// Whether whole transactions are stored.
        store_transactions: bool,
    },
    /// The Pyth push oracle: one `prices` row per applied update.
    Pyth {
        /// Feeds that name a symbol.
        feeds: Vec<Feed>,
    },
}

/// A resolved source.
#[derive(Clone, Debug)]
pub struct Source {
    /// Source name in the tables.
    pub name: String,
    /// The program (venues) or the price feed accounts (Pyth) whose transactions are walked.
    pub addresses: Vec<String>,
    /// Whether an address may have no history down to the floor (a Pyth feed account created
    /// after it); a venue program must reach the floor.
    pub may_start_late: bool,
    /// Slots per window (RPC sources).
    pub window_slots: i64,
    /// Where its transactions come from.
    pub via: Via,
    /// Newest slot.
    pub top: i64,
    /// What it collects.
    pub kind: Kind,
    /// IDL files, newest first (empty for Pyth).
    pub idls: Vec<PathBuf>,
    /// Where the newest IDL came from.
    pub idl_source: Option<String>,
}

impl Source {
    /// The source's IDL with its fallbacks (empty for Pyth).
    ///
    /// # Errors
    ///
    /// A missing or unparsable IDL file.
    pub fn idl(&self) -> Result<Idl, StoreError> {
        let mut out: Option<Idl> = None;
        for path in &self.idls {
            let text = std::fs::read_to_string(path)
                .map_err(|e| StoreError::Check(format!("IDL {}: {e}", path.display())))?;
            let idl = Idl::parse(&text)
                .map_err(|e| StoreError::Check(format!("IDL {}: {e}", path.display())))?;
            out = Some(match out {
                None => idl,
                Some(newer) => newer.with_fallback(idl),
            });
        }
        Ok(out.unwrap_or_default())
    }
}

impl HistoryConfig {
    /// Parse a document; `config_dir` resolves relative paths.
    ///
    /// # Errors
    ///
    /// Invalid JSON or values out of range.
    pub fn parse(text: &str, config_dir: &Path) -> Result<HistoryConfig, StoreError> {
        let mut config: HistoryConfig = serde_json::from_str(text)
            .map_err(|e| StoreError::Check(format!("history config: {e}")))?;
        config.config_dir = config_dir.to_path_buf();
        if !(0.1..=10_000.0).contains(&config.credits_per_second)
            || config.concurrency == 0
            || config
                .sources
                .iter()
                .any(|s| s.window_slots <= 0 || s.top <= 0)
            || config.batch_windows == 0
            || config.flush_seconds == 0
            || config.flush_rows == 0
        {
            return Err(StoreError::Check(
                "creditsPerSecond, concurrency, batchWindows, flush sizes and every source's windowSlots and top must be positive".into(),
            ));
        }
        floor_seconds(&config.floor_time)?;
        let archived = config.sources.iter().any(|s| s.via == Via::Archive);
        match &config.archive {
            Some(a) if a.chunk_slots <= 0 || a.parallel == 0 => {
                return Err(StoreError::Check(
                    "archive chunkSlots and parallel must be positive".into(),
                ));
            }
            None if archived => {
                return Err(StoreError::Check(
                    "a source reads the archive but no archive block is configured".into(),
                ));
            }
            _ => {}
        }
        Ok(config)
    }

    /// The venues config this file names.
    ///
    /// # Errors
    ///
    /// Unreadable or invalid file.
    pub fn venues(&self) -> Result<VenuesConfig, StoreError> {
        let path = self.config_dir.join(&self.venues_config);
        venues_config::load(Some(&path.to_string_lossy()))
    }

    /// The enabled sources, resolved against the venues config.
    ///
    /// # Errors
    ///
    /// A source that is neither `pyth` nor a venue of the venues config.
    pub fn resolve(&self, venues: &VenuesConfig) -> Result<Vec<Source>, StoreError> {
        let mut out = Vec::new();
        for source in self.sources.iter().filter(|s| s.enabled) {
            let fallbacks = source.idl_fallbacks.iter().map(|f| self.config_dir.join(f));
            if source.name == "pyth" {
                let feeds: Vec<Feed> = venues
                    .pyth
                    .feeds
                    .iter()
                    .filter(|f| source.feeds.is_empty() || source.feeds.contains(&f.symbol))
                    .cloned()
                    .collect();
                if feeds.is_empty() {
                    return Err(StoreError::Check("history pyth: no feed selected".into()));
                }
                out.push(Source {
                    name: "pyth".into(),
                    addresses: feeds.iter().map(|f| f.account.clone()).collect(),
                    may_start_late: true,
                    window_slots: source.window_slots,
                    via: source.via,
                    top: source.top,
                    kind: Kind::Pyth { feeds },
                    idls: Vec::new(),
                    idl_source: None,
                });
                continue;
            }
            let Some(venue) = venues.venues.iter().find(|v| v.name == source.name) else {
                return Err(StoreError::Check(format!(
                    "history source {} is not a venue",
                    source.name
                )));
            };
            let mut idls = vec![venues.idl_path(venue)];
            idls.extend(fallbacks);
            out.push(Source {
                name: venue.name.clone(),
                addresses: vec![venue.program_id.clone()],
                may_start_late: false,
                window_slots: source.window_slots,
                via: source.via,
                top: source.top,
                kind: Kind::Venue {
                    store_transactions: venue.store_transactions,
                },
                idls,
                idl_source: venue.idl_source.clone(),
            });
        }
        Ok(out)
    }

    /// Credit prices of the venues config, by method.
    #[must_use]
    pub fn costs(venues: &VenuesConfig) -> BTreeMap<String, f64> {
        venues.credit_costs.clone()
    }
}

/// `floorTime` in Unix seconds.
///
/// # Errors
///
/// Not an RFC 3339 instant.
pub fn floor_seconds(text: &str) -> Result<i64, StoreError> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp())
        .map_err(|e| StoreError::Check(format!("floorTime: {e}")))
}

/// Load `path` (default `config/venues-history.json`); `SOLOS_DATA_AUGMENT_DIR` overrides the
/// venues config's `dataDir`, as for the live lane.
///
/// # Errors
///
/// Unreadable or invalid file.
pub fn load(path: Option<&str>) -> Result<HistoryConfig, StoreError> {
    let path = Path::new(path.unwrap_or("config/venues-history.json"));
    let text = std::fs::read_to_string(path)?;
    HistoryConfig::parse(&text, path.parent().unwrap_or(Path::new(".")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_committed_config_resolves_with_every_idl() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/venues-history.json"
        );
        let config = load(Some(path)).unwrap();
        let venues = config.venues().unwrap();
        let sources = config.resolve(&venues).unwrap();
        let names: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["jupiter", "flash", "pyth"]);
        assert_eq!(floor_seconds(&config.floor_time).unwrap(), 1_759_276_800);
        for source in &sources {
            let idl = source.idl().unwrap();
            match source.kind {
                Kind::Venue { .. } => {
                    assert_eq!(source.addresses.len(), 1);
                    assert!(!source.may_start_late);
                    assert_eq!(idl.fallback_count(), source.idls.len() - 1);
                }
                Kind::Pyth { ref feeds } => {
                    assert_eq!(source.addresses.len(), feeds.len());
                    assert!(source.may_start_late);
                }
            }
        }
        let text = serde_json::to_string(&serde_json::json!({
            "venuesConfig": "venues.json", "floorTime": "2025-10-01T00:00:00Z",
            "creditsPerSecond": 1, "concurrency": 1, "batchWindows": 1, "flushSeconds": 1,
            "flushRows": 1, "sources": [{ "name": "pyth", "top": 5, "windowSlots": 0 }]
        }))
        .unwrap();
        assert!(HistoryConfig::parse(&text, Path::new(".")).is_err());
    }
}
