//! Collector settings: `config/phoenix.json` with the TypeScript defaults, environment overrides
//! and validation messages.

use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A request lane: the live tail has reserved capacity, the backfill borrows idle capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lane {
    /// Live collection.
    Tail,
    /// Historical collection.
    Backfill,
}

impl Lane {
    /// The lane name as the TypeScript code spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Lane::Tail => "tail",
            Lane::Backfill => "backfill",
        }
    }
}

/// Per-lane values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lanes {
    /// Tail lane value.
    pub tail: u32,
    /// Backfill lane value.
    pub backfill: u32,
}

impl Lanes {
    /// Value for a lane.
    #[must_use]
    pub fn get(&self, lane: Lane) -> u32 {
        match lane {
            Lane::Tail => self.tail,
            Lane::Backfill => self.backfill,
        }
    }
}

/// `config/phoenix.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// Phoenix program id.
    pub program_id: String,
    /// Exchange snapshot URL.
    pub exchange_url: String,
    /// Raw data root.
    pub data_dir: PathBuf,
    /// Plan ceiling in compute units per second.
    pub cu_per_second: f64,
    /// Share of the ceiling the collector may use.
    pub utilization: f64,
    /// Share reserved for the tail lane.
    pub tail_share: f64,
    /// Request concurrency per lane.
    pub concurrency: Lanes,
    /// Compute-unit weight per RPC method.
    pub cu_weights: BTreeMap<String, u32>,
    /// Seconds between tail cycles.
    pub tail_interval_seconds: u64,
    /// Slots re-walked below the watermark.
    pub overlap_slots: i64,
    /// Largest tail chunk.
    pub max_slots_per_chunk: i64,
    /// Standard fetch batch size.
    pub fetch_batch_size: i64,
    /// Freshness target, seconds.
    pub freshness_slo_seconds: u64,
    /// Whether the historical lane runs.
    pub backfill_enabled: bool,
    /// Seconds between exchange refreshes.
    pub exchange_refresh_seconds: u64,
    /// Legacy compaction interval (unused since ADR-0006, kept for the file format).
    pub compaction_interval_seconds: u64,
    /// RPC retries.
    pub max_retries: u32,
    /// Probe sample size.
    pub probe_transaction_limit: u64,
    /// Probe page limit.
    pub probe_page_limit: u64,
    /// Whether the bulk endpoint is used.
    pub bulk_fetch_enabled: bool,
    /// Slots per bulk window.
    #[serde(default = "d_bulk_window")]
    pub bulk_fetch_window_slots: i64,
    /// Bulk windows in flight.
    #[serde(default = "d_bulk_concurrency")]
    pub bulk_fetch_concurrency: u32,
    /// Bulk pages per commit.
    #[serde(default = "d_bulk_commit_pages")]
    pub bulk_commit_pages: u32,
    /// Transaction version accepted from the provider.
    pub max_supported_transaction_version: u8,
    /// Slots per historical chunk.
    #[serde(default = "d_backfill_chunk")]
    pub backfill_chunk_slots: i64,
    /// Slots kept hot in the checkpoint behind the watermark.
    #[serde(default = "d_hot_slots")]
    pub checkpoint_hot_slots: i64,
    /// Seconds between maintenance passes.
    #[serde(default = "d_maintenance")]
    pub maintenance_interval_seconds: u64,
    /// Grace before superseded files are removed.
    #[serde(default = "d_garbage")]
    pub garbage_grace_seconds: i64,
    /// Whether the archive (Jetstreamer) lane may collect ranges below the archive tip.
    #[serde(default)]
    pub archive_backfill_enabled: bool,
    /// Slots per archive range.
    #[serde(default = "d_archive_chunk")]
    pub archive_chunk_slots: i64,
    /// Share of multi-transaction slots cross-checked with `getBlock` in archive ranges.
    #[serde(default = "d_archive_sample")]
    pub archive_ordering_sample: f64,
    /// Slots a routine retention pass may trim from the checkpoint (`maintain --all` is unbounded).
    #[serde(default = "d_retention_pass")]
    pub retention_slots_per_pass: i64,
}

fn d_bulk_window() -> i64 {
    128
}
fn d_bulk_concurrency() -> u32 {
    8
}
fn d_bulk_commit_pages() -> u32 {
    10
}
fn d_backfill_chunk() -> i64 {
    1000
}
fn d_hot_slots() -> i64 {
    10_000
}
fn d_maintenance() -> u64 {
    60
}
fn d_garbage() -> i64 {
    600
}
fn d_archive_chunk() -> i64 {
    10_000
}
fn d_archive_sample() -> f64 {
    0.02
}
fn d_retention_pass() -> i64 {
    16_000
}

/// Load, apply `SOLOS_DATA_DIR` and `SOLOS_DATA_CU_PER_SECOND`, validate.
pub fn load_config(path: Option<&str>) -> Result<Config, StoreError> {
    let path = path.unwrap_or("config/phoenix.json");
    let text = std::fs::read_to_string(path)?;
    let mut config: Config =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let cwd = std::env::current_dir()?;
    let data_dir = std::env::var("SOLOS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| config.data_dir.clone());
    config.data_dir = crate::fsutil::resolve(&cwd, &data_dir);
    if let Ok(cu) = std::env::var("SOLOS_DATA_CU_PER_SECOND") {
        config.cu_per_second = cu.parse().unwrap_or(f64::NAN);
    }
    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<(), StoreError> {
    if config.cu_per_second <= 0.0 || !config.cu_per_second.is_finite() {
        return Err(StoreError::Check("Invalid CU/s".into()));
    }
    if config.tail_share <= 0.0 || config.tail_share >= 1.0 {
        return Err(StoreError::Check("Invalid tail share".into()));
    }
    if config.utilization <= 0.0 || config.utilization > 1.0 {
        return Err(StoreError::Check("Invalid utilization".into()));
    }
    let positives: [(&str, i64); 11] = [
        ("overlapSlots", config.overlap_slots),
        ("maxSlotsPerChunk", config.max_slots_per_chunk),
        ("backfillChunkSlots", config.backfill_chunk_slots),
        ("fetchBatchSize", config.fetch_batch_size),
        ("maxRetries", i64::from(config.max_retries)),
        ("bulkFetchWindowSlots", config.bulk_fetch_window_slots),
        (
            "bulkFetchConcurrency",
            i64::from(config.bulk_fetch_concurrency),
        ),
        ("bulkCommitPages", i64::from(config.bulk_commit_pages)),
        ("checkpointHotSlots", config.checkpoint_hot_slots),
        (
            "maintenanceIntervalSeconds",
            i64::try_from(config.maintenance_interval_seconds).unwrap_or(0),
        ),
        ("garbageGraceSeconds", config.garbage_grace_seconds),
    ];
    for (key, value) in positives {
        if value <= 0 {
            return Err(StoreError::Check(format!("Invalid {key}")));
        }
    }
    if config.checkpoint_hot_slots <= config.overlap_slots + config.max_slots_per_chunk {
        return Err(StoreError::Check(
            "checkpoint hot slots must retain tail overlap and a full chunk".into(),
        ));
    }
    if config.bulk_commit_pages > 32 {
        return Err(StoreError::Check(
            "bulkCommitPages must be at most 32".into(),
        ));
    }
    if config.retention_slots_per_pass <= 0 {
        return Err(StoreError::Check("Invalid retentionSlotsPerPass".into()));
    }
    if config.archive_chunk_slots <= 0 || !(0.0..=1.0).contains(&config.archive_ordering_sample) {
        return Err(StoreError::Check("Invalid archive settings".into()));
    }
    Ok(())
}

/// `SOLANA_RPC_URL`: https, or http on `127.0.0.1`.
pub fn provider_url() -> Result<String, StoreError> {
    let value = std::env::var("SOLANA_RPC_URL")
        .map_err(|_| StoreError::Check("SOLANA_RPC_URL is required".into()))?;
    if value.is_empty() {
        return Err(StoreError::Check("SOLANA_RPC_URL is required".into()));
    }
    let parsed =
        url_parts(&value).ok_or_else(|| StoreError::Check("SOLANA_RPC_URL is not a URL".into()))?;
    if parsed.0 != "https" && parsed.1 != "127.0.0.1" {
        return Err(StoreError::Check("RPC requires HTTPS".into()));
    }
    Ok(value)
}

/// `(scheme, host)` of a URL, without a URL crate.
fn url_parts(value: &str) -> Option<(String, String)> {
    let (scheme, rest) = value.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit('@').next()?.split(':').next()?;
    Some((scheme.to_lowercase(), host.to_lowercase()))
}

/// The config file shipped in the repository, for tests.
#[must_use]
pub fn repository_config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/phoenix.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_the_repository_config_with_defaults() {
        let text = std::fs::read_to_string(repository_config_path()).unwrap();
        let config: Config = serde_json::from_str(&text).unwrap();
        validate(&config).unwrap();
        assert_eq!(config.archive_chunk_slots, 50_000);
        assert_eq!(config.retention_slots_per_pass, 100_000);
        assert_eq!(config.cu_weights["getBlock"], 40);
        assert_eq!(
            url_parts("https://u:p@host.example:443/v2/key?x=1").unwrap(),
            ("https".into(), "host.example".into())
        );
    }
}
