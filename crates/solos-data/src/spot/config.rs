//! `config/spot-history.json` (ADR-0014): the spot backfill's window, programs, stream size,
//! manifest sample and RPC budget.

use super::dex::{Dex, Programs};
use crate::store::StoreError;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The whole file.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotConfig {
    /// The augment root (`SOLOS_DATA_AUGMENT_DIR` overrides it).
    pub data_dir: PathBuf,
    /// The oldest instant collected (RFC 3339), resolved once to a slot and recorded.
    pub floor_time: String,
    /// The newest slot collected: the last slot of the newest epoch the archive served when the
    /// run was set up.
    pub top: i64,
    /// Programs by table name; every program when empty.
    #[serde(default)]
    pub programs: Vec<String>,
    /// Slots one archive stream covers.
    pub chunk_slots: u64,
    /// Streams in flight; a batch is `chunkSlots × parallel` slots.
    pub parallel: usize,
    /// Slots of each batch whose signatures are checked against each program's RPC manifest.
    pub manifest_slots: u64,
    /// Helius credits a second the manifests and block lists may spend.
    pub credits_per_second: f64,
    /// Requests in flight.
    pub concurrency: u32,
    /// Credits per request by method.
    #[serde(default)]
    pub credit_costs: BTreeMap<String, f64>,
    /// DuckDB memory for writing a batch's files.
    #[serde(default = "writer_memory")]
    pub writer_memory: String,
    /// Minutes without progress (no block streamed, no RPC answer, no batch written) after
    /// which the process exits with [`crate::stall::STALL_EXIT`] for its unit to restart it; 0
    /// disables the watchdog.
    #[serde(default = "stall_minutes")]
    pub stall_minutes: u64,
}

fn writer_memory() -> String {
    "16GB".into()
}

fn stall_minutes() -> u64 {
    20
}

impl SpotConfig {
    /// Parse a document.
    ///
    /// # Errors
    ///
    /// Invalid JSON, an unknown program or values out of range.
    pub fn parse(text: &str) -> Result<SpotConfig, StoreError> {
        let config: SpotConfig = serde_json::from_str(text)
            .map_err(|e| StoreError::Check(format!("spot config: {e}")))?;
        if config.top <= 0
            || config.chunk_slots == 0
            || config.parallel == 0
            || config.manifest_slots == 0
            || config.concurrency == 0
            || !(0.1..=10_000.0).contains(&config.credits_per_second)
        {
            return Err(StoreError::Check(
                "top, chunkSlots, parallel, manifestSlots, concurrency and creditsPerSecond must be positive".into(),
            ));
        }
        crate::venues::history::config::floor_seconds(&config.floor_time)?;
        config.programs()?;
        Ok(config)
    }

    /// The programs the run reads.
    ///
    /// # Errors
    ///
    /// A name that is not a spot program.
    pub fn programs(&self) -> Result<Programs, StoreError> {
        if self.programs.is_empty() {
            return Ok(Programs::all());
        }
        let mut out = Vec::new();
        for name in &self.programs {
            out.push(
                Dex::from_name(name)
                    .ok_or_else(|| StoreError::Check(format!("unknown spot program {name}")))?,
            );
        }
        out.sort_unstable();
        out.dedup();
        Ok(Programs::new(out))
    }

    /// Slots of one batch.
    #[must_use]
    pub fn batch_slots(&self) -> u64 {
        self.chunk_slots.saturating_mul(self.parallel as u64)
    }
}

/// Load `path` (default `config/spot-history.json`); `SOLOS_DATA_AUGMENT_DIR` overrides
/// `dataDir`.
///
/// # Errors
///
/// Unreadable or invalid file.
pub fn load(path: Option<&str>) -> Result<SpotConfig, StoreError> {
    let path = Path::new(path.unwrap_or("config/spot-history.json"));
    let mut config = SpotConfig::parse(&std::fs::read_to_string(path)?)?;
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

    #[test]
    fn the_committed_config_reads_every_program() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/spot-history.json"
        );
        let config = load(Some(path)).unwrap();
        assert_eq!(config.programs().unwrap().list(), super::super::dex::ALL);
        assert_eq!(
            crate::venues::history::config::floor_seconds(&config.floor_time).unwrap(),
            1_759_276_800
        );
        let bad = r#"{"dataDir": "x", "floorTime": "2025-10-01T00:00:00Z", "top": 5,
            "programs": ["nope"], "chunkSlots": 1, "parallel": 1, "manifestSlots": 1,
            "creditsPerSecond": 1, "concurrency": 1}"#;
        assert!(SpotConfig::parse(bad).is_err());
    }
}
