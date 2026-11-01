//! Offline all-coin Hyperliquid fills (ADR-0020).
//!
//! `solos-data augment hl-fills` reads local `node_fills_by_block/hourly/YYYYMMDD/H.lz4` files
//! and writes Parquet under `<out>/train` and `<out>/forward`. It does not download, does not
//! open the collector checkpoint, and does not register files in the augment ledger. The live
//! `hyperliquid_s3` source stays disabled.

mod batch;
mod checksum;
mod decode;
mod publish;

#[cfg(test)]
mod behavior;

pub use batch::{Report, Request, Stop, run};

use serde_json::Value;

/// Schema name recorded in receipts and the watermark.
pub const SCHEMA: &str = "hl-fills-v1";

/// First eval-only instant: 2026-10-23 00:00:00 UTC. Rows at or after this go under `forward/`.
pub const FORWARD_START_SECS: i64 = 1_792_713_600;

/// [`FORWARD_START_SECS`] in milliseconds, the fill `time` unit.
pub const FORWARD_START_MS: i64 = FORWARD_START_SECS * 1000;

/// Default free-space floor: 1.0 TB (10^12 bytes), matching the lane's GB = 10^9 convention.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 1_000_000_000_000;

/// Default DuckDB threads for one hour's sort and write.
pub const DEFAULT_THREADS: u32 = 4;

/// Default DuckDB memory cap for one hour's sort and write.
pub const DEFAULT_MEMORY: &str = "8GB";

/// Columns of one fill, in file order.
///
/// Decimal amounts are DOUBLE, the same conversion the live `fill_rows` path already applies
/// (`number` to `f64`). A missing field, JSON null, or empty string is NULL. `coin`, `user`,
/// and `time` are required: without them the row cannot be classed or split, so the hour fails.
pub const FILL_COLUMNS: &[(&str, &str)] = &[
    ("time_ms", "BIGINT"),
    ("coin", "VARCHAR"),
    ("coin_class", "VARCHAR"),
    ("user", "VARCHAR"),
    ("px", "DOUBLE"),
    ("sz", "DOUBLE"),
    ("side", "VARCHAR"),
    ("dir", "VARCHAR"),
    ("start_position", "DOUBLE"),
    ("closed_pnl", "DOUBLE"),
    ("fee", "DOUBLE"),
    ("fee_token", "VARCHAR"),
    ("crossed", "BOOLEAN"),
    ("oid", "BIGINT"),
    ("tid", "BIGINT"),
    ("hash", "VARCHAR"),
    ("liquidation", "VARCHAR"),
    ("block_number", "BIGINT"),
    ("block_time", "VARCHAR"),
    ("twap_id", "BIGINT"),
    ("cloid", "VARCHAR"),
    ("builder", "VARCHAR"),
    ("builder_fee", "DOUBLE"),
    ("deployer_fee", "DOUBLE"),
    ("priority_gas", "DOUBLE"),
];

/// Where a row is written. The directory name is this type, not a string a caller concatenates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Corpus {
    /// `time_ms` before [`FORWARD_START_MS`]. Training rows.
    Train,
    /// `time_ms` at or after [`FORWARD_START_MS`]. Eval-only rows.
    Forward,
}

impl Corpus {
    /// The corpus of one fill time.
    #[must_use]
    pub const fn of_time_ms(time_ms: i64) -> Self {
        if time_ms >= FORWARD_START_MS {
            Self::Forward
        } else {
            Self::Train
        }
    }

    /// Directory name under the output root.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Train => "train",
            Self::Forward => "forward",
        }
    }
}

/// Class of a Hyperliquid coin name.
///
/// `BTC` is a native perp, `@N` is spot, `dex:SYM` is a HIP-3 builder perp, `#N` is a HIP-4
/// outcome. `@` and `#` win over `:`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoinClass {
    /// Native perpetual (`BTC`).
    Perp,
    /// Spot pair (`@107`).
    Spot,
    /// HIP-3 builder perpetual (`xyz:SP500`).
    Hip3,
    /// HIP-4 outcome (`#35001`).
    Outcome,
}

impl CoinClass {
    /// Classify `coin`.
    #[must_use]
    pub fn of(coin: &str) -> Self {
        if coin.starts_with('@') {
            Self::Spot
        } else if coin.starts_with('#') {
            Self::Outcome
        } else if coin.contains(':') {
            Self::Hip3
        } else {
            Self::Perp
        }
    }

    /// Column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Perp => "perp",
            Self::Spot => "spot",
            Self::Hip3 => "hip3",
            Self::Outcome => "outcome",
        }
    }
}

/// UTC calendar date of a fill time, `YYYY-MM-DD`.
#[must_use]
pub fn utc_date(time_ms: i64) -> Option<String> {
    let secs = time_ms.div_euclid(1000);
    chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.format("%Y-%m-%d").to_string())
}

/// Relative Parquet path for one corpus, row date, and source hour (`YYYYMMDD/H`).
///
/// The file name is the source hour, so a rerun of that hour replaces these files and no other
/// hour's file shares the path. The directory date is the row's UTC date.
#[must_use]
pub fn output_relative(corpus: Corpus, row_date: &str, hour_key: &str) -> String {
    format!(
        "{}/date={row_date}/hour={}.parquet",
        corpus.as_str(),
        hour_key.replace('/', "-")
    )
}

/// Pinned [`config_hash`]. A schema or writer change has to update this on purpose.
pub const CONFIG_HASH: &str = "1e75e605d88eb8e632f777fe39e3256eb1e65921373128a106e4fadd70cfce62";

/// SHA-256 of the writer settings that change row bytes. Threads, memory, and the disk floor
/// are not part of it: they do not change the table.
#[must_use]
pub fn config_hash() -> String {
    let body = serde_json::json!({
        "schema": SCHEMA,
        "columns": FILL_COLUMNS,
        "sort": ["coin", "time_ms", "tid"],
        "forwardStartSecs": FORWARD_START_SECS,
        "decimals": "double",
        "nulls": "missing, json null, and empty string",
        "copy": crate::decoder::parquet::COPY_OPTIONS,
    });
    crate::fsutil::sha256_hex(stable_json(&body).as_bytes())
}

/// Canonical JSON: object keys sorted, compact. `serde_json::Value` object maps are a `BTreeMap`.
fn stable_json(value: &Value) -> String {
    value.to_string()
}

/// `git rev-parse HEAD`, or `SOLOS_DATA_GIT_SHA` when that is set and non-empty.
///
/// A build that cannot see a git checkout records `unknown`. The hour still verifies; the
/// receipt says so.
#[must_use]
pub fn git_sha() -> String {
    if let Ok(value) = std::env::var("SOLOS_DATA_GIT_SHA") {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_owned();
        }
    }
    let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    else {
        return "unknown".to_owned();
    };
    if !output.status.success() {
        return "unknown".to_owned();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let text = text.trim();
    if text.len() == 40 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
        text.to_owned()
    } else {
        "unknown".to_owned()
    }
}

/// A DuckDB size: digits plus `KB`, `MB`, `GB`, or `TB`. The unit is normalized to upper case.
///
/// # Errors
///
/// The text is empty, has no unit, or the unit is not one of those four.
pub fn parse_memory_limit(value: &str) -> Result<String, crate::store::StoreError> {
    let split = value
        .as_bytes()
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, unit) = value.split_at(split);
    let unit = unit.to_ascii_uppercase();
    if digits.is_empty()
        || digits.len() > 6
        || digits.bytes().all(|byte| byte == b'0')
        || !matches!(unit.as_str(), "KB" | "MB" | "GB" | "TB")
    {
        return Err(crate::store::StoreError::Check(format!(
            "memory limit '{value}' is not a positive size (for example 8GB)"
        )));
    }
    Ok(format!("{digits}{unit}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coin_class_and_forward_boundary() {
        assert_eq!(CoinClass::of("BTC").as_str(), "perp");
        assert_eq!(CoinClass::of("@107").as_str(), "spot");
        assert_eq!(CoinClass::of("xyz:SP500").as_str(), "hip3");
        assert_eq!(CoinClass::of("#35001").as_str(), "outcome");
        assert_eq!(CoinClass::of("#1:2").as_str(), "outcome");
        assert_eq!(CoinClass::of("@1:2").as_str(), "spot");
        assert_eq!(utc_date(FORWARD_START_MS).as_deref(), Some("2026-10-23"));
        assert_eq!(
            utc_date(FORWARD_START_MS - 1).as_deref(),
            Some("2026-10-22")
        );
        assert_eq!(Corpus::of_time_ms(FORWARD_START_MS - 1), Corpus::Train);
        assert_eq!(Corpus::of_time_ms(FORWARD_START_MS), Corpus::Forward);
        assert_eq!(
            output_relative(Corpus::Forward, "2026-10-23", "20261022/23"),
            "forward/date=2026-10-23/hour=20261022-23.parquet"
        );
    }

    #[test]
    fn config_hash_is_stable_and_memory_limit_is_strict() {
        assert_eq!(
            config_hash(),
            CONFIG_HASH,
            "schema pin moved; update CONFIG_HASH"
        );
        assert_eq!(parse_memory_limit("8gb").unwrap(), "8GB");
        assert!(parse_memory_limit("8").is_err());
        assert!(parse_memory_limit("8XB").is_err());
        assert!(parse_memory_limit("0GB").is_err());
        assert!(parse_memory_limit("00MB").is_err());
    }
}
