//! Offline Hyperliquid `l2Book` hours (ADR-0021).
//!
//! `solos-data augment hl-l2book` reads local LZ4 JSONL snapshots and writes Parquet under
//! `<out>/train` and `<out>/forward`. It does not download, does not open the collector
//! checkpoint, and does not register files in the augment ledger. The live `hyperliquid_s3`
//! source stays disabled. Raw `.lz4` files are removed only with `--drop-raw-after-verify`,
//! and only after that hour's watermark entry is durable.

mod batch;
mod checksum;
mod decode;
pub mod manifest;
mod md5;
mod publish;

#[cfg(test)]
mod behavior;
#[cfg(test)]
mod manifest_tests;

pub use batch::{Report, Request, Stop, run};
pub use publish::Fault;

use serde_json::Value;

/// Schema name recorded in receipts and the watermark.
pub const SCHEMA: &str = "hl-l2book-v1";

/// Schema name of one hour's raw-deletion record.
pub const DROP_SCHEMA: &str = "hl-l2book-drop-v1";

/// Levels kept on each side. The archive sample is 20; a shorter side is stored as nulls.
pub const BOOK_LEVELS: usize = 20;

/// First eval-only instant: 2026-10-23 00:00:00 UTC. Rows at or after this go under `forward/`.
pub const FORWARD_START_SECS: i64 = 1_792_713_600;

/// [`FORWARD_START_SECS`] in milliseconds, the snapshot `time` unit.
pub const FORWARD_START_MS: i64 = FORWARD_START_SECS * 1000;

/// Default free-space floor: 1.0 TB (10^12 bytes).
pub const DEFAULT_MIN_FREE_BYTES: u64 = 1_000_000_000_000;

/// Default DuckDB threads for one hour's sort and write.
pub const DEFAULT_THREADS: u32 = 4;

/// Default DuckDB memory cap for one hour's sort and write.
pub const DEFAULT_MEMORY: &str = "8GB";

/// Default ratio guard (ADR-0021): an hour whose Parquet bytes exceed its `.lz4` bytes times
/// this keeps its raw files even under `--drop-raw-after-verify`.
pub const DEFAULT_MAX_RATIO: f64 = 1.0;

/// Parquet writer settings for this lane.
///
/// `PARQUET_VERSION 'V1'`: DuckDB 1.4.5 writes `BYTE_STREAM_SPLIT` on the DOUBLE child of a
/// LIST column under V2, and that page-granular split defeats ZSTD on real book sizes. On the
/// real 20251001/0 BTC+ETH hour the array file is 1.13x the `.lz4` under V2 and 0.58x under
/// V1, where `sz` is PLAIN (ZSTD) and `px` keeps its dictionary. `DELTA_BINARY_PACKED` is also
/// V2-only; `time_ms` is small either way. ZSTD level 16, not 9: the 2023-era hour is the
/// hardest (its lz4 compresses best) and 16 is what holds it near 0.70x while 9 sits at
/// 0.75x. These options are not the decoder's V1 pin (ADR-0017); this lane does not share
/// that golden hash.
pub const COPY_OPTIONS: &str = concat!(
    "FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 16, ",
    "ROW_GROUP_SIZE 122880, PARQUET_VERSION 'V1', WRITE_BLOOM_FILTER true"
);

/// How many snapshots are re-read from Parquet and compared to the source line.
pub const SAMPLE_CAP: usize = 16;

/// Column names and DuckDB types, in file order.
///
/// Each side is three fixed arrays of [`BOOK_LEVELS`]. On a 720-snapshot fixture that is smaller
/// than 120 flat columns, and DuckDB reads an element as `bid_px[1]` (ADR-0021).
#[must_use]
pub fn column_specs() -> Vec<(String, &'static str)> {
    let mut cols = vec![
        ("coin".to_owned(), "VARCHAR"),
        ("time_ms".to_owned(), "BIGINT"),
        ("recv_time_ms".to_owned(), "BIGINT"),
    ];
    for side in ["bid", "ask"] {
        cols.push((format!("{side}_px"), "DOUBLE[20]"));
        cols.push((format!("{side}_sz"), "DOUBLE[20]"));
        cols.push((format!("{side}_n"), "BIGINT[20]"));
    }
    cols
}

/// Where a row is written. The directory name is this type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Corpus {
    /// `time_ms` before [`FORWARD_START_MS`].
    Train,
    /// `time_ms` at or after [`FORWARD_START_MS`]. Eval-only.
    Forward,
}

impl Corpus {
    /// The corpus of one snapshot time.
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

/// UTC calendar date of a snapshot time, `YYYY-MM-DD`.
#[must_use]
pub fn utc_date(time_ms: i64) -> Option<String> {
    let secs = time_ms.div_euclid(1000);
    chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.format("%Y-%m-%d").to_string())
}

/// `YYYYMMDD` of a snapshot time, for the source-hour file name's sibling date check.
#[must_use]
pub fn utc_compact(time_ms: i64) -> Option<String> {
    let secs = time_ms.div_euclid(1000);
    chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.format("%Y%m%d").to_string())
}

/// Relative Parquet path for one corpus, row date, and source hour (`YYYYMMDD/H`).
///
/// One file holds every coin of that source hour whose snapshot time falls on that UTC date.
/// A per-coin file is below the 10 MB floor this lane wants (ADR-0021).
#[must_use]
pub fn output_relative(corpus: Corpus, row_date: &str, hour_key: &str) -> String {
    format!(
        "{}/date={row_date}/hour={}.parquet",
        corpus.as_str(),
        hour_key.replace('/', "-")
    )
}

/// Archive object a deleted local file can be fetched from again.
#[must_use]
pub fn s3_url(date: &str, hour: u32, coin: &str) -> String {
    format!("s3://hyperliquid-archive/market_data/{date}/{hour}/l2Book/{coin}.lz4")
}

/// Pinned [`config_hash`]. A schema or writer change has to update this on purpose.
pub const CONFIG_HASH: &str = "2b25aaa389f415966ae6b4192528fb17fbb8ea3976e62bbc4230b09619c0032e";

/// SHA-256 of the writer settings that change row bytes.
///
/// Threads, memory, the disk floor, and `--coins` / `--since` / `--until` are not part of it.
/// A coin filter changes the input identity instead, so a different coin set is rewritten.
#[must_use]
pub fn config_hash() -> String {
    let specs = column_specs();
    let columns: Vec<Vec<&str>> = specs
        .iter()
        .map(|(name, kind)| vec![name.as_str(), *kind])
        .collect();
    let body = serde_json::json!({
        "schema": SCHEMA,
        "columns": columns,
        "sort": ["coin", "time_ms", "_seq"],
        "duplicateRule": "identical resend dropped, distinct kept in archive line order",
        "forwardStartSecs": FORWARD_START_SECS,
        "levels": BOOK_LEVELS,
        "decimals": "double",
        "nulls": "missing level, json null, and empty string",
        "copy": COPY_OPTIONS,
    });
    crate::fsutil::sha256_hex(stable_json(&body).as_bytes())
}

/// Canonical JSON: object keys sorted, compact.
fn stable_json(value: &Value) -> String {
    value.to_string()
}

/// The git commit embedded by `build.rs` at build time.
///
/// Precedence there: a non-empty `SOLOS_DATA_GIT_SHA` in the build environment (verbatim),
/// then `git rev-parse HEAD` of the repository, then `unknown` when git is unavailable.
/// A runtime override is deliberately absent: a receipt names the binary that wrote it, not
/// the shell that happened to start it.
#[must_use]
pub fn git_sha() -> String {
    env!("SOLOS_DATA_GIT_SHA").to_owned()
}

/// A DuckDB size: digits plus `KB`, `MB`, `GB`, or `TB`.
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

/// Comma-separated coin names. Empty tokens are rejected.
///
/// # Errors
///
/// The list is empty or a token is empty.
pub fn parse_coins(
    value: &str,
) -> Result<std::collections::BTreeSet<String>, crate::store::StoreError> {
    let mut coins = std::collections::BTreeSet::new();
    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() || !checksum::valid_coin(token) {
            return Err(crate::store::StoreError::Check(format!(
                "--coins has an empty or invalid name in '{value}'"
            )));
        }
        if !coins.insert(token.to_owned()) {
            return Err(crate::store::StoreError::Check(format!(
                "--coins lists {token} twice"
            )));
        }
    }
    if coins.is_empty() {
        return Err(crate::store::StoreError::Check("--coins is empty".into()));
    }
    Ok(coins)
}

/// `YYYYMMDD` calendar date.
///
/// # Errors
///
/// The text is not an eight-digit calendar date.
pub fn parse_day(value: &str) -> Result<String, crate::store::StoreError> {
    if checksum::calendar_date(value).is_none() {
        return Err(crate::store::StoreError::Check(format!(
            "'{value}' is not a YYYYMMDD date"
        )));
    }
    Ok(value.to_owned())
}

/// `--max-ratio`: a positive, finite ratio.
///
/// # Errors
///
/// The text is not a number, or it is zero, negative, or not finite.
pub fn parse_ratio(value: &str) -> Result<f64, crate::store::StoreError> {
    let parsed: f64 = value.parse().map_err(|_| {
        crate::store::StoreError::Check(format!("--max-ratio '{value}' is not a number"))
    })?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(crate::store::StoreError::Check(format!(
            "--max-ratio {parsed} must be positive and finite"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_boundary_and_output_path() {
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
        assert_eq!(column_specs().len(), 9);
        assert_eq!(
            s3_url("20261022", 0, "BTC"),
            "s3://hyperliquid-archive/market_data/20261022/0/l2Book/BTC.lz4"
        );
    }

    #[test]
    fn config_hash_is_stable_and_memory_limit_is_strict() {
        assert_eq!(
            config_hash(),
            CONFIG_HASH,
            "schema pin moved; update CONFIG_HASH to {}",
            config_hash()
        );
        assert_eq!(parse_memory_limit("8gb").unwrap(), "8GB");
        assert!(parse_memory_limit("8").is_err());
        assert!(parse_memory_limit("8XB").is_err());
        assert!(parse_memory_limit("0GB").is_err());
        let coins = parse_coins("BTC, ETH").unwrap();
        assert!(coins.contains("BTC") && coins.contains("ETH"));
        assert!(parse_coins("").is_err());
        assert!(parse_coins("BTC,BTC").is_err());
        assert_eq!(parse_day("20261022").unwrap(), "20261022");
        assert!(parse_day("20261301").is_err());
        assert!(parse_day("2026102").is_err());
        assert_eq!(parse_ratio("1.0").unwrap(), 1.0);
        assert_eq!(parse_ratio("0.7").unwrap(), 0.7);
        assert!(parse_ratio("0").is_err());
        assert!(parse_ratio("-1").is_err());
        assert!(parse_ratio("inf").is_err());
        assert!(parse_ratio("nan").is_err());
        assert!(parse_ratio("half").is_err());
    }

    #[test]
    fn git_sha_is_embedded_by_the_build() {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(env!("CARGO_MANIFEST_DIR"))
            .args(["rev-parse", "HEAD"])
            .output();
        let Ok(output) = output else {
            eprintln!("skipped: git is unavailable, a source export");
            return;
        };
        if !output.status.success() {
            eprintln!("skipped: no repository at the crate, a source export");
            return;
        }
        let head = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let sha = git_sha();
        assert_eq!(
            sha, head,
            "git_sha() is '{sha}' but this checkout is '{head}': build.rs did not embed the commit"
        );
    }
}
