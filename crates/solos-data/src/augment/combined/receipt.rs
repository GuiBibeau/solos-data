//! The combined store's unit receipt: what a unit run wrote, over which inputs,
//! with which lockbox and config, written durably only after the outputs are.
//!
//! Fields follow PLAN.md: unit, unit version, rows, outputs with sha256, input
//! watermarks, lockbox sha256 and hours dropped, config sha256, provenance
//! passthrough, started and finished UTC. The provenance object is copied
//! verbatim from the operator's `--provenance` JSON (key order is canonicalized
//! by the JSON writer; values are untouched).

use crate::fsutil::{create_dir_all_durable, write_durable};
use crate::store::StoreError;
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// One sealed output file, path relative to the combined root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OutputFile {
    /// Path relative to the combined root (`mapping/assets.csv`).
    pub path: String,
    /// Data rows in the file (not a header).
    pub rows: u64,
    /// File size in bytes.
    pub bytes: u64,
    /// SHA-256 of the file bytes.
    pub sha256: String,
}

/// Receipt for one combined unit run.
#[derive(Clone, Debug, Serialize)]
pub struct Receipt {
    /// The unit that ran (`mapping`).
    pub unit: String,
    /// Bumped on an output-shape change; 1 is the first version.
    pub unit_version: u32,
    /// Data rows across all outputs.
    pub rows: u64,
    /// Sealed outputs.
    pub outputs: Vec<OutputFile>,
    /// Input watermarks and file lists of everything the unit read.
    pub inputs: Value,
    /// SHA-256 of the lockbox bytes, as checked against `--lockbox-sha256`.
    pub lockbox_sha256: String,
    /// Distinct UTC hours the lockbox drops from every table.
    pub lockbox_hours_dropped: u64,
    /// SHA-256 of the unit's semantic config (columns, thresholds, check days).
    pub config_sha256: String,
    /// The operator's `--provenance` object, verbatim.
    pub provenance: Value,
    /// Run start, UTC.
    pub started_utc: String,
    /// Run finish, UTC (written after every output is durable).
    pub finished_utc: String,
}

/// The receipt path of a unit: `<root>/mapping/receipt-<unit>.json`.
#[must_use]
pub fn receipt_path(root: &Path, unit: &str) -> PathBuf {
    root.join("mapping").join(format!("receipt-{unit}.json"))
}

/// Serialize `receipt` (canonical JSON plus a trailing newline) and write it
/// durably to [`receipt_path`].
///
/// # Errors
///
/// Serialization or the durable write fails.
pub fn write(root: &Path, receipt: &Receipt) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    let path = receipt_path(root, &receipt.unit);
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_path_sits_under_mapping() {
        assert_eq!(
            receipt_path(Path::new("/store"), "mapping"),
            Path::new("/store/mapping/receipt-mapping.json")
        );
    }

    #[test]
    fn a_written_receipt_reads_back_field_for_field() {
        let root = std::env::temp_dir().join(format!("combined-receipt-{}", uuid::Uuid::new_v4()));
        let receipt = Receipt {
            unit: "mapping".into(),
            unit_version: 1,
            rows: 7,
            outputs: vec![OutputFile {
                path: "mapping/assets.csv".into(),
                rows: 7,
                bytes: 123,
                sha256: "abc".into(),
            }],
            inputs: serde_json::json!({"check_days": ["2026-10-15"]}),
            lockbox_sha256: "deadbeef".into(),
            lockbox_hours_dropped: 864,
            config_sha256: "cafe".into(),
            provenance: serde_json::json!({"solos_data_git_sha": "x"}),
            started_utc: "2026-11-01T18:00:00Z".into(),
            finished_utc: "2026-11-01T18:01:00Z".into(),
        };
        write(&root, &receipt).unwrap();
        let text = std::fs::read_to_string(receipt_path(&root, "mapping")).unwrap();
        let back: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back["unit"], "mapping");
        assert_eq!(back["unit_version"], 1);
        assert_eq!(back["rows"], 7);
        assert_eq!(back["outputs"][0]["sha256"], "abc");
        assert_eq!(back["lockbox_hours_dropped"], 864);
        assert_eq!(back["provenance"]["solos_data_git_sha"], "x");
        assert_eq!(back["started_utc"], "2026-11-01T18:00:00Z");
        let _ = std::fs::remove_dir_all(root);
    }
}
