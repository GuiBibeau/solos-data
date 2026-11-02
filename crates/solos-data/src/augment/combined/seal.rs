//! Shared per-hour seal decisions for every combined unit (ADR-0023).
//!
//! A unit that seals one file per hour decides a rerun's skip through [`check`],
//! never through a hand-rolled comparison. The skip happens only when all of
//! these hold:
//!
//! 1. the hour's receipt parses and carries a `skipKey` equal to the current
//!    run's [`SkipKey`] field for field (unit, unit version, config sha256,
//!    lockbox sha256, inputs sha256),
//! 2. the receipt's stored inputs still hash to its stored `inputsSha256`, so
//!    the receipt itself was not edited after the fact,
//! 3. the sealed file still exists and its streamed sha256 (via the shared
//!    [`crate::fsutil::file_hash`]) equals the receipt's output sha256.
//!
//! Every other outcome, including a missing or garbled receipt, is a rebuild:
//! so an input change such as a renamed symbol in `mapping/assets.csv`, a new
//! ctx day file, a different requested window, a new lockbox, or a tampered or
//! truncated sealed file can never be skipped away.
//!
//! The module also owns the one definition of the receipt's lockbox counts
//! ([`lockbox_drops`]) and the mapping input fact ([`mapping_assets_sha256`]).

use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::Path;

/// Every input fact that can change one hour's output bytes, in the exact
/// shape the caller decides: source file facts (names and shas), the mapping
/// assets.csv sha256 ([`mapping_assets_sha256`]), ctx day-file shas, and the
/// requested range or window when it affects the output.
///
/// The `inputs` value is hashed as canonical JSON: `serde_json` keeps object
/// keys sorted, so the same facts always produce the same
/// [`SkipKey::inputs_sha256`] whatever order the caller built them in.
#[derive(Clone, Debug, PartialEq)]
pub struct SkipKey {
    /// The unit that seals the hour (`hlhours`, `spotdex`, `phoenix_hours`).
    pub unit: String,
    /// Bumped on an output-shape change; a new version never skips old hours.
    pub unit_version: u32,
    /// SHA-256 of the unit's semantic config (columns, thresholds, windows).
    pub config_sha256: String,
    /// SHA-256 of the lockbox bytes, as verified at load time.
    pub lockbox_sha256: String,
    /// The canonical JSON of every input fact that can change the hour's bytes.
    pub inputs: Value,
}

impl SkipKey {
    /// Build a key from the run's facts. `lockbox_sha256` is normalized to
    /// lowercase, the same form the receipt records.
    #[must_use]
    pub fn new(
        unit: &str,
        unit_version: u32,
        config_sha256: &str,
        lockbox_sha256: &str,
        inputs: Value,
    ) -> Self {
        Self {
            unit: unit.to_owned(),
            unit_version,
            config_sha256: config_sha256.to_ascii_lowercase(),
            lockbox_sha256: lockbox_sha256.trim().to_ascii_lowercase(),
            inputs,
        }
    }

    /// SHA-256 of the canonical JSON of [`SkipKey::inputs`].
    #[must_use]
    pub fn inputs_sha256(&self) -> String {
        crate::fsutil::sha256_hex(canonical_json(&self.inputs).as_bytes())
    }

    /// The `skipKey` object a unit embeds in each hour's receipt: the key's
    /// own fields plus the inputs themselves, so a later run can re-hash them.
    #[must_use]
    pub fn to_receipt_value(&self) -> Value {
        json!({
            "unit": self.unit,
            "unitVersion": self.unit_version,
            "configSha256": self.config_sha256,
            "lockboxSha256": self.lockbox_sha256,
            "inputsSha256": self.inputs_sha256(),
            "inputs": self.inputs,
        })
    }
}

/// Canonical JSON text of a `Value`: compact, object keys sorted (`serde_json`
/// keeps them in a `BTreeMap`), so equal facts give equal bytes.
fn canonical_json(value: &Value) -> String {
    serde_json::to_string(value).expect("serializing a serde_json::Value cannot fail")
}

/// The outcome of [`check`] for one already-sealed hour.
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    /// The sealed file is verified against the receipt and the key: reuse it.
    Skip {
        /// Data rows in the sealed file, from the receipt.
        rows: u64,
        /// File size in bytes, from the receipt.
        bytes: u64,
        /// SHA-256 of the file bytes, from the receipt (re-verified on disk).
        sha256: String,
    },
    /// Rebuild the hour; the reason says which check failed.
    Rebuild {
        /// Why the sealed hour cannot be reused.
        reason: String,
    },
}

/// The idempotent skip decision for one hour, per the module doc. Missing or
/// garbled receipts are [`Decision::Rebuild`], never an error; the `Result`
/// exists so a unit's `run` can `?` the call like its other checks.
///
/// # Errors
///
/// Never in practice: every decision failure, including unreadable paths, is
/// reported as [`Decision::Rebuild`] with the reason in its text.
pub fn check(
    hour_receipt_path: &Path,
    sealed_path: &Path,
    key: &SkipKey,
) -> Result<Decision, StoreError> {
    let text = match std::fs::read_to_string(hour_receipt_path) {
        Ok(text) => text,
        Err(error) => {
            return Ok(Decision::Rebuild {
                reason: format!("no readable hour receipt: {error}"),
            });
        }
    };
    let receipt: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(Decision::Rebuild {
                reason: format!("garbled hour receipt: {error}"),
            });
        }
    };
    let stored = match receipt.get("skipKey") {
        Some(value) if value.is_object() => value,
        _ => {
            return Ok(Decision::Rebuild {
                reason: "receipt has no skipKey object".into(),
            });
        }
    };
    if let Some(field) = first_changed_field(stored, key) {
        return Ok(Decision::Rebuild {
            reason: format!("skipKey {field} changed"),
        });
    }
    // The receipt's stored inputs must still hash to its stored inputsSha256:
    // a receipt edited after the fact (inputs swapped, sha kept) rebuilds.
    let stored_inputs = stored.get("inputs").cloned().unwrap_or(Value::Null);
    let stored_sha = stored
        .get("inputsSha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !crate::fsutil::sha256_hex(canonical_json(&stored_inputs).as_bytes())
        .eq_ignore_ascii_case(stored_sha)
    {
        return Ok(Decision::Rebuild {
            reason: "stored inputs do not hash to skipKey.inputsSha256".into(),
        });
    }
    if !sealed_path.is_file() {
        return Ok(Decision::Rebuild {
            reason: "sealed file is missing".into(),
        });
    }
    let actual = match crate::fsutil::file_hash(sealed_path) {
        Ok(sha) => sha,
        Err(error) => {
            return Ok(Decision::Rebuild {
                reason: format!("sealed file unreadable: {error}"),
            });
        }
    };
    let output = receipt
        .get("outputs")
        .and_then(Value::as_array)
        .and_then(|outputs| outputs.first())
        .or_else(|| receipt.get("output"));
    let Some(output) = output else {
        return Ok(Decision::Rebuild {
            reason: "receipt has no sealed output entry".into(),
        });
    };
    let rows = output.get("rows").and_then(Value::as_u64);
    let bytes = output.get("bytes").and_then(Value::as_u64);
    let sha = output.get("sha256").and_then(Value::as_str);
    let (Some(rows), Some(bytes), Some(sha)) = (rows, bytes, sha) else {
        return Ok(Decision::Rebuild {
            reason: "receipt's sealed output entry is garbled".into(),
        });
    };
    if !actual.eq_ignore_ascii_case(sha) {
        return Ok(Decision::Rebuild {
            reason: "sealed file no longer matches the receipt sha256".into(),
        });
    }
    Ok(Decision::Skip {
        rows,
        bytes,
        sha256: sha.to_owned(),
    })
}

/// The first skipKey field where the receipt disagrees with the current key,
/// or `None` when they match field for field.
fn first_changed_field(stored: &Value, key: &SkipKey) -> Option<&'static str> {
    if stored.get("unit").and_then(Value::as_str) != Some(key.unit.as_str()) {
        return Some("unit");
    }
    if stored.get("unitVersion").and_then(Value::as_u64) != Some(u64::from(key.unit_version)) {
        return Some("unitVersion");
    }
    if !hex_field_is(stored, "configSha256", &key.config_sha256) {
        return Some("configSha256");
    }
    if !hex_field_is(stored, "lockboxSha256", &key.lockbox_sha256) {
        return Some("lockboxSha256");
    }
    if !hex_field_is(stored, "inputsSha256", &key.inputs_sha256()) {
        return Some("inputsSha256");
    }
    None
}

/// `true` when `stored[field]` is the hex string `ours`, case-insensitively.
fn hex_field_is(stored: &Value, field: &str, ours: &str) -> bool {
    stored
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|text| text.eq_ignore_ascii_case(ours))
}

/// The receipt's two lockbox counts, in the one definition ADR-0023
/// ("Lockbox") and `receipt::Receipt` document:
///
/// - `dropped`: distinct UTC hours **the inputs covered** that fall inside the
///   lockbox exclusion set. An hour inside the set that no input covered is
///   not dropped: there was nothing there to remove. It is never derived from
///   the requested range alone.
/// - `excluded`: the exclusion set's own size (864 for v2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockboxDrops {
    /// Covered hours the lockbox actually removed.
    pub dropped: u64,
    /// Distinct hours in the exclusion set itself.
    pub excluded: u64,
}

/// Both receipt counts at once: [`lockbox_hours_dropped`] for the covered
/// hours, and the set's own size for `excluded`.
pub fn lockbox_drops(
    covered_hours: impl IntoIterator<Item = i64>,
    excluded: &BTreeSet<i64>,
) -> LockboxDrops {
    LockboxDrops {
        dropped: lockbox_hours_dropped(covered_hours, excluded),
        excluded: excluded.len() as u64,
    }
}

/// Distinct covered hours inside the exclusion set: the hours the inputs
/// covered that the lockbox removes (see [`LockboxDrops`] for the exact
/// definition, and `receipt::Receipt::lockbox_hours_dropped` for where the
/// number lands). Duplicates count once; covered hours outside the set and
/// set hours nothing covered count never.
pub fn lockbox_hours_dropped(
    covered_hours: impl IntoIterator<Item = i64>,
    excluded: &BTreeSet<i64>,
) -> u64 {
    let mut dropped = BTreeSet::new();
    for hour in covered_hours {
        if excluded.contains(&hour) {
            dropped.insert(hour);
        }
    }
    dropped.len() as u64
}

/// The mapping input fact for a unit whose bytes depend on U1's universe:
/// the SHA-256 of `<root>/mapping/assets.csv`, or JSON `null` when the file
/// does not exist yet. Drop the value straight into a [`SkipKey::inputs`]
/// object, so renaming a symbol (SOL to SOLX, say) changes the inputs sha256
/// and rebuilds instead of skipping.
///
/// # Errors
///
/// Reading or hashing an existing `assets.csv` fails.
pub fn mapping_assets_sha256(root: &Path) -> Result<Value, StoreError> {
    let path = root.join("mapping").join("assets.csv");
    if !path.is_file() {
        return Ok(Value::Null);
    }
    Ok(json!(crate::fsutil::file_hash(&path)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway directory for one test.
    struct Tmp(PathBuf);

    impl Tmp {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "combined-seal-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn dir(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    use std::path::PathBuf;

    /// The key every test starts from.
    fn key() -> SkipKey {
        SkipKey::new(
            "hlhours",
            1,
            "cafe",
            "deadbeef",
            serde_json::json!({
                "assets_csv_sha256": "aa",
                "ctx": {"2026-10-15": "bb"},
                "range": [1_792_000_000, 1_792_100_000],
            }),
        )
    }

    /// Seal `body` at `<dir>/<name>.parquet` with a matching receipt at
    /// `<dir>/<name>.json` embedding `key`.
    fn seal(dir: &Path, name: &str, key: &SkipKey, body: &[u8]) -> (PathBuf, PathBuf) {
        let sealed = dir.join(format!("{name}.parquet"));
        std::fs::write(&sealed, body).unwrap();
        let receipt = serde_json::json!({
            "schema": "hour-v1",
            "hour": name,
            "skipKey": key.to_receipt_value(),
            "output": {
                "path": format!("tbl/{name}.parquet"),
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            },
        });
        let receipt_path = dir.join(format!("{name}.json"));
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        (receipt_path, sealed)
    }

    #[test]
    fn the_same_inputs_skip_with_the_receipts_numbers() {
        let tmp = Tmp::new("skip");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "20261015-13", &key(), body);
        let decision = check(&receipt_path, &sealed_path, &key()).unwrap();
        assert_eq!(
            decision,
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: crate::fsutil::sha256_hex(body),
            }
        );
    }

    #[test]
    fn a_changed_key_component_rebuilds() {
        let tmp = Tmp::new("keys");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        // Each component alone, changed: unit version, config, lockbox, and
        // three kinds of input facts (the assets sha, a ctx day sha, the
        // requested range).
        let changed: Vec<SkipKey> = vec![
            SkipKey::new("hlhours", 2, "cafe", "deadbeef", key().inputs),
            SkipKey::new("hlhours", 1, "f00d", "deadbeef", key().inputs),
            SkipKey::new("hlhours", 1, "cafe", "beefcafe", key().inputs),
            SkipKey::new(
                "hlhours",
                1,
                "cafe",
                "deadbeef",
                serde_json::json!({
                    "assets_csv_sha256": "cc",
                    "ctx": {"2026-10-15": "bb"},
                    "range": [1_792_000_000, 1_792_100_000],
                }),
            ),
            SkipKey::new(
                "hlhours",
                1,
                "cafe",
                "deadbeef",
                serde_json::json!({
                    "assets_csv_sha256": "aa",
                    "ctx": {"2026-10-16": "bb"},
                    "range": [1_792_000_000, 1_792_100_000],
                }),
            ),
            SkipKey::new(
                "hlhours",
                1,
                "cafe",
                "deadbeef",
                serde_json::json!({
                    "assets_csv_sha256": "aa",
                    "ctx": {"2026-10-15": "bb"},
                    "range": [1_792_000_000, 1_792_100_001],
                }),
            ),
        ];
        for key in &changed {
            let decision = check(&receipt_path, &sealed_path, key).unwrap();
            let Decision::Rebuild { reason } = decision else {
                panic!("{key:?} must rebuild");
            };
            assert!(reason.starts_with("skipKey "), "{reason}");
        }
        // A different unit never skips another unit's hour.
        let decision = check(
            &receipt_path,
            &sealed_path,
            &SkipKey::new("spotdex", 1, "cafe", "deadbeef", key().inputs),
        )
        .unwrap();
        assert!(matches!(decision, Decision::Rebuild { .. }));
    }

    #[test]
    fn a_tampered_or_missing_sealed_file_rebuilds() {
        let tmp = Tmp::new("tamper");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        std::fs::write(&sealed_path, b"parquet-bytez").unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a rewritten sealed file must rebuild");
        };
        assert!(reason.contains("sha256"), "{reason}");
        std::fs::remove_file(&sealed_path).unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a missing sealed file must rebuild");
        };
        assert!(reason.contains("missing"), "{reason}");
    }

    #[test]
    fn an_edited_receipt_input_rebuilds() {
        let tmp = Tmp::new("edit");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        // Swap a stored input fact but keep the stored inputsSha256: the
        // receipt no longer describes what the inputs hash says.
        let mut receipt: Value =
            serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
        receipt["skipKey"]["inputs"]["assets_csv_sha256"] = serde_json::json!("zz");
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("an edited receipt must rebuild");
        };
        assert!(reason.contains("do not hash"), "{reason}");
    }

    #[test]
    fn a_garbled_or_missing_receipt_rebuilds() {
        let tmp = Tmp::new("garble");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        std::fs::write(&receipt_path, "{not json").unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a garbled receipt must rebuild");
        };
        assert!(reason.contains("garbled"), "{reason}");
        std::fs::remove_file(&receipt_path).unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a missing receipt must rebuild");
        };
        assert!(reason.contains("hour receipt"), "{reason}");
        // A receipt without a skipKey object is not a sealed hour either.
        std::fs::write(&receipt_path, b"{\"schema\":\"hour-v1\"}").unwrap();
        let Decision::Rebuild { reason } = check(&receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a receipt without skipKey must rebuild");
        };
        assert!(reason.contains("no skipKey"), "{reason}");
    }

    #[test]
    fn the_outputs_array_shape_is_accepted_too() {
        let tmp = Tmp::new("outputs");
        let body = b"parquet-bytes";
        let sealed = tmp.dir().join("h.parquet");
        std::fs::write(&sealed, body).unwrap();
        let receipt = serde_json::json!({
            "unit": "hlhours",
            "skipKey": key().to_receipt_value(),
            "outputs": [{
                "path": "hlhours/h.parquet",
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            }],
        });
        let receipt_path = tmp.dir().join("h.json");
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_eq!(
            check(&receipt_path, &sealed, &key()).unwrap(),
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: crate::fsutil::sha256_hex(body),
            }
        );
    }

    #[test]
    fn canonical_keys_behave() {
        // Key order in the inputs cannot change the inputs sha256.
        let left = SkipKey::new(
            "u",
            1,
            "c",
            "l",
            serde_json::json!({"a": "1", "b": {"y": 2, "x": 1}}),
        );
        let right = SkipKey::new(
            "u",
            1,
            "c",
            "l",
            serde_json::json!({"b": {"x": 1, "y": 2}, "a": "1"}),
        );
        assert_eq!(left, right);
        assert_eq!(left.inputs_sha256(), right.inputs_sha256());
        // The receipt value carries every field, with the inputs embedded.
        let value = left.to_receipt_value();
        assert_eq!(value["unit"], "u");
        assert_eq!(value["unitVersion"], 1);
        assert_eq!(value["configSha256"], "c");
        assert_eq!(value["lockboxSha256"], "l");
        assert_eq!(value["inputsSha256"], left.inputs_sha256());
        assert_eq!(value["inputs"], left.inputs);
    }

    #[test]
    fn dropped_counts_covered_hours_inside_the_set_only() {
        let excluded = BTreeSet::from([10, 11, 12, 13]);
        // 12 and 13 are covered and inside; 10 and 11 sit in the set but
        // nothing covered them; 9, 14, 40 are covered but free; 12 twice.
        let dropped = lockbox_hours_dropped([9, 12, 12, 13, 14, 40], &excluded);
        assert_eq!(dropped, 2);
        // Nothing covered inside the set: nothing dropped, whatever the set holds.
        assert_eq!(lockbox_hours_dropped([9, 14], &excluded), 0);
        // Both counts at once: excluded is the set's own size.
        let drops = lockbox_drops([9, 12, 13, 14], &excluded);
        assert_eq!(
            drops,
            LockboxDrops {
                dropped: 2,
                excluded: 4
            }
        );
    }

    #[test]
    fn the_v2_fixture_gives_864_and_864_when_fully_covered() {
        let tmp = Tmp::new("v2drops");
        let fix = super::super::testfix::v2_lockbox(tmp.dir());
        assert_eq!(fix.hours.len(), 864);
        let covered: Vec<i64> = fix.hours.iter().copied().chain([1, 2]).collect();
        let drops = lockbox_drops(covered, &fix.hours);
        assert_eq!(drops.dropped, 864);
        assert_eq!(drops.excluded, 864);
        // The named hours: inside ones are in the set, the neighbors are not.
        assert!(fix.hours.contains(&fix.first_hour()));
        assert!(fix.hours.contains(&fix.mid_hour()));
        assert!(fix.hours.contains(&fix.last_hour()));
        assert!(!fix.hours.contains(&fix.hour_before()));
        assert!(!fix.hours.contains(&fix.hour_after()));
    }

    #[test]
    fn mapping_assets_sha_is_the_file_or_null() {
        let tmp = Tmp::new("assets");
        assert_eq!(mapping_assets_sha256(tmp.dir()).unwrap(), Value::Null);
        std::fs::create_dir_all(tmp.dir().join("mapping")).unwrap();
        std::fs::write(tmp.dir().join("mapping/assets.csv"), b"asset_id\nSOL\n").unwrap();
        let sha = mapping_assets_sha256(tmp.dir()).unwrap();
        assert_eq!(sha, json!(crate::fsutil::sha256_hex(b"asset_id\nSOL\n")));
        // Renaming a symbol changes the fact, so the key changes with it.
        std::fs::write(tmp.dir().join("mapping/assets.csv"), b"asset_id\nSOLX\n").unwrap();
        assert_ne!(mapping_assets_sha256(tmp.dir()).unwrap(), sha);
    }
}
