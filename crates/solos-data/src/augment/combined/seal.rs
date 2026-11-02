//! Shared per-hour seal decisions for every combined unit (ADR-0023).
//!
//! A unit that seals one file per hour decides a rerun's skip through [`check`],
//! never through a hand-rolled comparison. The skip happens only when all of
//! these hold:
//!
//! 1. the hour's receipt parses and carries a `skipKey` equal to the current
//!    run's [`SkipKey`] field for field (unit, unit version, config sha256,
//!    lockbox sha256, inputs sha256); every field must be present, a receipt
//!    missing one rebuilds,
//! 2. the receipt's stored inputs still hash to its stored `inputsSha256`, so
//!    the receipt itself was not edited after the fact,
//! 3. the receipt names exactly one sealed output (a singular `output`, or an
//!    `outputs` array holding exactly one entry; both keys present rebuilds
//!    whatever their types, and so do any other count and a non-array
//!    `outputs`): a multi-output receipt never skips, because a changed extra
//!    entry could hide behind the first,
//! 4. the receipt output's `path` is the file `check` was pointed at: the
//!    path stored in the receipt, taken relative to the store root, equals
//!    `sealed_path` relative to that same root, so a receipt for one file can
//!    never skip a different file with identical bytes,
//! 5. the sealed file still exists and its streamed sha256 (via the shared
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
use std::path::{Component, Path};

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
    /// Build a key from the run's facts. `config_sha256` and `lockbox_sha256`
    /// are trimmed and normalized to lowercase, the same form the receipt
    /// records, so a caller whose sha picked up padding or casing still
    /// matches its own receipt instead of rebuilding the hour forever.
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
            config_sha256: config_sha256.trim().to_ascii_lowercase(),
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

/// The idempotent skip decision for one hour, per the module doc. `root` is
/// the combined store root the hour receipt and the sealed file live under:
/// the receipt's output `path` (stored relative to that root, the shape
/// `receipt::OutputFile` and every hour receipt write) is compared against
/// `sealed_path` relative to the same root, so a receipt is bound to its own
/// file, not just to its bytes. `root` and `sealed_path` must be the same
/// form - both absolute, or both relative under the same prefix - and the
/// compare is lexical (`strip_prefix` plus cleaned components, no symlink
/// resolution): a form mismatch or an escaping path fails the compare and
/// the hour rebuilds forever, which is the safe direction. Missing or
/// garbled receipts are [`Decision::Rebuild`], never an error; the `Result`
/// exists so a unit's `run` can `?` the call like its other checks.
///
/// # Errors
///
/// Never in practice: every decision failure, including unreadable paths, is
/// reported as [`Decision::Rebuild`] with the reason in its text.
pub fn check(
    root: &Path,
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
    // Exactly one sealed output, and it must be the file this decision is
    // about: a receipt naming several outputs can hide a changed extra entry
    // behind outputs[0], a receipt carrying both keys is ambiguous whatever
    // their types (a non-array `outputs` is not "no array"), and a receipt
    // for another file with identical bytes must not skip this one.
    let singular = receipt.get("output");
    let plural = receipt.get("outputs");
    let output = match (singular, plural) {
        (Some(_), Some(_)) => {
            return Ok(Decision::Rebuild {
                reason: "receipt names both output and outputs".into(),
            });
        }
        (Some(output), None) => Some(output),
        (None, Some(outputs)) => {
            let Some(entries) = outputs.as_array() else {
                return Ok(Decision::Rebuild {
                    reason: "receipt's outputs is not an array".into(),
                });
            };
            if entries.len() == 1 {
                entries.first()
            } else {
                return Ok(Decision::Rebuild {
                    reason: format!("receipt has {} outputs, not exactly one", entries.len()),
                });
            }
        }
        (None, None) => None,
    };
    let Some(output) = output else {
        return Ok(Decision::Rebuild {
            reason: "receipt has no sealed output entry".into(),
        });
    };
    let Some(receipt_path) = output.get("path").and_then(Value::as_str) else {
        return Ok(Decision::Rebuild {
            reason: "receipt's sealed output entry has no path".into(),
        });
    };
    let sealed_relative = sealed_path.strip_prefix(root).ok().and_then(clean_relative);
    let stored_relative = clean_relative(Path::new(receipt_path));
    let Some((sealed_relative, stored_relative)) = sealed_relative.zip(stored_relative) else {
        return Ok(Decision::Rebuild {
            reason: "sealed file is not a clean path under the store root".into(),
        });
    };
    if sealed_relative != stored_relative {
        return Ok(Decision::Rebuild {
            reason: format!("receipt names {stored_relative}, asked to verify {sealed_relative}"),
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

/// `true` when `stored[field]` is present and is the hex string `ours`:
/// trimmed on the receipt side (the key's side is trimmed in
/// [`SkipKey::new`]) and compared case-insensitively, so an edited receipt
/// with other casing or stray whitespace still matches, while a missing or
/// non-string field never does.
fn hex_field_is(stored: &Value, field: &str, ours: &str) -> bool {
    stored
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|text| text.trim().eq_ignore_ascii_case(ours))
}

/// The canonical `/`-joined text of a relative path: plain component names
/// only (a leading `./`, any `..`, and root or prefix components refuse), so
/// the receipt's stored path and the verified file's path compare component
/// for component, never by a string suffix. Separator redundancy carries no
/// components - a trailing slash, repeated `/`, and a `.` between names
/// normalize to the same text - so those forms compare equal.
fn clean_relative(path: &Path) -> Option<String> {
    let mut joined = String::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            return None;
        };
        if !joined.is_empty() {
            joined.push('/');
        }
        joined.push_str(&name.to_string_lossy());
    }
    Some(joined)
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

/// The covered hours inside the exclusion set, as the set itself: the hours
/// the inputs covered that the lockbox removes (see [`LockboxDrops`] for the
/// exact definition, and `receipt::Receipt::lockbox_hours_dropped` for where
/// the number lands). Every venue's dropped-hour set runs through here, so a
/// store-wide number can be built as the union of the venues' sets without a
/// second definition of "inside the lockbox".
pub fn lockbox_dropped_hours(
    covered_hours: impl IntoIterator<Item = i64>,
    excluded: &BTreeSet<i64>,
) -> BTreeSet<i64> {
    covered_hours
        .into_iter()
        .filter(|hour| excluded.contains(hour))
        .collect()
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
    lockbox_dropped_hours(covered_hours, excluded).len() as u64
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
    /// `<dir>/<name>.json` embedding `key`. The receipt's output path is the
    /// sealed file's path relative to `dir` (the store root in these tests).
    fn seal(dir: &Path, name: &str, key: &SkipKey, body: &[u8]) -> (PathBuf, PathBuf) {
        let sealed = dir.join(format!("{name}.parquet"));
        std::fs::write(&sealed, body).unwrap();
        let receipt = serde_json::json!({
            "schema": "hour-v1",
            "hour": name,
            "skipKey": key.to_receipt_value(),
            "output": {
                "path": format!("{name}.parquet"),
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            },
        });
        let receipt_path = dir.join(format!("{name}.json"));
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        (receipt_path, sealed)
    }

    /// Write a raw receipt body over `<dir>/<name>.json` (for tampered or
    /// hand-built receipts), with `body` sealed at `<dir>/<name>.parquet`.
    fn seal_raw(dir: &Path, name: &str, receipt: Value, body: &[u8]) -> (PathBuf, PathBuf) {
        let sealed = dir.join(format!("{name}.parquet"));
        std::fs::write(&sealed, body).unwrap();
        let receipt_path = dir.join(format!("{name}.json"));
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        (receipt_path, sealed)
    }

    #[test]
    fn the_same_inputs_skip_with_the_receipts_numbers() {
        let tmp = Tmp::new("skip");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "20261015-13", &key(), body);
        let decision = check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap();
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
            let decision = check(tmp.dir(), &receipt_path, &sealed_path, key).unwrap();
            let Decision::Rebuild { reason } = decision else {
                panic!("{key:?} must rebuild");
            };
            assert!(reason.starts_with("skipKey "), "{reason}");
        }
        // A different unit never skips another unit's hour.
        let decision = check(
            tmp.dir(),
            &receipt_path,
            &sealed_path,
            &SkipKey::new("spotdex", 1, "cafe", "deadbeef", key().inputs),
        )
        .unwrap();
        assert!(matches!(decision, Decision::Rebuild { .. }));
    }

    /// A receipt whose skipKey is missing a field never skips, whatever the
    /// other fields say (R10): every key field must be present to trust the
    /// receipt.
    #[test]
    fn a_receipt_missing_a_skip_key_field_rebuilds() {
        let tmp = Tmp::new("missingfield");
        let body = b"parquet-bytes";
        let mut receipt = serde_json::json!({
            "schema": "hour-v1",
            "skipKey": key().to_receipt_value(),
            "output": {
                "path": "h.parquet",
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            },
        });
        for field in [
            "unit",
            "unitVersion",
            "configSha256",
            "lockboxSha256",
            "inputsSha256",
        ] {
            receipt["skipKey"].as_object_mut().unwrap().remove(field);
            let (receipt_path, sealed_path) = seal_raw(tmp.dir(), "h", receipt.clone(), body);
            let Decision::Rebuild { reason } =
                check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
            else {
                panic!("missing skipKey {field} must rebuild");
            };
            assert!(reason.contains(field), "{field}: {reason}");
            receipt["skipKey"][field] = key().to_receipt_value()[field].clone();
        }
        // A non-string hex field is as good as a missing one.
        receipt["skipKey"]["lockboxSha256"] = serde_json::json!(12);
        let (receipt_path, sealed_path) = seal_raw(tmp.dir(), "h", receipt, body);
        assert!(matches!(
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap(),
            Decision::Rebuild { .. }
        ));
    }

    /// Lockbox sha normalization (R18): the receipt's sha is matched trimmed
    /// and case-insensitively on both sides, while a different sha still
    /// rebuilds.
    #[test]
    fn lockbox_shas_match_trimmed_and_case_insensitively_only() {
        let tmp = Tmp::new("shas");
        let body = b"parquet-bytes";
        // The receipt on disk records the sha raw: uppercased, with
        // whitespace (a hand-written or legacy receipt). The run's key is
        // built from an equally sloppy string. Both sides are normalized -
        // trimmed and lowercased - before they compare, so this skips.
        let mut receipt = serde_json::json!({
            "schema": "hour-v1",
            "skipKey": key().to_receipt_value(),
            "output": {
                "path": "h.parquet",
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            },
        });
        receipt["skipKey"]["lockboxSha256"] = serde_json::json!("  DEADBEEF\t");
        let (receipt_path, sealed_path) = seal_raw(tmp.dir(), "h", receipt, body);
        let run_key = SkipKey::new("hlhours", 1, "cafe", " DeadBeef ", key().inputs);
        assert_eq!(
            check(tmp.dir(), &receipt_path, &sealed_path, &run_key).unwrap(),
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: crate::fsutil::sha256_hex(body),
            }
        );
        // A different sha still rebuilds, whatever its casing or padding.
        let other = SkipKey::new("hlhours", 1, "cafe", " beefcafe ", key().inputs);
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &other).unwrap()
        else {
            panic!("a different lockbox sha must rebuild");
        };
        assert!(reason.contains("lockboxSha256"), "{reason}");
    }

    /// The receipt is bound to the file it names (fix 2): the receipt's
    /// output path, relative to the store root, must be the file `check` was
    /// asked to verify. Same bytes elsewhere rebuild; the matching path
    /// skips.
    #[test]
    fn the_receipt_only_skips_the_file_it_names() {
        let tmp = Tmp::new("pathbind");
        // The store root is a directory of its own, so a file one level above
        // it (a path escaping the root) is still inside this test's own temp
        // dir - nothing is written to the temp root or any shared path.
        let root = tmp.dir().join("store");
        std::fs::create_dir_all(&root).unwrap();
        let body = b"parquet-bytes";
        let receipt = serde_json::json!({
            "schema": "hour-v1",
            "skipKey": key().to_receipt_value(),
            "output": {
                "path": "tables/spot/h.parquet",
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            },
        });
        let receipt_path = root.join("h.json");
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        // Identical bytes at a different path under the same root: rebuild.
        std::fs::create_dir_all(root.join("tables/other")).unwrap();
        let elsewhere = root.join("tables/other/h.parquet");
        std::fs::write(&elsewhere, body).unwrap();
        let Decision::Rebuild { reason } = check(&root, &receipt_path, &elsewhere, &key()).unwrap()
        else {
            panic!("a receipt for tables/spot/h.parquet must not skip another file");
        };
        assert!(reason.contains("tables/spot/h.parquet"), "{reason}");
        assert!(reason.contains("tables/other/h.parquet"), "{reason}");
        // The file the receipt names: skip.
        std::fs::create_dir_all(root.join("tables/spot")).unwrap();
        let named = root.join("tables/spot/h.parquet");
        std::fs::write(&named, body).unwrap();
        assert_eq!(
            check(&root, &receipt_path, &named, &key()).unwrap(),
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: crate::fsutil::sha256_hex(body),
            }
        );
        // A receipt path escaping the root (or a sealed path outside it)
        // never matches anything. The file exists at exactly the given path
        // with the receipted bytes, so only the path binding can refuse it.
        let outside = root.join("../outside.parquet");
        std::fs::write(&outside, body).unwrap();
        assert!(matches!(
            check(&root, &receipt_path, &outside, &key()).unwrap(),
            Decision::Rebuild { .. }
        ));
        // A `.` between names is not refused either - `components()` just
        // normalizes it away, so the receipt now names a different path
        // (tables/h.parquet) than the sealed file, and that mismatch rebuilds.
        let mut receipt = receipt.clone();
        receipt["output"]["path"] = serde_json::json!("tables/./h.parquet");
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert!(matches!(
            check(&root, &receipt_path, &named, &key()).unwrap(),
            Decision::Rebuild { .. }
        ));
    }

    /// The documented path normalization: redundant separators in the
    /// receipt's stored path (a trailing slash, repeated `/`) carry no extra
    /// components and compare equal, while a path that is only a string
    /// suffix of the sealed path - a different component list - rebuilds.
    /// The compare is component-wise, never a string-suffix compare, and by
    /// the same token never a string-prefix compare: a receipt naming a
    /// strict directory prefix of the sealed file (its directory, or the
    /// root of the tree) rebuilds even though the sealed bytes, rows and sha
    /// match the receipt exactly.
    #[test]
    fn the_path_compare_is_component_wise_over_cleaned_components() {
        let tmp = Tmp::new("pathnorm");
        let body = b"parquet-bytes";
        let sealed = tmp.dir().join("tables/spot/h.parquet");
        std::fs::create_dir_all(sealed.parent().unwrap()).unwrap();
        std::fs::write(&sealed, body).unwrap();
        let check_with = |path: &str| {
            let receipt = serde_json::json!({
                "schema": "hour-v1",
                "skipKey": key().to_receipt_value(),
                "output": {
                    "path": path,
                    "rows": 3,
                    "bytes": body.len(),
                    "sha256": crate::fsutil::sha256_hex(body),
                },
            });
            let receipt_path = tmp.dir().join("h.json");
            std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap()
        };
        // Redundant forms normalize exactly as documented: skip.
        for sloppy in ["tables/spot/h.parquet/", "tables//spot/h.parquet"] {
            assert_eq!(
                check_with(sloppy),
                Decision::Skip {
                    rows: 3,
                    bytes: body.len() as u64,
                    sha256: crate::fsutil::sha256_hex(body),
                },
                "{sloppy} must normalize to the same path"
            );
        }
        // "spot/h.parquet" is a string suffix of the sealed path but a
        // different component path: rebuild, never skip.
        let Decision::Rebuild { reason } = check_with("spot/h.parquet") else {
            panic!("the path compare must not be a string-suffix compare");
        };
        assert!(reason.contains("receipt names spot/h.parquet"), "{reason}");
        // The prefix direction is pinned the same way: "tables/spot" (the
        // file's directory) and "tables" (the tree root) are clean component
        // paths and string prefixes of the sealed path, but neither is the
        // file the receipt sealed - both rebuild, whatever the bytes say.
        for prefix in ["tables/spot", "tables"] {
            let Decision::Rebuild { reason } = check_with(prefix) else {
                panic!("a receipt naming the prefix {prefix} must not skip the file below it");
            };
            assert!(
                reason.contains(&format!("receipt names {prefix}")),
                "{prefix}: {reason}"
            );
        }
    }

    /// `clean_relative` refuses anything that is not a plain relative name: a
    /// leading `./`, any `..`, root and prefix components. A receipt for a
    /// file outside the root never skips even when the receipted bytes sit
    /// exactly at the given path (the files exist here, so only the path
    /// check can refuse each case).
    #[test]
    fn a_receipt_path_outside_or_unclean_rebuilds() {
        let tmp = Tmp::new("pathclean");
        let root = tmp.dir().join("store");
        std::fs::create_dir_all(&root).unwrap();
        let body = b"parquet-bytes";
        std::fs::write(tmp.dir().join("outside.parquet"), body).unwrap();
        std::fs::write(root.join("x.parquet"), body).unwrap();
        std::fs::write(tmp.dir().join("x.parquet"), body).unwrap();
        let check_with = |stored: &str, sealed: &Path, name: &str| {
            let receipt = serde_json::json!({
                "schema": "hour-v1",
                "skipKey": key().to_receipt_value(),
                "output": {
                    "path": stored,
                    "rows": 3,
                    "bytes": body.len(),
                    "sha256": crate::fsutil::sha256_hex(body),
                },
            });
            let receipt_path = tmp.dir().join(format!("{name}.json"));
            std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
            check(&root, &receipt_path, sealed, &key()).unwrap()
        };
        // The receipt names "outside.parquet"; the sealed path steps out of
        // the root, whose bytes match the receipt at exactly that path.
        let Decision::Rebuild { reason } =
            check_with("outside.parquet", &root.join("../outside.parquet"), "esc")
        else {
            panic!("a sealed path outside the root must never skip");
        };
        assert!(reason.contains("not a clean path"), "{reason}");
        // A receipt naming "../x.parquet" with the file (and its receipted
        // bytes) at exactly <root>/../x.parquet: the .. must refuse.
        let Decision::Rebuild { reason } =
            check_with("../x.parquet", &root.join("../x.parquet"), "dotdot")
        else {
            panic!("a receipt path through .. must never skip");
        };
        assert!(reason.contains("not a clean path"), "{reason}");
        // A leading ./ and an absolute path refuse the same way (both files
        // exist at the sealed path, so only cleanliness can rebuild).
        for (stored, name) in [("./x.parquet", "dot"), ("/abs/x.parquet", "abs")] {
            let Decision::Rebuild { reason } = check_with(stored, &root.join("x.parquet"), name)
            else {
                panic!("{stored} must refuse");
            };
            assert!(reason.contains("not a clean path"), "{stored}: {reason}");
        }
    }

    /// The config sha is normalized like the lockbox sha (trimmed and
    /// lowercased in [`SkipKey::new`]): a rerun whose config sha picked up
    /// whitespace or casing still matches its own receipt, while a genuinely
    /// different sha still rebuilds. Without the trim, a padded key would
    /// never equal its own receipt and the hour would rebuild forever.
    #[test]
    fn config_shas_match_trimmed_and_case_insensitively_only() {
        let tmp = Tmp::new("configsha");
        let body = b"parquet-bytes";
        // Sealed by a run whose key carried the canonical sha ("cafe").
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        // The rerun's config sha came back padded and uppercase: same sha.
        let padded = SkipKey::new("hlhours", 1, "  CAFE\t", "deadbeef", key().inputs);
        assert_eq!(padded.config_sha256, "cafe");
        assert_eq!(
            check(tmp.dir(), &receipt_path, &sealed_path, &padded).unwrap(),
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: crate::fsutil::sha256_hex(body),
            }
        );
        // A different sha still rebuilds, whatever its casing or padding.
        let other = SkipKey::new("hlhours", 1, " f00d ", "deadbeef", key().inputs);
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &other).unwrap()
        else {
            panic!("a different config sha must rebuild");
        };
        assert!(reason.contains("configSha256"), "{reason}");
    }

    /// Exactly one output or rebuild (fix 1): a two-output receipt must
    /// rebuild whether the extra entry was tampered, deleted, or is intact,
    /// and so must a receipt with no output at all. The reason names the
    /// count.
    #[test]
    fn a_receipt_without_exactly_one_output_rebuilds() {
        let tmp = Tmp::new("oneoutput");
        let body = b"parquet-bytes";
        let sha = crate::fsutil::sha256_hex(body);
        let entry = |name: &str, sha: &str| {
            serde_json::json!({
                "path": format!("{name}.parquet"),
                "rows": 3,
                "bytes": body.len(),
                "sha256": sha,
            })
        };
        let receipt_with = |outputs: Value| {
            serde_json::json!({
                "schema": "hour-v1",
                "skipKey": key().to_receipt_value(),
                "outputs": outputs,
            })
        };
        let cases: Vec<(Value, &str)> = vec![
            (
                receipt_with(serde_json::json!([
                    entry("a", &sha),
                    entry("b", "tampered")
                ])),
                "2 outputs",
            ),
            (
                receipt_with(serde_json::json!([entry("a", &sha), entry("b", &sha)])),
                "2 outputs",
            ),
            (receipt_with(serde_json::json!([entry("a", &sha)])), "skip"),
            (receipt_with(serde_json::json!([])), "0 outputs"),
        ];
        let sealed = tmp.dir().join("a.parquet");
        std::fs::write(&sealed, body).unwrap();
        for (receipt, expected) in &cases {
            let (receipt_path, _) = seal_raw(tmp.dir(), "a", receipt.clone(), body);
            let decision = check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap();
            if *expected == "skip" {
                assert_eq!(
                    decision,
                    Decision::Skip {
                        rows: 3,
                        bytes: body.len() as u64,
                        sha256: sha.clone(),
                    },
                );
            } else {
                let Decision::Rebuild { reason } = decision else {
                    panic!("{expected} must rebuild");
                };
                assert!(reason.contains(expected), "{expected}: {reason}");
            }
        }
        // Deleting outputs[1] from a two-output receipt also rebuilds: the
        // receipt that sealed the hour is no longer the receipt on disk.
        let mut two: Value = receipt_with(serde_json::json!([entry("a", &sha), entry("b", &sha)]));
        two["outputs"].as_array_mut().unwrap().pop();
        let (receipt_path, _) = seal_raw(tmp.dir(), "a", two, body);
        // A one-entry receipt now, but the sealed file b.parquet is gone;
        // this exact case skips only when everything matches, so seal b too.
        std::fs::write(tmp.dir().join("b.parquet"), body).unwrap();
        assert_eq!(
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap(),
            Decision::Skip {
                rows: 3,
                bytes: body.len() as u64,
                sha256: sha.clone(),
            }
        );
        // Both keys at once is ambiguous, never a skip - whatever the types:
        // the guard fires on key presence, so a non-array `outputs` beside a
        // singular `output` cannot sneak through as "no array".
        let both = serde_json::json!({
            "schema": "hour-v1",
            "skipKey": key().to_receipt_value(),
            "output": entry("a", &sha),
            "outputs": [entry("a", &sha)],
        });
        let (receipt_path, _) = seal_raw(tmp.dir(), "a", both, body);
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap()
        else {
            panic!("output and outputs together must rebuild");
        };
        assert!(reason.contains("output and outputs"), "{reason}");
        let both_junk = serde_json::json!({
            "schema": "hour-v1",
            "skipKey": key().to_receipt_value(),
            "output": entry("a", &sha),
            "outputs": "junk",
        });
        let (receipt_path, _) = seal_raw(tmp.dir(), "a", both_junk, body);
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap()
        else {
            panic!("output and a non-array outputs must rebuild");
        };
        assert!(reason.contains("output and outputs"), "{reason}");
        // A non-array outputs alone is not an exactly-one array either.
        let (receipt_path, _) = seal_raw(
            tmp.dir(),
            "a",
            receipt_with(serde_json::json!("junk")),
            body,
        );
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap()
        else {
            panic!("a non-array outputs must rebuild");
        };
        assert!(reason.contains("not an array"), "{reason}");
    }

    #[test]
    fn a_tampered_or_missing_sealed_file_rebuilds() {
        let tmp = Tmp::new("tamper");
        let body = b"parquet-bytes";
        let (receipt_path, sealed_path) = seal(tmp.dir(), "h", &key(), body);
        std::fs::write(&sealed_path, b"parquet-bytez").unwrap();
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a rewritten sealed file must rebuild");
        };
        assert!(reason.contains("sha256"), "{reason}");
        std::fs::remove_file(&sealed_path).unwrap();
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
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
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
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
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a garbled receipt must rebuild");
        };
        assert!(reason.contains("garbled"), "{reason}");
        std::fs::remove_file(&receipt_path).unwrap();
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
        else {
            panic!("a missing receipt must rebuild");
        };
        assert!(reason.contains("hour receipt"), "{reason}");
        // A receipt without a skipKey object is not a sealed hour either.
        std::fs::write(&receipt_path, b"{\"schema\":\"hour-v1\"}").unwrap();
        let Decision::Rebuild { reason } =
            check(tmp.dir(), &receipt_path, &sealed_path, &key()).unwrap()
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
                "path": "h.parquet",
                "rows": 3,
                "bytes": body.len(),
                "sha256": crate::fsutil::sha256_hex(body),
            }],
        });
        let receipt_path = tmp.dir().join("h.json");
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_eq!(
            check(tmp.dir(), &receipt_path, &sealed, &key()).unwrap(),
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
        // The set form carries the hours, so a store-wide number can be the
        // union of the venues' sets.
        let hours = lockbox_dropped_hours([9, 12, 12, 13, 14, 40], &excluded);
        assert_eq!(hours, BTreeSet::from([12, 13]));
        let merged: BTreeSet<i64> = hours
            .union(&lockbox_dropped_hours([10, 14], &excluded))
            .copied()
            .collect();
        assert_eq!(merged, BTreeSet::from([10, 12, 13]));
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
