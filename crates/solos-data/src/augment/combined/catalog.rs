//! The combined root's `catalog.json`: one entry per sealed output file, with
//! row count, sha256, unit and unit version (DESIGN.md 5). The catalog is
//! rewritten durably, and only after the unit's checks have passed.

use crate::fsutil::{create_dir_all_durable, write_durable};
use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Top level of `catalog.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Catalog {
    /// `combined-v1`.
    pub schema: String,
    /// Last rewrite, UTC.
    pub updated_utc: String,
    /// Input watermarks of the latest run per unit (as in the receipts).
    #[serde(default)]
    pub watermarks: BTreeMap<String, Value>,
    /// One entry per sealed file.
    #[serde(default)]
    pub files: Vec<FileEntry>,
}

/// One sealed file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the combined root.
    pub path: String,
    /// Data rows.
    pub rows: u64,
    /// Size in bytes.
    pub bytes: u64,
    /// SHA-256 of the file bytes.
    pub sha256: String,
    /// Unit that wrote it (`mapping`).
    pub unit: String,
    /// Unit version.
    pub unit_version: u32,
}

/// Schema name of the combined catalog.
pub const SCHEMA: &str = "combined-v1";

/// The catalog path: `<root>/catalog.json`.
#[must_use]
pub fn catalog_path(root: &Path) -> PathBuf {
    root.join("catalog.json")
}

/// Load the catalog, or start a fresh one when the file does not exist yet.
///
/// # Errors
///
/// The file exists but is not a [`SCHEMA`] catalog.
pub fn load(root: &Path) -> Result<Catalog, StoreError> {
    let path = catalog_path(root);
    if !path.is_file() {
        return Ok(Catalog {
            schema: SCHEMA.to_owned(),
            updated_utc: String::new(),
            watermarks: BTreeMap::new(),
            files: Vec::new(),
        });
    }
    let text = std::fs::read_to_string(&path)?;
    let catalog: Catalog =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(format!("catalog: {e}")))?;
    if catalog.schema != SCHEMA {
        return Err(StoreError::Check(format!(
            "catalog schema is {}, this command writes {SCHEMA}",
            catalog.schema
        )));
    }
    Ok(catalog)
}

impl Catalog {
    /// Replace (by path) or insert `entry`.
    pub fn upsert_file(&mut self, entry: FileEntry) {
        self.files.retain(|file| file.path != entry.path);
        self.files.push(entry);
        self.files.sort_by(|a, b| a.path.cmp(&b.path));
    }

    /// Record `watermarks` for a unit.
    pub fn set_watermarks(&mut self, unit: &str, watermarks: Value) {
        self.watermarks.insert(unit.to_owned(), watermarks);
    }

    /// Replace (by path) or insert `entry`, and record `watermarks` for its unit.
    pub fn upsert(&mut self, entry: FileEntry, unit: &str, watermarks: Value) {
        self.upsert_file(entry);
        self.set_watermarks(unit, watermarks);
    }

    /// Rewrite the catalog durably with a fresh `updated_utc`.
    ///
    /// # Errors
    ///
    /// Serialization or the durable write fails.
    pub fn save(&mut self, root: &Path) -> Result<(), StoreError> {
        self.updated_utc = super::grid::now_rfc3339();
        let mut body = serde_json::to_vec(self).map_err(|e| StoreError::Check(e.to_string()))?;
        body.push(b'\n');
        let path = catalog_path(root);
        if let Some(parent) = path.parent() {
            create_dir_all_durable(parent)?;
        }
        #[cfg(test)]
        fire_before_save();
        write_durable(&path, &body)?;
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static BEFORE_SAVE: std::cell::Cell<Option<fn()>> = const { std::cell::Cell::new(None) };
}

/// Test hook: run `hook` immediately before the catalog's durable write, so a
/// test can arm an fsync fault at exactly this step (swapping in `fs::write`
/// never fires the fault, which is what the mutant check looks for).
#[cfg(test)]
pub fn on_before_save(hook: fn()) {
    BEFORE_SAVE.with(|slot| slot.set(Some(hook)));
}

/// Clear the hook without firing it.
#[cfg(test)]
pub fn clear_before_save() {
    BEFORE_SAVE.with(|slot| slot.set(None));
}

#[cfg(test)]
fn fire_before_save() {
    if let Some(hook) = BEFORE_SAVE.with(std::cell::Cell::take) {
        hook();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_replaces_by_path_and_saves_durably() {
        let root = std::env::temp_dir().join(format!("combined-catalog-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut catalog = load(&root).unwrap();
        let entry = |sha: &str| FileEntry {
            path: "mapping/assets.csv".into(),
            rows: 5,
            bytes: 100,
            sha256: sha.into(),
            unit: "mapping".into(),
            unit_version: 1,
        };
        catalog.upsert(entry("old"), "mapping", serde_json::json!({"a": 1}));
        catalog.upsert(entry("new"), "mapping", serde_json::json!({"a": 2}));
        assert_eq!(catalog.files.len(), 1);
        assert_eq!(catalog.files[0].sha256, "new");
        catalog.save(&root).unwrap();
        let back = load(&root).unwrap();
        assert_eq!(back.schema, SCHEMA);
        assert_eq!(back.files[0].sha256, "new");
        assert_eq!(back.watermarks["mapping"]["a"], 2);
        let _ = std::fs::remove_dir_all(root);
    }
}
