//! The raw collector's `catalog.json` as the decoder reads it.

use serde::{Deserialize, Deserializer};

/// A registered raw file. `row_count` arrives as a decimal string from DuckDB or a number from
/// hand-written fixtures.
#[derive(Clone, Debug, Deserialize)]
pub struct CatalogFile {
    /// Absolute (legacy) or root-relative path.
    pub path: String,
    /// `transactions`, `signatures`, ...
    pub table_name: String,
    /// Rows in the file.
    #[serde(deserialize_with = "number_or_string")]
    pub row_count: u64,
    /// SHA-256 of the file.
    pub sha256: String,
    /// Publication instant, ISO-8601.
    #[serde(default)]
    pub created_at: String,
    /// Compaction inputs, when the file is a merge.
    #[serde(default)]
    pub parents: Vec<Parent>,
}

/// A compaction input: its hash and row count.
#[derive(Clone, Debug, Deserialize)]
pub struct Parent {
    /// SHA-256 of the input file.
    pub sha256: String,
    /// Rows in the input file.
    #[serde(deserialize_with = "number_or_string")]
    pub row_count: u64,
}

/// `catalog.json` of a raw root.
#[derive(Clone, Debug, Deserialize)]
pub struct Catalog {
    /// When the catalog was written.
    #[serde(default)]
    pub at: String,
    /// Registered files.
    pub files: Vec<CatalogFile>,
}

/// Read and parse `<root>/catalog.json`.
pub fn read_catalog(root: &std::path::Path) -> Result<Catalog, String> {
    let text = std::fs::read_to_string(root.join("catalog.json")).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map_err(|e| format!("invalid raw catalog: {e}"))
}

fn number_or_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Number(u64),
        Text(String),
    }
    match Raw::deserialize(deserializer)? {
        Raw::Number(n) => Ok(n),
        Raw::Text(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_strings_and_numbers() {
        let catalog: Catalog = serde_json::from_str(
            r#"{"at":"x","files":[{"path":"a","table_name":"transactions","row_count":"2500","sha256":"h","created_at":"t","parents":[{"sha256":"p","row_count":10}]},{"path":"b","table_name":"signatures","row_count":3,"sha256":"i"}]}"#,
        )
        .unwrap();
        assert_eq!(catalog.files[0].row_count, 2500);
        assert_eq!(catalog.files[0].parents[0].row_count, 10);
        assert_eq!(catalog.files[1].row_count, 3);
        assert_eq!(catalog.files[1].created_at, "");
    }
}
