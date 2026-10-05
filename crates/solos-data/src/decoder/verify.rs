//! Offline hash and row-count verification of every registered decoded file.

use crate::fsutil::file_hash;
use crate::jsonout::Obj;
use crate::store::{Store, StoreError, sql_string};

/// Check every file in `files`; the first mismatch is an error.
pub fn verify_decoded(store: &mut Store) -> Result<Obj, StoreError> {
    let files = store.rows("SELECT * FROM files", &[])?;
    for file in &files {
        let path = store.root.join(file.str("path").unwrap_or(""));
        if file_hash(&path)? != file.str("sha256").unwrap_or("") {
            return Err(StoreError::Check("decoded file hash mismatch".into()));
        }
        let count = store.rows(
            &format!(
                "SELECT count(*) AS n FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )?;
        if count.first().and_then(|r| r.int("n")) != file.int("row_count") {
            return Err(StoreError::Check("decoded file count mismatch".into()));
        }
    }
    Ok(Obj::new().with("files", files.len()).with("ok", true))
}
