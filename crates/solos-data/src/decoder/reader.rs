//! Read-only SQL over the registered decoded Parquet, with the latest revision of every row. A
//! port of `decode/reader.ts`.

use super::parquet::require_connection_duckdb;
use super::schema::{TABLES, definition, key_column};
use super::source::source_path;
use crate::jsonout::Obj;
use crate::lease::with_read_lease;
use crate::store::{StoreError, for_each_row, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

/// Slots per epoch; the publisher partitions every table into `epoch=<slot / 432000>` directories.
pub const EPOCH_SLOTS: i64 = 432_000;

/// Run one `SELECT` or `WITH` statement against a decoded root.
pub fn query_decoded(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    query_decoded_slots(root, sql, None)
}

/// The same, reading only the epoch partitions that cover `slots` (inclusive) when given. The
/// latest-revision views join every table to the deduplicated `decoded_transactions`, so an
/// unscoped query over hundreds of millions of transactions needs memory in proportion; every
/// revision of a transaction shares its slot, so a slot range is exact.
pub fn query_decoded_slots(
    root: &Path,
    sql: &str,
    slots: Option<(i64, i64)>,
) -> Result<Obj, StoreError> {
    with_decoded_session(root, slots, false, |session| session.rows(sql))
}

/// Several statements against one snapshot: one lease, one catalog read, one connection.
///
/// # Errors
///
/// A statement is not a single `SELECT`/`WITH`, or the catalog or a file cannot be read.
pub fn query_decoded_many(root: &Path, statements: &[&str]) -> Result<Vec<Obj>, StoreError> {
    with_decoded_session(root, None, true, |session| {
        statements
            .iter()
            .map(|sql| session.rows(sql))
            .collect::<Result<Vec<_>, _>>()
    })
}

/// Open one snapshot of the decoded root and run `visit` with it, under a read lease.
///
/// The session holds one connection with the reader's deduplicated views and the digest's raw
/// views attached. `visit` can stream rows through [`DecodedSession::stream`]; the lease is held
/// until `visit` returns.
pub(crate) fn with_decoded_session<T>(
    root: &Path,
    slots: Option<(i64, i64)>,
    check_version: bool,
    visit: impl FnOnce(&DecodedSession) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let epochs = match slots {
        Some((from, to)) if from <= to => {
            Some(from.div_euclid(EPOCH_SLOTS)..=to.div_euclid(EPOCH_SLOTS))
        }
        Some(_) => return Err(StoreError::Check("slot range must be from<=to".into())),
        None => None,
    };
    with_read_lease(root, || {
        let session = DecodedSession::open(root, epochs.as_ref(), check_version)?;
        visit(&session)
    })
}

/// Removes the spill directory when the session ends. Declared after `conn` in
/// [`DecodedSession`] so field drop order closes the connection (releasing any spilled files)
/// before the directory is removed.
struct SpillDir(PathBuf);

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One connection over a decoded root with the reader views attached.
pub(crate) struct DecodedSession {
    conn: duckdb::Connection,
    catalog_at: Value,
    epochs: Option<RangeInclusive<i64>>,
    decoder_version: Value,
    acceptance: Value,
    /// Private spill directory; removed by [`SpillDir::drop`] after `conn` is dropped.
    _temp_dir: SpillDir,
}

impl DecodedSession {
    fn open(
        root: &Path,
        epochs: Option<&RangeInclusive<i64>>,
        check_version: bool,
    ) -> Result<Self, StoreError> {
        let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
        // `SOLOS_DATA_QUERY_TEMP_DIR` overrides the spill root; the default is the system
        // temp directory, which can be RAM-backed on some hosts.
        let spill_root = std::env::var("SOLOS_DATA_QUERY_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        Self::open_with(root, epochs, check_version, &memory, &spill_root)
    }

    /// [`Self::open`] with explicit settings, so tests can prove `memory_limit` and
    /// `temp_directory` are applied without racing on process-wide environment variables.
    fn open_with(
        root: &Path,
        epochs: Option<&RangeInclusive<i64>>,
        check_version: bool,
        memory: &str,
        spill_root: &Path,
    ) -> Result<Self, StoreError> {
        let catalog: Value =
            serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
                .map_err(|error| StoreError::Check(error.to_string()))?;
        let conn = memory_connection("4", memory)?;
        // The digest streams its window/sort results; without a spill directory DuckDB keeps
        // that buffer in memory and peak RSS grows with the day. A private directory per
        // session keeps concurrent sessions from sharing a spill root.
        let temp_dir = spill_root.join(format!("solos-decode-tmp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir)?;
        conn.execute_batch(&format!(
            "SET temp_directory = {}",
            sql_string(&temp_dir.to_string_lossy())
        ))?;
        if check_version {
            require_connection_duckdb(&conn)?;
        }
        let files = catalog
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|file| {
                // A file outside the requested epochs is left out; one whose path carries no epoch
                // (there are none today) stays in so a scoped query never silently loses rows.
                epochs.is_none_or(|range| {
                    file.get("path")
                        .and_then(Value::as_str)
                        .and_then(path_epoch)
                        .is_none_or(|epoch| range.contains(&epoch))
                })
            })
            .collect::<Vec<_>>();
        attach_views(&conn, root, &files)?;
        Ok(DecodedSession {
            conn,
            catalog_at: catalog.get("at").cloned().unwrap_or(Value::Null),
            epochs: epochs.cloned(),
            decoder_version: catalog
                .get("decoderVersion")
                .cloned()
                .unwrap_or(Value::Null),
            acceptance: catalog.get("acceptance").cloned().unwrap_or(Value::Null),
            _temp_dir: SpillDir(temp_dir),
        })
    }

    /// The connection with the reader views and registrations attached.
    pub(crate) fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }

    /// Run one statement and return it as the reader's ordered-JSON result.
    pub(crate) fn rows(&self, sql: &str) -> Result<Obj, StoreError> {
        accept_select(sql)?;
        let rows = query_rows(&self.conn, sql, &[])?;
        Ok(self.wrap(rows))
    }

    /// Run one statement and pass each row to `visit` as it is produced, without collecting rows.
    pub(crate) fn stream(
        &self,
        sql: &str,
        visit: impl FnMut(&Obj) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        accept_select(sql)?;
        for_each_row(&self.conn, sql, &[], visit)
    }

    fn wrap(&self, rows: Vec<Obj>) -> Obj {
        Obj::new()
            .with("catalogAt", self.catalog_at.clone())
            .with(
                "epochs",
                self.epochs.as_ref().map_or(Value::Null, |range| {
                    Value::Array(vec![Value::from(*range.start()), Value::from(*range.end())])
                }),
            )
            .with("decoderVersion", self.decoder_version.clone())
            .with("acceptance", self.acceptance.clone())
            .with_rows("rows", rows)
    }
}

/// The epoch a published path belongs to, when its directory says so.
fn path_epoch(path: &str) -> Option<i64> {
    let start = path.find("epoch=")? + "epoch=".len();
    let digits: String = path[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn accept_select(sql: &str) -> Result<(), StoreError> {
    let head = sql.trim_start().to_lowercase();
    let is_select = ["select", "with"].iter().any(|kw| {
        head.starts_with(kw)
            && !head[kw.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
    });
    if !is_select || sql.contains(';') {
        return Err(StoreError::Check(
            "query accepts one SELECT or WITH statement".into(),
        ));
    }
    Ok(())
}

/// Attach the deduplicated reader views (`{table}`) and the digest's raw views
/// (`{table}_raw`, which keep `batch_id` and every revision) to `conn`.
fn attach_views(conn: &duckdb::Connection, root: &Path, files: &[Value]) -> Result<(), StoreError> {
    for table in TABLES {
        let registered: Vec<&Value> = files
            .iter()
            .filter(|file| file.get("table_name").and_then(Value::as_str) == Some(table))
            .collect();
        if registered.is_empty() {
            conn.execute_batch(&format!("CREATE TABLE {table}({})", definition(table)))?;
            conn.execute_batch(&format!(
                "CREATE VIEW {table}_raw AS SELECT *, NULL::BIGINT AS batch_id FROM {table}"
            ))?;
            conn.execute_batch(&format!(
                "CREATE TABLE {table}_registrations(path VARCHAR, batch_id BIGINT)"
            ))?;
            continue;
        }
        let mut paths = Vec::new();
        let mut registrations = Vec::new();
        for file in registered {
            let path = source_path(root, file.get("path").and_then(Value::as_str).unwrap_or(""))?
                .to_string_lossy()
                .into_owned();
            let batch = file
                .get("batch_id")
                .map(|value| {
                    value
                        .as_str()
                        .and_then(|text| text.parse::<i64>().ok())
                        .or_else(|| value.as_i64())
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            registrations.push(Value::Object(
                [
                    ("path".to_owned(), Value::String(path.clone())),
                    ("batch_id".to_owned(), Value::from(batch)),
                ]
                .into_iter()
                .collect(),
            ));
            paths.push(sql_string(&path));
        }
        conn.execute(
            &format!("CREATE TABLE {table}_registrations AS SELECT value->>'path' AS path, (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?::JSON)"),
            [Value::Array(registrations).to_string()],
        )?;
        let base = format!(
            "SELECT p.* EXCLUDE(filename), r.batch_id FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN {table}_registrations r ON r.path=p.filename",
            paths.join(",")
        );
        let key = key_column(table);
        let relation = if table == "decoded_transactions" {
            format!("({base}) p")
        } else {
            format!(
                "({base}) p JOIN decoded_transactions d ON d.signature=p.signature AND d.source_hash=p.source_hash"
            )
        };
        conn.execute_batch(&format!("CREATE VIEW {table} AS SELECT p.* EXCLUDE(batch_id) FROM {relation} QUALIFY row_number() OVER(PARTITION BY p.{key} ORDER BY p.batch_id DESC)=1"))?;
        conn.execute_batch(&format!(
            "CREATE VIEW {table}_raw AS SELECT p.* FROM {relation}"
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_applies_memory_limit_and_temp_directory() {
        let root =
            std::env::temp_dir().join(format!("solos-decoded-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("catalog.json"), "{}").unwrap();
        let spill = std::env::temp_dir().join(format!("solos-spill-root-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spill).unwrap();

        let session = DecodedSession::open_with(&root, None, false, "64MB", &spill).unwrap();
        let memory: String = session
            .conn()
            .query_row("SELECT current_setting('memory_limit')", [], |row| {
                row.get(0)
            })
            .unwrap();
        let temp: String = session
            .conn()
            .query_row("SELECT current_setting('temp_directory')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(
            std::path::Path::new(&temp).starts_with(&spill),
            "temp_directory {temp} is not under {spill:?}"
        );
        assert!(
            memory.to_ascii_lowercase().ends_with("mib"),
            "memory_limit was not applied: {memory}"
        );
        drop(session);
        std::fs::remove_dir_all(&spill).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }
}
