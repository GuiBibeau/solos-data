//! The DuckDB checkpoint store: one connection per process, settings from the same environment
//! variables as the TypeScript `Store`, rows rendered the way `getRowObjectsJson()` renders them
//! (64-bit integers as decimal strings), and per-statement timings for `status.json`.

use crate::jsonout::Obj;
use duckdb::types::{TimeUnit, Value as DbValue};
use duckdb::{Config, Connection, ToSql, params_from_iter};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// An owned SQL parameter that crosses the writer-thread channel.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// SQL NULL.
    Null,
    /// 64-bit integer.
    Int(i64),
    /// Double.
    Float(f64),
    /// Text.
    Text(String),
    /// Boolean.
    Bool(bool),
}

impl ToSql for Param {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        use duckdb::types::{ToSqlOutput, Value as V};
        Ok(match self {
            Param::Null => ToSqlOutput::Owned(V::Null),
            Param::Int(n) => ToSqlOutput::Owned(V::BigInt(*n)),
            Param::Float(f) => ToSqlOutput::Owned(V::Double(*f)),
            Param::Text(t) => ToSqlOutput::Owned(V::Text(t.clone())),
            Param::Bool(b) => ToSqlOutput::Owned(V::Boolean(*b)),
        })
    }
}

impl From<i64> for Param {
    fn from(value: i64) -> Self {
        Param::Int(value)
    }
}
impl From<i32> for Param {
    fn from(value: i32) -> Self {
        Param::Int(i64::from(value))
    }
}
impl From<u64> for Param {
    fn from(value: u64) -> Self {
        Param::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}
impl From<f64> for Param {
    fn from(value: f64) -> Self {
        Param::Float(value)
    }
}
impl From<bool> for Param {
    fn from(value: bool) -> Self {
        Param::Bool(value)
    }
}
impl From<String> for Param {
    fn from(value: String) -> Self {
        Param::Text(value)
    }
}
impl From<&str> for Param {
    fn from(value: &str) -> Self {
        Param::Text(value.to_owned())
    }
}

/// Statement timing aggregate, keyed by `VERB:table` as in the TypeScript store.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Timing {
    /// Number of statements.
    pub calls: u64,
    /// Total seconds.
    pub seconds: f64,
    /// Worst single statement, seconds.
    #[serde(rename = "maxSeconds")]
    pub max_seconds: f64,
}

/// Errors from the store, wrapping DuckDB's.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// DuckDB reported an error.
    #[error("{0}")]
    Db(#[from] duckdb::Error),
    /// A file system operation failed.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// A domain check failed; the text is the TypeScript error message.
    #[error("{0}")]
    Check(String),
    /// Another process holds the advisory lock beside this checkpoint.
    ///
    /// Taken in [`Store::open`] and [`Store::open_read_only`] before DuckDB is opened, so the
    /// second owner fails here and never opens the database. ADR-0016.
    #[error("checkpoint already owned ({path})")]
    AlreadyOwned {
        /// `checkpoint.duckdb.lock`, beside `checkpoint.duckdb`.
        path: PathBuf,
    },
}

/// Exclusive `flock` on `checkpoint.duckdb.lock`. Drop releases it. The kernel releases it if the
/// process is killed. [`Store`] drops the connection first, then this lock.
///
/// Pass a borrow into work that must run while the checkpoint is closed (`repack_checkpoint`).
/// Taking a second lock on the same file conflicts, including from this process.
pub struct DbLock {
    file: nix::fcntl::Flock<File>,
    /// The checkpoint root this lock was taken for. `repack_checkpoint` refuses a lock that was
    /// taken for a different root, so `repack_checkpoint(root_a, &lock_b)` cannot run.
    root: PathBuf,
}

impl DbLock {
    /// The caller holds this lock for the duration of the borrow.
    pub fn prove_held(&self) {
        let _ = &self.file;
    }

    /// Refuse a checkpoint operation on `root` unless this lock was taken for `root`.
    pub fn assert_root(&self, root: &Path) -> Result<(), StoreError> {
        if self.root != root {
            return Err(StoreError::Check(format!(
                "lock root mismatch: lock holds {}, requested {}",
                self.root.display(),
                root.display()
            )));
        }
        Ok(())
    }
}

fn lock_path(root: &Path) -> PathBuf {
    root.join("checkpoint.duckdb.lock")
}

fn open_lock_file(path: &Path) -> Result<File, StoreError> {
    let mut created = OpenOptions::new();
    created.read(true).write(true).create_new(true);
    created.mode(0o600);
    match created.open(path) {
        Ok(file) => {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            Ok(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(OpenOptions::new().read(true).write(true).open(path)?)
        }
        Err(error) => Err(error.into()),
    }
}

fn same_inode(file: &File, path: &Path) -> bool {
    let Ok(fd_meta) = file.metadata() else {
        return false;
    };
    let Ok(path_meta) = std::fs::metadata(path) else {
        return false;
    };
    fd_meta.dev() == path_meta.dev() && fd_meta.ino() == path_meta.ino()
}

/// Non-blocking exclusive lock. A newly created file is mode `0600`; an existing file is not
/// chmodded. After the lock is held, the descriptor's inode must still be the path's inode.
pub fn acquire_lock(root: &Path) -> Result<DbLock, StoreError> {
    let path = lock_path(root);
    for _ in 0..3 {
        let file = open_lock_file(&path)?;
        match nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
            Ok(file) => {
                if same_inode(&file, &path) {
                    return Ok(DbLock {
                        file,
                        root: root.to_path_buf(),
                    });
                }
            }
            Err((_, errno))
                if errno == nix::errno::Errno::EWOULDBLOCK
                    || errno == nix::errno::Errno::EAGAIN =>
            {
                return Err(StoreError::AlreadyOwned { path });
            }
            Err((_, errno)) => return Err(StoreError::Io(std::io::Error::from(errno))),
        }
    }
    Err(StoreError::Check(
        "checkpoint lock file was replaced".into(),
    ))
}

/// `Err(StoreError::Check(..))` with a formatted message.
#[macro_export]
macro_rules! check_fail {
    ($($arg:tt)*) => { return Err($crate::store::StoreError::Check(format!($($arg)*)).into()) };
}

/// One checkpoint database.
///
/// `conn` is declared before `lock` so it is dropped first (struct fields drop in declaration
/// order). The advisory lock stays held until the database connection is closed.
pub struct Store {
    conn: Connection,
    /// Advisory lock. [`Store::close`] releases it after the connection; other paths drop it with
    /// the struct, after `conn`.
    lock: DbLock,
    /// Set when reopening the checkpoint after [`Store::while_closed`] fails. Later calls fail
    /// instead of using the in-memory placeholder.
    unusable: Option<String>,
    /// Data root that holds `checkpoint.duckdb`.
    pub root: PathBuf,
    ddl: &'static str,
    timings: Vec<(String, Timing)>,
}

/// Connection settings: `SOLOS_DATA_DB_MEMORY` (4GB), `SOLOS_DATA_DB_THREADS` (4),
/// `SOLOS_DATA_DB_CHECKPOINT` (256MB). Extension auto-install stays off: the bundled build has
/// JSON and Parquet linked in, and nothing else may be fetched over the network.
fn config(read_only: bool) -> Result<Config, duckdb::Error> {
    let memory = std::env::var("SOLOS_DATA_DB_MEMORY").unwrap_or_else(|_| "4GB".into());
    #[cfg(feature = "failpoints")]
    let memory = if crate::failpoints::db_config_fails() {
        "invalid failpoint".into()
    } else {
        memory
    };
    let threads = std::env::var("SOLOS_DATA_DB_THREADS").unwrap_or_else(|_| "4".into());
    let checkpoint = std::env::var("SOLOS_DATA_DB_CHECKPOINT").unwrap_or_else(|_| "256MB".into());
    let mut config = Config::default()
        .with("memory_limit", memory)?
        .with("threads", threads)?
        .with("checkpoint_threshold", checkpoint)?
        .with("autoinstall_known_extensions", "false")?
        .with("autoload_known_extensions", "false")?;
    if read_only {
        config = config.access_mode(duckdb::AccessMode::ReadOnly)?;
    }
    Ok(config)
}

/// An independent in-memory database for readers and offline tools.
pub fn memory_connection(threads: &str, memory_limit: &str) -> Result<Connection, duckdb::Error> {
    let config = Config::default()
        .with("threads", threads)?
        .with("memory_limit", memory_limit)?
        .with("autoinstall_known_extensions", "false")?
        .with("autoload_known_extensions", "false")?;
    Connection::open_in_memory_with_flags(config)
}

impl Store {
    /// Open (creating the directory) and apply the schema.
    ///
    /// Takes the advisory lock beside the database before DuckDB opens it. A second process gets
    /// [`StoreError::AlreadyOwned`] and does not open the file.
    pub fn open(root: &Path, ddl: &'static str) -> Result<Self, StoreError> {
        std::fs::create_dir_all(root)?;
        let lock = acquire_lock(root)?;
        let conn = Connection::open_with_flags(root.join("checkpoint.duckdb"), config(false)?)?;
        let mut store = Store {
            conn,
            lock,
            unusable: None,
            root: root.to_path_buf(),
            ddl,
            timings: Vec::new(),
        };
        store.exec_batch(ddl)?;
        Ok(store)
    }

    /// Open read-only without applying the schema; for preflight and read-only tools.
    ///
    /// Takes the same exclusive lock as [`Store::open`]. One process owns the checkpoint, including
    /// a read-only opener.
    pub fn open_read_only(root: &Path) -> Result<Self, StoreError> {
        let lock = acquire_lock(root)?;
        let conn = Connection::open_with_flags(root.join("checkpoint.duckdb"), config(true)?)?;
        Ok(Store {
            conn,
            lock,
            unusable: None,
            root: root.to_path_buf(),
            ddl: "",
            timings: Vec::new(),
        })
    }

    fn ensure_usable(&self) -> Result<(), StoreError> {
        if let Some(reason) = &self.unusable {
            return Err(StoreError::Check(format!("checkpoint unusable: {reason}")));
        }
        Ok(())
    }

    /// The raw connection, for `COPY`, appenders and attached databases.
    #[must_use]
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Run several statements.
    pub fn exec_batch(&mut self, sql: &str) -> Result<(), StoreError> {
        self.ensure_usable()?;
        let started = std::time::Instant::now();
        let result = self.conn.execute_batch(sql);
        self.record(sql, started);
        Ok(result?)
    }

    /// Run one statement with parameters; returns the changed row count.
    pub fn exec(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<usize, StoreError> {
        self.ensure_usable()?;
        let started = std::time::Instant::now();
        let result = self.conn.execute(sql, params_from_iter(params.iter()));
        self.record(sql, started);
        Ok(result?)
    }

    /// Query rows as ordered JSON objects.
    pub fn rows(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Obj>, StoreError> {
        self.ensure_usable()?;
        query_rows(&self.conn, sql, params)
    }

    /// `kv` lookup, parsed JSON.
    pub fn get(&self, name: &str) -> Result<Option<Value>, StoreError> {
        let rows = self.rows(
            "SELECT value::VARCHAR AS value FROM kv WHERE name=?",
            &[&name],
        )?;
        match rows.first().and_then(|row| row.str("value")) {
            Some(text) => Ok(Some(
                serde_json::from_str(text).map_err(|e| StoreError::Check(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    /// `kv` upsert, value stored as JSON.
    pub fn set(&mut self, name: &str, value: &Value) -> Result<(), StoreError> {
        let text = serde_json::to_string(value).map_err(|e| StoreError::Check(e.to_string()))?;
        self.exec(
            "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
            &[&name, &text],
        )?;
        Ok(())
    }

    /// `BEGIN` ... `COMMIT`, rolling back on error.
    pub fn transaction<T>(
        &mut self,
        job: impl FnOnce(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.exec_batch("BEGIN")?;
        match job(self) {
            Ok(value) => {
                self.exec_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.exec_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Timings snapshot as `{label: {calls, seconds, maxSeconds}}` in first-seen order.
    #[must_use]
    pub fn timings(&self) -> Obj {
        let mut obj = Obj::new();
        for (label, timing) in &self.timings {
            obj.set(
                label,
                serde_json::to_value(timing).expect("timing serializes"),
            );
        }
        obj
    }

    /// Close the connection, then run `job` with the lock this store already holds, then reopen
    /// with the same schema. `job` must not lock the checkpoint again. Every failure path marks
    /// the store unusable, so later calls fail instead of writing to the in-memory placeholder.
    pub fn while_closed<T>(
        &mut self,
        job: impl FnOnce(&Path, &DbLock) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.ensure_usable()?;
        let placeholder = Connection::open_in_memory()?;
        let conn = std::mem::replace(&mut self.conn, placeholder);
        if let Err((_, error)) = conn.close() {
            self.unusable = Some(error.to_string());
            return Err(error.into());
        }
        let result = job(&self.root, &self.lock);
        let db_config = match config(false) {
            Ok(config) => config,
            Err(error) => {
                self.unusable = Some(error.to_string());
                return Err(error.into());
            }
        };
        let reopened =
            match Connection::open_with_flags(self.root.join("checkpoint.duckdb"), db_config) {
                Ok(conn) => conn,
                Err(error) => {
                    self.unusable = Some(error.to_string());
                    return Err(error.into());
                }
            };
        self.conn = reopened;
        if let Err(error) = self.exec_batch(self.ddl) {
            self.unusable = Some(error.to_string());
            return Err(error);
        }
        result
    }

    /// Close explicitly. The database connection closes before the advisory lock is released.
    pub fn close(self) -> Result<(), StoreError> {
        let Store { conn, lock, .. } = self;
        let result = conn.close().map_err(|(_, error)| error);
        drop(lock);
        result?;
        Ok(())
    }

    fn record(&mut self, sql: &str, started: std::time::Instant) {
        let seconds = started.elapsed().as_secs_f64();
        let label = statement_label(sql);
        let entry = match self.timings.iter_mut().find(|(k, _)| *k == label) {
            Some(entry) => &mut entry.1,
            None => {
                self.timings.push((label, Timing::default()));
                &mut self.timings.last_mut().expect("just pushed").1
            }
        };
        entry.calls += 1;
        entry.seconds += seconds;
        entry.max_seconds = entry.max_seconds.max(seconds);
    }
}

/// `VERB:table` from the first keyword and the first `INTO|UPDATE|FROM <name>`.
#[must_use]
pub fn statement_label(sql: &str) -> String {
    let verb = sql.split_whitespace().next().unwrap_or("").to_uppercase();
    let lower = sql.to_lowercase();
    let mut table = String::new();
    let words: Vec<&str> = lower.split_whitespace().collect();
    for pair in words.windows(2) {
        if matches!(pair[0], "into" | "update" | "from") {
            let name: String = pair[1]
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            if !name.is_empty() {
                table = name;
                break;
            }
        }
    }
    format!("{verb}:{table}")
}

/// Query rows on any connection as ordered JSON objects.
pub fn query_rows(
    conn: &Connection,
    sql: &str,
    params: &[&dyn ToSql],
) -> Result<Vec<Obj>, StoreError> {
    let mut statement = conn.prepare(sql)?;
    let mut rows = statement.query(params_from_iter(params.iter()))?;
    let mut out = Vec::new();
    let mut names: Option<Vec<String>> = None;
    let mut tz: Option<Vec<bool>> = None;
    while let Some(row) = rows.next()? {
        let statement = row.as_ref();
        let names = names.get_or_insert_with(|| statement.column_names());
        let tz = tz.get_or_insert_with(|| {
            (0..statement.column_count())
                .map(|i| {
                    matches!(
                        statement.column_type(i),
                        duckdb::arrow::datatypes::DataType::Timestamp(_, Some(_))
                    )
                })
                .collect()
        });
        let mut obj = Obj::new();
        for (i, name) in names.iter().enumerate() {
            let value: DbValue = row.get(i)?;
            obj.set(name, json_of(value, tz[i]));
        }
        out.push(obj);
    }
    Ok(out)
}

/// One scalar from a query.
pub fn query_scalar(
    conn: &Connection,
    sql: &str,
    params: &[&dyn ToSql],
) -> Result<Value, StoreError> {
    let rows = query_rows(conn, sql, params)?;
    Ok(rows
        .first()
        .and_then(|row| row.iter().next().map(|(_, v)| v.clone()))
        .unwrap_or(Value::Null))
}

/// Render a DuckDB value as `@duckdb/node-api` `getRowObjectsJson()` does.
#[must_use]
pub fn json_of(value: DbValue, with_timezone: bool) -> Value {
    match value {
        DbValue::Null => Value::Null,
        DbValue::Boolean(b) => Value::Bool(b),
        DbValue::TinyInt(n) => Value::from(n),
        DbValue::SmallInt(n) => Value::from(n),
        DbValue::Int(n) => Value::from(n),
        DbValue::UTinyInt(n) => Value::from(n),
        DbValue::USmallInt(n) => Value::from(n),
        DbValue::UInt(n) => Value::from(n),
        DbValue::BigInt(n) => Value::String(n.to_string()),
        DbValue::UBigInt(n) => Value::String(n.to_string()),
        DbValue::HugeInt(n) => Value::String(n.to_string()),
        DbValue::Float(f) => {
            serde_json::Number::from_f64(f64::from(f)).map_or(Value::Null, Value::Number)
        }
        DbValue::Double(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        DbValue::Decimal(d) => Value::String(d.to_string()),
        DbValue::Timestamp(unit, n) => Value::String(timestamp_text(unit, n, with_timezone)),
        DbValue::Text(s) => Value::String(s),
        DbValue::Blob(b) => Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        DbValue::Date32(days) => Value::String(
            chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
        ),
        DbValue::Time64(unit, n) => Value::String(
            timestamp_text(unit, n, false)
                .split(' ')
                .nth(1)
                .unwrap_or("")
                .to_owned(),
        ),
        DbValue::Interval {
            months,
            days,
            nanos,
        } => Value::Object(
            [
                ("months", months.into()),
                ("days", days.into()),
                ("nanos", nanos.to_string().into()),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect(),
        ),
        DbValue::List(items) | DbValue::Array(items) => {
            Value::Array(items.into_iter().map(|v| json_of(v, false)).collect())
        }
        DbValue::Enum(s) => Value::String(s),
        DbValue::Struct(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), json_of(v.clone(), false)))
                .collect(),
        ),
        DbValue::Map(entries) => Value::Object(
            entries
                .iter()
                .map(|(k, v)| {
                    (
                        json_of(k.clone(), false)
                            .to_string()
                            .trim_matches('"')
                            .to_owned(),
                        json_of(v.clone(), false),
                    )
                })
                .collect(),
        ),
        DbValue::Union(inner) => json_of(*inner, false),
    }
}

fn timestamp_text(unit: TimeUnit, n: i64, with_timezone: bool) -> String {
    let micros = match unit {
        TimeUnit::Second => n.saturating_mul(1_000_000),
        TimeUnit::Millisecond => n.saturating_mul(1_000),
        TimeUnit::Microsecond => n,
        TimeUnit::Nanosecond => n / 1_000,
    };
    let Some(time) = chrono::DateTime::from_timestamp_micros(micros) else {
        return n.to_string();
    };
    let mut text = time.format("%Y-%m-%d %H:%M:%S").to_string();
    let fraction = micros.rem_euclid(1_000_000);
    if fraction != 0 {
        text.push_str(format!(".{fraction:06}").trim_end_matches('0'));
    }
    if with_timezone {
        text.push_str("+00");
    }
    text
}

/// `'text'` quoted for SQL, as the TypeScript `sqlString`.
#[must_use]
pub fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_follow_the_typescript_regex() {
        assert_eq!(
            statement_label("INSERT INTO files VALUES (?)"),
            "INSERT:files"
        );
        assert_eq!(
            statement_label("COPY (SELECT * FROM events WHERE x) TO 'f'"),
            "COPY:events"
        );
        assert_eq!(
            statement_label("SELECT count(*) AS n FROM read_parquet('x')"),
            "SELECT:read_parquet"
        );
        assert_eq!(
            statement_label("CREATE OR REPLACE TEMP TABLE x(a INT)"),
            "CREATE:"
        );
        assert_eq!(statement_label("BEGIN"), "BEGIN:");
    }

    #[test]
    fn renders_like_node_api() {
        assert_eq!(
            json_of(DbValue::BigInt(7), false),
            Value::String("7".into())
        );
        assert_eq!(json_of(DbValue::Int(7), false), Value::from(7));
        assert_eq!(
            json_of(
                DbValue::Timestamp(TimeUnit::Microsecond, 1_791_122_463_000_000),
                true
            ),
            Value::String("2026-10-04 14:01:03+00".into())
        );
        assert_eq!(sql_string("a'b"), "'a''b'");
    }

    #[test]
    fn in_memory_rows_and_kv() {
        let root = tempdir();
        let mut store = Store::open(
            &root,
            "CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);",
        )
        .unwrap();
        store.set("k", &serde_json::json!({"a": 1})).unwrap();
        assert_eq!(store.get("k").unwrap(), Some(serde_json::json!({"a": 1})));
        let rows = store
            .rows(
                "SELECT 1::BIGINT AS big, 2::INTEGER AS small, true AS flag, 'x' AS text",
                &[],
            )
            .unwrap();
        assert_eq!(
            rows[0].to_json(),
            r#"{"big":"1","small":2,"flag":true,"text":"x"}"#
        );
        assert!(store.timings().get("INSERT:kv").is_some());
        store.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_failed_reopen_marks_the_store_unusable() {
        let root = tempdir();
        let mut store = Store::open(
            &root,
            "CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);",
        )
        .unwrap();
        let error = store.while_closed(|root, lock| {
            lock.prove_held();
            let path = root.join("checkpoint.duckdb");
            std::fs::remove_file(&path)?;
            std::fs::create_dir(&path)?;
            Ok(())
        });
        let message = error.unwrap_err().to_string();
        assert!(!message.is_empty());
        let again = store.rows("SELECT 1", &[]).unwrap_err().to_string();
        assert!(again.contains("unusable"), "{again}");
        store.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_failed_reopen_config_marks_the_store_unusable() {
        let root = tempdir();
        let mut store = Store::open(
            &root,
            "CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);",
        )
        .unwrap();
        crate::failpoints::inject_db_config_failure(true);
        let error = store.while_closed(|_root, _lock| Ok(()));
        crate::failpoints::inject_db_config_failure(false);
        let message = error.unwrap_err().to_string();
        assert!(!message.is_empty());
        let again = store.rows("SELECT 1", &[]).unwrap_err().to_string();
        assert!(again.contains("unusable"), "{again}");
        store.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("solos-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
