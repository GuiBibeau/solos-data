//! Staging and publication. Rows are appended to one NDJSON file per dataset under `staging/`
//! as they are read; a flush moves the current files aside (the loops keep appending to fresh
//! ones), converts each into one Parquet file with DuckDB `COPY` (explicit column types, ZSTD),
//! makes it durable (fsync, rename, fsync of the directory), counts the rows back, hashes it and
//! registers it in the lane's `checkpoint.duckdb` before the staged file is deleted, then rewrites
//! `catalog.json`. A flush never spans two UTC dates. On start, leftover staged files (a crash)
//! are cut back to their last complete line and published, and Parquet the ledger does not know
//! is removed.

use crate::fsutil::{file_hash, sync_path, tmp_path, write_durable};
use crate::jsonout::{Obj, log, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The ledger schema.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files(
  path VARCHAR PRIMARY KEY, dataset VARCHAR, date VARCHAR, row_count BIGINT, bytes BIGINT,
  sha256 VARCHAR, min_slot UBIGINT, max_slot UBIGINT, created_at VARCHAR);
CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);
";

/// Column names and DuckDB types of every dataset, in file order. `captured_at` (a TIMESTAMP
/// derived from `captured_ms`) is added to every file; `raw_accounts.data_zstd` is staged as
/// base64 and stored as a BLOB.
pub const DATASETS: &[(&str, &[(&str, &str)])] = &[
    (
        "raw_accounts",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("kind", "VARCHAR"),
            ("pubkey", "VARCHAR"),
            ("symbol", "VARCHAR"),
            ("reason", "VARCHAR"),
            ("owner", "VARCHAR"),
            ("lamports", "UBIGINT"),
            ("data_len", "BIGINT"),
            ("sha256", "VARCHAR"),
            ("data_zstd", "VARCHAR"),
        ],
    ),
    (
        "book_snapshots",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("symbol", "VARCHAR"),
            ("asset_id", "UINTEGER"),
            ("market", "VARCHAR"),
            ("reason", "VARCHAR"),
            ("sha256", "VARCHAR"),
            ("decode_error", "VARCHAR"),
            ("market_status", "UTINYINT"),
            ("sequence_number", "UBIGINT"),
            ("sequence_slot", "UBIGINT"),
            ("order_sequence_number", "UBIGINT"),
            ("trade_sequence_number", "UBIGINT"),
            ("tick_size", "UBIGINT"),
            ("base_lots_decimals", "TINYINT"),
            ("taker_fee_micro", "UINTEGER"),
            ("maker_fee_micro", "INTEGER"),
            ("total_maker_fees", "BIGINT"),
            ("total_taker_fees", "UBIGINT"),
            ("bids", "UINTEGER"),
            ("asks", "UINTEGER"),
            ("best_bid_ticks", "UBIGINT"),
            ("best_ask_ticks", "UBIGINT"),
            ("bid_lots", "UBIGINT"),
            ("ask_lots", "UBIGINT"),
            ("last_trade_slot", "UBIGINT"),
            ("last_trade_sequence_number", "UBIGINT"),
            ("last_trade_price_ticks", "UBIGINT"),
        ],
    ),
    (
        "book_orders",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("symbol", "VARCHAR"),
            ("side", "VARCHAR"),
            ("rank", "UINTEGER"),
            ("price_ticks", "UBIGINT"),
            ("order_sequence_number", "UBIGINT"),
            ("raw_order_id", "UBIGINT"),
            ("trader_id", "UINTEGER"),
            ("trader", "VARCHAR"),
            ("asset_id", "UINTEGER"),
            ("base_lots_remaining", "UBIGINT"),
            ("initial_trade_size", "UBIGINT"),
            ("initial_slot", "UBIGINT"),
            ("last_valid_slot", "UBIGINT"),
            ("order_flags", "UTINYINT"),
            ("reduce_only", "BOOLEAN"),
            ("is_stop_loss", "BOOLEAN"),
            ("is_conditional", "BOOLEAN"),
            ("conditional_order_index", "UTINYINT"),
        ],
    ),
    (
        "splines",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("symbol", "VARCHAR"),
            ("market", "VARCHAR"),
            ("spline_collection", "VARCHAR"),
            ("reason", "VARCHAR"),
            ("spline_index", "UINTEGER"),
            ("trader", "VARCHAR"),
            ("is_active", "BOOLEAN"),
            ("mid_price_ticks", "UBIGINT"),
            ("bid_offset", "UBIGINT"),
            ("ask_offset", "UBIGINT"),
            ("bid_filled", "UBIGINT"),
            ("ask_filled", "UBIGINT"),
            ("bid_num_regions", "UBIGINT"),
            ("ask_num_regions", "UBIGINT"),
            ("sequence_number", "UBIGINT"),
            ("sequence_slot", "UBIGINT"),
            ("user_update_slot", "UBIGINT"),
            ("user_price_sequence_number", "UBIGINT"),
            ("user_parameter_sequence_number", "UBIGINT"),
            ("flags", "UINTEGER"),
            ("leverage_decrease_bps", "UBIGINT"),
            ("max_position_size_long", "UBIGINT"),
            ("max_position_size_short", "UBIGINT"),
            ("regions", "VARCHAR"),
        ],
    ),
    (
        "market_state",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("symbol", "VARCHAR"),
            ("asset_id", "UINTEGER"),
            ("market", "VARCHAR"),
            ("tick_size", "UBIGINT"),
            ("base_lots_decimals", "TINYINT"),
            ("map_sequence_number", "UBIGINT"),
            ("map_sequence_slot", "UBIGINT"),
            ("price_sequence_number", "UBIGINT"),
            ("price_sequence_slot", "UBIGINT"),
            ("mark_price_ticks", "UBIGINT"),
            ("mark_price_slot", "UBIGINT"),
            ("finalized_mark_price_ticks", "UBIGINT"),
            ("last_spot_price", "VARCHAR"),
            ("last_perp_price", "VARCHAR"),
            ("book_mid_ticks", "UBIGINT"),
            ("book_mid_slot", "UBIGINT"),
            ("book_best_bid_ticks", "UBIGINT"),
            ("book_best_ask_ticks", "UBIGINT"),
            ("book_last_trade_ticks", "UBIGINT"),
            ("book_last_trade_slot", "UBIGINT"),
            ("mid_spot_diff_ema_ticks", "BIGINT"),
            ("cumulative_funding_rate", "BIGINT"),
            ("funding_acc", "HUGEINT"),
            ("funding_last_diff", "HUGEINT"),
            ("funding_start_interval_ts", "UBIGINT"),
            ("funding_last_update_ts", "UBIGINT"),
            ("funding_interval_seconds", "UBIGINT"),
            ("funding_period_seconds", "UBIGINT"),
            ("max_funding_rate", "BIGINT"),
            ("open_interest", "UBIGINT"),
            ("open_interest_cap", "UBIGINT"),
            ("last_known_index_price_ticks", "UBIGINT"),
            ("last_index_expiry_ts", "UBIGINT"),
            ("asset_flags", "UTINYINT"),
            ("risk_params", "VARCHAR"),
        ],
    ),
    (
        "trader_index",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("trader_id", "UINTEGER"),
            ("trader", "VARCHAR"),
            ("collateral", "BIGINT"),
            ("flags", "UINTEGER"),
        ],
    ),
    (
        "active_positions",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("reason", "VARCHAR"),
            ("trader_id", "UINTEGER"),
            ("trader", "VARCHAR"),
            ("asset_id", "UINTEGER"),
            ("symbol", "VARCHAR"),
            ("base_lot_position", "BIGINT"),
            ("virtual_quote_lot_position", "BIGINT"),
            ("cumulative_funding_snapshot", "BIGINT"),
            ("accumulated_funding", "BIGINT"),
            ("position_sequence_number", "UTINYINT"),
            ("best_ask_ticks", "UINTEGER"),
            ("ask_ref", "UINTEGER"),
            ("best_bid_ticks", "UINTEGER"),
            ("bid_ref", "UINTEGER"),
            ("ask_lots", "UINTEGER"),
            ("ask_reduce_only_lots", "UINTEGER"),
            ("bid_lots", "UINTEGER"),
            ("bid_reduce_only_lots", "UINTEGER"),
        ],
    ),
    (
        "traders",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("trader", "VARCHAR"),
            ("trader_id", "UINTEGER"),
            ("sha256", "VARCHAR"),
            ("key", "VARCHAR"),
            ("authority", "VARCHAR"),
            ("position_authority", "VARCHAR"),
            ("trader_pda_index", "UTINYINT"),
            ("trader_subaccount_index", "UTINYINT"),
            ("collateral", "BIGINT"),
            ("flags", "UINTEGER"),
            ("global_position_sequence_number", "UTINYINT"),
            ("maker_fee_override", "TINYINT"),
            ("taker_fee_override", "TINYINT"),
            ("sequence_number", "UBIGINT"),
            ("sequence_slot", "UBIGINT"),
            ("max_positions", "UINTEGER"),
            ("positions", "UINTEGER"),
            ("native_sol_collateral", "UBIGINT"),
            ("last_deposit_slot", "UBIGINT"),
            ("withdraw_queue_node", "UINTEGER"),
            ("num_markets_with_splines", "USMALLINT"),
            ("data_len", "BIGINT"),
        ],
    ),
    (
        "trader_positions",
        &[
            ("captured_ms", "BIGINT"),
            ("slot", "UBIGINT"),
            ("trader", "VARCHAR"),
            ("asset_id", "UBIGINT"),
            ("symbol", "VARCHAR"),
            ("base_lot_position", "BIGINT"),
            ("virtual_quote_lot_position", "BIGINT"),
            ("cumulative_funding_snapshot", "BIGINT"),
            ("accumulated_funding", "BIGINT"),
            ("position_sequence_number", "UTINYINT"),
        ],
    ),
];

/// Columns of a dataset.
#[must_use]
pub fn columns(dataset: &str) -> Option<&'static [(&'static str, &'static str)]> {
    DATASETS
        .iter()
        .find(|(name, _)| *name == dataset)
        .map(|(_, c)| *c)
}

/// Open (creating) the ledger under `root`.
pub fn open(root: &Path) -> Result<Store, StoreError> {
    std::fs::create_dir_all(root)?;
    Store::open(root, SCHEMA)
}

struct Open {
    file: std::io::BufWriter<std::fs::File>,
    rows: u64,
    bytes: u64,
}

/// The staging area: one append-only NDJSON file per dataset, all of one UTC date.
pub struct Stager {
    dir: PathBuf,
    open: BTreeMap<String, Open>,
    date: Option<String>,
}

/// A staged set moved aside for publication.
#[derive(Debug)]
pub struct Batch {
    /// Directory holding `<dataset>.ndjson` files.
    pub dir: PathBuf,
    /// The UTC date of every row in it.
    pub date: String,
}

impl Stager {
    /// A stager over `<root>/staging`.
    pub fn new(root: &Path) -> Result<Stager, StoreError> {
        let dir = root.join("staging");
        std::fs::create_dir_all(&dir)?;
        Ok(Stager {
            dir,
            open: BTreeMap::new(),
            date: None,
        })
    }

    /// Bytes staged so far.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.open.values().map(|o| o.bytes).sum()
    }

    /// Rows staged per dataset.
    #[must_use]
    pub fn rows(&self) -> BTreeMap<String, u64> {
        self.open.iter().map(|(k, o)| (k.clone(), o.rows)).collect()
    }

    /// The date of the staged rows, if any.
    #[must_use]
    pub fn date(&self) -> Option<&str> {
        self.date.as_deref()
    }

    /// Append rows of `dataset` captured on `date` (`YYYY-MM-DD`). The caller rotates first
    /// when the date changes.
    pub fn append(&mut self, dataset: &str, date: &str, rows: &[Value]) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        if columns(dataset).is_none() {
            return Err(StoreError::Check(format!("unknown dataset {dataset}")));
        }
        if self.date.as_deref().is_some_and(|d| d != date) {
            return Err(StoreError::Check(
                "staged rows of two dates; rotate first".into(),
            ));
        }
        self.date = Some(date.to_owned());
        if !self.open.contains_key(dataset) {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(format!("{dataset}.ndjson")))?;
            self.open.insert(
                dataset.to_owned(),
                Open {
                    file: std::io::BufWriter::with_capacity(1 << 20, file),
                    rows: 0,
                    bytes: 0,
                },
            );
        }
        let open = self.open.get_mut(dataset).expect("opened above");
        for row in rows {
            let mut line = serde_json::to_vec(row).map_err(|e| StoreError::Check(e.to_string()))?;
            line.push(b'\n');
            open.file.write_all(&line)?;
            open.bytes += line.len() as u64;
            open.rows += 1;
        }
        Ok(())
    }

    /// Close the current files and move them to a batch directory; `None` when nothing is staged.
    pub fn rotate(&mut self) -> Result<Option<Batch>, StoreError> {
        let Some(date) = self.date.take() else {
            return Ok(None);
        };
        let open = std::mem::take(&mut self.open);
        let batch = self.dir.join(format!("batch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&batch)?;
        for (dataset, mut o) in open {
            o.file.flush()?;
            o.file.get_ref().sync_all()?;
            let name = format!("{dataset}.ndjson");
            std::fs::rename(self.dir.join(&name), batch.join(&name))?;
        }
        std::fs::write(batch.join("date"), &date)?;
        Ok(Some(Batch { dir: batch, date }))
    }
}

/// Batches left by an earlier process, plus loose staged files moved into one; each file is cut
/// back to its last complete line.
pub fn leftover_batches(root: &Path) -> Result<Vec<Batch>, StoreError> {
    let dir = root.join("staging");
    std::fs::create_dir_all(&dir)?;
    let loose: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "ndjson"))
        .collect();
    if !loose.is_empty() {
        let first = loose.iter().find_map(|p| first_date(p));
        let batch = dir.join(format!("batch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&batch)?;
        for path in &loose {
            std::fs::rename(path, batch.join(path.file_name().unwrap_or_default()))?;
        }
        std::fs::write(batch.join("date"), first.unwrap_or_default())?;
    }
    let mut batches = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let date = std::fs::read_to_string(path.join("date")).unwrap_or_default();
        for file in std::fs::read_dir(&path)? {
            let file = file?.path();
            if file.extension().is_some_and(|x| x == "ndjson") {
                truncate_partial_line(&file)?;
            }
        }
        batches.push(Batch {
            dir: path,
            date: date.trim().to_owned(),
        });
    }
    Ok(batches)
}

fn first_date(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let line = text.lines().next()?;
    let value: Value = serde_json::from_str(line).ok()?;
    value
        .get("captured_ms")
        .and_then(Value::as_i64)
        .map(date_of_ms)
}

fn truncate_partial_line(path: &Path) -> Result<(), StoreError> {
    let bytes = std::fs::read(path)?;
    let keep = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
    if keep < bytes.len() {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(keep as u64)?;
        file.sync_all()?;
        log(
            "snapshot_staging_truncated",
            Obj::new()
                .with(
                    "file",
                    path.file_name().map(|n| n.to_string_lossy().into_owned()),
                )
                .with("droppedBytes", bytes.len() - keep),
        );
    }
    Ok(())
}

/// `YYYY-MM-DD` of epoch milliseconds, UTC.
#[must_use]
pub fn date_of_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%d")
        .to_string()
}

fn select_for(dataset: &str, source: &Path) -> Result<String, StoreError> {
    let cols = columns(dataset).ok_or_else(|| StoreError::Check("unknown dataset".into()))?;
    let map: Vec<String> = cols
        .iter()
        .map(|(name, kind)| format!("{}: '{kind}'", sql_string(name)))
        .collect();
    let projection: Vec<String> = cols
        .iter()
        .map(|(name, _)| {
            if *name == "data_zstd" {
                "from_base64(data_zstd) AS data_zstd".to_owned()
            } else {
                (*name).to_owned()
            }
        })
        .collect();
    let order = if cols.iter().any(|(n, _)| *n == "symbol") {
        "slot, symbol"
    } else {
        "slot"
    };
    Ok(format!(
        "SELECT make_timestamp(captured_ms * 1000) AS captured_at, {} FROM read_json({}, format='newline_delimited', columns={{{}}}, maximum_object_size=67108864) ORDER BY {order}",
        projection.join(", "),
        sql_string(&source.to_string_lossy()),
        map.join(", ")
    ))
}

/// One published file.
#[derive(Clone, Debug)]
pub struct Published {
    /// Root-relative path.
    pub path: String,
    /// Dataset.
    pub dataset: String,
    /// Rows.
    pub rows: i64,
    /// Bytes on disk.
    pub bytes: i64,
}

/// Publish every staged file of a batch, then remove the batch directory.
pub fn publish(
    store: &mut Store,
    root: &Path,
    batch: &Batch,
) -> Result<Vec<Published>, StoreError> {
    let mut out = Vec::new();
    let date = if batch.date.is_empty() {
        date_of_ms(chrono::Utc::now().timestamp_millis())
    } else {
        batch.date.clone()
    };
    let stamp = chrono::Utc::now().format("%H%M%S").to_string();
    for (dataset, _) in DATASETS {
        let source = batch.dir.join(format!("{dataset}.ndjson"));
        if !source.is_file() || std::fs::metadata(&source)?.len() == 0 {
            continue;
        }
        let id = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let relative = format!("tables/{dataset}/date={date}/{stamp}-{id}.parquet");
        let path = root.join(&relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = tmp_path(&path);
        store.exec_batch(&format!(
            "COPY ({}) TO {} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 262144)",
            select_for(dataset, &source)?,
            sql_string(&tmp.to_string_lossy())
        ))?;
        sync_path(&tmp)?;
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            sync_path(parent)?;
        }
        let counted = store.rows(
            &format!(
                "SELECT count(*) AS n, min(slot) AS lo, max(slot) AS hi FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )?;
        let first = counted.first();
        let rows = first.and_then(|r| r.int("n")).unwrap_or(0);
        let bytes = i64::try_from(std::fs::metadata(&path)?.len()).unwrap_or(i64::MAX);
        let sha = file_hash(&path)?;
        let lo = first.and_then(|r| r.int("lo"));
        let hi = first.and_then(|r| r.int("hi"));
        store.exec(
            "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                &relative,
                dataset,
                &date,
                &rows,
                &bytes,
                &sha,
                &lo,
                &hi,
                &now(),
            ],
        )?;
        std::fs::remove_file(&source)?;
        out.push(Published {
            path: relative,
            dataset: (*dataset).to_owned(),
            rows,
            bytes,
        });
    }
    std::fs::remove_dir_all(&batch.dir)?;
    write_catalog(store, root)?;
    Ok(out)
}

/// `catalog.json`: every registered file, durably written.
pub fn write_catalog(store: &mut Store, root: &Path) -> Result<(), StoreError> {
    let files = store.rows(
        "SELECT path, dataset, date, row_count, bytes, sha256, min_slot, max_slot, created_at
         FROM files ORDER BY dataset, path",
        &[],
    )?;
    let catalog = Obj::new()
        .with("at", now())
        .with("schemaVersion", 1)
        .with_rows("files", files);
    write_durable(
        &root.join("catalog.json"),
        format!("{}\n", catalog.to_json()).as_bytes(),
    )?;
    Ok(())
}

/// Per dataset: files, rows, bytes, dates.
pub fn summary(store: &Store) -> Result<Vec<Obj>, StoreError> {
    store.rows(
        "SELECT dataset, count(*) AS files, sum(row_count) AS rows, sum(bytes) AS bytes,
                min(date) AS oldest, max(date) AS newest
         FROM files GROUP BY dataset ORDER BY dataset",
        &[],
    )
}

/// Remove `.tmp` files and Parquet under `tables/` that the ledger does not register.
pub fn remove_unregistered(store: &Store, root: &Path) -> Result<u64, StoreError> {
    let registered: std::collections::HashSet<PathBuf> = store
        .rows("SELECT path FROM files", &[])?
        .iter()
        .filter_map(|r| r.str("path"))
        .map(|p| root.join(p))
        .collect();
    let mut removed = 0;
    let mut stack = vec![root.join("tables")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if !registered.contains(&path) {
                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn staged_rows_publish_as_typed_parquet_and_crashed_staging_recovers() {
        let dir = std::env::temp_dir().join(format!("snap-ledger-{}", uuid::Uuid::new_v4()));
        let mut store = open(&dir).unwrap();
        let mut stager = Stager::new(&dir).unwrap();
        let day = date_of_ms(1_792_137_600_000);
        let rows = vec![
            json!({ "captured_ms": 1_792_137_600_000i64, "slot": 2u64, "trader_id": 7, "trader": "T", "collateral": -5, "flags": 1 }),
            json!({ "captured_ms": 1_792_137_600_000i64, "slot": 1u64, "trader_id": 8, "trader": "U", "collateral": 9, "flags": 0 }),
        ];
        stager.append("trader_index", &day, &rows).unwrap();
        let raw = json!({ "captured_ms": 1_792_137_600_000i64, "slot": 1u64, "kind": "trader", "pubkey": "T",
            "reason": "baseline", "owner": "P", "lamports": 1u64, "data_len": 3, "sha256": "x",
            "data_zstd": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1u8, 2, 3]) });
        stager.append("raw_accounts", &day, &[raw]).unwrap();
        assert!(stager.append("trader_index", "1999-01-01", &rows).is_err());
        assert!(stager.append("nope", &day, &rows).is_err());
        let batch = stager.rotate().unwrap().unwrap();
        assert!(stager.rotate().unwrap().is_none());
        let published = publish(&mut store, &dir, &batch).unwrap();
        assert_eq!(published.len(), 2);
        let index = published
            .iter()
            .find(|p| p.dataset == "trader_index")
            .unwrap();
        assert_eq!(index.rows, 2);
        assert!(
            index
                .path
                .starts_with(&format!("tables/trader_index/date={day}/"))
        );
        let read = store
            .rows(&format!("SELECT trader, collateral, typeof(slot) AS t FROM read_parquet('{}') ORDER BY slot",
                dir.join(&index.path).display()), &[])
            .unwrap();
        assert_eq!(read[0].str("trader"), Some("U"));
        assert_eq!(read[1].int("collateral"), Some(-5));
        assert_eq!(read[0].str("t"), Some("UBIGINT"));
        let blob = store
            .rows(&format!("SELECT octet_length(data_zstd) AS n FROM read_parquet('{}/tables/raw_accounts/*/*.parquet')", dir.display()), &[])
            .unwrap();
        assert_eq!(blob[0].int("n"), Some(3));
        // A crash mid-line: the partial line is dropped, the rest is published on the next start.
        let mut stager = Stager::new(&dir).unwrap();
        stager.append("trader_index", &day, &rows).unwrap();
        drop(stager);
        let loose = dir.join("staging/trader_index.ndjson");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&loose)
            .unwrap();
        file.write_all(b"{\"captured_ms\":1").unwrap();
        let batches = leftover_batches(&dir).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].date, day);
        let again = publish(&mut store, &dir, &batches[0]).unwrap();
        assert_eq!(again[0].rows, 2);
        std::fs::write(dir.join("tables/trader_index/stray.parquet"), b"x").unwrap();
        assert_eq!(remove_unregistered(&store, &dir).unwrap(), 1);
        let catalog: Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("catalog.json")).unwrap())
                .unwrap();
        assert_eq!(catalog["files"].as_array().unwrap().len(), 3);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
