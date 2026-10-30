//! Writing a batch's rows as Parquet (ADR-0014): the rows go into an in-memory DuckDB through
//! its appender, then `COPY` to a `.tmp` sibling with ZSTD, fsync, rename, fsync the directory,
//! and the row count is read back and the file hashed for the catalog. Keys are written as
//! base58 through a cache (pools, mints and signers repeat). Rows are sorted by pool, then
//! slot, transaction and position, which keeps each pool's trades together in a row group and
//! lets the dictionary and run-length encodings collapse the repeated columns.

use super::rows::{Liquidity, Swap};
use super::tx::{Key, b58};
use crate::augment::ledger::FileRecord;
use crate::fsutil::{
    create_dir_all_durable, file_hash, sync_dir_after_rename, sync_tmp_file, tmp_path,
};
use crate::store::{StoreError, memory_connection, sql_string};
use duckdb::{Connection, params};
use std::collections::HashMap;
use std::path::Path;

/// The swaps table.
pub const SWAPS_DDL: &str = "CREATE TABLE swaps(slot BIGINT, block_time BIGINT, tx_index INTEGER,
  seq INTEGER, ix INTEGER, depth TINYINT, program VARCHAR, pool VARCHAR, mint_a VARCHAR,
  mint_b VARCHAR, decimals_a UTINYINT, decimals_b UTINYINT, a_to_b BOOLEAN, amount_in UBIGINT,
  amount_out UBIGINT, fee UBIGINT, fee_on_input BOOLEAN, reserve_a UBIGINT, reserve_b UBIGINT,
  sqrt_price DOUBLE, tick INTEGER, liquidity DOUBLE, signer VARCHAR)";

/// The liquidity table.
pub const LIQUIDITY_DDL: &str = "CREATE TABLE liquidity(slot BIGINT, block_time BIGINT,
  tx_index INTEGER, seq INTEGER, ix INTEGER, depth TINYINT, program VARCHAR, pool VARCHAR,
  kind VARCHAR, mint_a VARCHAR, mint_b VARCHAR, amount_a UBIGINT, amount_b UBIGINT,
  signer VARCHAR)";

/// The batches table: one row per batch and program, and one `ALL` row with the block check.
pub const BATCHES_DDL: &str = "CREATE TABLE batches(from_slot BIGINT, to_slot BIGINT,
  program VARCHAR, blocks UBIGINT, rpc_blocks UBIGINT, transactions UBIGINT,
  truncated_logs UBIGINT, txs UBIGINT, failed UBIGINT, frames UBIGINT, swap_ixs UBIGINT,
  swaps UBIGINT, liquidity UBIGINT, missing_event UBIGINT, undecoded UBIGINT,
  unresolved UBIGINT, manifest_from BIGINT, manifest_to BIGINT, manifest UBIGINT,
  manifest_missing UBIGINT, unlisted UBIGINT, seconds DOUBLE, at_ms BIGINT)";

/// The row order of the files.
pub const ORDER: &str = "pool, slot, tx_index, seq";

/// Base58 of keys, each encoded once.
#[derive(Default)]
pub struct Names(HashMap<Key, String>);

impl Names {
    /// The base58 of `key`.
    pub fn get(&mut self, key: &Key) -> &str {
        self.0.entry(*key).or_insert_with(|| b58(key))
    }

    /// The base58 of `key`, owned (for an optional column).
    pub fn owned(&mut self, key: Option<&Key>) -> Option<String> {
        key.map(|k| self.get(k).to_owned())
    }
}

fn slot_i64(slot: u64) -> i64 {
    i64::try_from(slot).unwrap_or(i64::MAX)
}

/// Load swaps into a fresh `swaps` table on `conn`.
///
/// # Errors
///
/// A DuckDB failure.
pub fn load_swaps(
    conn: &Connection,
    rows: &[Swap],
    time: &dyn Fn(u64) -> Option<i64>,
    names: &mut Names,
) -> Result<(), duckdb::Error> {
    conn.execute_batch(&format!("DROP TABLE IF EXISTS swaps; {SWAPS_DDL}"))?;
    let mut app = conn.appender("swaps")?;
    for s in rows {
        let pool = names.get(&s.pool).to_owned();
        let mint_a = names.get(&s.mint_a).to_owned();
        let mint_b = names.get(&s.mint_b).to_owned();
        let signer = names.get(&s.signer).to_owned();
        app.append_row(params![
            slot_i64(s.slot),
            time(s.slot),
            s.tx_index,
            s.seq,
            s.ix,
            s.depth,
            s.dex.name(),
            pool,
            mint_a,
            mint_b,
            s.decimals_a,
            s.decimals_b,
            s.a_to_b,
            s.amount_in,
            s.amount_out,
            s.fee,
            s.fee_on_input,
            s.reserve_a,
            s.reserve_b,
            s.sqrt_price,
            s.tick,
            s.liquidity,
            signer,
        ])?;
    }
    app.flush()
}

/// Load liquidity rows into a fresh `liquidity` table on `conn`.
///
/// # Errors
///
/// A DuckDB failure.
pub fn load_liquidity(
    conn: &Connection,
    rows: &[Liquidity],
    time: &dyn Fn(u64) -> Option<i64>,
    names: &mut Names,
) -> Result<(), duckdb::Error> {
    conn.execute_batch(&format!("DROP TABLE IF EXISTS liquidity; {LIQUIDITY_DDL}"))?;
    let mut app = conn.appender("liquidity")?;
    for l in rows {
        let pool = names.get(&l.pool).to_owned();
        let mint_a = names.owned(l.mint_a.as_ref());
        let mint_b = names.owned(l.mint_b.as_ref());
        let signer = names.get(&l.signer).to_owned();
        app.append_row(params![
            slot_i64(l.slot),
            time(l.slot),
            l.tx_index,
            l.seq,
            l.ix,
            l.depth,
            l.dex.name(),
            pool,
            l.kind.name(),
            mint_a,
            mint_b,
            l.amount_a,
            l.amount_b,
            signer,
        ])?;
    }
    app.flush()
}

/// An in-memory connection for one batch's tables.
///
/// # Errors
///
/// DuckDB could not open.
pub fn connection(threads: &str, memory: &str) -> Result<Connection, StoreError> {
    Ok(memory_connection(threads, memory)?)
}

/// `COPY (<select>) TO <root>/<relative>` durably, and describe the file for the catalog.
///
/// # Errors
///
/// A DuckDB or file-system failure.
pub fn copy_to(
    conn: &Connection,
    select: &str,
    root: &Path,
    relative: &str,
    describe: (&str, &str, &str),
) -> Result<FileRecord, StoreError> {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    let tmp = tmp_path(&path);
    conn.execute_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)",
        sql_string(&tmp.to_string_lossy())
    ))?;
    sync_tmp_file(&tmp)?;
    std::fs::rename(&tmp, &path)?;
    if let Some(parent) = path.parent() {
        sync_dir_after_rename(parent)?;
    }
    let row_count: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM read_parquet({})",
            sql_string(&path.to_string_lossy())
        ),
        [],
        |row| row.get(0),
    )?;
    let (source, dataset, period) = describe;
    Ok(FileRecord {
        path: relative.to_owned(),
        source: source.to_owned(),
        dataset: dataset.to_owned(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: period.to_owned(),
        complete: true,
        row_count,
        bytes: i64::try_from(std::fs::metadata(&path)?.len()).unwrap_or(i64::MAX),
        sha256: file_hash(&path)?,
    })
}
