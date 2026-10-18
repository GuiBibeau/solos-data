//! The lane's tables and their files. Rows are buffered in memory with the cursor they advance
//! to, and flushed every `flushSeconds` into one Parquet file per (source, dataset, UTC hour),
//! `tables/<source>/<dataset>/ALL/<YYYY-MM-DD>T<HH>.parquet`, merged on the dataset's key with
//! any rows the hour already holds, then registered in the lane's ledger. A file is replaced by
//! an atomic rename and never deleted, so readers of an older catalog keep valid files. Cursors
//! are recorded after every file they cover is registered; a crash in between refetches the
//! window and the merge drops the duplicates (ADR-0012).

use crate::augment::ledger::{register, set_progress};
use crate::augment::parquet::{Target, read_json_array, stage_json, write_merged};
use crate::augment::series::Ctx;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

/// A table: its key and typed columns. `time` names the column the hour is taken from and
/// whether it holds seconds or milliseconds.
pub struct Table {
    /// Dataset name.
    pub dataset: &'static str,
    /// Merge key.
    pub key: &'static str,
    /// Typed columns of the staged JSON.
    pub columns: &'static [(&'static str, &'static str)],
    /// The time column and whether it is in milliseconds.
    pub time: (&'static str, bool),
}

const TX: [(&str, &str); 3] = [
    ("signature", "VARCHAR"),
    ("slot", "BIGINT"),
    ("block_time", "BIGINT"),
];

/// Whole transactions.
pub const TRANSACTIONS: Table = Table {
    dataset: "transactions",
    key: "signature",
    columns: &[
        TX[0],
        TX[1],
        TX[2],
        ("tx_b64", "VARCHAR"),
        ("meta_json", "VARCHAR"),
        ("fetched_via", "VARCHAR"),
    ],
    time: ("block_time", false),
};

/// Anchor events.
pub const EVENTS: Table = Table {
    dataset: "events",
    key: "signature, ordinal",
    columns: &[
        TX[0],
        TX[1],
        TX[2],
        ("ordinal", "INTEGER"),
        ("via", "VARCHAR"),
        ("path", "VARCHAR"),
        ("discriminator", "VARCHAR"),
        ("name", "VARCHAR"),
        ("payload_b64", "VARCHAR"),
        ("fields", "JSON"),
        ("decode_error", "VARCHAR"),
    ],
    time: ("block_time", false),
};

/// The program's instructions.
pub const INSTRUCTIONS: Table = Table {
    dataset: "instructions",
    key: "signature, path",
    columns: &[
        TX[0],
        TX[1],
        TX[2],
        ("path", "VARCHAR"),
        ("discriminator", "VARCHAR"),
        ("name", "VARCHAR"),
        ("data_b64", "VARCHAR"),
        ("accounts", "JSON"),
    ],
    time: ("block_time", false),
};

/// Polled accounts.
pub const ACCOUNTS: Table = Table {
    dataset: "accounts",
    key: "at_ms, account",
    columns: &[
        ("at_ms", "BIGINT"),
        ("context_slot", "BIGINT"),
        ("account", "VARCHAR"),
        ("kind", "VARCHAR"),
        ("label", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("type", "VARCHAR"),
        ("data_b64", "VARCHAR"),
        ("fields", "JSON"),
        ("decode_error", "VARCHAR"),
        ("changed", "BOOLEAN"),
    ],
    time: ("at_ms", true),
};

/// Pyth prices.
pub const PRICES: Table = Table {
    dataset: "prices",
    key: "feed_id, publish_time",
    columns: &[
        ("at_ms", "BIGINT"),
        ("context_slot", "BIGINT"),
        ("symbol", "VARCHAR"),
        ("feed_id", "VARCHAR"),
        ("account", "VARCHAR"),
        ("price", "BIGINT"),
        ("conf", "UBIGINT"),
        ("exponent", "INTEGER"),
        ("publish_time", "BIGINT"),
        ("prev_publish_time", "BIGINT"),
        ("ema_price", "BIGINT"),
        ("ema_conf", "UBIGINT"),
        ("posted_slot", "UBIGINT"),
        ("verification", "VARCHAR"),
        ("price_f", "DOUBLE"),
        ("conf_f", "DOUBLE"),
    ],
    time: ("publish_time", false),
};

/// Every table.
pub const TABLES: [&Table; 5] = [&TRANSACTIONS, &EVENTS, &INSTRUCTIONS, &ACCOUNTS, &PRICES];

fn table(dataset: &str) -> Option<&'static Table> {
    TABLES.into_iter().find(|t| t.dataset == dataset)
}

/// `YYYY-MM-DDTHH` of an instant in milliseconds.
#[must_use]
pub fn hour_label(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H")
        .to_string()
}

/// Rows waiting for the next flush, and the cursors they advance.
#[derive(Default)]
pub struct Buffer {
    inner: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    rows: BTreeMap<(String, String), Vec<Value>>,
    cursors: BTreeMap<String, Value>,
}

impl Buffer {
    /// Add rows of `source`/`dataset`.
    pub fn push(&self, source: &str, dataset: &str, rows: Vec<Value>) {
        if rows.is_empty() {
            return;
        }
        let mut guard = self.inner.lock().expect("venue buffer");
        guard
            .rows
            .entry((source.to_owned(), dataset.to_owned()))
            .or_default()
            .extend(rows);
    }

    /// Add a cycle's rows and the progress record they complete, together.
    pub fn push_cycle(
        &self,
        source: &str,
        sets: Vec<(&str, Vec<Value>)>,
        key: &str,
        progress: Value,
    ) {
        let mut guard = self.inner.lock().expect("venue buffer");
        for (dataset, rows) in sets {
            if !rows.is_empty() {
                guard
                    .rows
                    .entry((source.to_owned(), dataset.to_owned()))
                    .or_default()
                    .extend(rows);
            }
        }
        guard.cursors.insert(key.to_owned(), progress);
    }

    /// Rows buffered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("venue buffer")
            .rows
            .values()
            .map(Vec::len)
            .sum()
    }

    /// Whether nothing is buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn take(&self) -> Pending {
        std::mem::take(&mut *self.inner.lock().expect("venue buffer"))
    }

    fn restore(&self, pending: Pending) {
        let mut guard = self.inner.lock().expect("venue buffer");
        for (key, rows) in pending.rows {
            guard.rows.entry(key).or_default().splice(0..0, rows);
        }
        for (key, value) in pending.cursors {
            guard.cursors.entry(key).or_insert(value);
        }
    }
}

/// What a flush wrote.
#[derive(Debug, Default, Clone, Copy)]
pub struct Flushed {
    /// Files written.
    pub files: u64,
    /// Rows added.
    pub rows: u64,
    /// Files that failed (their rows and every cursor stay buffered).
    pub errors: u64,
}

fn hour_of(row: &Value, table: &Table, now_ms: i64) -> String {
    let value = row.get(table.time.0).and_then(Value::as_i64);
    let ms = match (value, table.time.1) {
        (Some(v), true) => v,
        (Some(v), false) => v.saturating_mul(1000),
        (None, _) => now_ms,
    };
    hour_label(ms)
}

/// The `SELECT` that types a staged file and adds `ts`.
#[must_use]
pub fn select(table: &Table, staged: &Path) -> String {
    let ts = if table.time.1 {
        format!("make_timestamp({} * 1000)", table.time.0)
    } else {
        format!("make_timestamp({} * 1000000)", table.time.0)
    };
    format!(
        "SELECT *, {ts} AS ts FROM {}",
        read_json_array(staged, table.columns)
    )
}

/// Write everything buffered; on any failure the failed groups' rows and all cursors go back
/// to the buffer.
pub async fn flush(ctx: &Ctx, buffer: &Buffer, now_ms: i64) -> Flushed {
    let mut pending = buffer.take();
    let mut out = Flushed::default();
    let mut failed = Pending::default();
    let rows = std::mem::take(&mut pending.rows);
    for ((source, dataset), rows) in rows {
        let Some(table) = table(&dataset) else {
            continue;
        };
        let mut by_hour: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for row in rows {
            by_hour
                .entry(hour_of(&row, table, now_ms))
                .or_default()
                .push(row);
        }
        for (hour, rows) in by_hour {
            match write_hour(ctx, &source, table, &hour, &rows, now_ms).await {
                Ok(count) => {
                    out.files += 1;
                    out.rows += rows.len() as u64;
                    log(
                        "venues_file",
                        Obj::new()
                            .with("source", source.as_str())
                            .with("dataset", dataset.as_str())
                            .with("period", hour.as_str())
                            .with("rows", count)
                            .with("added", rows.len()),
                    );
                }
                Err(error) => {
                    out.errors += 1;
                    log(
                        "venues_error",
                        Obj::new()
                            .with("source", source.as_str())
                            .with("dataset", dataset.as_str())
                            .with("item", hour.as_str())
                            .with("error", safe_error(&error.to_string())),
                    );
                    failed
                        .rows
                        .entry((source.clone(), dataset.clone()))
                        .or_default()
                        .extend(rows);
                }
            }
        }
    }
    if out.errors > 0 {
        failed.cursors = pending.cursors;
        buffer.restore(failed);
        return out;
    }
    let cursors = pending.cursors;
    if !cursors.is_empty() {
        let saved = cursors.clone();
        let result = ctx
            .db
            .run(move |store| {
                for (key, value) in &saved {
                    set_progress(store, key, value)?;
                }
                Ok(())
            })
            .await;
        if result.is_err() {
            out.errors += 1;
            buffer.restore(Pending {
                rows: BTreeMap::new(),
                cursors,
            });
        }
    }
    out
}

async fn write_hour(
    ctx: &Ctx,
    source: &str,
    table: &'static Table,
    hour: &str,
    rows: &[Value],
    now_ms: i64,
) -> Result<i64, StoreError> {
    let staged = stage_json(&ctx.staging(), rows)?;
    let end_ms =
        chrono::NaiveDateTime::parse_from_str(&format!("{hour}:00:00"), "%Y-%m-%dT%H:%M:%S")
            .map(|t| t.and_utc().timestamp_millis() + 3_600_000)
            .unwrap_or(i64::MAX);
    let target = Target {
        source: source.to_owned(),
        dataset: table.dataset.to_owned(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: hour.to_owned(),
        // An hour is closed once it ended an hour ago; a late flush may still merge into it.
        complete: now_ms >= end_ms.saturating_add(3_600_000),
    };
    let sql = select(table, &staged);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &sql, table.key, &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

/// The progress record of a venue's transaction cursor.
#[must_use]
pub fn cursor_key(venue: &str) -> String {
    format!("venues/{venue}/cursor")
}

/// A cursor value: the next slot to collect.
#[must_use]
pub fn cursor_value(next: i64) -> Value {
    json!({ "next": next, "updatedAt": now() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_land_in_their_utc_hour() {
        assert_eq!(hour_label(1_760_000_000_000), "2025-10-09T08");
        let row = json!({ "block_time": 1_760_000_000 });
        assert_eq!(hour_of(&row, &EVENTS, 0), "2025-10-09T08");
        let row = json!({ "at_ms": 1_760_003_600_000_i64 });
        assert_eq!(hour_of(&row, &ACCOUNTS, 0), "2025-10-09T09");
        assert_eq!(
            hour_of(&json!({}), &EVENTS, 1_760_000_000_000),
            "2025-10-09T08"
        );
    }

    #[test]
    fn a_restored_buffer_keeps_rows_first_and_newer_cursors() {
        let buffer = Buffer::default();
        buffer.push_cycle(
            "jupiter",
            vec![("events", vec![json!({"n": 1})])],
            "k",
            json!(1),
        );
        let taken = buffer.take();
        buffer.push_cycle(
            "jupiter",
            vec![("events", vec![json!({"n": 2})])],
            "k",
            json!(2),
        );
        buffer.restore(taken);
        let pending = buffer.take();
        assert_eq!(
            pending.rows[&("jupiter".into(), "events".into())],
            vec![json!({"n": 1}), json!({"n": 2})]
        );
        assert_eq!(pending.cursors["k"], json!(2));
    }
}
