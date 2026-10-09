//! Paged histories as period files. A series is one (source, dataset, symbol); the driver walks
//! the periods from the start date, fetches each period's rows, writes one Parquet file per
//! period and records how far the series is complete. The current, open period is rewritten on
//! every run; a closed period is fetched once.

use super::http::Http;
use super::ledger::{get_progress, register};
use super::parquet::{Target, stage_json, write_parquet};
use super::periods::{Granularity, Period, periods_through};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use chrono::NaiveDate;
use serde_json::{Value, json};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Everything a sync run shares.
pub struct Ctx {
    /// The HTTP client.
    pub http: Http,
    /// The ledger's writer thread.
    pub db: Db,
    /// Augment root (`tables/` lives here).
    pub root: PathBuf,
    /// The lane this process runs as; names the ledger, staging and snapshots.
    pub lane: super::ledger::Lane,
    /// First day stored.
    pub start: NaiveDate,
    /// The instant the run considers "now", milliseconds.
    pub now_ms: i64,
    /// Set on SIGTERM; sources stop between items.
    pub stop: Arc<AtomicBool>,
    /// Bytes the large datasets may bring the lane's files to; 0 disables them.
    pub disk_budget_bytes: i64,
}

impl Ctx {
    /// Whether a stop was requested.
    #[must_use]
    pub fn stopping(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// The lane's staging directory.
    #[must_use]
    pub fn staging(&self) -> PathBuf {
        self.lane.staging_dir(&self.root)
    }

    /// Whether a large dataset may add a file: the lane's registered bytes are under the budget.
    /// Logs `augment_budget_reached` once per call that refuses.
    pub async fn within_budget(&self, dataset: &str) -> bool {
        if self.disk_budget_bytes <= 0 {
            return false;
        }
        let used = self
            .db
            .run(|store| {
                let rows = store.rows("SELECT coalesce(sum(bytes), 0) AS bytes FROM files", &[])?;
                Ok(rows.first().and_then(|row| row.int("bytes")).unwrap_or(0))
            })
            .await
            .unwrap_or(i64::MAX);
        if used < self.disk_budget_bytes {
            return true;
        }
        log(
            "augment_budget_reached",
            Obj::new()
                .with("dataset", dataset)
                .with("usedBytes", used)
                .with("budgetBytes", self.disk_budget_bytes),
        );
        false
    }
}

/// A boxed future of rows.
pub type RowsFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Value>, StoreError>> + Send + 'a>>;

/// One paged history.
pub trait Series: Send + Sync {
    /// Source name.
    fn source(&self) -> &str;
    /// Dataset name.
    fn dataset(&self) -> &str;
    /// The venue's symbol.
    fn symbol(&self) -> &str;
    /// Phoenix symbol, when the series is a Phoenix market.
    fn phoenix_symbol(&self) -> Option<&str>;
    /// Day or month files.
    fn granularity(&self) -> Granularity;
    /// Publication lag: a period is closed once `now >= end + lag`.
    fn lag_ms(&self) -> i64;
    /// First day the series stores; the lane's `startDate` unless the series reaches further
    /// back (a full history that is cheap to keep).
    fn start_date(&self, lane_start: NaiveDate) -> NaiveDate {
        lane_start
    }
    /// Every row with a timestamp in `[start_ms, end_ms)`, as JSON objects.
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a>;
    /// The `SELECT` that types the rows staged as a JSON array at `staged`; `constants` is the
    /// SQL of the two symbol columns every file carries.
    fn select(&self, staged: &Path, constants: &str) -> String;
}

/// What one series did in a run.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    /// Files written (closed and open periods).
    pub files: u64,
    /// Rows written.
    pub rows: u64,
    /// Fetch failures (the series stops for this run at the first).
    pub errors: u64,
    /// Requests the series could skip because the periods were already closed.
    pub skipped: u64,
}

impl Outcome {
    /// Sum two outcomes.
    pub fn add(&mut self, other: &Outcome) {
        self.files += other.files;
        self.rows += other.rows;
        self.errors += other.errors;
        self.skipped += other.skipped;
    }

    /// As a JSON object.
    #[must_use]
    pub fn to_obj(&self) -> Obj {
        Obj::new()
            .with("files", self.files)
            .with("rows", self.rows)
            .with("errors", self.errors)
            .with("skipped", self.skipped)
    }
}

/// `diskBudgetGb` as bytes.
#[must_use]
pub fn budget_bytes(gb: f64) -> i64 {
    if gb <= 0.0 || !gb.is_finite() {
        return 0;
    }
    (gb * 1e9).min(i64::MAX as f64) as i64
}

/// Progress key of a series.
#[must_use]
pub fn progress_key(source: &str, dataset: &str, symbol: &str) -> String {
    format!("{source}/{dataset}/{symbol}")
}

/// The label the series is complete through, from its progress record.
pub async fn complete_through(ctx: &Ctx, series: &dyn Series) -> Option<String> {
    let key = progress_key(series.source(), series.dataset(), series.symbol());
    ctx.db
        .run(move |store| get_progress(store, &key))
        .await
        .ok()
        .flatten()
        .and_then(|p| {
            p.get("completeThrough")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

/// Whether a series whose last row can be no later than `end_ms` has nothing left to fetch.
pub async fn finished(ctx: &Ctx, series: &dyn Series, end_ms: i64) -> bool {
    let last = Period::containing(super::periods::date_of_ms(end_ms), series.granularity());
    complete_through(ctx, series)
        .await
        .is_some_and(|label| label >= last.label())
}

/// Walk the series' periods from its progress record to now.
pub async fn sync_series(ctx: &Ctx, series: &dyn Series) -> Outcome {
    let mut outcome = Outcome::default();
    let key = progress_key(series.source(), series.dataset(), series.symbol());
    let key_lookup = key.clone();
    let progress = match ctx
        .db
        .run(move |store| get_progress(store, &key_lookup))
        .await
    {
        Ok(progress) => progress,
        Err(error) => {
            log_error(series, "progress", &error.to_string());
            outcome.errors += 1;
            return outcome;
        }
    };
    let first = progress
        .as_ref()
        .and_then(|p| p.get("completeThrough"))
        .and_then(Value::as_str)
        .and_then(Period::parse)
        .map_or_else(
            || Period::containing(series.start_date(ctx.start), series.granularity()),
            Period::next,
        );
    for period in periods_through(first, ctx.now_ms) {
        if ctx.stopping() {
            break;
        }
        let end = period.end_ms().min(ctx.now_ms);
        let rows = match series.fetch(&ctx.http, period.start_ms(), end).await {
            Ok(rows) => rows,
            Err(error) => {
                log_error(series, &period.label(), &error.to_string());
                outcome.errors += 1;
                break;
            }
        };
        let complete = period.is_complete(ctx.now_ms, series.lag_ms());
        if rows.is_empty() && !complete {
            break;
        }
        let progress =
            complete.then(|| json!({ "completeThrough": period.label(), "updatedAt": now() }));
        if rows.is_empty() {
            if let Err(error) = record_empty(ctx, &key, &progress).await {
                log_error(series, &period.label(), &error.to_string());
                outcome.errors += 1;
                break;
            }
            continue;
        }
        match publish(ctx, series, &period, complete, &rows, &key, progress).await {
            Ok(count) => {
                outcome.files += 1;
                outcome.rows += count;
            }
            Err(error) => {
                log_error(series, &period.label(), &error.to_string());
                outcome.errors += 1;
                break;
            }
        }
    }
    outcome
}

async fn record_empty(ctx: &Ctx, key: &str, progress: &Option<Value>) -> Result<(), StoreError> {
    let Some(value) = progress.clone() else {
        return Ok(());
    };
    let key = key.to_owned();
    ctx.db
        .run(move |store| super::ledger::set_progress(store, &key, &value))
        .await
}

async fn publish(
    ctx: &Ctx,
    series: &dyn Series,
    period: &Period,
    complete: bool,
    rows: &[Value],
    key: &str,
    progress: Option<Value>,
) -> Result<u64, StoreError> {
    let staged = stage_json(&ctx.staging(), rows)?;
    let target = Target {
        source: series.source().to_owned(),
        dataset: series.dataset().to_owned(),
        symbol: series.symbol().to_owned(),
        phoenix_symbol: series.phoenix_symbol().map(str::to_owned),
        period: period.label(),
        complete,
    };
    let select = series.select(&staged, &target.constant_columns());
    let expected = i64::try_from(rows.len()).unwrap_or(i64::MAX);
    let key = key.to_owned();
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_parquet(store, &root, &select, &target)?;
            if record.row_count != expected {
                return Err(StoreError::Check(format!(
                    "{} row count mismatch: {} written, {expected} fetched",
                    record.path, record.row_count
                )));
            }
            register(store, &record, progress.as_ref().map(|p| (key.as_str(), p)))?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    let count = result?;
    log(
        "augment_file",
        Obj::new()
            .with("source", series.source())
            .with("dataset", series.dataset())
            .with("symbol", series.symbol())
            .with("period", period.label())
            .with("rows", count)
            .with("complete", complete),
    );
    Ok(u64::try_from(count).unwrap_or(0))
}

fn log_error(series: &dyn Series, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", series.source())
            .with("dataset", series.dataset())
            .with("symbol", series.symbol())
            .with("item", item)
            .with("error", crate::jsonout::safe_error(error)),
    );
}
