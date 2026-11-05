//! Exchange dump files walked newest first (ADR-0009, exchange dumps 2026-10-19): one zip or
//! gzip CSV per venue symbol and day (or month) at a URL the period names, converted to one
//! Parquet file per period. Every series of a source is walked period by period from the newest
//! published one down to `walk.startDate`, all series of a period before the next older period,
//! so whatever a run fetches is whole cross-sections of the recent past. The ledger's `files`
//! table is the progress record; a period the venue does not serve is remembered as `absent` once
//! it is old enough not to be "not published yet". A mark is logged and counted, an interior one
//! (a hole between published periods) is asked for again on a schedule, and a late file fills it
//! (ADR-0024). A run stops the walk after `walk.runMinutes`, at the walk's `budgetGb`, or at the
//! lane's `diskBudgetGb`.

use super::absent::{self, AbsentSet, Counts};
use super::config::{AbsentReprobe, DumpWalk};
use super::http::HttpError;
use super::ledger::{complete_periods, get_progress, register, set_progress};
use super::parquet::{Target, staging_path, write_parquet};
use super::periods::{Granularity, Period, date_of_ms, parse_date};
use super::series::{Ctx, Outcome, budget_bytes};
use crate::fsutil::sha256_hex;
use crate::jsonout::{Obj, log, safe_error};
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A period whose end is younger than this and that the venue does not serve is "not
/// published yet" and is asked for again; an older one is absent.
pub const UNPUBLISHED_DAYS: i64 = 3;
/// The lane-wide `diskBudgetGb` is re-read after this many files.
const GLOBAL_BUDGET_EVERY: u64 = 50;
/// The note a run leaves when the walk yielded to the next run.
pub const YIELDED: &str = "run time budget spent; the next runs continue the walk";
/// The note when the walk's own `budgetGb` is reached.
pub const BUDGET_REACHED: &str = "walk budgetGb reached";

/// How the file is packed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Container {
    /// A zip of one CSV.
    Zip,
    /// A gzip CSV, read by DuckDB as it is.
    Gzip,
}

/// One series of dump files: where each period's file is and how its CSV is typed.
pub trait Kind: Send + Sync {
    /// Source name.
    fn source(&self) -> &str;
    /// Dataset name.
    fn dataset(&self) -> &str;
    /// The venue's symbol.
    fn symbol(&self) -> &str;
    /// Phoenix symbol.
    fn phoenix(&self) -> &str;
    /// Day or month files.
    fn granularity(&self) -> Granularity;
    /// URL of a period's file.
    fn url(&self, label: &str) -> String;
    /// URL of the file's published SHA-256 (`.CHECKSUM`), when the venue publishes one.
    fn checksum_url(&self, _label: &str) -> Option<String> {
        None
    }
    /// How the file is packed.
    fn container(&self) -> Container;
    /// The `SELECT` typing the staged CSV; runs on the ledger's thread (it may inspect the file).
    fn select(
        &self,
        store: &mut Store,
        staged: &Path,
        constants: &str,
    ) -> Result<String, StoreError>;
}

/// Progress key of a series: the periods the venue does not serve.
#[must_use]
pub fn progress_key(kind: &dyn Kind) -> String {
    format!("{}/{}/{}", kind.source(), kind.dataset(), kind.symbol())
}

/// The periods a walk wants, newest first: from the last whole period before `now` down to the
/// one containing `start`.
#[must_use]
pub fn wanted(start: chrono::NaiveDate, granularity: Granularity, now_ms: i64) -> Vec<Period> {
    let current = Period::containing(date_of_ms(now_ms), granularity);
    let first = Period::containing(start, granularity);
    let mut out = Vec::new();
    let mut period = first;
    while period < current {
        out.push(period);
        period = period.next();
    }
    out.reverse();
    out
}

/// Whether a period the venue does not serve may still be published.
#[must_use]
pub fn maybe_unpublished(period: &Period, now_ms: i64) -> bool {
    now_ms - period.end_ms() < UNPUBLISHED_DAYS * 86_400_000
}

struct Series {
    kind: Arc<dyn Kind>,
    done: BTreeSet<String>,
    absent: AbsentSet,
    failed: bool,
}

/// What one run of the walk has done and why it stopped early, if it did.
#[derive(Default)]
struct Walker {
    outcome: Outcome,
    absent: Counts,
    used: i64,
    budget: i64,
    deadline: Option<Instant>,
    since_global_check: u64,
    stop_note: Option<&'static str>,
}

/// Walk every series of one source. `reprobe` is the schedule on which an interior absent period
/// is asked for again.
pub async fn walk(
    ctx: &Ctx,
    source: &str,
    cfg: &DumpWalk,
    kinds: Vec<Arc<dyn Kind>>,
    reprobe: &AbsentReprobe,
) -> Outcome {
    let mut walker = Walker {
        budget: budget_bytes(cfg.budget_gb),
        deadline: Instant::now().checked_add(Duration::from_secs(cfg.run_minutes * 60)),
        ..Walker::default()
    };
    let Some(start) = parse_date(&cfg.start_date) else {
        walker.outcome.errors += 1;
        return walker.outcome;
    };
    if kinds.is_empty() {
        return walker.outcome;
    }
    let mut series = Vec::new();
    for kind in kinds {
        match load(ctx, kind.clone()).await {
            Ok(s) => series.push(s),
            Err(error) => {
                walker.outcome.errors += 1;
                log_error(
                    source,
                    kind.dataset(),
                    kind.symbol(),
                    "ledger",
                    &error.to_string(),
                );
            }
        }
    }
    if !ctx.within_budget(source).await {
        walker.outcome.note = Some("diskBudgetGb is 0 or reached".into());
        return walker.finish(&series, reprobe);
    }
    let datasets: BTreeSet<String> = series.iter().map(|s| s.kind.dataset().to_owned()).collect();
    walker.used = match registered_bytes(ctx, source, &datasets).await {
        Ok(used) => used,
        Err(error) => {
            walker.outcome.errors += 1;
            log_error(source, "*", "*", "ledger", &error.to_string());
            return walker.finish(&series, reprobe);
        }
    };
    run(ctx, source, start, &mut series, &mut walker, reprobe).await;
    if let Some(note) = walker.stop_note {
        walker.outcome.note = Some(note.into());
    }
    walker.finish(&series, reprobe)
}

/// Interior holes first, then the periods newest first.
async fn run(
    ctx: &Ctx,
    source: &str,
    start: chrono::NaiveDate,
    series: &mut [Series],
    walker: &mut Walker,
    reprobe: &AbsentReprobe,
) {
    // A day the venue skipped is asked for again before the walk goes on, so a late file lands
    // even while a long backfill is still running.
    let mut asked = 0;
    'holes: for s in series.iter_mut() {
        for label in s.absent.due_interior(&s.done, ctx.now_ms, reprobe) {
            if asked >= reprobe.max_per_run {
                break 'holes;
            }
            if !walker.may_continue(ctx, source).await {
                return;
            }
            asked += 1;
            reprobe_period(ctx, s, &label, walker, reprobe).await;
        }
    }
    // Months first: the monthly series are a few small files and close the cross-section the
    // daily ones are walked against.
    for granularity in [Granularity::Month, Granularity::Day] {
        for period in wanted(start, granularity, ctx.now_ms) {
            for s in series
                .iter_mut()
                .filter(|s| s.kind.granularity() == granularity)
            {
                if !walker.may_continue(ctx, source).await {
                    return;
                }
                sync_period(ctx, s, &period, walker).await;
            }
        }
    }
}

impl Walker {
    /// The run's outcome with the absent counts: what this run marked and filled, and the
    /// interior holes the series carry now.
    fn finish(mut self, series: &[Series], reprobe: &AbsentReprobe) -> Outcome {
        let mut counts = self.absent;
        for s in series {
            let (open, given_up) = s.absent.interior(&s.done, reprobe);
            counts.interior += open;
            counts.given_up += given_up;
        }
        self.outcome.absent = Some(counts);
        self.outcome
    }

    async fn may_continue(&mut self, ctx: &Ctx, source: &str) -> bool {
        if self.stop_note.is_some() || ctx.stopping() {
            return false;
        }
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            log("augment_dump_yield", Obj::new().with("source", source));
            self.stop_note = Some(YIELDED);
            return false;
        }
        if self.used >= self.budget {
            log(
                "augment_budget_reached",
                Obj::new()
                    .with("source", source)
                    .with("usedBytes", self.used)
                    .with("budgetBytes", self.budget),
            );
            self.stop_note = Some(BUDGET_REACHED);
            return false;
        }
        if self.since_global_check >= GLOBAL_BUDGET_EVERY {
            self.since_global_check = 0;
            if !ctx.within_budget(source).await {
                self.stop_note = Some("diskBudgetGb reached");
                return false;
            }
        }
        true
    }
}

async fn registered_bytes(
    ctx: &Ctx,
    source: &str,
    datasets: &BTreeSet<String>,
) -> Result<i64, StoreError> {
    let source = source.to_owned();
    let datasets: Vec<String> = datasets.iter().cloned().collect();
    ctx.db
        .run(move |store| {
            let mut total = 0;
            for dataset in &datasets {
                let rows = store.rows(
                    "SELECT coalesce(sum(bytes), 0) AS bytes FROM files WHERE source = ? AND dataset = ?",
                    &[&source, dataset],
                )?;
                total += rows.first().and_then(|row| row.int("bytes")).unwrap_or(0);
            }
            Ok(total)
        })
        .await
}

async fn load(ctx: &Ctx, kind: Arc<dyn Kind>) -> Result<Series, StoreError> {
    let (source, dataset, symbol) = (
        kind.source().to_owned(),
        kind.dataset().to_owned(),
        kind.symbol().to_owned(),
    );
    let key = progress_key(kind.as_ref());
    let (done, progress) = ctx
        .db
        .run(move |store| {
            let done = complete_periods(store, &source, &dataset, &symbol)?;
            Ok((done, get_progress(store, &key)?))
        })
        .await?;
    Ok(Series {
        kind,
        done: done.into_iter().collect(),
        absent: AbsentSet::from_progress(progress.as_ref()),
        failed: false,
    })
}

async fn sync_period(ctx: &Ctx, s: &mut Series, period: &Period, walker: &mut Walker) {
    let label = period.label();
    if s.failed || s.done.contains(&label) || s.absent.contains(&label) {
        return;
    }
    let kind = s.kind.clone();
    match fetch(ctx, kind.as_ref(), &label).await {
        Ok(body) => match convert(ctx, kind.clone(), &label, body, None).await {
            Ok((rows, bytes)) => stored(s, walker, &label, rows, bytes),
            Err(error) => fail(s, walker, &label, &error.to_string()),
        },
        Err(HttpError::NotFound) => {
            if maybe_unpublished(period, ctx.now_ms) {
                return;
            }
            mark_absent(ctx, s, walker, &label).await;
        }
        Err(error) => fail(s, walker, &label, &error.to_string()),
    }
}

/// Book a file that was converted and registered.
fn stored(s: &mut Series, walker: &mut Walker, label: &str, rows: i64, bytes: i64) {
    s.done.insert(label.to_owned());
    walker.used += bytes;
    walker.since_global_check += 1;
    walker.outcome.files += 1;
    walker.outcome.rows += u64::try_from(rows).unwrap_or(0);
    log(
        "augment_file",
        Obj::new()
            .with("source", s.kind.source())
            .with("dataset", s.kind.dataset())
            .with("symbol", s.kind.symbol())
            .with("period", label)
            .with("rows", rows)
            .with("complete", true),
    );
}

/// Remember an old 404 as absent, durably, then log and count it (ADR-0024).
async fn mark_absent(ctx: &Ctx, s: &mut Series, walker: &mut Walker, label: &str) {
    let position = absent::position(label, &s.done);
    s.absent.mark(label, ctx.now_ms);
    if let Err(error) = save(ctx, s.kind.as_ref(), &s.absent).await {
        s.absent.clear(label);
        fail(s, walker, label, &error.to_string());
        return;
    }
    walker.absent.marked += 1;
    absent::log_marked(
        s.kind.source(),
        s.kind.dataset(),
        s.kind.symbol(),
        label,
        position,
    );
}

/// Ask again for an interior absent period. A file is verified, written durably and registered
/// in one transaction with the progress record that no longer lists the period as absent, so a
/// crash before it leaves the mark and no registered file. A 404 spends one probe of the
/// schedule; any other failure is an error that waits for the next interval.
async fn reprobe_period(
    ctx: &Ctx,
    s: &mut Series,
    label: &str,
    walker: &mut Walker,
    reprobe: &AbsentReprobe,
) {
    let kind = s.kind.clone();
    let failure = match fetch(ctx, kind.as_ref(), label).await {
        Ok(body) => {
            let mut after = s.absent.clone();
            after.clear(label);
            let progress = Some((progress_key(kind.as_ref()), after.to_progress()));
            match convert(ctx, kind.clone(), label, body, progress).await {
                Ok((rows, bytes)) => {
                    let misses = s.absent.misses(label);
                    s.absent = after;
                    stored(s, walker, label, rows, bytes);
                    walker.absent.filled += 1;
                    absent::log_filled(kind.source(), kind.dataset(), kind.symbol(), label, misses);
                    return;
                }
                Err(error) => error.to_string(),
            }
        }
        Err(HttpError::NotFound) => {
            let spent = s.absent.missed(label, ctx.now_ms, reprobe);
            match save(ctx, kind.as_ref(), &s.absent).await {
                Ok(()) if spent => absent::log_given_up(
                    kind.source(),
                    kind.dataset(),
                    kind.symbol(),
                    label,
                    s.absent.misses(label),
                ),
                Ok(()) => {}
                Err(error) => {
                    walker.outcome.errors += 1;
                    log_error(
                        kind.source(),
                        kind.dataset(),
                        kind.symbol(),
                        label,
                        &error.to_string(),
                    );
                }
            }
            return;
        }
        Err(error) => error.to_string(),
    };
    // The series' own walk goes on: this is an old hole, not the period it is waiting for.
    walker.outcome.errors += 1;
    log_error(
        kind.source(),
        kind.dataset(),
        kind.symbol(),
        label,
        &failure,
    );
    s.absent.attempted(label, ctx.now_ms);
    let _ = save(ctx, kind.as_ref(), &s.absent).await;
}

/// Write a series' absent record.
async fn save(ctx: &Ctx, kind: &dyn Kind, absent: &AbsentSet) -> Result<(), StoreError> {
    let (key, value) = (progress_key(kind), absent.to_progress());
    ctx.db
        .run(move |store| set_progress(store, &key, &value))
        .await
}

/// A failed period stops its series for this run; the next run asks again.
fn fail(s: &mut Series, walker: &mut Walker, label: &str, error: &str) {
    s.failed = true;
    walker.outcome.errors += 1;
    log_error(
        s.kind.source(),
        s.kind.dataset(),
        s.kind.symbol(),
        label,
        error,
    );
}

/// The file's bytes, checked against its published SHA-256 when there is one. The two requests
/// go out together (still spaced by the host limiter), so their round trips overlap.
async fn fetch(ctx: &Ctx, kind: &dyn Kind, label: &str) -> Result<Vec<u8>, HttpError> {
    let url = kind.url(label);
    let checksum_url = kind.checksum_url(label);
    let (body, checksum) = tokio::join!(ctx.http.get_bytes(&url, &[]), async {
        match &checksum_url {
            Some(u) => ctx.http.get_bytes(u, &[]).await.map(Some),
            None => Ok(None),
        }
    });
    let body = body?.body;
    if let Some(checksum) = checksum? {
        let expected = super::binance::parse_checksum(&String::from_utf8_lossy(&checksum.body))
            .ok_or_else(|| HttpError::Decode("unreadable CHECKSUM".into()))?;
        if sha256_hex(&body) != expected {
            return Err(HttpError::Decode("checksum mismatch".into()));
        }
    }
    Ok(body)
}

/// Stage the CSV (unzipped off the async threads), type it into Parquet, register it, and with
/// `progress` write that progress record in the same transaction.
async fn convert(
    ctx: &Ctx,
    kind: Arc<dyn Kind>,
    label: &str,
    body: Vec<u8>,
    progress: Option<(String, Value)>,
) -> Result<(i64, i64), StoreError> {
    let staging = ctx.staging();
    let container = kind.container();
    let staged = tokio::task::spawn_blocking(move || stage(&staging, container, &body))
        .await
        .map_err(|e| StoreError::Check(format!("staging task: {e}")))??;
    let target = Target {
        source: kind.source().to_owned(),
        dataset: kind.dataset().to_owned(),
        symbol: kind.symbol().to_owned(),
        phoenix_symbol: Some(kind.phoenix().to_owned()),
        period: label.to_owned(),
        complete: true,
    };
    let (root, path) = (ctx.root.clone(), staged.clone());
    let result = ctx
        .db
        .run(move |store| {
            let select = kind.select(store, &path, &target.constant_columns())?;
            let record = write_parquet(store, &root, &select, &target)?;
            let progress = progress.as_ref().map(|(key, value)| (key.as_str(), value));
            register(store, &record, progress)?;
            Ok((record.row_count, record.bytes))
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

/// Write the CSV to `staging`: a zip's one entry streamed out (its CRC-32 is checked at the end
/// of the entry), a gzip as it came.
pub fn stage(staging: &Path, container: Container, body: &[u8]) -> Result<PathBuf, StoreError> {
    std::fs::create_dir_all(staging)?;
    match container {
        Container::Gzip => {
            let path = staging_path(staging, "csv.gz");
            std::fs::write(&path, body)?;
            Ok(path)
        }
        Container::Zip => {
            let path = staging_path(staging, "csv");
            let mut archive = zip::ZipArchive::new(std::io::Cursor::new(body))
                .map_err(|e| StoreError::Check(format!("zip: {e}")))?;
            if archive.is_empty() {
                return Err(StoreError::Check("zip has no entry".into()));
            }
            let mut entry = archive
                .by_index(0)
                .map_err(|e| StoreError::Check(format!("zip: {e}")))?;
            let mut file = std::fs::File::create(&path)?;
            let copied = std::io::copy(&mut entry, &mut file);
            if let Err(error) = copied {
                let _ = std::fs::remove_file(&path);
                return Err(StoreError::Check(format!("zip: {error}")));
            }
            Ok(path)
        }
    }
}

/// Whether the staged CSV starts with a header line (a digit first means it does not).
pub fn staged_has_header(path: &Path) -> Result<bool, StoreError> {
    let mut first = [0u8; 1];
    let read = std::fs::File::open(path)?.read(&mut first)?;
    Ok(read == 0 || !first[0].is_ascii_digit())
}

/// Whether the staged CSV starts with `prefix` (a header line whose first column is known).
pub fn staged_starts_with(path: &Path, prefix: &str) -> Result<bool, StoreError> {
    let mut head = vec![0u8; prefix.len()];
    let mut file = std::fs::File::open(path)?;
    let mut read = 0;
    while read < head.len() {
        let n = file.read(&mut head[read..])?;
        if n == 0 {
            break;
        }
        read += n;
    }
    Ok(read == 0 || head[..read] == *prefix.as_bytes())
}

fn log_error(source: &str, dataset: &str, symbol: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", source)
            .with("dataset", dataset)
            .with("symbol", symbol)
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn periods_newest_first() {
        let start = parse_date("2026-09-29").unwrap();
        let now = 1_791_547_200_000; // 2026-10-09T12:00Z
        let days: Vec<String> = wanted(start, Granularity::Day, now)
            .iter()
            .map(Period::label)
            .collect();
        assert_eq!(days.first().map(String::as_str), Some("2026-10-08"));
        assert_eq!(days.last().map(String::as_str), Some("2026-09-29"));
        assert_eq!(days.len(), 10);
        let months: Vec<String> =
            wanted(parse_date("2026-07-15").unwrap(), Granularity::Month, now)
                .iter()
                .map(Period::label)
                .collect();
        assert_eq!(months, ["2026-09", "2026-08", "2026-07"]);
        let yesterday = Period::parse("2026-10-08").unwrap();
        assert!(maybe_unpublished(&yesterday, now));
        assert!(!maybe_unpublished(
            &Period::parse("2026-10-01").unwrap(),
            now
        ));
    }

    #[test]
    fn stages_zip_and_gzip() {
        let dir = std::env::temp_dir().join(format!("dumps-{}", uuid::Uuid::new_v4()));
        let csv = b"a,b\n1,2\n";
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buffer);
            writer
                .start_file("x.csv", zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::Write::write_all(&mut writer, csv).unwrap();
            writer.finish().unwrap();
        }
        let path = stage(&dir, Container::Zip, buffer.get_ref()).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), csv);
        assert!(staged_has_header(&path).unwrap());
        let raw = stage(&dir, Container::Gzip, b"1,2\n").unwrap();
        assert!(raw.to_string_lossy().ends_with(".csv.gz"));
        assert!(!staged_has_header(&raw).unwrap());
        assert!(stage(&dir, Container::Zip, b"not a zip").is_err());
        let _ = std::fs::remove_dir_all(dir);
    }
}
