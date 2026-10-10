//! Elfa v3 (free today, undocumented): events, calls, call episodes and the crypto call-book
//! bars, pulled from the last `to` with ascending cursors into day files (episodes page newest
//! first instead, see `episodes`); calls, episodes and
//! bars hourly, events every minute (two pages at most, so the poll stays within two requests
//! a minute of the key's sixty). Every row records the instant it was received. The credit
//! guard is per endpoint: a v3 answer whose `x-elfa-credits` header declares a credit disables
//! that endpoint for the rest of the process (`elfa_billing_started`), and the others keep
//! running. Only when an answer carries no header at all does the hourly cycle fall back to the
//! key-wide check (`credits.used` before and after, under the billing lock shared with the Auto
//! lane) and disable the whole lane; with headers present a movement of `credits.used` that no
//! answer declared is logged as unattributed (`elfa_cycle_credits`) and disables nothing.

use super::http::{Http, with_query};
use super::ledger::{get_progress, register};
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Pages per stream per cycle by default; the rest waits for the next cycle, which resumes from
/// the newest row received.
pub const MAX_PAGES: usize = 200;

/// One Elfa stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    /// `/v3/events`.
    Events,
    /// `/v3/calls`.
    Calls,
    /// `/v3/calls/episodes`.
    Episodes,
    /// `/v3/market/crypto/call-book`.
    CallBook,
}

impl Stream {
    /// Every stream.
    pub const ALL: [Stream; 4] = [
        Stream::Events,
        Stream::Calls,
        Stream::Episodes,
        Stream::CallBook,
    ];

    /// Dataset name.
    #[must_use]
    pub fn dataset(self) -> &'static str {
        match self {
            Stream::Events => "events",
            Stream::Calls => "calls",
            Stream::Episodes => "episodes",
            Stream::CallBook => "call_book",
        }
    }

    /// API path.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Stream::Events => "/v3/events",
            Stream::Calls => "/v3/calls",
            Stream::Episodes => "/v3/calls/episodes",
            Stream::CallBook => "/v3/market/crypto/call-book",
        }
    }

    /// The array field of an answer.
    #[must_use]
    pub fn field(self) -> &'static str {
        match self {
            Stream::Events => "events",
            Stream::Calls => "calls",
            Stream::Episodes => "episodes",
            Stream::CallBook => "bars",
        }
    }

    /// Page size.
    #[must_use]
    pub fn limit(self) -> u32 {
        match self {
            Stream::CallBook => 200,
            _ => 30,
        }
    }

    /// Dedupe key column in the Parquet file.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Stream::CallBook => "bar_at",
            _ => "id",
        }
    }
}

/// The mutual exclusion between the hourly cycle's credit guard and the Auto lane's paid
/// calls: whoever holds it owns the movement of `credits.used` meanwhile.
pub type Billing = Arc<tokio::sync::Mutex<()>>;

/// Counters of the minute poll.
#[derive(Default)]
pub struct PollCounters {
    /// Polls run.
    pub polls: AtomicU64,
    /// Requests those polls made.
    pub requests: AtomicU64,
    /// Rows received.
    pub rows: AtomicU64,
    /// Failed polls.
    pub errors: AtomicU64,
}

/// The lane's settings and state.
pub struct ElfaLane {
    /// API base.
    pub base_url: String,
    /// `x-elfa-api-key`.
    pub key: String,
    /// First instant to pull, seconds.
    pub start_s: i64,
    /// Pages per stream per hourly cycle.
    pub max_pages: usize,
    /// Pages per events poll; 0 keeps events in the hourly cycle.
    pub events_pages: usize,
    /// The billing lock.
    pub billing: Billing,
    /// The minute poll's counters.
    pub poll: PollCounters,
    disabled: AtomicBool,
    billed: Mutex<BTreeSet<String>>,
}

impl ElfaLane {
    /// A lane for a key.
    #[must_use]
    pub fn new(base_url: &str, key: &str, start_s: i64) -> ElfaLane {
        ElfaLane {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            start_s,
            max_pages: MAX_PAGES,
            events_pages: 0,
            billing: Arc::new(tokio::sync::Mutex::new(())),
            poll: PollCounters::default(),
            disabled: AtomicBool::new(false),
            billed: Mutex::new(BTreeSet::new()),
        }
    }

    /// Whether the key-wide fallback guard switched the whole lane off.
    #[must_use]
    pub fn disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    /// Whether a stream's endpoint may be called: neither it nor the lane was disabled.
    #[must_use]
    pub fn stream_enabled(&self, stream: Stream) -> bool {
        !self.disabled() && !self.billed.lock().expect("billed").contains(stream.path())
    }

    /// The endpoints the guard disabled because an answer declared credits.
    #[must_use]
    pub fn billed_endpoints(&self) -> Vec<String> {
        self.billed
            .lock()
            .expect("billed")
            .iter()
            .cloned()
            .collect()
    }

    /// Whether the guard switched anything off (the lane or one endpoint).
    #[must_use]
    pub fn guard_tripped(&self) -> bool {
        self.disabled() || !self.billed.lock().expect("billed").is_empty()
    }

    /// Apply the per-endpoint guard to one answer's declared credits: anything above zero
    /// disables the endpoint. Returns whether it did.
    pub fn guard(&self, path: &str, credits: Option<i64>) -> bool {
        let Some(credits) = credits.filter(|c| *c > 0) else {
            return false;
        };
        if self.billed.lock().expect("billed").insert(path.to_owned()) {
            log(
                "elfa_billing_started",
                Obj::new()
                    .with("endpoint", path)
                    .with("credits", credits)
                    .with("action", "endpoint disabled until restart"),
            );
        }
        true
    }

    /// The streams the hourly cycle pulls: everything, or everything but the events when they
    /// have their own poll.
    #[must_use]
    pub fn hourly_streams(&self) -> Vec<Stream> {
        Stream::ALL
            .into_iter()
            .filter(|s| self.events_pages == 0 || *s != Stream::Events)
            .collect()
    }

    fn headers(&self) -> [(&str, &str); 2] {
        headers(&self.key)
    }

    /// One v3 page. A disabled endpoint is refused; an answer that declares credits disables
    /// its endpoint (the page already paid for is still returned).
    pub(super) async fn get(
        &self,
        http: &Http,
        path: &str,
        pairs: &[(&str, String)],
    ) -> Result<Value, StoreError> {
        if self.disabled() || self.billed.lock().expect("billed").contains(path) {
            return Err(StoreError::Check(format!(
                "{path} disabled by the credit guard"
            )));
        }
        let url = with_query(&format!("{}{path}", self.base_url), pairs);
        let fetched = http.get_bytes(&url, &self.headers()).await?;
        let credits = fetched
            .header("x-elfa-credits")
            .and_then(|c| c.trim().parse::<f64>().ok())
            .map(|c| c.ceil() as i64);
        self.guard(path, credits);
        serde_json::from_slice(&fetched.body).map_err(|e| StoreError::Check(e.to_string()))
    }

    /// `credits.used` and `historyFrom` from `/v3/key-status`.
    pub async fn key_status(&self, http: &Http) -> Result<(i64, Option<i64>), StoreError> {
        key_status(http, &self.base_url, &self.key).await
    }
}

/// The headers every Elfa request carries.
#[must_use]
pub fn headers(key: &str) -> [(&str, &str); 2] {
    [("x-elfa-api-key", key), ("accept", "application/json")]
}

/// `credits.used` and `historyFrom` from `/v3/key-status` (free).
pub async fn key_status(
    http: &Http,
    base_url: &str,
    key: &str,
) -> Result<(i64, Option<i64>), StoreError> {
    let status = http
        .get_json(
            &format!("{}/v3/key-status", base_url.trim_end_matches('/')),
            &headers(key),
        )
        .await?;
    let used = status
        .get("credits")
        .and_then(|c| c.get("used"))
        .and_then(Value::as_i64)
        .ok_or_else(|| StoreError::Check("key-status has no credits.used".into()))?;
    Ok((used, status.get("historyFrom").and_then(Value::as_i64)))
}

/// One events poll: the next pages since the last `to`, free, outside the billing lock.
pub async fn poll_events(ctx: &Ctx, lane: &ElfaLane, now_s: i64) -> Outcome {
    let mut outcome = Outcome::default();
    if !lane.stream_enabled(Stream::Events) || capped(ctx) {
        outcome.skipped += 1;
        return outcome;
    }
    let result = pull_stream(ctx, lane, Stream::Events, None, now_s, lane.events_pages).await;
    lane.poll.polls.fetch_add(1, Ordering::Relaxed);
    match result {
        Ok(result) => {
            lane.poll
                .requests
                .fetch_add(result.requests, Ordering::Relaxed);
            lane.poll.rows.fetch_add(result.rows, Ordering::Relaxed);
            outcome.add(&result);
        }
        Err(error) => {
            lane.poll.errors.fetch_add(1, Ordering::Relaxed);
            outcome.errors += 1;
            log_error(Stream::Events, "poll", &error.to_string());
        }
    }
    log(
        "elfa_events_poll",
        Obj::new()
            .with("requests", outcome.requests)
            .with("rows", outcome.rows)
            .with("errors", outcome.errors),
    );
    outcome
}

/// One hourly cycle: the hourly streams from their last `to` up to `now_s`, under the credit
/// guard and the billing lock.
pub async fn cycle(ctx: &Ctx, lane: &ElfaLane, now_s: i64) -> Outcome {
    let mut outcome = Outcome::default();
    if lane.disabled() || capped(ctx) {
        outcome.skipped += 1;
        return outcome;
    }
    let _billing = lane.billing.lock().await;
    let metered_before = metered(ctx);
    let (used_before, history_from) = match lane.key_status(&ctx.http).await {
        Ok(status) => status,
        Err(error) => {
            outcome.errors += 1;
            log_error(Stream::Events, "key-status", &error.to_string());
            return outcome;
        }
    };
    for stream in lane.hourly_streams() {
        if ctx.stopping() {
            break;
        }
        if !lane.stream_enabled(stream) {
            outcome.skipped += 1;
            continue;
        }
        let pulled = if stream == Stream::Episodes {
            let start_s = lane.start_s.max(history_from.unwrap_or(0));
            super::episodes::pull(ctx, lane, start_s, now_s, lane.max_pages).await
        } else {
            pull_stream(ctx, lane, stream, history_from, now_s, lane.max_pages).await
        };
        match pulled {
            Ok(result) => outcome.add(&result),
            Err(error) => {
                outcome.errors += 1;
                log_error(stream, "cycle", &error.to_string());
            }
        }
    }
    match lane.key_status(&ctx.http).await {
        Ok((used_after, _)) => {
            let mut reading =
                CycleCredits::between(metered_before, metered(ctx), used_before, used_after);
            if ctx.http.meter().is_none() {
                // Without a meter no header was read: every movement is the key-wide guard's.
                reading.unheadered = reading.unheadered.max(1);
            }
            if reading.disables_lane() {
                lane.disabled.store(true, Ordering::Relaxed);
            }
            reading.log(reading.disables_lane());
        }
        Err(error) => {
            outcome.errors += 1;
            log_error(Stream::Events, "key-status", &error.to_string());
        }
    }
    outcome
}

/// Whether the Elfa client's monthly credit cap is reached.
#[must_use]
pub fn capped(ctx: &Ctx) -> bool {
    ctx.http
        .meter()
        .is_some_and(|m| !m.allows(0, chrono::Utc::now().timestamp_millis()))
}

/// The meter's month total and its count of answers without a header, for one cycle's reading.
fn metered(ctx: &Ctx) -> (i64, u64) {
    ctx.http.meter().map_or((0, 0), |m| {
        let state = m.snapshot();
        let no_header = state
            .endpoints
            .iter()
            .filter(|(path, _)| path.starts_with("/v3/") && path.as_str() != "/v3/key-status")
            .map(|(_, cost)| cost.no_header)
            .sum();
        (state.spent, no_header)
    })
}

/// What one hourly cycle did to the key: `credits.used` moved by `used`, the answers declared
/// `declared`, and `unheadered` v3 data answers carried no header.
#[derive(Debug, PartialEq, Eq)]
pub struct CycleCredits {
    /// Movement of `credits.used`.
    pub used: i64,
    /// Credits the answers declared.
    pub declared: i64,
    /// v3 data answers without `x-elfa-credits`.
    pub unheadered: u64,
}

impl CycleCredits {
    /// The reading from the meter's totals and `credits.used` before and after.
    #[must_use]
    pub fn between(
        before: (i64, u64),
        after: (i64, u64),
        used_before: i64,
        used_after: i64,
    ) -> Self {
        CycleCredits {
            used: used_after - used_before,
            declared: after.0 - before.0,
            unheadered: after.1.saturating_sub(before.1),
        }
    }

    /// `credits.used` moved by more than the answers declared.
    #[must_use]
    pub fn unattributed(&self) -> i64 {
        (self.used - self.declared).max(0)
    }

    /// The key-wide fallback: the key moved by more than declared while some v3 answer carried
    /// no header, so the movement may be the v3 reads themselves.
    #[must_use]
    pub fn disables_lane(&self) -> bool {
        self.unattributed() > 0 && self.unheadered > 0
    }

    fn log(&self, disabled: bool) {
        log(
            "elfa_cycle_credits",
            Obj::new()
                .with("usedDelta", self.used)
                .with("declared", self.declared)
                .with("unattributed", self.unattributed())
                .with("answersWithoutHeader", self.unheadered)
                .with(
                    "action",
                    if disabled {
                        "lane disabled until restart (answers without a header)"
                    } else {
                        "none"
                    },
                ),
        );
    }
}

/// Progress key of a stream.
#[must_use]
pub fn progress_key(stream: Stream) -> String {
    format!("elfa/{}", stream.dataset())
}

async fn pull_stream(
    ctx: &Ctx,
    lane: &ElfaLane,
    stream: Stream,
    history_from: Option<i64>,
    now_s: i64,
    max_pages: usize,
) -> Result<Outcome, StoreError> {
    let key = progress_key(stream);
    let lookup = key.clone();
    let last_to = ctx
        .db
        .run(move |store| get_progress(store, &lookup))
        .await?
        .and_then(|p| p.get("lastTo").and_then(Value::as_i64));
    let from = last_to.map_or(lane.start_s.max(history_from.unwrap_or(0)), |t| t + 1);
    let to = now_s - 60;
    let mut outcome = Outcome::default();
    if from > to {
        return Ok(outcome);
    }
    let mut rows: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    let mut has_more = true;
    let received_at_ms = chrono::Utc::now().timestamp_millis();
    while has_more && pages < max_pages {
        let mut pairs: Vec<(&str, String)> = vec![
            ("from", from.to_string()),
            ("to", to.to_string()),
            ("limit", stream.limit().to_string()),
        ];
        if stream != Stream::CallBook {
            pairs.push(("order", "asc".into()));
        }
        if let Some(c) = &cursor {
            pairs.push(("cursor", c.clone()));
        }
        let answer = lane.get(&ctx.http, stream.path(), &pairs).await?;
        let page = answer
            .get(stream.field())
            .and_then(Value::as_array)
            .ok_or_else(|| {
                StoreError::Check(format!(
                    "{} answer has no {}",
                    stream.dataset(),
                    stream.field()
                ))
            })?;
        rows.extend(
            page.iter()
                .filter_map(|row| flatten(stream, row, received_at_ms)),
        );
        has_more = answer
            .get("hasMore")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        cursor = answer
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            has_more = false;
        }
        pages += 1;
    }
    outcome.requests = pages as u64;
    // Where the next cycle resumes: `to` when the pull finished, otherwise one second before the
    // newest row received (rows sharing that second may be split across pages; the merge
    // deduplicates on id).
    let newest = rows
        .iter()
        .filter_map(|r| r.get(day_field(stream)).and_then(Value::as_i64))
        .max();
    let complete_to = match (has_more, newest) {
        (false, _) => Some(to),
        (true, Some(newest)) => Some((newest - 1).max(from)),
        (true, None) => None,
    };
    let by_day = group_by_day(rows);
    for (day, day_rows) in by_day {
        let count = write_day(
            ctx,
            stream,
            day,
            &day_rows,
            day.start < date_of_ms(now_s * 1000),
            complete_to,
            &key,
        )
        .await?;
        outcome.files += 1;
        outcome.rows += u64::try_from(day_rows.len()).unwrap_or(0);
        log(
            "augment_file",
            Obj::new()
                .with("source", "elfa")
                .with("dataset", stream.dataset())
                .with("symbol", "ALL")
                .with("period", day.label())
                .with("rows", count)
                .with("added", day_rows.len()),
        );
    }
    if let Some(to) = complete_to
        && outcome.files == 0
    {
        let value = json!({ "lastTo": to, "updatedAt": now() });
        ctx.db
            .run(move |store| super::ledger::set_progress(store, &key, &value))
            .await?;
    }
    Ok(outcome)
}

pub(super) async fn write_day(
    ctx: &Ctx,
    stream: Stream,
    day: Period,
    rows: &[Value],
    complete: bool,
    complete_to: Option<i64>,
    key: &str,
) -> Result<i64, StoreError> {
    let staged = stage_json(&ctx.staging(), rows)?;
    let target = Target {
        source: "elfa".into(),
        dataset: stream.dataset().into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete,
    };
    let select = select(stream, &staged, &target.constant_columns());
    let progress = complete_to.map(|to| json!({ "lastTo": to, "updatedAt": now() }));
    let (root, key) = (ctx.root.clone(), key.to_owned());
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, stream.key(), &target)?;
            register(store, &record, progress.as_ref().map(|p| (key.as_str(), p)))?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

/// Unix seconds from a field that may be seconds or milliseconds.
fn seconds(value: Option<&Value>) -> Option<i64> {
    let n = value?.as_i64()?;
    Some(if n > 100_000_000_000 { n / 1000 } else { n })
}

fn text(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Null) | None => Value::Null,
        Some(Value::String(s)) => Value::String(s.clone()),
        Some(other) => Value::String(other.to_string()),
    }
}

/// A flat row for one stream item with the instant it was received; `None` when the item has
/// no id or time.
#[must_use]
pub fn flatten(stream: Stream, row: &Value, received_at_ms: i64) -> Option<Value> {
    let raw = row.to_string();
    let mut flat = flatten_fields(stream, row)?;
    if let Some(object) = flat.as_object_mut() {
        object.insert("raw_json".into(), Value::String(raw));
        object.insert("received_at".into(), Value::from(received_at_ms));
    }
    Some(flat)
}

fn flatten_fields(stream: Stream, row: &Value) -> Option<Value> {
    Some(match stream {
        Stream::Events => {
            let entities = row.get("primaryEntities").and_then(Value::as_array);
            let ids: Vec<Value> = entities
                .into_iter()
                .flatten()
                .filter_map(|e| e.get("id").cloned())
                .collect();
            let symbols: Vec<Value> = entities
                .into_iter()
                .flatten()
                .filter_map(|e| e.get("symbol").cloned())
                .filter(|s| !s.is_null())
                .collect();
            json!({
                "id": row.get("id")?.as_str()?,
                "first_seen_at": seconds(row.get("firstSeenAt"))?,
                "analyzed_at": seconds(row.get("analyzedAt")),
                "event_class": row.get("eventClass"), "event_type": row.get("eventType"),
                "alert_title": row.get("alertTitle"), "summary": row.get("summary"), "implication": row.get("implication"),
                "alert_action": row.get("alertAction"), "novelty": row.get("novelty"), "storyline_id": row.get("storylineId"),
                "origin_count": row.get("originCount"),
                "primary_entity_ids": ids, "primary_symbols": symbols,
                "impacts_json": text(row.get("impacts")), "cited_sources_json": text(row.get("citedSources")),
            })
        }
        Stream::Calls => json!({
            "id": row.get("id")?.as_str()?,
            "occurred_at": seconds(row.get("occurredAt"))?,
            "episode_id": row.get("episodeId"), "handle": row.get("handle"), "source": row.get("source"), "channel": row.get("channel"),
            "asset_id": row.get("asset").and_then(|a| a.get("id")), "asset_symbol": row.get("asset").and_then(|a| a.get("symbol")),
            "asset_name": row.get("asset").and_then(|a| a.get("name")), "call_action": row.get("callAction"),
        }),
        Stream::Episodes => json!({
            "id": row.get("id")?.as_str()?,
            "observed_at": seconds(row.get("observedAt")).or_else(|| seconds(row.get("openedAt")))?,
            "opened_at": seconds(row.get("openedAt")), "closed_at": seconds(row.get("closedAt")),
            "handle": row.get("handle"), "source": row.get("source"),
            "asset_id": row.get("asset").and_then(|a| a.get("id")), "asset_symbol": row.get("asset").and_then(|a| a.get("symbol")),
            "direction": row.get("direction"), "horizon": row.get("horizon"), "status": row.get("status"),
            "entry_price": row.get("entryPrice"), "exit_price": row.get("exitPrice"), "close_reason": row.get("closeReason"),
            "alpha_score": row.get("track").and_then(|t| t.get("alphaScore")),
        }),
        Stream::CallBook => json!({
            "bar_at": seconds(row.get("barAt"))?,
            "live": row.get("live"), "long_share": row.get("longShare"), "short_share": row.get("shortShare"),
            "crowding_percentile": row.get("crowdingPercentile"), "account_count": row.get("accountCount"),
            "pulse24": row.get("pulse24"), "pulse24_long_share": row.get("pulse24LongShare"),
            "pulse6": row.get("pulse6"), "pulse6_long_share": row.get("pulse6LongShare"),
            "formula_version": row.get("formulaVersion"), "computed_at": seconds(row.get("computedAt")),
        }),
    })
}

/// The time column a stream's rows are filed under.
#[must_use]
pub fn day_field(stream: Stream) -> &'static str {
    match stream {
        Stream::Events => "first_seen_at",
        Stream::Calls => "occurred_at",
        Stream::Episodes => "observed_at",
        Stream::CallBook => "bar_at",
    }
}

pub(super) fn group_by_day(rows: Vec<Value>) -> BTreeMap<Period, Vec<Value>> {
    let mut by_day: BTreeMap<Period, Vec<Value>> = BTreeMap::new();
    for row in rows {
        let field = row
            .get("first_seen_at")
            .or_else(|| row.get("occurred_at"))
            .or_else(|| row.get("observed_at"))
            .or_else(|| row.get("bar_at"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        by_day
            .entry(Period::containing(
                date_of_ms(field * 1000),
                Granularity::Day,
            ))
            .or_default()
            .push(row);
    }
    by_day
}

/// The typed `SELECT` of a stream's staged rows.
#[must_use]
pub fn select(stream: Stream, staged: &Path, constants: &str) -> String {
    let time = day_field(stream);
    let columns: &[(&str, &str)] = match stream {
        Stream::Events => &[
            ("id", "VARCHAR"),
            ("first_seen_at", "BIGINT"),
            ("analyzed_at", "BIGINT"),
            ("event_class", "VARCHAR"),
            ("event_type", "VARCHAR"),
            ("alert_title", "VARCHAR"),
            ("summary", "VARCHAR"),
            ("implication", "VARCHAR"),
            ("alert_action", "VARCHAR"),
            ("novelty", "VARCHAR"),
            ("storyline_id", "VARCHAR"),
            ("origin_count", "BIGINT"),
            ("primary_entity_ids", "VARCHAR[]"),
            ("primary_symbols", "VARCHAR[]"),
            ("impacts_json", "VARCHAR"),
            ("cited_sources_json", "VARCHAR"),
            ("raw_json", "VARCHAR"),
        ],
        Stream::Calls => &[
            ("id", "VARCHAR"),
            ("occurred_at", "BIGINT"),
            ("episode_id", "VARCHAR"),
            ("handle", "VARCHAR"),
            ("source", "VARCHAR"),
            ("channel", "VARCHAR"),
            ("asset_id", "VARCHAR"),
            ("asset_symbol", "VARCHAR"),
            ("asset_name", "VARCHAR"),
            ("call_action", "VARCHAR"),
            ("raw_json", "VARCHAR"),
        ],
        Stream::Episodes => &[
            ("id", "VARCHAR"),
            ("observed_at", "BIGINT"),
            ("opened_at", "BIGINT"),
            ("closed_at", "BIGINT"),
            ("handle", "VARCHAR"),
            ("source", "VARCHAR"),
            ("asset_id", "VARCHAR"),
            ("asset_symbol", "VARCHAR"),
            ("direction", "VARCHAR"),
            ("horizon", "VARCHAR"),
            ("status", "VARCHAR"),
            ("entry_price", "DOUBLE"),
            ("exit_price", "DOUBLE"),
            ("close_reason", "VARCHAR"),
            ("alpha_score", "DOUBLE"),
            ("raw_json", "VARCHAR"),
        ],
        Stream::CallBook => &[
            ("bar_at", "BIGINT"),
            ("live", "BOOLEAN"),
            ("long_share", "DOUBLE"),
            ("short_share", "DOUBLE"),
            ("crowding_percentile", "DOUBLE"),
            ("account_count", "BIGINT"),
            ("pulse24", "DOUBLE"),
            ("pulse24_long_share", "DOUBLE"),
            ("pulse6", "DOUBLE"),
            ("pulse6_long_share", "DOUBLE"),
            ("formula_version", "VARCHAR"),
            ("computed_at", "BIGINT"),
            ("raw_json", "VARCHAR"),
        ],
    };
    let mut typed: Vec<(&str, &str)> = columns.to_vec();
    typed.push(("received_at", "BIGINT"));
    format!(
        "SELECT * EXCLUDE (received_at), received_at AS received_at_ms, make_timestamp({time} * 1000000) AS ts, {constants} FROM {}",
        read_json_array(staged, &typed)
    )
}

fn log_error(stream: Stream, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "elfa")
            .with("dataset", stream.dataset())
            .with("symbol", "ALL")
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_each_stream() {
        let event = json!({ "id": "e1", "firstSeenAt": 1_791_500_000, "analyzedAt": 1_791_500_100_000_i64, "eventClass": "news",
            "primaryEntities": [{ "id": "a", "symbol": "SOL" }, { "id": "b", "symbol": null }], "impacts": [{ "x": 1 }] });
        let row = flatten(Stream::Events, &event, 77).unwrap();
        assert_eq!(row["analyzed_at"], 1_791_500_100);
        assert_eq!(row["received_at"], 77);
        assert!(
            row["raw_json"]
                .as_str()
                .unwrap()
                .contains("primaryEntities")
        );
        assert_eq!(row["primary_entity_ids"], json!(["a", "b"]));
        assert_eq!(row["primary_symbols"], json!(["SOL"]));
        assert_eq!(row["impacts_json"], "[{\"x\":1}]");
        assert!(flatten(Stream::Events, &json!({ "id": "x" }), 0).is_none());
        let bar = flatten(
            Stream::CallBook,
            &json!({ "barAt": 1_791_500_000, "longShare": 0.6 }),
            0,
        )
        .unwrap();
        assert_eq!(bar["bar_at"], 1_791_500_000);
        let call = flatten(
            Stream::Calls,
            &json!({ "id": "c", "occurredAt": 1, "asset": { "symbol": "BTC" } }),
            0,
        )
        .unwrap();
        assert_eq!(call["asset_symbol"], "BTC");
        let episode = flatten(
            Stream::Episodes,
            &json!({ "id": "p", "openedAt": 5, "track": { "alphaScore": 0.2 } }),
            0,
        )
        .unwrap();
        assert_eq!(episode["observed_at"], 5);
        assert_eq!(episode["alpha_score"], 0.2);
        assert_eq!(progress_key(Stream::CallBook), "elfa/call_book");
        assert_eq!(Stream::Episodes.path(), "/v3/calls/episodes");
        let mut lane = ElfaLane::new("https://x", "k", 0);
        assert_eq!(lane.hourly_streams().len(), 4);
        lane.events_pages = 2;
        assert_eq!(
            lane.hourly_streams(),
            [Stream::Calls, Stream::Episodes, Stream::CallBook]
        );
    }

    #[test]
    fn the_guard_disables_one_billed_endpoint_and_keeps_the_others() {
        let lane = ElfaLane::new("https://x", "k", 0);
        assert!(!lane.guard(Stream::Events.path(), Some(0)));
        assert!(!lane.guard(Stream::Events.path(), None));
        assert!(!lane.guard_tripped());
        assert!(lane.guard(Stream::Calls.path(), Some(1)));
        assert!(!lane.stream_enabled(Stream::Calls));
        assert!(lane.stream_enabled(Stream::Events));
        assert!(lane.stream_enabled(Stream::Episodes));
        assert!(lane.guard_tripped());
        assert!(!lane.disabled(), "the lane as a whole keeps running");
        assert_eq!(lane.billed_endpoints(), ["/v3/calls"]);
    }

    #[test]
    fn cycle_readings_attribute_or_fall_back() {
        // Headers present, the key moved by 4, the answers declared nothing: unattributed,
        // spent elsewhere (an Auto evaluation, another client), nothing is disabled.
        let r = CycleCredits::between((10, 0), (10, 0), 277, 281);
        assert_eq!((r.unattributed(), r.disables_lane()), (4, false));
        // Declared accounts for the movement.
        let r = CycleCredits::between((10, 0), (12, 0), 100, 102);
        assert_eq!((r.unattributed(), r.disables_lane()), (0, false));
        // A v3 answer carried no header and the key moved: the key-wide fallback.
        let r = CycleCredits::between((10, 3), (10, 5), 100, 101);
        assert!(r.disables_lane());
        // No header but no movement: nothing.
        assert!(!CycleCredits::between((0, 0), (0, 9), 5, 5).disables_lane());
    }
}
