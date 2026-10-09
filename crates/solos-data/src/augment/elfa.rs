//! Elfa v3 (free today, undocumented): events, calls, call episodes and the crypto call-book
//! bars, pulled hourly from the last `to` with ascending cursors into day files. A credit guard
//! reads `credits.used` before and after every cycle; if it moved, the lane logs
//! `elfa_billing_started` and disables itself for the rest of the process.

use super::http::{Http, with_query};
use super::ledger::{get_progress, register};
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Pages per stream per cycle; the rest waits for the next cycle.
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

/// The lane's settings and state.
pub struct ElfaLane {
    /// API base.
    pub base_url: String,
    /// `x-elfa-api-key`.
    pub key: String,
    /// First instant to pull, seconds.
    pub start_s: i64,
    disabled: AtomicBool,
}

impl ElfaLane {
    /// A lane for a key.
    #[must_use]
    pub fn new(base_url: &str, key: &str, start_s: i64) -> ElfaLane {
        ElfaLane {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            start_s,
            disabled: AtomicBool::new(false),
        }
    }

    /// Whether the credit guard switched the lane off.
    #[must_use]
    pub fn disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    fn headers(&self) -> [(&str, &str); 2] {
        [
            ("x-elfa-api-key", self.key.as_str()),
            ("accept", "application/json"),
        ]
    }

    async fn get(
        &self,
        http: &Http,
        path: &str,
        pairs: &[(&str, String)],
    ) -> Result<Value, StoreError> {
        let url = with_query(&format!("{}{path}", self.base_url), pairs);
        Ok(http.get_json(&url, &self.headers()).await?)
    }

    /// `credits.used` and `historyFrom` from `/v3/key-status`.
    pub async fn key_status(&self, http: &Http) -> Result<(i64, Option<i64>), StoreError> {
        let status = self.get(http, "/v3/key-status", &[]).await?;
        let used = status
            .get("credits")
            .and_then(|c| c.get("used"))
            .and_then(Value::as_i64)
            .ok_or_else(|| StoreError::Check("key-status has no credits.used".into()))?;
        Ok((used, status.get("historyFrom").and_then(Value::as_i64)))
    }
}

/// One cycle: every stream from its last `to` up to `now_s`, under the credit guard.
pub async fn cycle(ctx: &Ctx, lane: &ElfaLane, now_s: i64) -> Outcome {
    let mut outcome = Outcome::default();
    if lane.disabled() {
        outcome.skipped += 1;
        return outcome;
    }
    let (used_before, history_from) = match lane.key_status(&ctx.http).await {
        Ok(status) => status,
        Err(error) => {
            outcome.errors += 1;
            log_error(Stream::Events, "key-status", &error.to_string());
            return outcome;
        }
    };
    for stream in Stream::ALL {
        if ctx.stopping() {
            break;
        }
        match pull_stream(ctx, lane, stream, history_from, now_s).await {
            Ok(result) => outcome.add(&result),
            Err(error) => {
                outcome.errors += 1;
                log_error(stream, "cycle", &error.to_string());
            }
        }
    }
    match lane.key_status(&ctx.http).await {
        Ok((used_after, _)) if used_after > used_before => {
            lane.disabled.store(true, Ordering::Relaxed);
            log(
                "elfa_billing_started",
                Obj::new()
                    .with("usedBefore", used_before)
                    .with("usedAfter", used_after)
                    .with("action", "lane disabled until restart"),
            );
        }
        Ok(_) => {}
        Err(error) => {
            outcome.errors += 1;
            log_error(Stream::Events, "key-status", &error.to_string());
        }
    }
    outcome
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
    while has_more && pages < MAX_PAGES {
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
        rows.extend(page.iter().filter_map(|row| flatten(stream, row)));
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
    let complete_to = if has_more { None } else { Some(to) };
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

async fn write_day(
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

/// A flat row for one stream item; `None` when the item has no id or time.
#[must_use]
pub fn flatten(stream: Stream, row: &Value) -> Option<Value> {
    let raw = row.to_string();
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
                "raw_json": raw,
            })
        }
        Stream::Calls => json!({
            "id": row.get("id")?.as_str()?,
            "occurred_at": seconds(row.get("occurredAt"))?,
            "episode_id": row.get("episodeId"), "handle": row.get("handle"), "source": row.get("source"), "channel": row.get("channel"),
            "asset_id": row.get("asset").and_then(|a| a.get("id")), "asset_symbol": row.get("asset").and_then(|a| a.get("symbol")),
            "asset_name": row.get("asset").and_then(|a| a.get("name")), "call_action": row.get("callAction"),
            "raw_json": raw,
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
            "raw_json": raw,
        }),
        Stream::CallBook => json!({
            "bar_at": seconds(row.get("barAt"))?,
            "live": row.get("live"), "long_share": row.get("longShare"), "short_share": row.get("shortShare"),
            "crowding_percentile": row.get("crowdingPercentile"), "account_count": row.get("accountCount"),
            "pulse24": row.get("pulse24"), "pulse24_long_share": row.get("pulse24LongShare"),
            "pulse6": row.get("pulse6"), "pulse6_long_share": row.get("pulse6LongShare"),
            "formula_version": row.get("formulaVersion"), "computed_at": seconds(row.get("computedAt")),
            "raw_json": raw,
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

fn group_by_day(rows: Vec<Value>) -> BTreeMap<Period, Vec<Value>> {
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
    format!(
        "SELECT *, make_timestamp({time} * 1000000) AS ts, {constants} FROM {}",
        read_json_array(staged, columns)
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
        let row = flatten(Stream::Events, &event).unwrap();
        assert_eq!(row["analyzed_at"], 1_791_500_100);
        assert_eq!(row["primary_entity_ids"], json!(["a", "b"]));
        assert_eq!(row["primary_symbols"], json!(["SOL"]));
        assert_eq!(row["impacts_json"], "[{\"x\":1}]");
        assert!(flatten(Stream::Events, &json!({ "id": "x" })).is_none());
        let bar = flatten(
            Stream::CallBook,
            &json!({ "barAt": 1_791_500_000, "longShare": 0.6 }),
        )
        .unwrap();
        assert_eq!(bar["bar_at"], 1_791_500_000);
        let call = flatten(
            Stream::Calls,
            &json!({ "id": "c", "occurredAt": 1, "asset": { "symbol": "BTC" } }),
        )
        .unwrap();
        assert_eq!(call["asset_symbol"], "BTC");
        let episode = flatten(
            Stream::Episodes,
            &json!({ "id": "p", "openedAt": 5, "track": { "alphaScore": 0.2 } }),
        )
        .unwrap();
        assert_eq!(episode["observed_at"], 5);
        assert_eq!(episode["alpha_score"], 0.2);
        assert_eq!(progress_key(Stream::CallBook), "elfa/call_book");
        assert_eq!(Stream::Episodes.path(), "/v3/calls/episodes");
    }
}
