//! Elfa Auto alerts: a small configured set of EQL queries (`notify` action only) that the
//! lane creates once (validate, then create: five credits each), renews before they expire,
//! and listens to on the account-wide server-sent event stream, appending every notification
//! to the day's `auto_events` file with the instant it was received. Paid calls run under the
//! billing lock shared with the v3 cycle and are measured through `credits.used`; a monthly
//! credit budget and the alert ceiling bound the spend.

use super::config::{AlertDef, ElfaAuto};
use super::elfa::{Billing, headers, key_status};
use super::http::HttpError;
use super::ledger::{get_progress, register, set_progress};
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::Ctx;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// Progress key of the alerts the lane holds.
pub const PROGRESS_QUERIES: &str = "elfa/auto/queries";
/// Progress key of the month's spend.
pub const PROGRESS_SPEND: &str = "elfa/auto/spend";
/// Credits one creation is expected to cost (the manifest's baseline).
pub const CREATE_COST: i64 = 5;

/// One alert the account holds.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveQuery {
    /// Query id (UUID).
    pub id: String,
    /// Title, the idempotency key.
    pub title: String,
    /// When it was created, milliseconds.
    pub created_at_ms: i64,
    /// When it expires, milliseconds.
    pub expires_at_ms: i64,
}

/// What the lane has done so far.
#[derive(Default)]
pub struct Counters {
    /// Alerts created (including renewals).
    pub created: AtomicU64,
    /// Alerts renewed.
    pub renewed: AtomicU64,
    /// Old alerts cancelled.
    pub cancelled: AtomicU64,
    /// Notifications recorded.
    pub fired: AtomicU64,
    /// Failed operations.
    pub errors: AtomicU64,
    /// Stream connections opened.
    pub connections: AtomicU64,
    /// Whether the stream is connected now.
    pub connected: AtomicBool,
    /// Credits spent this month, as measured.
    pub spent_month: AtomicI64,
}

/// The lane's settings and state.
pub struct AutoLane {
    /// API base.
    pub base_url: String,
    /// `x-elfa-api-key`.
    pub key: String,
    /// The configuration.
    pub cfg: ElfaAuto,
    /// The billing lock.
    pub billing: Billing,
    /// Seconds without a frame (keep-alives count) before the stream is reopened.
    pub idle_seconds: u64,
    /// Counters for the status.
    pub counters: Counters,
    active: Mutex<Vec<ActiveQuery>>,
    last_reconcile: Mutex<Option<String>>,
}

impl AutoLane {
    /// A lane for a key.
    #[must_use]
    pub fn new(base_url: &str, key: &str, cfg: ElfaAuto, billing: Billing) -> AutoLane {
        AutoLane {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            cfg,
            billing,
            idle_seconds: 60,
            counters: Counters::default(),
            active: Mutex::new(Vec::new()),
            last_reconcile: Mutex::new(None),
        }
    }

    /// The alerts the account holds, as of the last reconciliation.
    #[must_use]
    pub fn active(&self) -> Vec<ActiveQuery> {
        self.active.lock().expect("active queries").clone()
    }

    /// The lane's status object.
    #[must_use]
    pub fn status(&self) -> Obj {
        let c = &self.counters;
        let active = self.active();
        Obj::new()
            .with("enabled", true)
            .with("active", active.len())
            .with(
                "titles",
                active.iter().map(|q| q.title.clone()).collect::<Vec<_>>(),
            )
            .with("created", c.created.load(Ordering::Relaxed))
            .with("renewed", c.renewed.load(Ordering::Relaxed))
            .with("cancelled", c.cancelled.load(Ordering::Relaxed))
            .with("fired", c.fired.load(Ordering::Relaxed))
            .with("errors", c.errors.load(Ordering::Relaxed))
            .with("connections", c.connections.load(Ordering::Relaxed))
            .with("streamConnected", c.connected.load(Ordering::Relaxed))
            .with("creditsSpentMonth", c.spent_month.load(Ordering::Relaxed))
            .with("creditBudgetPerMonth", self.cfg.credit_budget_per_month)
            .with(
                "lastReconcileAt",
                self.last_reconcile.lock().expect("reconcile").clone(),
            )
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

/// The request body of one alert: conditions, the `notify` action, the expiry, the repeat.
#[must_use]
pub fn query_body(def: &AlertDef, expires_in: &str) -> Value {
    let mut query = json!({
        "conditions": def.conditions,
        "actions": [{ "stepId": "step_1", "type": "notify", "params": { "message": def.title } }],
        "expiresIn": expires_in,
    });
    if let Some(repeat) = &def.repeat {
        query["repeat"] = repeat.clone();
    }
    json!({ "query": query, "title": def.title, "description": def.description })
}

/// `720h` as milliseconds.
#[must_use]
pub fn expiry_ms(expires_in: &str) -> i64 {
    expires_in
        .strip_suffix('h')
        .and_then(|h| h.parse::<i64>().ok())
        .unwrap_or(720)
        * 3_600_000
}

/// `YYYY-MM` of an instant.
#[must_use]
pub fn month_label(now_ms: i64) -> String {
    Period::containing(date_of_ms(now_ms), Granularity::Month).label()
}

fn ms_of_text(value: Option<&Value>) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value?.as_str()?)
        .ok()
        .map(|d| d.timestamp_millis())
}

/// The active queries of a `GET /v2/auto/queries` answer, with expiry from the answer, the
/// stored records, or the configured expiry from the creation time.
#[must_use]
pub fn active_from_listing(
    listing: &Value,
    records: &[ActiveQuery],
    expiry: i64,
    now_ms: i64,
) -> Vec<ActiveQuery> {
    listing
        .get("queries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|q| q.get("status").and_then(Value::as_str) == Some("active"))
        .filter_map(|q| {
            let id = q
                .get("id")
                .or_else(|| q.get("queryId"))?
                .as_str()?
                .to_owned();
            let record = records.iter().find(|r| r.id == id);
            let created = ms_of_text(q.get("createdAt"))
                .or_else(|| record.map(|r| r.created_at_ms))
                .unwrap_or(now_ms);
            let expires = ms_of_text(q.get("expiresAt"))
                .or_else(|| record.map(|r| r.expires_at_ms))
                .unwrap_or(created + expiry);
            Some(ActiveQuery {
                id,
                title: q
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                created_at_ms: created,
                expires_at_ms: expires,
            })
        })
        .collect()
}

/// Reconcile the account with the configured alerts: list (one credit), create what is
/// missing, renew what expires within the window, cancel the renewed, record the spend.
pub async fn reconcile(ctx: &Ctx, lane: &AutoLane, now_ms: i64) -> Result<(), StoreError> {
    let _billing = lane.billing.lock().await;
    let (used_before, _) = key_status(&ctx.http, &lane.base_url, &lane.key).await?;
    let records: Vec<ActiveQuery> = ctx
        .db
        .run(|store| get_progress(store, PROGRESS_QUERIES))
        .await?
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let listing = ctx
        .http
        .get_json(&lane.url("/v2/auto/queries?limit=100"), &headers(&lane.key))
        .await?;
    let expiry = expiry_ms(&lane.cfg.expires_in);
    let mut active = active_from_listing(&listing, &records, expiry, now_ms);
    let mut spent = load_spend(ctx, now_ms).await?;
    let renew_ms = lane.cfg.renew_within_hours * 3_600_000;
    for def in &lane.cfg.alerts {
        if ctx.stopping() {
            break;
        }
        let existing = active.iter().position(|q| q.title == def.title);
        let renewing = existing.is_some_and(|i| active[i].expires_at_ms - now_ms < renew_ms);
        if existing.is_some() && !renewing {
            continue;
        }
        if spent + CREATE_COST > lane.cfg.credit_budget_per_month
            || (existing.is_none() && active.len() >= lane.cfg.max_alerts)
        {
            log(
                "elfa_auto_budget_reached",
                Obj::new()
                    .with("title", def.title.as_str())
                    .with("creditsSpentMonth", spent)
                    .with("creditBudgetPerMonth", lane.cfg.credit_budget_per_month)
                    .with("active", active.len())
                    .with("maxAlerts", lane.cfg.max_alerts),
            );
            break;
        }
        let (created, credits) = create(ctx, lane, def, now_ms, expiry).await?;
        spent += credits.unwrap_or(CREATE_COST);
        lane.counters.created.fetch_add(1, Ordering::Relaxed);
        if let Some(i) = existing {
            let old = active.remove(i);
            cancel(ctx, lane, &old).await?;
            lane.counters.renewed.fetch_add(1, Ordering::Relaxed);
            log(
                "elfa_auto_renewed",
                Obj::new()
                    .with("title", def.title.as_str())
                    .with("oldQueryId", old.id)
                    .with("queryId", created.id.as_str()),
            );
        }
        active.push(created);
    }
    let (used_after, _) = key_status(&ctx.http, &lane.base_url, &lane.key).await?;
    let month = month_label(now_ms);
    let measured = (used_after - used_before).max(0);
    let total = load_spend(ctx, now_ms).await? + measured;
    log(
        "elfa_auto_credits",
        Obj::new()
            .with("usedBefore", used_before)
            .with("usedAfter", used_after)
            .with("delta", measured)
            .with("month", month.as_str())
            .with("creditsSpentMonth", total),
    );
    lane.counters.spent_month.store(total, Ordering::Relaxed);
    let value = json!({ "month": month, "credits": total, "updatedAt": now() });
    let saved = serde_json::to_value(&active).map_err(|e| StoreError::Check(e.to_string()))?;
    ctx.db
        .run(move |store| {
            store.transaction(|store| {
                set_progress(store, PROGRESS_SPEND, &value)?;
                set_progress(store, PROGRESS_QUERIES, &saved)
            })
        })
        .await?;
    *lane.active.lock().expect("active queries") = active;
    *lane.last_reconcile.lock().expect("reconcile") = Some(now());
    Ok(())
}

async fn load_spend(ctx: &Ctx, now_ms: i64) -> Result<i64, StoreError> {
    let month = month_label(now_ms);
    Ok(ctx
        .db
        .run(|store| get_progress(store, PROGRESS_SPEND))
        .await?
        .filter(|v| v.get("month").and_then(Value::as_str) == Some(month.as_str()))
        .and_then(|v| v.get("credits").and_then(Value::as_i64))
        .unwrap_or(0))
}

/// Validate (free), then create; returns the query and the credits the answer declared.
async fn create(
    ctx: &Ctx,
    lane: &AutoLane,
    def: &AlertDef,
    now_ms: i64,
    expiry: i64,
) -> Result<(ActiveQuery, Option<i64>), StoreError> {
    let body = query_body(def, &lane.cfg.expires_in);
    let validation = ctx
        .http
        .post_json(
            &lane.url("/v2/auto/queries/validate"),
            &body,
            &headers(&lane.key),
        )
        .await?;
    if validation.get("valid").and_then(Value::as_bool) != Some(true) {
        return Err(StoreError::Check(format!(
            "{} did not validate: {}",
            def.title,
            validation.get("errors").cloned().unwrap_or(Value::Null)
        )));
    }
    let answer = ctx
        .http
        .post(&lane.url("/v2/auto/queries"), &body, &headers(&lane.key))
        .await?;
    let credits = answer
        .header("x-elfa-credits")
        .and_then(|c| c.parse::<i64>().ok());
    let created: Value =
        serde_json::from_slice(&answer.body).map_err(|e| StoreError::Check(e.to_string()))?;
    let id = created
        .get("queryId")
        .or_else(|| created.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| StoreError::Check("create answer has no queryId".into()))?
        .to_owned();
    log(
        "elfa_auto_created",
        Obj::new()
            .with("title", def.title.as_str())
            .with("queryId", id.as_str())
            .with(
                "status",
                created.get("status").cloned().unwrap_or(Value::Null),
            )
            .with("credits", credits)
            .with(
                "estimatedCredits",
                created
                    .get("estimatedCredits")
                    .cloned()
                    .unwrap_or(Value::Null),
            )
            .with("expiresIn", lane.cfg.expires_in.as_str()),
    );
    Ok((
        ActiveQuery {
            id,
            title: def.title.clone(),
            created_at_ms: now_ms,
            expires_at_ms: ms_of_text(created.get("expiresAt")).unwrap_or(now_ms + expiry),
        },
        credits,
    ))
}

async fn cancel(ctx: &Ctx, lane: &AutoLane, query: &ActiveQuery) -> Result<(), StoreError> {
    let url = lane.url(&format!("/v2/auto/queries/{}/cancel", query.id));
    match ctx
        .http
        .post(
            &url,
            &Value::Object(Default::default()),
            &headers(&lane.key),
        )
        .await
    {
        // Already terminal or gone: the cancellation is idempotent.
        Ok(_) | Err(HttpError::Status(409) | HttpError::NotFound) => {
            lane.counters.cancelled.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// Reconcile now and then every `interval_seconds`.
pub async fn reconcile_loop(ctx: &Ctx, lane: &AutoLane, interval_seconds: u64) {
    while !ctx.stopping() {
        if let Err(error) = reconcile(ctx, lane, chrono::Utc::now().timestamp_millis()).await {
            lane.counters.errors.fetch_add(1, Ordering::Relaxed);
            log_error("reconcile", &error.to_string());
        }
        pause(ctx, interval_seconds.max(60)).await;
    }
}

/// One server-sent event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SseFrame {
    /// `id:` line.
    pub id: Option<String>,
    /// `event:` line.
    pub event: Option<String>,
    /// `data:` lines joined by newlines.
    pub data: String,
}

/// Take every complete frame (blank-line terminated) off the front of `buffer`.
pub fn parse_sse(buffer: &mut Vec<u8>) -> Vec<SseFrame> {
    let mut frames = Vec::new();
    loop {
        let text = String::from_utf8_lossy(buffer).into_owned();
        let Some((end, skip)) = frame_end(&text) else {
            return frames;
        };
        let mut frame = SseFrame::default();
        let mut data: Vec<&str> = Vec::new();
        for line in text[..end].split('\n') {
            let line = line.trim_end_matches('\r');
            let Some((field, value)) = line.split_once(':') else {
                continue;
            };
            if field.is_empty() {
                continue;
            }
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "id" => frame.id = Some(value.to_owned()),
                "event" => frame.event = Some(value.to_owned()),
                "data" => data.push(value),
                _ => {}
            }
        }
        frame.data = data.join("\n");
        if frame.id.is_some() || frame.event.is_some() || !frame.data.is_empty() {
            frames.push(frame);
        }
        buffer.drain(..end + skip);
    }
}

fn frame_end(text: &str) -> Option<(usize, usize)> {
    let lf = text.find("\n\n").map(|i| (i, 2));
    let crlf = text.find("\r\n\r\n").map(|i| (i, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}

/// Hold the account-wide stream, reconnecting with backoff; `410` means no active query.
pub async fn stream_loop(ctx: &Ctx, lane: &AutoLane) {
    let mut backoff = Duration::from_secs(1);
    while !ctx.stopping() {
        if lane.active().is_empty() {
            pause(ctx, 30).await;
            continue;
        }
        let opened = ctx
            .http
            .open(&lane.url("/v2/auto/queries/stream"), &headers(&lane.key))
            .await;
        match opened {
            Ok(response) => {
                lane.counters.connections.fetch_add(1, Ordering::Relaxed);
                lane.counters.connected.store(true, Ordering::Relaxed);
                backoff = Duration::from_secs(1);
                let outcome = read_stream(ctx, lane, response).await;
                lane.counters.connected.store(false, Ordering::Relaxed);
                match outcome {
                    Ok(()) => {
                        log("elfa_auto_stream_ended", Obj::new());
                        pause(ctx, 15).await;
                    }
                    Err(error) => {
                        lane.counters.errors.fetch_add(1, Ordering::Relaxed);
                        log_error("stream", &error.to_string());
                        pause(ctx, backoff.as_secs()).await;
                        backoff = (backoff * 2).min(Duration::from_secs(300));
                    }
                }
            }
            Err(HttpError::Status(410)) => {
                log(
                    "elfa_auto_stream_closed",
                    Obj::new().with("reason", "no active queries (410)"),
                );
                pause(ctx, 300).await;
            }
            Err(error) => {
                lane.counters.errors.fetch_add(1, Ordering::Relaxed);
                log_error("stream", &error.to_string());
                pause(ctx, backoff.as_secs()).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
            }
        }
    }
}

async fn read_stream(
    ctx: &Ctx,
    lane: &AutoLane,
    mut response: reqwest::Response,
) -> Result<(), StoreError> {
    let mut buffer: Vec<u8> = Vec::new();
    loop {
        if ctx.stopping() {
            return Ok(());
        }
        let idle = Duration::from_secs(lane.idle_seconds.max(5));
        let chunk = match tokio::time::timeout(idle, response.chunk()).await {
            Ok(Ok(Some(bytes))) => bytes,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(error)) => return Err(StoreError::Check(safe_error(&error.to_string()))),
            Err(_) => return Err(StoreError::Check("stream idle, reconnecting".into())),
        };
        buffer.extend_from_slice(&chunk);
        for frame in parse_sse(&mut buffer) {
            match frame.event.as_deref() {
                Some("end") => return Ok(()),
                Some("error") => return Err(StoreError::Check(frame.data)),
                Some("notification") | None if !frame.data.is_empty() => {
                    record(ctx, lane, &frame, chrono::Utc::now().timestamp_millis()).await?;
                }
                _ => {}
            }
        }
    }
}

/// A notification frame as a flat row.
#[must_use]
pub fn event_row(frame: &SseFrame, titles: &[ActiveQuery], received_at_ms: i64) -> Value {
    let data: Value = serde_json::from_str(&frame.data).unwrap_or(Value::Null);
    let query_id = data
        .get("queryId")
        .or_else(|| data.get("data").and_then(|d| d.get("queryId")))
        .and_then(Value::as_str)
        .unwrap_or("");
    let title = titles
        .iter()
        .find(|q| q.id == query_id)
        .map(|q| q.title.clone());
    let text = |key: &str| data.get(key).cloned().unwrap_or(Value::Null);
    json!({
        "event_id": frame.id.clone().unwrap_or_else(|| format!("{received_at_ms}-{query_id}")),
        "received_at": received_at_ms, "query_id": query_id, "query_title": title,
        "status": text("status"), "type": text("type"), "category": text("category"), "priority": text("priority"),
        "title": text("title"), "body": text("body"), "execution_id": text("executionId"),
        "trigger_time": text("triggerTime"), "trigger_time_ms": ms_of_text(data.get("triggerTime")),
        "conditions_met": text("conditionsMet"), "timestamp_ms": text("timestamp"), "created_at": text("createdAt"),
        "auto_details_json": data.get("autoDetails").map(Value::to_string),
        "raw_json": frame.data,
    })
}

/// The typed `SELECT` of staged event rows.
#[must_use]
pub fn events_select(staged: &Path) -> String {
    format!(
        "SELECT event_id, received_at AS received_at_ms, make_timestamp(received_at * 1000) AS ts, query_id, query_title, status,
                type, category, priority, title, body, execution_id, trigger_time, trigger_time_ms, conditions_met, timestamp_ms,
                created_at, auto_details_json, raw_json, 'ALL' AS symbol
         FROM {}",
        read_json_array(
            staged,
            &[
                ("event_id", "VARCHAR"), ("received_at", "BIGINT"), ("query_id", "VARCHAR"), ("query_title", "VARCHAR"),
                ("status", "VARCHAR"), ("type", "VARCHAR"), ("category", "VARCHAR"), ("priority", "VARCHAR"),
                ("title", "VARCHAR"), ("body", "VARCHAR"), ("execution_id", "VARCHAR"), ("trigger_time", "VARCHAR"),
                ("trigger_time_ms", "BIGINT"), ("conditions_met", "BIGINT"), ("timestamp_ms", "BIGINT"),
                ("created_at", "VARCHAR"), ("auto_details_json", "VARCHAR"), ("raw_json", "VARCHAR")
            ]
        )
    )
}

async fn record(
    ctx: &Ctx,
    lane: &AutoLane,
    frame: &SseFrame,
    received_at_ms: i64,
) -> Result<(), StoreError> {
    let row = event_row(frame, &lane.active(), received_at_ms);
    let staged = stage_json(&ctx.staging(), std::slice::from_ref(&row))?;
    let day = Period::containing(date_of_ms(received_at_ms), Granularity::Day);
    let target = Target {
        source: "elfa".into(),
        dataset: "auto_events".into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete: false,
    };
    let select = events_select(&staged);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, "event_id", &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result?;
    lane.counters.fired.fetch_add(1, Ordering::Relaxed);
    log(
        "elfa_auto_fired",
        Obj::new()
            .with("eventId", row["event_id"].clone())
            .with("queryId", row["query_id"].clone())
            .with("queryTitle", row["query_title"].clone())
            .with("status", row["status"].clone())
            .with("title", row["title"].clone())
            .with("period", day.label()),
    );
    Ok(())
}

async fn pause(ctx: &Ctx, seconds: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline && !ctx.stopping() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn log_error(item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "elfa")
            .with("dataset", "auto_events")
            .with("symbol", "ALL")
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_frames_and_listings() {
        let def = AlertDef {
            title: "t".into(),
            description: "d".into(),
            conditions: json!({ "AND": [] }),
            repeat: Some(json!({ "cooldown": "1h", "maxTriggers": 5 })),
        };
        let body = query_body(&def, "720h");
        assert_eq!(body["query"]["actions"][0]["type"], "notify");
        assert_eq!(body["query"]["repeat"]["cooldown"], "1h");
        assert_eq!(body["query"]["expiresIn"], "720h");
        assert_eq!(expiry_ms("720h"), 720 * 3_600_000);
        assert_eq!(month_label(1_791_547_200_000), "2026-10");
        let mut buffer = b"id: e1\r\nevent: notification\r\ndata: {\"queryId\":\"q\"}\r\n\r\n: keep-alive\n\nevent: end\ndata: {\"code\":\"USER_STREAM_CLOSED\"}\n\nid: partial".to_vec();
        let frames = parse_sse(&mut buffer);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].id.as_deref(), Some("e1"));
        assert_eq!(frames[0].event.as_deref(), Some("notification"));
        assert_eq!(frames[0].data, "{\"queryId\":\"q\"}");
        assert_eq!(frames[1].event.as_deref(), Some("end"));
        assert_eq!(buffer, b"id: partial");
        let active = vec![ActiveQuery {
            id: "q".into(),
            title: "SOL funding".into(),
            created_at_ms: 0,
            expires_at_ms: 1,
        }];
        let row = event_row(&frames[0], &active, 5);
        assert_eq!(row["query_title"], "SOL funding");
        assert_eq!(row["event_id"], "e1");
        let listing = json!({ "queries": [
            { "id": "a", "title": "A", "status": "active", "createdAt": "2026-10-01T00:00:00.000Z" },
            { "id": "b", "title": "B", "status": "cancelled" }
        ]});
        let active = active_from_listing(&listing, &[], 3_600_000, 9);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].expires_at_ms, 1_790_812_800_000 + 3_600_000);
    }
}
