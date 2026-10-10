//! Elfa Auto alerts: a small configured set of EQL queries (`notify` action only) that the
//! lane creates once (validate, then create: five credits each), renews before they expire,
//! and listens to on the account-wide server-sent event stream, appending every notification
//! to the day's `auto_events` file with the instant it was received. Paid calls run under the
//! billing lock shared with the v3 cycle and are measured through `credits.used`; a monthly
//! credit budget and the alert ceiling bound the spend. The listing costs a credit, so the
//! lane lists at most every `reconcileIntervalHours` (and when an alert is missing or due for
//! renewal); in between, and across restarts, it holds the alerts stored in the ledger.

use super::config::{AlertDef, ElfaAuto};
use super::elfa::{Billing, headers, key_status};
use super::http::HttpError;
use super::ledger::{get_progress, register, set_progress};
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::Ctx;
pub use super::sse::{SseFrame, SseParser};
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
/// Progress key of the last listing's instant.
pub const PROGRESS_RECONCILED: &str = "elfa/auto/reconciledAt";
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
    /// Connections that ended without an error (end event, close, drop, idle) and were reopened.
    pub reconnects: AtomicU64,
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
            .with("reconnects", c.reconnects.load(Ordering::Relaxed))
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

/// Whether the account must be listed now: never listed, the interval passed, a configured
/// alert is not held, or a held one is due for renewal. Otherwise the stored alerts stand.
#[must_use]
pub fn listing_due(
    cfg: &ElfaAuto,
    records: &[ActiveQuery],
    reconciled_at_ms: Option<i64>,
    now_ms: i64,
) -> bool {
    let Some(at) = reconciled_at_ms else {
        return true;
    };
    let renew_ms = cfg.renew_within_hours * 3_600_000;
    now_ms - at >= cfg.reconcile_interval_hours * 3_600_000
        || cfg
            .alerts
            .iter()
            .any(|def| !records.iter().any(|r| r.title == def.title))
        || records.iter().any(|r| r.expires_at_ms - now_ms < renew_ms)
}

async fn load_records(ctx: &Ctx) -> Result<(Vec<ActiveQuery>, Option<i64>), StoreError> {
    ctx.db
        .run(|store| {
            let records = get_progress(store, PROGRESS_QUERIES)?
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();
            let at = get_progress(store, PROGRESS_RECONCILED)?
                .and_then(|v| v.get("atMs").and_then(Value::as_i64));
            Ok((records, at))
        })
        .await
}

/// What the listing says about each query: its scalar fields (ids, status, counters, times),
/// without conditions or descriptions. It is where per-query execution counts would show.
#[must_use]
pub fn listing_summary(listing: &Value) -> Obj {
    let queries: Vec<Value> = listing
        .get("queries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .map(|q| {
            Value::Object(
                q.iter()
                    .filter(|(k, v)| {
                        !matches!(k.as_str(), "description" | "query" | "conditions")
                            && !v.is_object()
                            && !v.is_array()
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )
        })
        .collect();
    Obj::new()
        .with(
            "total",
            listing.get("total").cloned().unwrap_or(Value::Null),
        )
        .with("queries", queries)
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
    log("elfa_auto_listing", listing_summary(&listing));
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
        let meter_refuses = ctx
            .http
            .meter()
            .is_some_and(|m| !m.allows(CREATE_COST, now_ms));
        if meter_refuses
            || spent + CREATE_COST > lane.cfg.credit_budget_per_month
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
    let reconciled = json!({ "atMs": now_ms, "at": now() });
    ctx.db
        .run(move |store| {
            store.transaction(|store| {
                set_progress(store, PROGRESS_SPEND, &value)?;
                set_progress(store, PROGRESS_RECONCILED, &reconciled)?;
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

/// Check every `check_seconds` (free, from the ledger) whether a listing is due, and reconcile
/// when it is; otherwise hold the stored alerts. Nothing is listed past the credit cap.
pub async fn reconcile_loop(ctx: &Ctx, lane: &AutoLane, check_seconds: u64) {
    let mut held_logged = false;
    while !ctx.stopping() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = match load_records(ctx).await {
            Ok((records, at))
                if super::elfa::capped(ctx) || !listing_due(&lane.cfg, &records, at, now_ms) =>
            {
                if !held_logged {
                    held_logged = true;
                    log(
                        "elfa_auto_held",
                        Obj::new()
                            .with("active", records.len())
                            .with("reconciledAtMs", at)
                            .with("reconcileIntervalHours", lane.cfg.reconcile_interval_hours)
                            .with("capReached", super::elfa::capped(ctx)),
                    );
                }
                if lane.active().is_empty() {
                    *lane.active.lock().expect("active queries") = records;
                    *lane.last_reconcile.lock().expect("reconcile") = at.and_then(|ms| {
                        chrono::DateTime::from_timestamp_millis(ms).map(|d| d.to_rfc3339())
                    });
                }
                Ok(())
            }
            Ok(_) => {
                held_logged = false;
                reconcile(ctx, lane, now_ms).await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            lane.counters.errors.fetch_add(1, Ordering::Relaxed);
            log_error("reconcile", &error.to_string());
        }
        pause(ctx, check_seconds.max(60)).await;
    }
}

/// How a stream connection ended when nothing went wrong on the lane's side.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// The server sent its `end` event.
    Ended,
    /// The body finished between events.
    Closed,
    /// The connection broke or closed mid-event (the reason, without URLs).
    Dropped(String),
    /// Nothing arrived, not even a keep-alive, for `idle_seconds`.
    Idle,
    /// The lane is stopping.
    Stopped,
}

impl StreamEnd {
    fn reason(&self) -> String {
        match self {
            StreamEnd::Ended => "end event".into(),
            StreamEnd::Closed => "server closed the stream".into(),
            StreamEnd::Dropped(reason) => format!("disconnected: {reason}"),
            StreamEnd::Idle => "idle, no keep-alive".into(),
            StreamEnd::Stopped => "stopping".into(),
        }
    }
}

/// Reconnection delays: doubling from one second to five minutes, back to one second after a
/// connection that held for a minute (a stream the server keeps closing at once backs off).
#[derive(Debug)]
pub struct Backoff {
    next: u64,
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff { next: 1 }
    }
}

impl Backoff {
    /// The wait after a connection that lasted `held`.
    pub fn after_connection(&mut self, held: Duration) -> u64 {
        if held >= Duration::from_secs(60) {
            self.next = 1;
        }
        self.after_failure()
    }

    /// The wait after a failed attempt.
    pub fn after_failure(&mut self) -> u64 {
        let wait = self.next;
        self.next = (self.next * 2).min(300);
        wait
    }
}

/// Hold the account-wide stream and reconnect with backoff. A server close, an `end` event, a
/// broken connection or an idle stream is a reconnection (`elfa_auto_stream_reconnect`), not an
/// error; `410` means no active query; errors are failures to open or to record.
pub async fn stream_loop(ctx: &Ctx, lane: &AutoLane) {
    let mut backoff = Backoff::default();
    while !ctx.stopping() {
        if lane.active().is_empty() || super::elfa::capped(ctx) {
            pause(ctx, 30).await;
            continue;
        }
        let opened = ctx
            .http
            .open(&lane.url("/v2/auto/queries/stream"), &headers(&lane.key))
            .await;
        let wait = match opened {
            Ok(response) => {
                lane.counters.connections.fetch_add(1, Ordering::Relaxed);
                lane.counters.connected.store(true, Ordering::Relaxed);
                let started = std::time::Instant::now();
                let outcome = read_stream(ctx, lane, response).await;
                lane.counters.connected.store(false, Ordering::Relaxed);
                let wait = backoff.after_connection(started.elapsed());
                match outcome {
                    Ok(StreamEnd::Stopped) => break,
                    Ok(end) => {
                        lane.counters.reconnects.fetch_add(1, Ordering::Relaxed);
                        log(
                            "elfa_auto_stream_reconnect",
                            Obj::new()
                                .with("reason", end.reason())
                                .with("connectedSeconds", started.elapsed().as_secs())
                                .with("waitSeconds", wait),
                        );
                    }
                    Err(error) => {
                        lane.counters.errors.fetch_add(1, Ordering::Relaxed);
                        log_error("stream", &error.to_string());
                    }
                }
                wait
            }
            Err(HttpError::Status(410)) => {
                log(
                    "elfa_auto_stream_closed",
                    Obj::new().with("reason", "no active queries (410)"),
                );
                300
            }
            Err(error) => {
                lane.counters.errors.fetch_add(1, Ordering::Relaxed);
                log_error("stream", &error.to_string());
                backoff.after_failure()
            }
        };
        pause(ctx, wait).await;
    }
}

/// A body error with its causes (`error decoding response body: ...: connection reset`).
fn describe(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    safe_error(&text)
}

/// The next body chunk, checking the stop flag every second; `Err` once the stream has been
/// quiet for `idle`.
async fn next_chunk<S>(ctx: &Ctx, body: &mut S, idle: Duration) -> Result<S::Item, StreamEnd>
where
    S: futures_util::Stream + Unpin,
{
    use futures_util::StreamExt;
    let mut quiet = Duration::ZERO;
    let slice = Duration::from_secs(1).min(idle);
    loop {
        if ctx.stopping() {
            return Err(StreamEnd::Stopped);
        }
        match tokio::time::timeout(slice, body.next()).await {
            Ok(Some(item)) => return Ok(item),
            Ok(None) => return Err(StreamEnd::Closed),
            Err(_) => {
                quiet += slice;
                if quiet >= idle {
                    return Err(StreamEnd::Idle);
                }
            }
        }
    }
}

/// Read one connection's events until it ends; record every notification as it arrives.
async fn read_stream(
    ctx: &Ctx,
    lane: &AutoLane,
    response: reqwest::Response,
) -> Result<StreamEnd, StoreError> {
    let mut body = Box::pin(response.bytes_stream());
    let mut parser = SseParser::new();
    let idle = Duration::from_secs(lane.idle_seconds.max(1));
    loop {
        let chunk = match next_chunk(ctx, &mut body, idle).await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => return Ok(StreamEnd::Dropped(describe(&error))),
            Err(StreamEnd::Closed) if parser.pending() => {
                return Ok(StreamEnd::Dropped("closed mid-event".into()));
            }
            Err(end) => return Ok(end),
        };
        for frame in parser.push(&chunk) {
            match frame.event.as_deref() {
                Some("end") => return Ok(StreamEnd::Ended),
                Some("error") => return Err(StoreError::Check(safe_error(&frame.data))),
                Some("notification" | "message") | None if !frame.data.is_empty() => {
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
        let frames = SseParser::new()
            .push(b"id: e1\r\nevent: notification\r\ndata: {\"queryId\":\"q\"}\r\n\r\n");
        assert_eq!(frames.len(), 1);
        let mut backoff = Backoff::default();
        assert_eq!(backoff.after_failure(), 1);
        assert_eq!(backoff.after_connection(Duration::from_secs(5)), 2);
        assert_eq!(backoff.after_connection(Duration::from_secs(5)), 4);
        assert_eq!(backoff.after_connection(Duration::from_secs(90)), 1);
        for _ in 0..12 {
            backoff.after_failure();
        }
        assert_eq!(backoff.after_failure(), 300);
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

    #[test]
    fn listing_is_due_only_when_needed() {
        let def = |title: &str| AlertDef {
            title: title.into(),
            description: String::new(),
            conditions: json!({ "AND": [] }),
            repeat: None,
        };
        let cfg = ElfaAuto {
            enabled: true,
            alerts: vec![def("A"), def("B")],
            ..ElfaAuto::default()
        };
        let hour = 3_600_000;
        let now = 1_791_547_200_000;
        let held = |title: &str, expires: i64| ActiveQuery {
            id: title.to_lowercase(),
            title: title.into(),
            created_at_ms: 0,
            expires_at_ms: expires,
        };
        let records = vec![held("A", now + 500 * hour), held("B", now + 500 * hour)];
        assert!(listing_due(&cfg, &records, None, now), "never listed");
        assert!(
            !listing_due(&cfg, &records, Some(now - hour), now),
            "listed an hour ago"
        );
        assert!(
            listing_due(&cfg, &records, Some(now - 12 * hour), now),
            "interval passed"
        );
        assert!(
            listing_due(&cfg, &records[..1], Some(now - hour), now),
            "B not held"
        );
        let expiring = vec![held("A", now + 500 * hour), held("B", now + 47 * hour)];
        assert!(
            listing_due(&cfg, &expiring, Some(now - hour), now),
            "B due for renewal"
        );
    }
}
