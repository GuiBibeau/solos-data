//! Polymarket: the curated events are expanded through the Gamma API into their markets and
//! outcome tokens; each market is a paged history of hourly CLOB prices per token, cut into
//! month files, and the market catalogue is merged into the day's `markets` file.

use super::config::Polymarket;
use super::http::{Http, with_query};
use super::ledger::register;
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome, RowsFuture, Series, finished, sync_series};
use crate::jsonout::{Obj, log, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Mutex;

/// One market: a price history per outcome token.
pub struct TokenPrices {
    /// CLOB base.
    pub clob_url: String,
    /// Gamma market id, the directory name.
    pub market_id: String,
    /// The market's question.
    pub question: String,
    /// Gamma event id and slug.
    pub event: (String, String),
    /// `(token id, outcome)` in outcome order.
    pub tokens: Vec<(String, String)>,
    /// Whether Gamma reports the market closed, and its end instant.
    pub closed: bool,
    /// `endDate` in milliseconds, when parseable.
    pub end_ms: Option<i64>,
    /// The run's full history from a start instant, kept for the following periods.
    cache: Mutex<Option<(i64, Vec<Value>)>>,
}

impl TokenPrices {
    /// A market from its Gamma object.
    #[must_use]
    pub fn from_market(clob_url: &str, market: &Value, event: (&str, &str)) -> Option<TokenPrices> {
        let outcomes = list_field(market, "outcomes");
        let token_ids = list_field(market, "clobTokenIds");
        if token_ids.is_empty() {
            return None;
        }
        Some(TokenPrices {
            clob_url: clob_url.trim_end_matches('/').to_owned(),
            market_id: id_of(market)?,
            question: market
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            event: (event.0.to_owned(), event.1.to_owned()),
            tokens: token_ids
                .into_iter()
                .enumerate()
                .map(|(i, id)| (id, outcomes.get(i).cloned().unwrap_or_default()))
                .collect(),
            closed: market
                .get("closed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            end_ms: market
                .get("endDate")
                .and_then(Value::as_str)
                .and_then(|d| chrono::DateTime::parse_from_rfc3339(d).ok())
                .map(|d| d.timestamp_millis()),
            cache: Mutex::new(None),
        })
    }

    async fn history(&self, http: &Http, start_ms: i64) -> Result<Vec<Value>, StoreError> {
        if let Some((from, rows)) = &*self.cache.lock().expect("price cache")
            && *from <= start_ms
        {
            return Ok(rows.clone());
        }
        let mut rows = Vec::new();
        for (index, (token, outcome)) in self.tokens.iter().enumerate() {
            let url = with_query(
                &format!("{}/prices-history", self.clob_url),
                &[
                    ("market", token.clone()),
                    ("startTs", (start_ms / 1000).to_string()),
                    ("fidelity", "60".into()),
                ],
            );
            let answer = http.get_json(&url, &[]).await?;
            let history = answer
                .get("history")
                .and_then(Value::as_array)
                .ok_or_else(|| StoreError::Check("prices-history has no history".into()))?;
            rows.extend(history.iter().filter_map(|point| {
                Some(json!({
                    "t": point.get("t")?.as_i64()?, "p": point.get("p")?.as_f64()?,
                    "token_id": token, "outcome": outcome, "outcome_index": index,
                }))
            }));
        }
        *self.cache.lock().expect("price cache") = Some((start_ms, rows.clone()));
        Ok(rows)
    }
}

/// A Gamma field that is a JSON array encoded as a string (`outcomes`, `clobTokenIds`).
#[must_use]
pub fn list_field(market: &Value, name: &str) -> Vec<String> {
    let value = match market.get(name) {
        Some(Value::String(text)) => serde_json::from_str::<Value>(text).unwrap_or(Value::Null),
        Some(other) => other.clone(),
        None => Value::Null,
    };
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

fn id_of(value: &Value) -> Option<String> {
    match value.get("id")? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

impl Series for TokenPrices {
    fn source(&self) -> &str {
        "polymarket"
    }
    fn dataset(&self) -> &str {
        "prices"
    }
    fn symbol(&self) -> &str {
        &self.market_id
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        None
    }
    fn granularity(&self) -> Granularity {
        Granularity::Month
    }
    fn lag_ms(&self) -> i64 {
        3_600_000
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let rows = self.history(http, start_ms).await?;
            Ok(rows
                .into_iter()
                .filter(|row| {
                    let ms = row.get("t").and_then(Value::as_i64).unwrap_or(0) * 1000;
                    ms >= start_ms && ms < end_ms
                })
                .collect())
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT t AS time_s, make_timestamp(t * 1000000) AS ts, p AS price, token_id, outcome, outcome_index,
                    {} AS market_id, {} AS question, {} AS event_id, {} AS event_slug, {constants}
             FROM {} ORDER BY t, outcome_index",
            crate::store::sql_string(&self.market_id),
            crate::store::sql_string(&self.question),
            crate::store::sql_string(&self.event.0),
            crate::store::sql_string(&self.event.1),
            read_json_array(
                staged,
                &[("t", "BIGINT"), ("p", "DOUBLE"), ("token_id", "VARCHAR"), ("outcome", "VARCHAR"), ("outcome_index", "BIGINT")]
            )
        )
    }
}

/// The catalogue rows of an event's markets.
#[must_use]
pub fn market_rows(event: &Value, fetched_at_ms: i64) -> Vec<Value> {
    let event_id = id_of(event).unwrap_or_default();
    let slug = event.get("slug").cloned().unwrap_or(Value::Null);
    let title = event.get("title").cloned().unwrap_or(Value::Null);
    event
        .get("markets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|m| {
            Some(json!({
                "market_id": id_of(m)?, "event_id": event_id, "event_slug": slug, "event_title": title,
                "question": m.get("question"), "group_item_title": m.get("groupItemTitle"), "condition_id": m.get("conditionId"),
                "outcomes": list_field(m, "outcomes"), "token_ids": list_field(m, "clobTokenIds"),
                "outcome_prices": list_field(m, "outcomePrices"),
                "start_date": m.get("startDate"), "end_date": m.get("endDate"), "active": m.get("active"), "closed": m.get("closed"),
                "volume": m.get("volumeNum"), "liquidity": m.get("liquidityNum"), "fetched_at": fetched_at_ms,
            }))
        })
        .collect()
}

/// The typed `SELECT` of staged catalogue rows.
#[must_use]
pub fn markets_select(staged: &Path) -> String {
    format!(
        "SELECT market_id, event_id, event_slug, event_title, question, group_item_title, condition_id, outcomes, token_ids,
                outcome_prices, start_date, end_date, active, closed, volume, liquidity,
                fetched_at AS fetched_at_ms, make_timestamp(fetched_at * 1000) AS ts, 'ALL' AS symbol
         FROM {}",
        read_json_array(
            staged,
            &[
                ("market_id", "VARCHAR"), ("event_id", "VARCHAR"), ("event_slug", "VARCHAR"), ("event_title", "VARCHAR"),
                ("question", "VARCHAR"), ("group_item_title", "VARCHAR"), ("condition_id", "VARCHAR"),
                ("outcomes", "VARCHAR[]"), ("token_ids", "VARCHAR[]"), ("outcome_prices", "VARCHAR[]"),
                ("start_date", "VARCHAR"), ("end_date", "VARCHAR"), ("active", "BOOLEAN"), ("closed", "BOOLEAN"),
                ("volume", "DOUBLE"), ("liquidity", "DOUBLE"), ("fetched_at", "BIGINT")
            ]
        )
    )
}

/// Sync every curated event: its catalogue row and every market's token prices.
pub async fn sync(ctx: &Ctx, cfg: &Polymarket, only_symbol: Option<&str>) -> Outcome {
    let mut outcome = Outcome::default();
    for event in &cfg.events {
        if ctx.stopping() {
            break;
        }
        let url = format!(
            "{}/events/{}",
            cfg.gamma_url.trim_end_matches('/'),
            event.id
        );
        let answer = match ctx.http.get_json(&url, &[]).await {
            Ok(answer) => answer,
            Err(error) => {
                outcome.errors += 1;
                log_error("markets", &event.slug, &error.to_string());
                continue;
            }
        };
        let rows = market_rows(&answer, ctx.now_ms);
        match write_markets(ctx, &rows).await {
            Ok(_) => outcome.files += 1,
            Err(error) => {
                outcome.errors += 1;
                log_error("markets", &event.slug, &error.to_string());
            }
        }
        for market in answer
            .get("markets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(series) =
                TokenPrices::from_market(&cfg.clob_url, market, (&event.id, &event.slug))
            else {
                continue;
            };
            if only_symbol.is_some_and(|s| s != series.market_id && s != event.slug)
                || ctx.stopping()
            {
                continue;
            }
            if series.closed
                && let Some(end) = series.end_ms
                && finished(ctx, &series, end).await
            {
                outcome.skipped += 1;
                continue;
            }
            outcome.add(&sync_series(ctx, &series).await);
        }
    }
    outcome
}

async fn write_markets(ctx: &Ctx, rows: &[Value]) -> Result<i64, StoreError> {
    if rows.is_empty() {
        return Err(StoreError::Check("event has no markets".into()));
    }
    let staged = stage_json(&ctx.staging(), rows)?;
    let day = Period::containing(date_of_ms(ctx.now_ms), Granularity::Day);
    let target = Target {
        source: "polymarket".into(),
        dataset: "markets".into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete: false,
    };
    let select = markets_select(&staged);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, "market_id", &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

fn log_error(dataset: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "polymarket")
            .with("dataset", dataset)
            .with("symbol", "ALL")
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/gamma-event.json");

    #[test]
    fn markets_expand_to_tokens_and_catalogue_rows() {
        let event: Value = serde_json::from_str(SAMPLE).unwrap();
        let rows = market_rows(&event, 7);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["market_id"], "2589811");
        assert_eq!(rows[0]["event_id"], "606422");
        assert_eq!(rows[0]["outcomes"], json!(["Yes", "No"]));
        assert_eq!(rows[0]["token_ids"].as_array().unwrap().len(), 2);
        let market = &event["markets"][1];
        let series = TokenPrices::from_market("https://clob/", market, ("606422", "fed")).unwrap();
        assert_eq!(series.market_id, "2589812");
        assert_eq!(series.tokens.len(), 2);
        assert_eq!(series.tokens[1].1, "No");
        assert!(!series.closed);
        assert_eq!(series.end_ms, Some(1_793_246_340_000));
        assert!(
            TokenPrices::from_market("https://clob", &json!({ "id": "1" }), ("e", "s")).is_none()
        );
    }
}
