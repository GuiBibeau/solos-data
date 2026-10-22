//! Kalshi's public read endpoints (no key) and their two tiers. Markets settled before
//! `GET /historical/cutoff` are served only under `/historical/...`: their listing, their
//! candlesticks (one market a call, at most 5,000 periods) and their trades. The live tier
//! lists the rest and serves candlesticks for up to 100 markets a call (10,000 candles at most),
//! but answers an empty list for an archived market, which is how a misrouted market shows.

use crate::augment::http::{Http, HttpError, with_query};
use crate::store::StoreError;
use serde_json::{Value, json};

/// Markets per listing page.
pub const PAGE: usize = 1000;
/// Periods one single-market candlestick call may span.
pub const SINGLE_PERIODS: i64 = 4_800;
/// Candles one batch call may return, all markets together (the API's ceiling is 10,000).
pub const BATCH_CANDLES: i64 = 9_000;
/// Markets one batch call may name.
pub const BATCH_MARKETS: usize = 100;

/// One page and the cursor of the next.
pub type Page = (Vec<Value>, Option<String>);

/// The API base on the lane's client.
pub struct Api<'a> {
    /// The lane's HTTP client (Kalshi's host has its own pace).
    pub http: &'a Http,
    base: String,
}

impl<'a> Api<'a> {
    /// The API at `base` (`…/trade-api/v2`).
    #[must_use]
    pub fn new(http: &'a Http, base: &str) -> Api<'a> {
        Api {
            http,
            base: base.trim_end_matches('/').to_owned(),
        }
    }

    async fn get(&self, path: &str, pairs: &[(&str, String)]) -> Result<Value, StoreError> {
        let url = with_query(&format!("{}{path}", self.base), pairs);
        match self.http.get_json(&url, &[]).await {
            Ok(value) => Ok(value),
            Err(HttpError::NotFound) => Ok(json!({})),
            Err(error) => Err(StoreError::Check(error.to_string())),
        }
    }

    async fn page(
        &self,
        path: &str,
        key: &str,
        pairs: Vec<(&str, String)>,
    ) -> Result<Page, StoreError> {
        let answer = self.get(path, &pairs).await?;
        let rows = answer
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok((rows, next_cursor(&answer)))
    }

    /// The archive boundaries, milliseconds: markets settled before the first are historical,
    /// trades created before the second are served only by `/historical/trades` (Kalshi moves
    /// the two separately; the trade one falls back to the market one when absent).
    pub async fn cutoffs(&self) -> Result<(i64, i64), StoreError> {
        let answer = self.get("/historical/cutoff", &[]).await?;
        let at = |key: &str| {
            answer
                .get(key)
                .and_then(Value::as_str)
                .and_then(super::rows::parse_ms)
        };
        let markets = at("market_settled_ts")
            .ok_or_else(|| StoreError::Check("cutoff has no market_settled_ts".into()))?;
        Ok((markets, at("trades_created_ts").unwrap_or(markets)))
    }

    /// One page of a series' live-tier markets closing at or after `min_close_s`.
    pub async fn live_markets(
        &self,
        series: &str,
        min_close_s: i64,
        cursor: Option<&str>,
    ) -> Result<Page, StoreError> {
        let mut pairs = vec![
            ("series_ticker", series.to_owned()),
            ("min_close_ts", min_close_s.to_string()),
            ("limit", PAGE.to_string()),
        ];
        if let Some(cursor) = cursor {
            pairs.push(("cursor", cursor.to_owned()));
        }
        self.page("/markets", "markets", pairs).await
    }

    /// One page of a series' archived markets, newest close first (the endpoint ignores the
    /// close-time filters, so the walk pages back by cursor).
    pub async fn historical_markets(
        &self,
        series: &str,
        cursor: Option<&str>,
    ) -> Result<Page, StoreError> {
        let mut pairs = vec![
            ("series_ticker", series.to_owned()),
            ("limit", PAGE.to_string()),
        ];
        if let Some(cursor) = cursor {
            pairs.push(("cursor", cursor.to_owned()));
        }
        self.page("/historical/markets", "markets", pairs).await
    }

    /// One archived market's candlesticks over `[start_s, end_s]`.
    pub async fn historical_candles(
        &self,
        ticker: &str,
        start_s: i64,
        end_s: i64,
        interval: u32,
    ) -> Result<Vec<Value>, StoreError> {
        let answer = self
            .get(
                &format!("/historical/markets/{ticker}/candlesticks"),
                &[
                    ("start_ts", start_s.to_string()),
                    ("end_ts", end_s.to_string()),
                    ("period_interval", interval.to_string()),
                ],
            )
            .await?;
        Ok(answer
            .get("candlesticks")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// Live-tier candlesticks of up to 100 markets, by market ticker. A market the tier does
    /// not serve is absent from the answer.
    pub async fn batch_candles(
        &self,
        tickers: &[String],
        start_s: i64,
        end_s: i64,
        interval: u32,
    ) -> Result<Vec<(String, Vec<Value>)>, StoreError> {
        let answer = self
            .get(
                "/markets/candlesticks",
                &[
                    ("market_tickers", tickers.join(",")),
                    ("start_ts", start_s.to_string()),
                    ("end_ts", end_s.to_string()),
                    ("period_interval", interval.to_string()),
                ],
            )
            .await?;
        let markets = answer.get("markets").and_then(Value::as_array);
        Ok(markets
            .into_iter()
            .flatten()
            .filter_map(|m| {
                let ticker = m.get("market_ticker")?.as_str()?.to_owned();
                let candles = m.get("candlesticks")?.as_array()?.clone();
                Some((ticker, candles))
            })
            .collect())
    }

    /// One page of a market's trades; `historical` picks the archive.
    pub async fn trades(
        &self,
        ticker: &str,
        historical: bool,
        cursor: Option<&str>,
    ) -> Result<Page, StoreError> {
        let path = if historical {
            "/historical/trades"
        } else {
            "/markets/trades"
        };
        let mut pairs = vec![("ticker", ticker.to_owned()), ("limit", PAGE.to_string())];
        if let Some(cursor) = cursor {
            pairs.push(("cursor", cursor.to_owned()));
        }
        self.page(path, "trades", pairs).await
    }
}

fn next_cursor(answer: &Value) -> Option<String> {
    answer
        .get("cursor")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
        .map(str::to_owned)
}

/// `[start_s, end_s]` cut into windows of at most `periods` candles of `interval` minutes.
#[must_use]
pub fn windows(start_s: i64, end_s: i64, interval: u32, periods: i64) -> Vec<(i64, i64)> {
    let span = i64::from(interval) * 60 * periods.max(1);
    let mut out = Vec::new();
    let mut at = start_s;
    while at < end_s {
        let until = (at + span).min(end_s);
        out.push((at, until));
        at = until;
    }
    out
}

/// How many markets one batch call over a `seconds` window may name.
#[must_use]
pub fn batch_width(seconds: i64, interval: u32) -> usize {
    let periods = (seconds / (i64::from(interval) * 60)).max(1) + 1;
    usize::try_from((BATCH_CANDLES / periods).max(1))
        .unwrap_or(1)
        .min(BATCH_MARKETS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_respect_the_period_ceiling() {
        assert_eq!(windows(0, 3_600, 1, 4_800), vec![(0, 3_600)]);
        let long = windows(0, 5 * 86_400, 1, 4_800);
        assert_eq!(long.len(), 2);
        assert_eq!(long[0], (0, 288_000));
        assert_eq!(long[1], (288_000, 432_000));
        assert!(windows(10, 10, 1, 4_800).is_empty());
    }

    #[test]
    fn batch_width_keeps_under_the_candle_ceiling() {
        assert_eq!(batch_width(3_600, 1), 100);
        assert_eq!(batch_width(26 * 3_600, 1), 5);
        assert_eq!(batch_width(90 * 86_400, 60), 4);
        assert_eq!(batch_width(400 * 86_400, 1), 1);
    }
}
