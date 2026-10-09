//! alternative.me's Fear and Greed index: one call returns the whole daily history since
//! February 2018; the series fetches it once per run and cuts it into year files.

use super::http::Http;
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::StoreError;
use chrono::NaiveDate;
use serde_json::{Value, json};
use std::path::Path;
use tokio::sync::OnceCell;

/// The index history.
pub struct FearGreed {
    /// `/fng/?limit=0&format=json`.
    pub url: String,
    cache: OnceCell<Vec<Value>>,
}

impl FearGreed {
    /// The series for an API base.
    #[must_use]
    pub fn new(base_url: &str) -> FearGreed {
        FearGreed {
            url: format!(
                "{}/fng/?limit=0&format=json",
                base_url.trim_end_matches('/')
            ),
            cache: OnceCell::new(),
        }
    }

    async fn history(&self, http: &Http) -> Result<&Vec<Value>, StoreError> {
        self.cache
            .get_or_try_init(|| async {
                let answer = http.get_json(&self.url, &[]).await?;
                let data = answer
                    .get("data")
                    .and_then(Value::as_array)
                    .ok_or_else(|| StoreError::Check("fng answer has no data".into()))?;
                Ok(data.iter().filter_map(flatten).collect())
            })
            .await
    }
}

/// One point as a flat row: `date` seconds, the integer value and its classification.
#[must_use]
pub fn flatten(point: &Value) -> Option<Value> {
    let as_i64 = |key: &str| match point.get(key)? {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    };
    Some(json!({
        "date": as_i64("timestamp")?,
        "value": as_i64("value")?,
        "classification": point.get("value_classification"),
    }))
}

impl Series for FearGreed {
    fn source(&self) -> &str {
        "alternative"
    }
    fn dataset(&self) -> &str {
        "fear_greed"
    }
    fn symbol(&self) -> &str {
        "ALL"
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        None
    }
    fn granularity(&self) -> Granularity {
        Granularity::Year
    }
    fn lag_ms(&self) -> i64 {
        86_400_000
    }
    fn start_date(&self, _lane_start: NaiveDate) -> NaiveDate {
        NaiveDate::from_ymd_opt(2018, 2, 1).unwrap_or(NaiveDate::MIN)
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let history = self.history(http).await?;
            Ok(history
                .iter()
                .filter(|row| {
                    let ms = row.get("date").and_then(Value::as_i64).unwrap_or(0) * 1000;
                    ms >= start_ms && ms < end_ms
                })
                .cloned()
                .collect())
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT date AS date_s, make_timestamp(date * 1000000) AS ts, CAST(make_timestamp(date * 1000000) AS DATE) AS day,
                    value, classification, {constants}
             FROM {} ORDER BY date",
            read_json_array(
                staged,
                &[("date", "BIGINT"), ("value", "BIGINT"), ("classification", "VARCHAR")]
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/fng.json");

    #[test]
    fn flattens_points_and_starts_in_2018() {
        let sample: Value = serde_json::from_str(SAMPLE).unwrap();
        let rows: Vec<Value> = sample["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(flatten)
            .collect();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["date"], 1_791_504_000);
        assert_eq!(rows[0]["value"], 59);
        assert_eq!(rows[0]["classification"], "Greed");
        assert_eq!(rows[3]["date"], 1_517_443_200);
        assert!(flatten(&json!({ "value": "x" })).is_none());
        let series = FearGreed::new("https://x/");
        assert_eq!(series.url, "https://x/fng/?limit=0&format=json");
        assert_eq!(
            series
                .start_date(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
                .to_string(),
            "2018-02-01"
        );
        assert_eq!(series.granularity(), Granularity::Year);
    }
}
