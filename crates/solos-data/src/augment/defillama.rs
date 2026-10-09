//! DefiLlama stablecoin charts: one call returns the whole daily history of the total (or of one
//! coin); the series fetches it once per run and cuts it into month files.

use super::http::Http;
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::StoreError;
use serde_json::Value;
use std::path::Path;
use tokio::sync::OnceCell;

/// The total chart or one stablecoin's chart.
pub struct StablecoinChart {
    /// Chart URL (`/stablecoincharts/all` or `?stablecoin=<id>`).
    pub url: String,
    /// `ALL` or the coin's symbol.
    pub symbol: String,
    /// DefiLlama id, empty for the total.
    pub id: String,
    cache: OnceCell<Vec<Value>>,
}

impl StablecoinChart {
    /// The total of every stablecoin.
    #[must_use]
    pub fn total(base_url: &str) -> StablecoinChart {
        StablecoinChart {
            url: format!("{}/stablecoincharts/all", base_url.trim_end_matches('/')),
            symbol: "ALL".into(),
            id: String::new(),
            cache: OnceCell::new(),
        }
    }

    /// One coin.
    #[must_use]
    pub fn coin(base_url: &str, id: &str, symbol: &str) -> StablecoinChart {
        StablecoinChart {
            url: format!(
                "{}/stablecoincharts/all?stablecoin={id}",
                base_url.trim_end_matches('/')
            ),
            symbol: symbol.to_owned(),
            id: id.to_owned(),
            cache: OnceCell::new(),
        }
    }

    async fn history(&self, http: &Http) -> Result<&Vec<Value>, StoreError> {
        self.cache
            .get_or_try_init(|| async {
                let answer = http.get_json(&self.url, &[]).await?;
                let points = answer
                    .as_array()
                    .ok_or_else(|| StoreError::Check("stablecoin chart is not an array".into()))?;
                Ok(points.iter().filter_map(|p| flatten(p, &self.id)).collect())
            })
            .await
    }
}

/// One chart point as a flat row: `date` seconds, the `peggedUSD` figures and the point's JSON.
#[must_use]
pub fn flatten(point: &Value, id: &str) -> Option<Value> {
    let date = match point.get("date")? {
        Value::String(s) => s.parse::<i64>().ok()?,
        Value::Number(n) => n.as_i64()?,
        _ => return None,
    };
    let usd = |key: &str| {
        point
            .get(key)
            .and_then(|v| v.get("peggedUSD"))
            .cloned()
            .unwrap_or(Value::Null)
    };
    Some(serde_json::json!({
        "date": date,
        "stablecoin_id": id,
        "total_circulating": usd("totalCirculating"),
        "total_circulating_usd": usd("totalCirculatingUSD"),
        "total_unreleased": usd("totalUnreleased"),
        "total_minted_usd": usd("totalMintedUSD"),
        "total_bridged_to_usd": usd("totalBridgedToUSD"),
        "raw_json": point.to_string(),
    }))
}

impl Series for StablecoinChart {
    fn source(&self) -> &str {
        "defillama"
    }
    fn dataset(&self) -> &str {
        "stablecoins"
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        None
    }
    fn granularity(&self) -> Granularity {
        Granularity::Month
    }
    fn lag_ms(&self) -> i64 {
        86_400_000
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
            "SELECT date AS date_s, make_timestamp(date * 1000000) AS ts, stablecoin_id, total_circulating,
                    total_circulating_usd, total_unreleased, total_minted_usd, total_bridged_to_usd, raw_json, {constants}
             FROM {} ORDER BY date",
            read_json_array(
                staged,
                &[
                    ("date", "BIGINT"), ("stablecoin_id", "VARCHAR"), ("total_circulating", "DOUBLE"),
                    ("total_circulating_usd", "DOUBLE"), ("total_unreleased", "DOUBLE"), ("total_minted_usd", "DOUBLE"),
                    ("total_bridged_to_usd", "DOUBLE"), ("raw_json", "VARCHAR")
                ]
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/llama-all.json");

    #[test]
    fn flattens_points() {
        let sample: Vec<Value> = serde_json::from_str(SAMPLE).unwrap();
        let rows: Vec<Value> = sample.iter().filter_map(|p| flatten(p, "")).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["date"], 1_791_504_000);
        assert!(rows[1]["total_circulating_usd"].as_f64().unwrap() > 3.0e11);
        let point = serde_json::json!({ "date": "1791504000", "totalCirculating": { "peggedUSD": 1.5, "peggedEUR": 2 }, "totalCirculatingUSD": { "peggedUSD": 1.6 } });
        let row = flatten(&point, "1").unwrap();
        assert_eq!(row["date"], 1_791_504_000);
        assert_eq!(row["total_circulating"], 1.5);
        assert_eq!(row["total_unreleased"], Value::Null);
        assert!(row["raw_json"].as_str().unwrap().contains("peggedEUR"));
        assert!(flatten(&serde_json::json!({ "x": 1 }), "1").is_none());
    }
}
