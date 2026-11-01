//! One `node_fills_by_block` line: an independent event count, and one row per event.
//!
//! `count_events` only reads the `events` array length. `rows_of_line` interprets fields. The
//! hour fails when the two disagree, so a later edit that drops a coin cannot publish a short
//! file.

use super::{CoinClass, FILL_COLUMNS};
use serde::Serialize;
use serde_json::{Map, Value};

/// One fill. Field order matches [`FILL_COLUMNS`]. Absent, JSON null, and `""` are NULL.
#[derive(Debug, Serialize)]
pub struct FillRow {
    time_ms: i64,
    coin: String,
    coin_class: &'static str,
    user: String,
    px: Option<f64>,
    sz: Option<f64>,
    side: Option<String>,
    dir: Option<String>,
    start_position: Option<f64>,
    closed_pnl: Option<f64>,
    fee: Option<f64>,
    fee_token: Option<String>,
    crossed: Option<bool>,
    oid: Option<i64>,
    tid: Option<i64>,
    hash: Option<String>,
    liquidation: Option<String>,
    block_number: Option<i64>,
    block_time: Option<String>,
    twap_id: Option<i64>,
    cloid: Option<String>,
    builder: Option<String>,
    builder_fee: Option<f64>,
    deployer_fee: Option<f64>,
    priority_gas: Option<f64>,
}

impl FillRow {
    /// Fill time, milliseconds. The forward split reads only this.
    pub(super) fn time_ms(&self) -> i64 {
        self.time_ms
    }
}

/// Number of fill events on one block line. Does not read fill fields.
///
/// # Errors
///
/// The line is not a JSON object with an `events` array.
pub fn count_events(line: &str) -> Result<u64, String> {
    let value: Value = serde_json::from_str(line).map_err(|e| format!("json: {e}"))?;
    let events = value
        .get("events")
        .and_then(Value::as_array)
        .ok_or("events is missing")?;
    u64::try_from(events.len()).map_err(|_| "too many events".to_owned())
}

/// One row per event on one block line.
///
/// # Errors
///
/// The line is not a `node_fills_by_block` object, an event is not a `[user, fill]` pair, or a
/// required field (`coin`, `user`, `time`) is missing. A present value of the wrong shape fails
/// the line; it is not stored as NULL or 0.
pub fn rows_of_line(line: &str) -> Result<Vec<FillRow>, String> {
    let value: Value = serde_json::from_str(line).map_err(|e| format!("json: {e}"))?;
    let block = value.as_object().ok_or("not a node_fills_by_block line")?;
    let events = block
        .get("events")
        .and_then(Value::as_array)
        .ok_or("events is missing")?;
    let block_number = optional_int_at(block.get("block_number"), "block_number")?;
    let block_time = optional_text_at(block.get("block_time"), "block_time")?;
    let mut rows = Vec::with_capacity(events.len());
    for event in events {
        rows.push(row_of_event(event, block_number, block_time.clone())?);
    }
    Ok(rows)
}

fn row_of_event(
    event: &Value,
    block_number: Option<i64>,
    block_time: Option<String>,
) -> Result<FillRow, String> {
    let pair = event.as_array().ok_or("event is not a pair")?;
    if pair.len() != 2 {
        return Err("event is not a [user, fill] pair".into());
    }
    let user = match &pair[0] {
        Value::String(text) if !text.is_empty() => text.clone(),
        _ => return Err("user is missing".into()),
    };
    let fill = pair[1].as_object().ok_or("fill is not an object")?;
    let coin = required_text(fill, "coin")?;
    let time_ms = required_int(fill, "time")?;
    Ok(FillRow {
        time_ms,
        coin_class: CoinClass::of(&coin).as_str(),
        coin,
        user,
        px: optional_decimal(fill, "px")?,
        sz: optional_decimal(fill, "sz")?,
        side: optional_text(fill, "side")?,
        dir: optional_text(fill, "dir")?,
        start_position: optional_decimal(fill, "startPosition")?,
        closed_pnl: optional_decimal(fill, "closedPnl")?,
        fee: optional_decimal(fill, "fee")?,
        fee_token: optional_text(fill, "feeToken")?,
        crossed: optional_bool(fill, "crossed")?,
        oid: optional_int(fill, "oid")?,
        tid: optional_int(fill, "tid")?,
        hash: optional_text(fill, "hash")?,
        liquidation: optional_liquidation(fill)?,
        block_number,
        block_time,
        twap_id: optional_int(fill, "twapId")?,
        cloid: optional_text(fill, "cloid")?,
        builder: optional_text(fill, "builder")?,
        builder_fee: optional_decimal(fill, "builderFee")?,
        deployer_fee: optional_decimal(fill, "deployerFee")?,
        priority_gas: optional_decimal(fill, "priorityGas")?,
    })
}

fn required_text(map: &Map<String, Value>, key: &str) -> Result<String, String> {
    optional_text(map, key)?.ok_or_else(|| format!("{key} is missing"))
}

fn required_int(map: &Map<String, Value>, key: &str) -> Result<i64, String> {
    optional_int(map, key)?.ok_or_else(|| format!("{key} is missing"))
}

fn optional_text(map: &Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    optional_text_at(map.get(key), key)
}

fn optional_text_at(value: Option<&Value>, key: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(format!("{key} is not a string")),
    }
}

fn optional_decimal(map: &Map<String, Value>, key: &str) -> Result<Option<f64>, String> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => parse_decimal(key, text).map(Some),
        Some(Value::Number(number)) => finite(key, number.as_f64()).map(Some),
        Some(_) => Err(format!("{key} is not a decimal")),
    }
}

fn parse_decimal(key: &str, text: &str) -> Result<f64, String> {
    let value: f64 = text
        .parse()
        .map_err(|_| format!("{key} is not a decimal"))?;
    finite(key, Some(value))
}

fn finite(key: &str, value: Option<f64>) -> Result<f64, String> {
    match value {
        Some(number) if number.is_finite() => Ok(number),
        _ => Err(format!("{key} is not a finite decimal")),
    }
}

fn optional_int(map: &Map<String, Value>, key: &str) -> Result<Option<i64>, String> {
    optional_int_at(map.get(key), key)
}

fn optional_int_at(value: Option<&Value>, key: &str) -> Result<Option<i64>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => text
            .parse()
            .map(Some)
            .map_err(|_| format!("{key} is not an integer")),
        Some(Value::Number(number)) => json_i64(number)
            .map(Some)
            .ok_or_else(|| format!("{key} is not an integer")),
        Some(_) => Err(format!("{key} is not an integer")),
    }
}

fn json_i64(number: &serde_json::Number) -> Option<i64> {
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn optional_bool(map: &Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("{key} is not a boolean")),
    }
}

fn optional_liquidation(map: &Map<String, Value>) -> Result<Option<String>, String> {
    match map.get("liquidation") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(Value::Object(object)) => serde_json::to_string(object)
            .map(Some)
            .map_err(|e| format!("liquidation: {e}")),
        Some(_) => Err("liquidation is not an object".into()),
    }
}

/// Column list for `SELECT`, in [`FILL_COLUMNS`] order.
pub fn select_list() -> String {
    FILL_COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_ignores_fields_and_rows_keep_nulls() {
        let line = r##"{"block_number":7,"block_time":"2026-10-22T12:00:00.000000000","events":[["0xa",{"coin":"BTC","px":"1.5","sz":"","side":"B","time":1,"fee":"-0.25","twapId":null,"hash":"0x0000000000000000000000000000000000000000000000000000000000000000"}],["0xb",{"coin":"#1","time":2,"px":"nope"}]]}"##;
        assert_eq!(count_events(line).unwrap(), 2);
        let rows = rows_of_line(line);
        assert!(rows.is_err(), "a bad decimal fails the line");
        let ok = r#"{"events":[["0xa",{"coin":"BTC","time":1,"px":"1.5","sz":"","fee":"-0.25","twapId":null,"cloid":"","hash":"0x0000000000000000000000000000000000000000000000000000000000000000"}]]}"#;
        assert_eq!(count_events(ok).unwrap(), 1);
        let rows = rows_of_line(ok).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coin_class, "perp");
        assert_eq!(rows[0].px, Some(1.5));
        assert_eq!(rows[0].sz, None);
        assert_eq!(rows[0].fee, Some("-0.25".parse::<f64>().unwrap()));
        assert_eq!(rows[0].twap_id, None);
        assert_eq!(rows[0].cloid, None);
        assert!(rows[0].hash.as_ref().unwrap().ends_with("0000"));
        assert!(count_events("{\"events\":").is_err());
        assert!(rows_of_line("[\"0xa\",{}]").is_err());
    }
}
