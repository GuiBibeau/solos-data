//! One `l2Book` line: an independent snapshot count, and one flat row.
//!
//! `count_snapshot` only checks that the line is one book object. `snapshot_of_line` reads the
//! fields. The hour fails when the two disagree, so a later edit that drops a line cannot
//! publish a short file. Prices and sizes are the same `f64` conversion as the fills lane: a
//! decimal string parsed as `f64` keeps those bits through JSON, DuckDB, and Parquet.

use super::BOOK_LEVELS;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// The duplicate rule for two snapshots of one coin with the same `time_ms` (ADR-0021).
///
/// A downloader sample found duplicate `time_ms` in 22 percent of real coin-hours: 207 of
/// 208 were byte-identical re-sends, one was a different book. The rule:
///
/// * [`DuplicateRule::Identical`]: the books are the same (both sides' `px`, `sz`, and `n`,
///   all [`BOOK_LEVELS`] levels, compared as exact `f64` bits). The later line is a re-send
///   of the same observation: it is dropped and counted `dup_identical`. This is book
///   identity, not byte identity: `recv_time_ms` is envelope metadata and is excluded, so a
///   re-send under a later envelope time is still `Identical`, and the first line's value
///   stays.
/// * [`DuplicateRule::Distinct`]: the books differ. Both rows are kept, in archive line
///   order (the staged `_seq` key), and the extra row is counted `dup_distinct`.
///
/// A duplicate is never an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuplicateRule {
    /// Same book: drop the later line, count `dup_identical`.
    Identical,
    /// Different book at the same `time_ms`: keep the row, count `dup_distinct`.
    Distinct,
}

impl DuplicateRule {
    /// Classify a later line against every book already kept for the same `(coin, time_ms)`.
    /// A third line that repeats an earlier book is [`DuplicateRule::Identical`] again.
    #[must_use]
    pub fn classify(seen: &[[u8; 32]], digest: &[u8; 32]) -> Self {
        if seen.contains(digest) {
            Self::Identical
        } else {
            Self::Distinct
        }
    }
}

/// SHA-256 over the book bits: for each side, each level, the `f64` bits of `px` and `sz`
/// and the `i64` bits of `n`. This is the identity [`DuplicateRule`] compares.
#[must_use]
pub fn book_digest(row: &Snapshot) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let sides = [
        (&row.bid_px, &row.bid_sz, &row.bid_n),
        (&row.ask_px, &row.ask_sz, &row.ask_n),
    ];
    for (px, sz, n) in sides {
        for level in 0..BOOK_LEVELS {
            hasher.update(tagged_f64(px[level]));
            hasher.update(tagged_f64(sz[level]));
            hasher.update(tagged_i64(n[level]));
        }
    }
    hasher.finalize().into()
}

/// A `None` level and a present `0.0` or `0` hash differently: one byte of presence tag,
/// then the value's bits.
fn tagged_f64(value: Option<f64>) -> [u8; 9] {
    let mut tagged = [0; 9];
    if let Some(number) = value {
        tagged[0] = 1;
        tagged[1..].copy_from_slice(&number.to_bits().to_le_bytes());
    }
    tagged
}

fn tagged_i64(value: Option<i64>) -> [u8; 9] {
    let mut tagged = [0; 9];
    if let Some(number) = value {
        tagged[0] = 1;
        tagged[1..].copy_from_slice(&number.to_le_bytes());
    }
    tagged
}

/// Two snapshots hold the same book. `recv_time_ms` may differ. The publish path compares
/// digests it keeps per `(coin, time_ms)`; this is the same rule stated over rows, for tests
/// and readers.
#[cfg(test)]
#[must_use]
pub fn same_book(a: &Snapshot, b: &Snapshot) -> bool {
    book_digest(a) == book_digest(b)
}

/// One snapshot. Index 0 is the best bid or ask (the first level in the archive array).
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// `data.coin`. Must match the file name.
    pub coin: String,
    /// `data.time`, milliseconds.
    pub time_ms: i64,
    /// Envelope `time` when it is present, milliseconds. The archive field is often an ISO
    /// timestamp; a number is milliseconds (or seconds when it is below 10^12).
    pub recv_time_ms: Option<i64>,
    /// Bid prices, null past the last published level.
    pub bid_px: [Option<f64>; BOOK_LEVELS],
    /// Bid sizes.
    pub bid_sz: [Option<f64>; BOOK_LEVELS],
    /// Bid order counts.
    pub bid_n: [Option<i64>; BOOK_LEVELS],
    /// Ask prices.
    pub ask_px: [Option<f64>; BOOK_LEVELS],
    /// Ask sizes.
    pub ask_sz: [Option<f64>; BOOK_LEVELS],
    /// Ask order counts.
    pub ask_n: [Option<i64>; BOOK_LEVELS],
}

/// One book line counts as one snapshot. Does not read prices.
///
/// # Errors
///
/// The line is not a JSON object with a two-sided `levels` array.
pub fn count_snapshot(line: &str) -> Result<u64, String> {
    let value: Value = serde_json::from_str(line).map_err(|e| format!("json: {e}"))?;
    let data = data_object(&value)?;
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .ok_or("levels is missing")?;
    if levels.len() != 2 {
        return Err("levels is not [bids, asks]".into());
    }
    Ok(1)
}

/// The flat row of one line.
///
/// # Errors
///
/// The line is not an `l2Book` snapshot, `coin` does not match `file_coin`, `time` is missing,
/// or a present price, size, or count is not a finite decimal or integer. Fewer than
/// [`BOOK_LEVELS`] levels leave the rest null. Levels past 20 are ignored.
pub fn snapshot_of_line(line: &str, file_coin: &str) -> Result<Snapshot, String> {
    let value: Value = serde_json::from_str(line).map_err(|e| format!("json: {e}"))?;
    let recv_time_ms = recv_time_ms(value.get("time"))?;
    let data = data_object(&value)?;
    let coin = required_text(data, "coin")?;
    if coin != file_coin {
        return Err(format!("coin {coin} does not match file {file_coin}"));
    }
    let time_ms = required_ms(data.get("time"), "time")?;
    // `data.time` is milliseconds. A seconds-scale value is about 1e9 and would
    // partition as 1970-01-21, so it is rejected rather than stored.
    if !(1_577_836_800_000..4_102_444_800_000).contains(&time_ms) {
        return Err(format!("time {time_ms} is out of range"));
    }
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .ok_or("levels is missing")?;
    if levels.len() != 2 {
        return Err("levels is not [bids, asks]".into());
    }
    let (bid_px, bid_sz, bid_n) = side(&levels[0], "bids")?;
    let (ask_px, ask_sz, ask_n) = side(&levels[1], "asks")?;
    Ok(Snapshot {
        coin,
        time_ms,
        recv_time_ms,
        bid_px,
        bid_sz,
        bid_n,
        ask_px,
        ask_sz,
        ask_n,
    })
}

/// Newline-delimited JSON of one snapshot, column names matching [`super::column_specs`],
/// plus `_seq`: the hour's archive line order, the COPY's sort tiebreak.
///
/// # Errors
///
/// A finite `f64` cannot be rendered as JSON (it is not finite). Callers reject those earlier.
pub fn snapshot_line(row: &Snapshot, seq: u64) -> Result<Vec<u8>, String> {
    let mut map = Map::new();
    map.insert("coin".into(), Value::String(row.coin.clone()));
    map.insert("time_ms".into(), Value::from(row.time_ms));
    map.insert("recv_time_ms".into(), opt_i64(row.recv_time_ms));
    map.insert("bid_px".into(), f64_array(&row.bid_px)?);
    map.insert("bid_sz".into(), f64_array(&row.bid_sz)?);
    map.insert("bid_n".into(), i64_array(&row.bid_n));
    map.insert("ask_px".into(), f64_array(&row.ask_px)?);
    map.insert("ask_sz".into(), f64_array(&row.ask_sz)?);
    map.insert("ask_n".into(), i64_array(&row.ask_n));
    map.insert("_seq".into(), Value::from(seq));
    let mut line = serde_json::to_vec(&Value::Object(map)).map_err(|e| e.to_string())?;
    line.push(b'\n');
    Ok(line)
}

fn f64_array(values: &[Option<f64>; BOOK_LEVELS]) -> Result<Value, String> {
    let mut out = Vec::with_capacity(BOOK_LEVELS);
    for value in values {
        out.push(opt_f64(*value)?);
    }
    Ok(Value::Array(out))
}

fn i64_array(values: &[Option<i64>; BOOK_LEVELS]) -> Value {
    Value::Array(values.iter().copied().map(opt_i64).collect())
}

fn opt_f64(value: Option<f64>) -> Result<Value, String> {
    match value {
        None => Ok(Value::Null),
        Some(number) => serde_json::Number::from_f64(number)
            .map(Value::Number)
            .ok_or_else(|| "price is not a finite decimal".to_owned()),
    }
}

fn opt_i64(value: Option<i64>) -> Value {
    value.map_or(Value::Null, Value::from)
}

fn data_object(value: &Value) -> Result<&Map<String, Value>, String> {
    if let Some(raw) = value.get("raw") {
        if let Some(channel) = raw.get("channel").and_then(Value::as_str)
            && channel != "l2Book"
        {
            return Err(format!("channel {channel} is not l2Book"));
        }
        if let Some(data) = raw.get("data").and_then(Value::as_object) {
            return Ok(data);
        }
    }
    if let Some(data) = value.get("data").and_then(Value::as_object) {
        return Ok(data);
    }
    value
        .as_object()
        .filter(|object| object.contains_key("levels"))
        .ok_or_else(|| "not an l2Book snapshot".to_owned())
}

type Levels = (
    [Option<f64>; BOOK_LEVELS],
    [Option<f64>; BOOK_LEVELS],
    [Option<i64>; BOOK_LEVELS],
);

fn side(value: &Value, name: &str) -> Result<Levels, String> {
    let levels = value
        .as_array()
        .ok_or_else(|| format!("{name} is not an array"))?;
    let mut px = [None; BOOK_LEVELS];
    let mut sz = [None; BOOK_LEVELS];
    let mut n = [None; BOOK_LEVELS];
    for (index, level) in levels.iter().take(BOOK_LEVELS).enumerate() {
        let object = level
            .as_object()
            .ok_or_else(|| format!("{name}[{index}] is not an object"))?;
        px[index] = optional_decimal(object, "px")?;
        sz[index] = optional_decimal(object, "sz")?;
        n[index] = optional_int(object, "n")?;
    }
    Ok((px, sz, n))
}

fn required_text(map: &Map<String, Value>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(Value::String(text)) if !text.is_empty() => Ok(text.clone()),
        _ => Err(format!("{key} is missing")),
    }
}

fn required_ms(value: Option<&Value>, key: &str) -> Result<i64, String> {
    match value {
        Some(Value::Number(number)) => {
            json_i64(number).ok_or_else(|| format!("{key} is not an integer"))
        }
        Some(Value::String(text)) if !text.is_empty() => text
            .parse::<i64>()
            .map_err(|_| format!("{key} is not an integer")),
        _ => Err(format!("{key} is missing")),
    }
}

fn recv_time_ms(value: Option<&Value>) -> Result<Option<i64>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => {
            if let Ok(number) = text.parse::<i64>() {
                return normalize_epoch(number).map(Some);
            }
            iso_millis(text)
                .map(Some)
                .ok_or_else(|| format!("time {text} is not a timestamp"))
        }
        Some(Value::Number(number)) => {
            let number = json_i64(number).ok_or_else(|| "time is not an integer".to_owned())?;
            normalize_epoch(number).map(Some)
        }
        Some(_) => Err("time is not a timestamp".into()),
    }
}

/// A value below 10^12 and at least 10^9 is seconds. Anything smaller is rejected.
fn normalize_epoch(number: i64) -> Result<i64, String> {
    if number >= 1_000_000_000_000 {
        Ok(number)
    } else if (1_000_000_000..1_000_000_000_000).contains(&number) {
        number
            .checked_mul(1000)
            .ok_or_else(|| "time is out of range".to_owned())
    } else {
        Err("time is out of range".into())
    }
}

fn iso_millis(text: &str) -> Option<i64> {
    let text = text.trim().trim_end_matches('Z');
    let (head, fraction) = text.split_once('.').unwrap_or((text, ""));
    if fraction.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    let naive = chrono::NaiveDateTime::parse_from_str(head, "%Y-%m-%dT%H:%M:%S").ok()?;
    let millis = naive.and_utc().timestamp_millis();
    if fraction.is_empty() {
        return Some(millis);
    }
    let digits: String = fraction.chars().take(3).collect();
    let padded = format!("{digits:0<3}");
    let extra: i64 = padded.parse().ok()?;
    millis.checked_add(extra)
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
    match map.get(key) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn line(levels: usize) -> String {
        let bids: Vec<Value> = (0..levels)
            .map(|i| serde_json::json!({"px": format!("{}", 100 - i), "sz": "1.5", "n": i + 1}))
            .collect();
        let asks: Vec<Value> = (0..levels)
            .map(|i| serde_json::json!({"px": format!("{}", 101 + i), "sz": "", "n": null}))
            .collect();
        serde_json::json!({
            "time": "2025-10-01T00:00:00.5",
            "ver_num": 1,
            "raw": {"channel": "l2Book", "data": {"coin": "BTC", "time": 1759276800500_i64, "levels": [bids, asks]}}
        })
        .to_string()
    }

    #[test]
    fn short_side_is_null_and_the_envelope_time_is_millis() {
        let text = line(1);
        assert_eq!(count_snapshot(&text).unwrap(), 1);
        let row = snapshot_of_line(&text, "BTC").unwrap();
        assert_eq!(row.time_ms, 1_759_276_800_500);
        assert_eq!(row.recv_time_ms, Some(1_759_276_800_000 + 500));
        assert_eq!(row.bid_px[0], Some(100.0));
        assert_eq!(row.bid_sz[0], Some(1.5));
        assert_eq!(row.bid_n[0], Some(1));
        assert!(row.bid_px[1].is_none());
        assert!(row.ask_sz[0].is_none());
        assert!(row.ask_n[0].is_none());
        assert!(snapshot_of_line(&text, "ETH").is_err());
        assert!(count_snapshot("{\"raw\":{\"channel\":\"trades\",\"data\":{}}}").is_err());
    }

    #[test]
    fn seventeen_digit_price_keeps_f64_bits() {
        let text = serde_json::json!({
            "raw": {"channel": "l2Book", "data": {
                "coin": "ETH",
                "time": 1_792_627_200_000_i64,
                "levels": [
                    [{"px": "3411232.9300000002", "sz": "0.1", "n": 2}, {"px": "0.3", "sz": "1", "n": 1}],
                    [{"px": "1", "sz": "1", "n": 1}]
                ]
            }}
        })
        .to_string();
        let row = snapshot_of_line(&text, "ETH").unwrap();
        assert_eq!(
            row.bid_px[0].unwrap().to_bits(),
            "3411232.9300000002".parse::<f64>().unwrap().to_bits()
        );
        assert_eq!(
            row.bid_sz[0].unwrap().to_bits(),
            "0.1".parse::<f64>().unwrap().to_bits()
        );
        assert_eq!(
            row.bid_px[1].unwrap().to_bits(),
            "0.3".parse::<f64>().unwrap().to_bits()
        );
        assert!(row.bid_px[2].is_none());
        let rendered = snapshot_line(&row, 0).unwrap();
        assert!(rendered.ends_with(b"\n"));
    }

    /// Mutation: delete the `data.time` range check in `snapshot_of_line`.
    #[test]
    fn a_seconds_scale_snapshot_time_is_rejected() {
        let text = serde_json::json!({
            "raw": {"channel": "l2Book", "data": {
                "coin": "BTC",
                "time": 1_759_276_800_i64,
                "levels": [[{"px": "1", "sz": "1", "n": 1}], [{"px": "2", "sz": "1", "n": 1}]]
            }}
        })
        .to_string();
        let error = snapshot_of_line(&text, "BTC").unwrap_err();
        assert!(error.contains("out of range"), "{error}");
    }

    #[test]
    fn twenty_first_level_is_ignored() {
        let text = line(21);
        let row = snapshot_of_line(&text, "BTC").unwrap();
        assert_eq!(row.bid_px[19], Some(81.0));
        assert!(row.ask_px[19].is_some());
    }

    fn book_with(px: &str, recv: &str) -> Snapshot {
        let text = serde_json::json!({
            "time": recv,
            "raw": {"channel": "l2Book", "data": {
                "coin": "BTC",
                "time": 1_792_627_200_000_i64,
                "levels": [
                    [{"px": px, "sz": "1", "n": 1}],
                    [{"px": "2", "sz": "1", "n": 1}]
                ]
            }}
        })
        .to_string();
        snapshot_of_line(&text, "BTC").unwrap()
    }

    #[test]
    fn the_duplicate_rule_compares_books_not_envelopes() {
        let first = book_with("1", "2026-10-22T00:00:00.100");
        let resent = book_with("1", "2026-10-22T00:00:00.900");
        let other = book_with("1.5", "2026-10-22T00:00:00.900");
        assert!(same_book(&first, &resent), "a re-send is the same book");
        assert!(!same_book(&first, &other));
        let seen = vec![book_digest(&first)];
        assert_eq!(
            DuplicateRule::classify(&seen, &book_digest(&resent)),
            DuplicateRule::Identical
        );
        assert_eq!(
            DuplicateRule::classify(&seen, &book_digest(&other)),
            DuplicateRule::Distinct
        );
        // A size that differs only past the decimal text, or a different level count, is a
        // different book: the digest covers every level of both sides.
        let mut short = other.clone();
        short.ask_px[0] = None;
        assert!(!same_book(&other, &short));
    }
}
