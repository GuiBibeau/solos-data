//! The combined store's UTC hour grid (DESIGN.md 3): unix hour
//! `t = floor(ts/3600)*3600`, identical construction to hl-train's full grid.
//! Dates and file keys are UTC everywhere.

use crate::store::StoreError;
use chrono::{NaiveDate, NaiveDateTime};

/// Seconds in one hour.
pub const HOUR_SECS: i64 = 3_600;

/// Milliseconds in one hour.
pub const HOUR_MS: i64 = HOUR_SECS * 1_000;

/// `a / b` rounded toward negative infinity, so instants before the epoch land in
/// the hour that contains them.
#[must_use]
pub fn floor_div(value: i64, divisor: i64) -> i64 {
    value.div_euclid(divisor)
}

/// The unix hour (its first second) that contains `secs`.
#[must_use]
pub fn hour_of_secs(secs: i64) -> i64 {
    floor_div(secs, HOUR_SECS)
}

/// The unix hour that contains `millis`.
#[must_use]
pub fn hour_of_ms(millis: i64) -> i64 {
    floor_div(millis, HOUR_MS)
}

/// The first second of the hour `hour` (an hour number is its own start).
#[must_use]
pub const fn hour_start(hour: i64) -> i64 {
    hour * HOUR_SECS
}

/// Parse `YYYY-MM-DD` as a UTC calendar date.
///
/// # Errors
///
/// The text is not `YYYY-MM-DD`.
pub fn parse_date(text: &str) -> Result<NaiveDate, StoreError> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
        .map_err(|error| StoreError::Check(format!("bad UTC date '{text}': {error}")))
}

/// Parse an RFC 3339 timestamp (`...Z`) as UTC seconds.
///
/// # Errors
///
/// The text is not an RFC 3339 timestamp.
pub fn parse_rfc3339_secs(text: &str) -> Result<i64, StoreError> {
    let dt = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%SZ")
        .map_err(|error| StoreError::Check(format!("bad UTC timestamp '{text}': {error}")))?;
    Ok(dt.and_utc().timestamp())
}

/// First second of a UTC day.
#[must_use]
pub fn day_start_secs(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight exists on every date")
        .and_utc()
        .timestamp()
}

/// The 24 unix hours of one UTC day, in order.
#[must_use]
pub fn day_hours(date: NaiveDate) -> Vec<i64> {
    let start = hour_of_secs(day_start_secs(date));
    (0..24).map(|h| start + h).collect()
}

/// `YYYY-MM-DD` of a unix hour.
#[must_use]
pub fn date_of_hour(hour: i64) -> String {
    let secs =
        chrono::DateTime::from_timestamp(hour_start(hour), 0).expect("hour start is in range");
    secs.format("%Y-%m-%d").to_string()
}

/// `YYYYMMDD-H` file key of a unix hour (`20261015-13`).
#[must_use]
pub fn hour_key(hour: i64) -> String {
    let secs =
        chrono::DateTime::from_timestamp(hour_start(hour), 0).expect("hour start is in range");
    secs.format("%Y%m%d-%H").to_string()
}

/// Current UTC time, RFC 3339 with a trailing `Z`.
#[must_use]
pub fn now_rfc3339() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hours_floor_and_keys_are_utc() {
        assert_eq!(hour_of_secs(0), 0);
        assert_eq!(hour_of_secs(-1), -1);
        assert_eq!(hour_of_secs(3_599), 0);
        assert_eq!(hour_of_ms(3_599_999), 0);
        assert_eq!(hour_of_ms(3_600_000), 1);
        // 2026-10-23T13:00:00Z (the forward start plus 13 h).
        let hour = (1_792_713_600 + 13 * 3_600) / 3_600;
        assert_eq!(date_of_hour(hour), "2026-10-23");
        assert_eq!(hour_key(hour), "20261023-13");
        assert_eq!(hour_start(hour), 1_792_713_600 + 13 * 3_600);
    }

    #[test]
    fn dates_parse_and_days_have_24_hours() {
        let day = parse_date("2026-10-15").unwrap();
        assert_eq!(day_start_secs(day), 1_792_713_600 - 8 * 86_400);
        let hours = day_hours(day);
        assert_eq!(hours.len(), 24);
        assert_eq!(hours[0], (1_792_713_600 - 8 * 86_400) / 3_600);
        assert_eq!(date_of_hour(hours[23]), "2026-10-15");
        assert!(parse_date("2026-10-15T00").is_err());
        assert!(parse_rfc3339_secs("2026-06-01T00:00:00Z").is_ok());
    }
}
