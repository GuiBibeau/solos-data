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

/// The UTC instant at the first second of `hour`.
///
/// # Errors
///
/// The hour's first second overflows `i64` or lies outside the calendar.
fn hour_datetime(hour: i64) -> Result<chrono::DateTime<chrono::Utc>, StoreError> {
    hour.checked_mul(HOUR_SECS)
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .ok_or_else(|| StoreError::Check(format!("unix hour {hour} is outside the calendar")))
}

/// `YYYY-MM-DD` of a unix hour.
///
/// # Errors
///
/// The hour is outside the calendar (a corrupt or out-of-range value).
pub fn date_of_hour(hour: i64) -> Result<String, StoreError> {
    Ok(hour_datetime(hour)?.format("%Y-%m-%d").to_string())
}

/// `YYYYMMDD-H` file key of a unix hour (`20261015-13`).
///
/// # Errors
///
/// The hour is outside the calendar (a corrupt or out-of-range value).
pub fn hour_key(hour: i64) -> Result<String, StoreError> {
    Ok(hour_datetime(hour)?.format("%Y%m%d-%H").to_string())
}

/// One hl-fills watermark hour: a UTC day and an hour of that day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceHour {
    /// The UTC day (the `YYYYMMDD` of the key).
    pub day: NaiveDate,
    /// The hour of the day, `0..=23`.
    pub hour: u32,
}

impl SourceHour {
    /// The unix hour this key names.
    #[must_use]
    pub fn unix_hour(self) -> i64 {
        hour_of_secs(day_start_secs(self.day)) + i64::from(self.hour)
    }
}

/// Parse an hl-fills watermark hour key, `YYYYMMDD/H`: the one shared form every
/// combined unit reads.
///
/// The key is canonical or refused, never skipped: exactly eight ASCII digits that
/// name a real UTC date, then `/`, then the hour unpadded in `0..=23`. So
/// `20261022/5` parses, while `20261022/05`, `20261022/24` and `2026102/5` are
/// errors.
///
/// # Errors
///
/// The key is not canonical. The message names the key.
pub fn parse_source_hour(key: &str) -> Result<SourceHour, StoreError> {
    let refuse = |why: &str| {
        StoreError::Check(format!(
            "watermark hour '{key}': {why}; expected YYYYMMDD/H with H unpadded in 0..=23"
        ))
    };
    let (date, hour) = key.split_once('/').ok_or_else(|| refuse("no '/'"))?;
    if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) {
        return Err(refuse("the day is not eight digits"));
    }
    let day = NaiveDate::parse_from_str(
        &format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..]),
        "%Y-%m-%d",
    )
    .map_err(|_| refuse("the day is not a real UTC date"))?;
    // Digits only (rejects a sign, which `parse` would accept), and no leading zero
    // unless the hour is exactly 0.
    let unpadded = !hour.is_empty()
        && hour.bytes().all(|b| b.is_ascii_digit())
        && (hour == "0" || !hour.starts_with('0'));
    let hour = hour
        .parse::<u32>()
        .ok()
        .filter(|_| unpadded)
        .ok_or_else(|| refuse("the hour is not an unpadded number"))?;
    if hour > 23 {
        return Err(refuse("the hour is out of range"));
    }
    Ok(SourceHour { day, hour })
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
        assert_eq!(date_of_hour(hour).unwrap(), "2026-10-23");
        assert_eq!(hour_key(hour).unwrap(), "20261023-13");
        assert_eq!(hour_start(hour), 1_792_713_600 + 13 * 3_600);
    }

    #[test]
    fn a_corrupt_hour_is_an_error_not_a_panic() {
        // `i64::MAX` overflows the seconds multiply; the second is past chrono's range.
        for hour in [i64::MAX, i64::MIN, 1_000_000_000_000] {
            assert!(date_of_hour(hour).is_err(), "date_of_hour({hour})");
            assert!(hour_key(hour).is_err(), "hour_key({hour})");
        }
    }

    #[test]
    fn source_hour_keys_are_canonical_or_refused() {
        // Valid keys parse to their unix hour; the unpadded `H` is the only form.
        let day = parse_date("2026-10-15").unwrap();
        let midnight = hour_of_secs(day_start_secs(day));
        for (key, hour) in [("20261015/0", 0), ("20261015/5", 5), ("20261015/23", 23)] {
            let parsed = parse_source_hour(key).unwrap();
            assert_eq!(parsed.day, day, "{key}");
            assert_eq!(parsed.unix_hour(), midnight + hour, "{key}");
        }
        // Each refusal is an error whose text names the key.
        for key in [
            "20261022/24",
            "20261022/05",
            "2026102/5",
            "abc",
            "20261022/",
            "20261022/+5",
            "20261022/-1",
            "20261032/5",
            "2026-10-2/5",
            "/5",
        ] {
            let error = parse_source_hour(key).expect_err(key).to_string();
            assert!(error.contains(key), "error for {key} must name it: {error}");
        }
    }

    #[test]
    fn dates_parse_and_days_have_24_hours() {
        let day = parse_date("2026-10-15").unwrap();
        assert_eq!(day_start_secs(day), 1_792_713_600 - 8 * 86_400);
        let hours = day_hours(day);
        assert_eq!(hours.len(), 24);
        assert_eq!(hours[0], (1_792_713_600 - 8 * 86_400) / 3_600);
        assert_eq!(date_of_hour(hours[23]).unwrap(), "2026-10-15");
        assert!(parse_date("2026-10-15T00").is_err());
        assert!(parse_rfc3339_secs("2026-06-01T00:00:00Z").is_ok());
    }
}
