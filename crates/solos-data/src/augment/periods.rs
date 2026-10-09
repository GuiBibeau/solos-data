//! Calendar periods for dated files and paged histories: one Parquet file covers one UTC day,
//! one UTC month or one UTC year, named `YYYY-MM-DD`, `YYYY-MM` or `YYYY`.

use chrono::{Datelike, NaiveDate};

/// How much time one file covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Granularity {
    /// One UTC day per file.
    Day,
    /// One UTC month per file.
    Month,
    /// One UTC year per file.
    Year,
}

/// One day or one month, identified by its first day.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Period {
    /// Day or month.
    pub granularity: Granularity,
    /// First day of the period.
    pub start: NaiveDate,
}

impl Period {
    /// The period of `granularity` that contains `date`.
    #[must_use]
    pub fn containing(date: NaiveDate, granularity: Granularity) -> Period {
        let start = match granularity {
            Granularity::Day => date,
            Granularity::Month => date.with_day(1).unwrap_or(date),
            Granularity::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap_or(date),
        };
        Period { granularity, start }
    }

    /// Parse a label (`YYYY-MM-DD`, `YYYY-MM` or `YYYY`); the shape decides the granularity.
    #[must_use]
    pub fn parse(label: &str) -> Option<Period> {
        if label.len() == 4 {
            let date = NaiveDate::parse_from_str(&format!("{label}-01-01"), "%Y-%m-%d").ok()?;
            return Some(Period {
                granularity: Granularity::Year,
                start: date,
            });
        }
        if label.len() == 7 {
            let date = NaiveDate::parse_from_str(&format!("{label}-01"), "%Y-%m-%d").ok()?;
            return Some(Period {
                granularity: Granularity::Month,
                start: date,
            });
        }
        let date = parse_date(label)?;
        Some(Period {
            granularity: Granularity::Day,
            start: date,
        })
    }

    /// The following period.
    #[must_use]
    pub fn next(self) -> Period {
        let start = match self.granularity {
            Granularity::Day => self.start.succ_opt().unwrap_or(self.start),
            Granularity::Month => {
                let (year, month) = if self.start.month() == 12 {
                    (self.start.year() + 1, 1)
                } else {
                    (self.start.year(), self.start.month() + 1)
                };
                NaiveDate::from_ymd_opt(year, month, 1).unwrap_or(self.start)
            }
            Granularity::Year => {
                NaiveDate::from_ymd_opt(self.start.year() + 1, 1, 1).unwrap_or(self.start)
            }
        };
        Period {
            granularity: self.granularity,
            start,
        }
    }

    /// First instant, milliseconds since the epoch.
    #[must_use]
    pub fn start_ms(&self) -> i64 {
        ms_of(self.start)
    }

    /// First instant after the period, milliseconds since the epoch.
    #[must_use]
    pub fn end_ms(&self) -> i64 {
        ms_of(self.next().start)
    }

    /// `YYYY-MM-DD`, `YYYY-MM` or `YYYY`.
    #[must_use]
    pub fn label(&self) -> String {
        match self.granularity {
            Granularity::Day => self.start.format("%Y-%m-%d").to_string(),
            Granularity::Month => self.start.format("%Y-%m").to_string(),
            Granularity::Year => self.start.format("%Y").to_string(),
        }
    }

    /// Whether the period has ended and the source has had `lag_ms` to publish its last rows.
    #[must_use]
    pub fn is_complete(&self, now_ms: i64, lag_ms: i64) -> bool {
        self.end_ms().saturating_add(lag_ms) <= now_ms
    }
}

/// Every period from `first` through the one containing `now_ms`, in order.
pub fn periods_through(first: Period, now_ms: i64) -> impl Iterator<Item = Period> {
    let mut current = Some(first);
    std::iter::from_fn(move || {
        let period = current?;
        if period.start_ms() > now_ms {
            current = None;
            return None;
        }
        current = Some(period.next());
        Some(period)
    })
}

/// `YYYY-MM-DD`.
#[must_use]
pub fn parse_date(text: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()
}

/// The UTC date of an instant in milliseconds.
#[must_use]
pub fn date_of_ms(ms: i64) -> NaiveDate {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.date_naive())
        .unwrap_or(NaiveDate::MIN)
}

/// Midnight UTC of a date, milliseconds since the epoch.
#[must_use]
pub fn ms_of(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .map(|t| t.and_utc().timestamp_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_bounds_and_successors() {
        let day = Period::parse("2026-01-31").unwrap();
        assert_eq!(day.granularity, Granularity::Day);
        assert_eq!(day.label(), "2026-01-31");
        assert_eq!(day.next().label(), "2026-02-01");
        assert_eq!(day.start_ms(), 1_769_817_600_000);
        assert_eq!(day.end_ms(), day.start_ms() + 86_400_000);
        let month = Period::parse("2026-12").unwrap();
        assert_eq!(month.granularity, Granularity::Month);
        assert_eq!(month.next().label(), "2027-01");
        assert_eq!(
            Period::containing(parse_date("2026-03-17").unwrap(), Granularity::Month).label(),
            "2026-03"
        );
        let year = Period::parse("2026").unwrap();
        assert_eq!(year.granularity, Granularity::Year);
        assert_eq!(year.label(), "2026");
        assert_eq!(year.next().label(), "2027");
        assert_eq!(year.end_ms(), Period::parse("2027-01").unwrap().start_ms());
        assert_eq!(
            Period::containing(parse_date("2026-03-17").unwrap(), Granularity::Year).label(),
            "2026"
        );
        assert!(Period::parse("202").is_none());
        assert!(Period::parse("2026-13-01").is_none());
    }

    #[test]
    fn completeness_needs_the_lag() {
        let day = Period::parse("2026-01-01").unwrap();
        assert!(!day.is_complete(day.end_ms(), 3_600_000));
        assert!(day.is_complete(day.end_ms() + 3_600_000, 3_600_000));
    }

    #[test]
    fn iteration_stops_at_now() {
        let first = Period::parse("2025-11").unwrap();
        let now = Period::parse("2026-02").unwrap().start_ms() + 5;
        let labels: Vec<String> = periods_through(first, now).map(|p| p.label()).collect();
        assert_eq!(labels, ["2025-11", "2025-12", "2026-01", "2026-02"]);
        assert_eq!(date_of_ms(now).to_string(), "2026-02-01");
    }
}
