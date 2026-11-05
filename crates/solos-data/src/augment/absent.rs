//! Dump periods the venue did not serve (ADR-0024). A period that still answers 404 three days
//! after it ended is remembered as `absent` in its series' progress record (ADR-0009). This
//! module is that record: which periods are absent, where each sits between the periods the
//! series holds (`leading`: before its first, a market listed later; `interior`: between two
//! published ones, a hole in a continuous series; `trailing`: after its last), and when each was
//! last asked for again. An interior period is asked for again on the configured schedule, so a
//! venue that publishes late is picked up; a mark, a fill and the end of the schedule are logged
//! and counted, so a hole is never silent.

use super::config::AbsentReprobe;
use crate::jsonout::{Obj, log, now};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;

/// Where an absent period sits among the periods the series holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    /// No published period before it: the market was listed later (or the walk has not reached
    /// the older periods yet).
    Leading,
    /// Published periods on both sides: a hole in a continuous series.
    Interior,
    /// No published period after it.
    Trailing,
}

impl Position {
    /// The name the logs use.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Position::Leading => "leading",
            Position::Interior => "interior",
            Position::Trailing => "trailing",
        }
    }
}

/// Where `label` sits relative to the labels in `held` (the series' registered periods). Labels of
/// one series share a granularity, so they order as strings.
#[must_use]
pub fn position(label: &str, held: &BTreeSet<String>) -> Position {
    let older = held
        .range::<str, _>((Bound::Unbounded, Bound::Excluded(label)))
        .next()
        .is_some();
    let newer = held
        .range::<str, _>((Bound::Excluded(label), Bound::Unbounded))
        .next()
        .is_some();
    match (older, newer) {
        (true, true) => Position::Interior,
        (true, false) => Position::Trailing,
        (false, _) => Position::Leading,
    }
}

/// What a period's re-probe history says about asking again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Due {
    /// Ask in this run.
    Now,
    /// Asked recently enough; ask in a later run.
    Later,
    /// The schedule is spent; the period stays absent.
    GivenUp,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Probe {
    /// Answers of 404 to a re-probe.
    misses: u32,
    /// The run clock of the last attempt (a re-probe, or the mark itself), milliseconds.
    last_ms: Option<i64>,
}

/// The absent periods of one series and their re-probe history.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AbsentSet {
    labels: BTreeSet<String>,
    probes: BTreeMap<String, Probe>,
}

impl AbsentSet {
    /// Read a series' progress record. A record written before ADR-0024 has the labels only; each
    /// of those has never been asked for again.
    #[must_use]
    pub fn from_progress(progress: Option<&Value>) -> AbsentSet {
        let mut set = AbsentSet::default();
        let Some(progress) = progress else {
            return set;
        };
        for label in progress
            .get("absent")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            set.labels.insert(label.to_owned());
        }
        if let Some(probes) = progress.get("reprobe").and_then(Value::as_object) {
            for (label, probe) in probes {
                if !set.labels.contains(label) {
                    continue;
                }
                set.probes.insert(
                    label.clone(),
                    Probe {
                        misses: probe
                            .get("n")
                            .and_then(Value::as_u64)
                            .and_then(|n| u32::try_from(n).ok())
                            .unwrap_or(0),
                        last_ms: probe.get("lastMs").and_then(Value::as_i64),
                    },
                );
            }
        }
        set
    }

    /// The progress record: the labels (what ADR-0009 stored) and the re-probe history.
    #[must_use]
    pub fn to_progress(&self) -> Value {
        let mut probes = Map::new();
        for (label, probe) in &self.probes {
            probes.insert(
                label.clone(),
                json!({ "n": probe.misses, "lastMs": probe.last_ms }),
            );
        }
        json!({
            "absent": self.labels.iter().collect::<Vec<_>>(),
            "reprobe": probes,
            "updatedAt": now(),
        })
    }

    /// Whether `label` is absent.
    #[must_use]
    pub fn contains(&self, label: &str) -> bool {
        self.labels.contains(label)
    }

    /// Mark `label` absent: it was just asked for.
    pub fn mark(&mut self, label: &str, now_ms: i64) {
        self.labels.insert(label.to_owned());
        self.probes.insert(
            label.to_owned(),
            Probe {
                misses: 0,
                last_ms: Some(now_ms),
            },
        );
    }

    /// `label` was published after all.
    pub fn clear(&mut self, label: &str) {
        self.labels.remove(label);
        self.probes.remove(label);
    }

    /// A re-probe of `label` answered 404. True when that spent the schedule.
    pub fn missed(&mut self, label: &str, now_ms: i64, schedule: &AbsentReprobe) -> bool {
        let probe = self.probes.entry(label.to_owned()).or_default();
        probe.misses = probe.misses.saturating_add(1);
        probe.last_ms = Some(now_ms);
        probe.misses == schedule.total()
    }

    /// A re-probe of `label` failed some other way: it waits for its interval again, and no miss
    /// is counted (a venue outage must not spend the schedule).
    pub fn attempted(&mut self, label: &str, now_ms: i64) {
        self.probes.entry(label.to_owned()).or_default().last_ms = Some(now_ms);
    }

    /// How many 404 re-probes `label` has had.
    #[must_use]
    pub fn misses(&self, label: &str) -> u32 {
        self.probes.get(label).map_or(0, |p| p.misses)
    }

    /// Whether to ask for `label` again at `now_ms`.
    #[must_use]
    pub fn due(&self, label: &str, now_ms: i64, schedule: &AbsentReprobe) -> Due {
        let probe = self.probes.get(label).copied().unwrap_or_default();
        if probe.misses >= schedule.total() {
            return Due::GivenUp;
        }
        let every = if probe.misses < schedule.daily_probes {
            DAY_MS
        } else {
            WEEK_MS
        };
        match probe.last_ms {
            Some(last) if now_ms.saturating_sub(last) < every => Due::Later,
            _ => Due::Now,
        }
    }

    /// The interior periods to ask for in this run, newest first. `held` is the series'
    /// registered periods.
    #[must_use]
    pub fn due_interior(
        &self,
        held: &BTreeSet<String>,
        now_ms: i64,
        schedule: &AbsentReprobe,
    ) -> Vec<String> {
        self.labels
            .iter()
            .rev()
            .filter(|label| {
                position(label, held) == Position::Interior
                    && self.due(label, now_ms, schedule) == Due::Now
            })
            .cloned()
            .collect()
    }

    /// The interior holes still on the schedule (`open`) and those whose schedule is spent.
    #[must_use]
    pub fn interior(&self, held: &BTreeSet<String>, schedule: &AbsentReprobe) -> (u64, u64) {
        let (mut open, mut given_up) = (0, 0);
        for label in &self.labels {
            if position(label, held) != Position::Interior {
                continue;
            }
            if self.misses(label) >= schedule.total() {
                given_up += 1;
            } else {
                open += 1;
            }
        }
        (open, given_up)
    }
}

/// What a source reports about absent periods in its run summary and `status.json`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Periods marked absent in this run.
    pub marked: u64,
    /// Interior holes the series carry at the end of the run that are still on the schedule.
    pub interior: u64,
    /// Interior holes whose schedule is spent: permanent gaps of the venue.
    pub given_up: u64,
    /// Absent periods a re-probe found published and stored in this run.
    pub filled: u64,
}

impl Counts {
    /// Sum two counts.
    pub fn add(&mut self, other: &Counts) {
        self.marked += other.marked;
        self.interior += other.interior;
        self.given_up += other.given_up;
        self.filled += other.filled;
    }

    /// The `status.json` fields.
    #[must_use]
    pub fn to_obj(self, obj: Obj) -> Obj {
        obj.with("absentMarked", self.marked)
            .with("absentInterior", self.interior)
            .with("absentGivenUp", self.given_up)
            .with("absentFilled", self.filled)
    }
}

fn series_fields(source: &str, dataset: &str, symbol: &str, period: &str) -> Obj {
    Obj::new()
        .with("source", source)
        .with("dataset", dataset)
        .with("symbol", symbol)
        .with("period", period)
}

/// The fields of `augment_absent_marked`. They name the period and say where it sits; they carry
/// no URL, header or provider text.
#[must_use]
pub fn marked_fields(
    source: &str,
    dataset: &str,
    symbol: &str,
    period: &str,
    position: Position,
) -> Obj {
    series_fields(source, dataset, symbol, period)
        .with("reason", "not_found")
        .with("position", position.as_str())
}

/// `augment_absent_marked`: a period was remembered as absent. `position` is judged from the
/// periods the series holds at that moment: during a newest-first backfill the older periods are
/// not fetched yet, so a hole shows as `leading` here and as `absentInterior` in the status once
/// they are.
pub fn log_marked(source: &str, dataset: &str, symbol: &str, period: &str, position: Position) {
    log(
        "augment_absent_marked",
        marked_fields(source, dataset, symbol, period, position),
    );
}

/// `augment_absent_filled`: a re-probe found the period published and stored it.
pub fn log_filled(source: &str, dataset: &str, symbol: &str, period: &str, misses: u32) {
    log(
        "augment_absent_filled",
        series_fields(source, dataset, symbol, period).with("misses", misses),
    );
}

/// `augment_absent_given_up`: the schedule is spent and the period stays absent.
pub fn log_given_up(source: &str, dataset: &str, symbol: &str, period: &str, misses: u32) {
    log(
        "augment_absent_given_up",
        series_fields(source, dataset, symbol, period).with("misses", misses),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(labels: &[&str]) -> BTreeSet<String> {
        labels.iter().map(|l| (*l).to_owned()).collect()
    }

    #[test]
    fn positions() {
        let days = held(&["2026-10-27", "2026-10-30"]);
        assert_eq!(position("2026-10-28", &days), Position::Interior);
        assert_eq!(position("2026-10-01", &days), Position::Leading);
        assert_eq!(position("2026-10-31", &days), Position::Trailing);
        // Nothing held: the walk has not found the series' first period yet.
        assert_eq!(position("2026-10-28", &BTreeSet::new()), Position::Leading);
        let months = held(&["2026-08", "2026-10"]);
        assert_eq!(position("2026-09", &months), Position::Interior);
    }

    #[test]
    fn schedule_is_daily_then_weekly_then_spent() {
        let schedule = AbsentReprobe {
            daily_probes: 2,
            weekly_probes: 1,
            ..AbsentReprobe::default()
        };
        let mut set = AbsentSet::default();
        set.mark("2026-10-28", 0);
        assert_eq!(set.due("2026-10-28", DAY_MS - 1, &schedule), Due::Later);
        assert_eq!(set.due("2026-10-28", DAY_MS, &schedule), Due::Now);
        assert!(!set.missed("2026-10-28", DAY_MS, &schedule));
        assert_eq!(set.due("2026-10-28", 2 * DAY_MS - 1, &schedule), Due::Later);
        assert!(!set.missed("2026-10-28", 2 * DAY_MS, &schedule));
        // The third probe is the first weekly one.
        assert_eq!(set.due("2026-10-28", 8 * DAY_MS, &schedule), Due::Later);
        assert_eq!(set.due("2026-10-28", 9 * DAY_MS, &schedule), Due::Now);
        assert!(set.missed("2026-10-28", 9 * DAY_MS, &schedule));
        assert_eq!(set.due("2026-10-28", 999 * DAY_MS, &schedule), Due::GivenUp);
        // A failure other than 404 neither counts nor hammers.
        let mut other = AbsentSet::default();
        other.mark("2026-10-28", 0);
        other.attempted("2026-10-28", 5 * DAY_MS);
        assert_eq!(other.misses("2026-10-28"), 0);
        assert_eq!(
            other.due("2026-10-28", 5 * DAY_MS + 1, &schedule),
            Due::Later
        );
        // Off.
        let off = AbsentReprobe {
            daily_probes: 0,
            weekly_probes: 0,
            ..AbsentReprobe::default()
        };
        assert_eq!(set.due("2026-10-28", 0, &off), Due::GivenUp);
    }

    #[test]
    fn progress_round_trips_and_reads_the_old_shape() {
        let schedule = AbsentReprobe::default();
        let mut set = AbsentSet::default();
        set.mark("2026-10-28", 7);
        set.missed("2026-10-28", 9, &schedule);
        set.mark("2026-10-29", 11);
        let back = AbsentSet::from_progress(Some(&set.to_progress()));
        assert_eq!(back, set);
        // ADR-0009 wrote the labels only.
        let old = json!({ "absent": ["2026-10-28", "2026-10-29"], "updatedAt": "x" });
        let old = AbsentSet::from_progress(Some(&old));
        assert!(old.contains("2026-10-28") && old.contains("2026-10-29"));
        assert_eq!(old.due("2026-10-28", 0, &schedule), Due::Now);
        assert_eq!(AbsentSet::from_progress(None), AbsentSet::default());
    }

    #[test]
    fn only_interior_periods_are_due_and_counted() {
        let schedule = AbsentReprobe {
            daily_probes: 1,
            weekly_probes: 0,
            ..AbsentReprobe::default()
        };
        let held = held(&["2026-10-10", "2026-10-20"]);
        let mut set = AbsentSet::default();
        for label in ["2026-10-01", "2026-10-15", "2026-10-16", "2026-10-25"] {
            set.mark(label, 0);
        }
        // 10-16 has had its one probe.
        set.missed("2026-10-16", 0, &schedule);
        assert_eq!(set.due_interior(&held, DAY_MS, &schedule), ["2026-10-15"]);
        assert_eq!(set.interior(&held, &schedule), (1, 1));
    }

    #[test]
    fn the_marked_event_names_the_period_and_nothing_else() {
        let fields = marked_fields(
            "binance",
            "aggTrades",
            "BTCUSDT",
            "2026-10-28",
            Position::Interior,
        );
        assert_eq!(
            fields.to_json(),
            r#"{"source":"binance","dataset":"aggTrades","symbol":"BTCUSDT","period":"2026-10-28","reason":"not_found","position":"interior"}"#
        );
    }
}
