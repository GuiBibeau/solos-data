//! Which strikes of an event's ladder are stored. Ladders hold up to 300 strikes and most never
//! trade; a ladder is not centred on the index (strikes are added as the index moves), so the
//! centre is taken causally: the settled index value of the same series' latest event that
//! closed at or before this event opened (`centre_source = previous_settle`). Only when there is
//! none within two days does the median listed strike stand in (`median`). The window is
//! `windowPct` of the centre for a one-hour life, scaled by the square root of the stored life
//! in hours, then capped to the `maxStrikes` nearest the centre. `windowPct = 0` keeps all.
//! A predecessor counts only once its settlement was published (`settlement_ts`, else its close
//! plus `SETTLE_GUARD_MS`) at or before the event opened: hourly events open as the previous one
//! closes and settle minutes later, so the centre is then the settlement of the event before.

use crate::augment::config::KalshiSeries;
use std::collections::BTreeMap;

const HOUR_MS: i64 = 3_600_000;
/// How far back a predecessor's settlement may serve as the centre.
const CENTRE_LOOKBACK_MS: i64 = 48 * HOUR_MS;
/// When a market carries no settlement time, its value is taken as known this long after close.
pub const SETTLE_GUARD_MS: i64 = 15 * 60_000;

/// One market as the walk needs it.
#[derive(Clone, Debug, PartialEq)]
pub struct Contract {
    /// Market ticker.
    pub ticker: String,
    /// Event ticker.
    pub event: String,
    /// Payoff level (`strike` column).
    pub strike: Option<f64>,
    /// Open and close, milliseconds.
    pub open_ms: i64,
    /// Close, milliseconds.
    pub close_ms: i64,
    /// The settled index value, once known.
    pub expiration_value: Option<f64>,
    /// When the settlement was published, milliseconds.
    pub settle_ms: Option<i64>,
    /// Lifetime volume (contracts).
    pub volume: f64,
    /// Listed by the archive tier.
    pub historical: bool,
}

/// One event: its markets, opening and close.
#[derive(Clone, Debug)]
pub struct Event {
    /// Event ticker.
    pub ticker: String,
    /// Earliest open of its markets.
    pub open_ms: i64,
    /// Latest close of its markets.
    pub close_ms: i64,
    /// The markets.
    pub markets: Vec<Contract>,
}

impl Event {
    /// The settled index value and when it was published: the first market that carries one.
    #[must_use]
    pub fn settled(&self) -> Option<(i64, f64)> {
        self.markets.iter().find_map(|m| {
            let value = m.expiration_value?;
            Some((m.settle_ms.unwrap_or(m.close_ms + SETTLE_GUARD_MS), value))
        })
    }
}

/// Group contracts into events, ordered by close.
#[must_use]
pub fn events(contracts: Vec<Contract>) -> Vec<Event> {
    let mut by: BTreeMap<String, Vec<Contract>> = BTreeMap::new();
    for c in contracts {
        by.entry(c.event.clone()).or_default().push(c);
    }
    let mut out: Vec<Event> = by
        .into_iter()
        .map(|(ticker, markets)| Event {
            open_ms: markets.iter().map(|m| m.open_ms).min().unwrap_or(0),
            close_ms: markets.iter().map(|m| m.close_ms).max().unwrap_or(0),
            ticker,
            markets,
        })
        .collect();
    out.sort_by(|a, b| (a.close_ms, &a.ticker).cmp(&(b.close_ms, &b.ticker)));
    out
}

/// The causal centre of `event` among `known` events of the same series, and its source.
#[must_use]
pub fn centre(event: &Event, known: &[Event]) -> (Option<f64>, &'static str) {
    let previous = known
        .iter()
        .filter(|e| e.ticker != event.ticker)
        .filter_map(|e| e.settled())
        .filter(|(at, _)| *at <= event.open_ms && *at >= event.open_ms - CENTRE_LOOKBACK_MS)
        .filter(|(_, v)| v.is_finite() && *v > 0.0)
        .max_by_key(|(at, _)| *at);
    if let Some((_, value)) = previous {
        return (Some(value), "previous_settle");
    }
    let mut strikes: Vec<f64> = event.markets.iter().filter_map(|m| m.strike).collect();
    strikes.sort_by(f64::total_cmp);
    (strikes.get(strikes.len() / 2).copied(), "median")
}

/// The stored life of an event's markets, hours, at least one.
#[must_use]
pub fn life_hours(event: &Event, spec: &KalshiSeries) -> f64 {
    let life = (event.close_ms - event.open_ms).max(HOUR_MS);
    let capped = life.min(spec.max_life_hours.max(1) * HOUR_MS);
    capped as f64 / HOUR_MS as f64
}

/// The markets of `event` kept around `centre`, ordered by strike.
#[must_use]
pub fn select<'e>(event: &'e Event, centre: Option<f64>, spec: &KalshiSeries) -> Vec<&'e Contract> {
    let mut kept: Vec<&Contract> = event.markets.iter().collect();
    if let Some(c) = centre.filter(|c| *c > 0.0) {
        if spec.window_pct > 0.0 {
            let width = spec.window_pct / 100.0 * life_hours(event, spec).sqrt();
            kept.retain(|m| m.strike.is_some_and(|k| ((k - c) / c).abs() <= width));
        }
        if spec.max_strikes > 0 && kept.len() > spec.max_strikes {
            kept.sort_by(|a, b| distance(a, c).total_cmp(&distance(b, c)));
            kept.truncate(spec.max_strikes);
        }
    }
    kept.sort_by(|a, b| {
        a.strike
            .unwrap_or(f64::MAX)
            .total_cmp(&b.strike.unwrap_or(f64::MAX))
            .then_with(|| a.ticker.cmp(&b.ticker))
    });
    kept
}

fn distance(m: &Contract, centre: f64) -> f64 {
    m.strike.map_or(f64::MAX, |k| (k - centre).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(window_pct: f64, max_strikes: usize) -> KalshiSeries {
        KalshiSeries {
            ticker: "KXBTCD".into(),
            asset: Some("BTC".into()),
            kind: "crypto".into(),
            interval: 1,
            window_pct,
            max_strikes,
            max_life_hours: 26,
            trades: false,
        }
    }

    fn ladder(event: &str, open_h: i64, close_h: i64, settle: Option<f64>) -> Vec<Contract> {
        (90..=110)
            .map(|k| Contract {
                ticker: format!("{event}-T{k}"),
                event: event.into(),
                strike: Some(f64::from(k)),
                open_ms: open_h * HOUR_MS,
                close_ms: close_h * HOUR_MS,
                expiration_value: settle,
                settle_ms: settle.map(|_| close_h * HOUR_MS + 60_000),
                volume: 0.0,
                historical: false,
            })
            .collect()
    }

    #[test]
    fn the_centre_is_the_previous_settlement_never_a_later_one() {
        let mut all = ladder("E1", 0, 9, Some(104.2));
        all.extend(ladder("E2", 9, 10, Some(96.0)));
        all.extend(ladder("E3", 10, 11, None));
        all.extend(ladder("E4", 11, 12, None));
        let evs = events(all);
        assert_eq!(
            evs.iter().map(|e| e.ticker.as_str()).collect::<Vec<_>>(),
            ["E1", "E2", "E3", "E4"]
        );
        // E2 closes as E3 opens but settles a minute later: E3 sees E1's settlement.
        assert_eq!(centre(&evs[2], &evs), (Some(104.2), "previous_settle"));
        assert_eq!(centre(&evs[3], &evs), (Some(96.0), "previous_settle"));
        let mut unstamped = evs[1].clone();
        for m in &mut unstamped.markets {
            m.settle_ms = None;
        }
        assert_eq!(
            unstamped.settled().unwrap().0,
            10 * HOUR_MS + SETTLE_GUARD_MS,
            "no settlement time: the close plus the guard"
        );
        // E1 has no predecessor: E2 and E3 close after it opened.
        assert_eq!(centre(&evs[0], &evs), (Some(100.0), "median"));
        let far = events(
            ladder("OLD", 0, 1, Some(50.0))
                .into_iter()
                .chain(ladder("NEW", 60, 61, None))
                .collect(),
        );
        assert_eq!(
            centre(&far[1], &far).1,
            "median",
            "a settlement older than two days is ignored"
        );
    }

    #[test]
    fn the_window_scales_with_life_and_caps_the_count() {
        let evs = events(ladder("E", 0, 1, None));
        let one_hour: Vec<_> = select(&evs[0], Some(100.0), &spec(3.0, 0))
            .iter()
            .map(|m| m.strike.unwrap())
            .collect();
        assert_eq!(one_hour, [97.0, 98.0, 99.0, 100.0, 101.0, 102.0, 103.0]);
        let capped: Vec<_> = select(&evs[0], Some(100.2), &spec(3.0, 3))
            .iter()
            .map(|m| m.strike.unwrap())
            .collect();
        assert_eq!(capped, [99.0, 100.0, 101.0]);
        let long = events(ladder("L", 0, 4, None));
        assert_eq!(life_hours(&long[0], &spec(3.0, 0)), 4.0);
        assert_eq!(
            select(&long[0], Some(100.0), &spec(3.0, 0)).len(),
            13,
            "6% at four hours"
        );
        assert_eq!(
            select(&long[0], Some(100.0), &spec(0.0, 0)).len(),
            21,
            "0 keeps every strike"
        );
        assert_eq!(
            select(&long[0], None, &spec(3.0, 2)).len(),
            21,
            "no centre keeps every strike"
        );
    }
}
