//! The Elfa credit meter: every response from the Elfa host is attributed to its endpoint (the
//! path, query dropped, UUID segments as `{id}`) with the credits its `x-elfa-credits` header
//! declared, and a hard monthly cap over the whole client stops every Elfa request once the
//! month's metered credits reach it (`elfa_credit_cap_reached`, logged once). The month's
//! totals are restored from and saved to the capture ledger (`elfa/credits`), so a restart
//! does not reset the cap. The meter sees only what the client's own calls declare: charges
//! the server makes on its own (an Auto alert's evaluations) appear in `credits.used` and
//! nowhere else, which is why the cycles also log the unattributed difference.

use super::periods::{Granularity, Period, date_of_ms};
use crate::jsonout::{Obj, log};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Progress key of the month's metered credits.
pub const PROGRESS_CREDITS: &str = "elfa/credits";

/// One endpoint's counters for the month.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EndpointCost {
    /// Responses received (any status).
    pub calls: u64,
    /// Responses whose header declared more than zero credits.
    pub billed_calls: u64,
    /// Credits declared.
    pub credits: i64,
    /// Responses without the header.
    pub no_header: u64,
}

/// The month's totals.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MeterState {
    /// `YYYY-MM`.
    pub month: String,
    /// Credits counted against the cap: the declared credits plus what was carried in.
    pub spent: i64,
    /// Credits carried in from before the meter existed (the Auto lane's measured spend).
    #[serde(default)]
    pub carried: i64,
    /// Per endpoint.
    pub endpoints: BTreeMap<String, EndpointCost>,
}

/// The meter of one host.
pub struct CreditMeter {
    host: String,
    cap: i64,
    state: Mutex<MeterState>,
    cap_logged: Mutex<Option<String>>,
}

/// `YYYY-MM` of an instant.
#[must_use]
pub fn month_of(now_ms: i64) -> String {
    Period::containing(date_of_ms(now_ms), Granularity::Month).label()
}

/// The endpoint of a URL: its path without query, ids replaced by `{id}`.
#[must_use]
pub fn endpoint_of(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = rest.find('/').map_or("/", |i| &rest[i..]);
    let path = path.split(['?', '#']).next().unwrap_or("/");
    path.split('/')
        .map(|seg| if is_id(seg) { "{id}" } else { seg })
        .collect::<Vec<_>>()
        .join("/")
}

fn is_id(seg: &str) -> bool {
    seg.len() >= 16
        && seg.bytes().any(|b| b.is_ascii_digit())
        && seg.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
}

impl CreditMeter {
    /// A meter for the host of `base_url` with a monthly cap.
    #[must_use]
    pub fn new(base_url: &str, cap: i64) -> CreditMeter {
        CreditMeter {
            host: super::http::host_of(base_url).to_owned(),
            cap,
            state: Mutex::new(MeterState::default()),
            cap_logged: Mutex::new(None),
        }
    }

    /// Whether a URL is on the metered host.
    #[must_use]
    pub fn covers(&self, url: &str) -> bool {
        super::http::host_of(url) == self.host
    }

    /// The monthly cap.
    #[must_use]
    pub fn cap(&self) -> i64 {
        self.cap
    }

    fn roll(&self, state: &mut MeterState, now_ms: i64) {
        let month = month_of(now_ms);
        if state.month != month {
            *state = MeterState {
                month,
                ..MeterState::default()
            };
        }
    }

    /// Restore the month's totals from the ledger record (another month's record is ignored);
    /// `carried` seeds a month the meter has no record of (credits spent before it existed).
    pub fn restore(&self, saved: Option<&Value>, carried: i64, now_ms: i64) {
        let mut state = self.state.lock().expect("meter");
        let month = month_of(now_ms);
        match saved.and_then(|v| serde_json::from_value::<MeterState>(v.clone()).ok()) {
            Some(saved) if saved.month == month => *state = saved,
            _ => {
                *state = MeterState {
                    month,
                    spent: carried.max(0),
                    carried: carried.max(0),
                    endpoints: BTreeMap::new(),
                }
            }
        }
    }

    /// Whether a request may go out: the month's credits are below the cap, or room for
    /// `cost` more when the caller knows the price.
    pub fn allows(&self, cost: i64, now_ms: i64) -> bool {
        let mut state = self.state.lock().expect("meter");
        self.roll(&mut state, now_ms);
        let allowed = if cost > 0 {
            state.spent + cost <= self.cap
        } else {
            state.spent < self.cap
        };
        if !allowed {
            let mut logged = self.cap_logged.lock().expect("meter log");
            if logged.as_deref() != Some(state.month.as_str()) {
                *logged = Some(state.month.clone());
                log(
                    "elfa_credit_cap_reached",
                    Obj::new()
                        .with("month", state.month.as_str())
                        .with("creditsSpentMonth", state.spent)
                        .with("creditCapPerMonth", self.cap)
                        .with("action", "every Elfa request refused until the month ends"),
                );
            }
        }
        allowed
    }

    /// Record one response's header; returns the credits it declared.
    pub fn record(&self, url: &str, header: Option<&str>, now_ms: i64) -> Option<i64> {
        let credits = header.and_then(|h| h.trim().parse::<f64>().ok());
        let credits = credits.map(|c| c.ceil() as i64);
        let endpoint = endpoint_of(url);
        let mut state = self.state.lock().expect("meter");
        self.roll(&mut state, now_ms);
        let entry = state.endpoints.entry(endpoint.clone()).or_default();
        entry.calls += 1;
        match credits {
            None => entry.no_header += 1,
            Some(c) if c > 0 => {
                entry.billed_calls += 1;
                entry.credits += c;
            }
            Some(_) => {}
        }
        if let Some(c) = credits.filter(|c| *c > 0) {
            state.spent += c;
            log(
                "elfa_credits",
                Obj::new()
                    .with("endpoint", endpoint)
                    .with("credits", c)
                    .with("creditsSpentMonth", state.spent)
                    .with("creditCapPerMonth", self.cap),
            );
        }
        credits
    }

    /// The month's totals (for the ledger).
    #[must_use]
    pub fn snapshot(&self) -> MeterState {
        self.state.lock().expect("meter").clone()
    }

    /// The status object.
    #[must_use]
    pub fn status(&self) -> Obj {
        let state = self.snapshot();
        let endpoints = state
            .endpoints
            .iter()
            .map(|(name, cost)| {
                Obj::new()
                    .with("endpoint", name.as_str())
                    .with("calls", cost.calls)
                    .with("billedCalls", cost.billed_calls)
                    .with("credits", cost.credits)
                    .with("noHeader", cost.no_header)
            })
            .collect();
        Obj::new()
            .with("month", state.month.as_str())
            .with("creditsSpentMonth", state.spent)
            .with("carriedIn", state.carried)
            .with("creditCapPerMonth", self.cap)
            .with("capReached", state.spent >= self.cap)
            .with_rows("endpoints", endpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OCT: i64 = 1_791_547_200_000; // 2026-10-09
    const NOV: i64 = 1_793_577_600_000; // 2026-11-02

    #[test]
    fn endpoints_drop_queries_and_ids() {
        assert_eq!(
            endpoint_of("https://api.elfa.ai/v3/events?from=1&to=2&cursor=x"),
            "/v3/events"
        );
        assert_eq!(
            endpoint_of(
                "https://api.elfa.ai/v2/auto/queries/050a04a4-b03e-415e-9a33-b87e3c2fc2fc/cancel"
            ),
            "/v2/auto/queries/{id}/cancel"
        );
        assert_eq!(
            endpoint_of("https://api.elfa.ai/v2/auto/queries?limit=100"),
            "/v2/auto/queries"
        );
        assert_eq!(endpoint_of("http://127.0.0.1:9/"), "/");
    }

    #[test]
    fn per_endpoint_accounting() {
        let meter = CreditMeter::new("https://api.elfa.ai", 100);
        meter.restore(None, 0, OCT);
        assert_eq!(
            meter.record("https://api.elfa.ai/v3/events?x=1", Some("0"), OCT),
            Some(0)
        );
        assert_eq!(
            meter.record("https://api.elfa.ai/v3/events", None, OCT),
            None
        );
        assert_eq!(
            meter.record(
                "https://api.elfa.ai/v2/auto/queries?limit=100",
                Some("1"),
                OCT
            ),
            Some(1)
        );
        assert_eq!(
            meter.record("https://api.elfa.ai/v2/auto/queries", Some("5"), OCT),
            Some(5)
        );
        let state = meter.snapshot();
        assert_eq!(state.spent, 6);
        let events = &state.endpoints["/v3/events"];
        assert_eq!(
            (
                events.calls,
                events.billed_calls,
                events.credits,
                events.no_header
            ),
            (2, 0, 0, 1)
        );
        let listing = &state.endpoints["/v2/auto/queries"];
        assert_eq!(
            (listing.calls, listing.billed_calls, listing.credits),
            (2, 2, 6)
        );
        assert!(meter.covers("https://api.elfa.ai/v3/key-status"));
        assert!(!meter.covers("https://api.hyperliquid.xyz/info"));
    }

    #[test]
    fn cap_stops_and_survives_restart_and_resets_next_month() {
        let meter = CreditMeter::new("https://api.elfa.ai", 10);
        meter.restore(None, 7, OCT);
        assert!(meter.allows(0, OCT));
        assert!(meter.allows(2, OCT));
        assert!(!meter.allows(5, OCT), "a creation would pass the cap");
        meter.record("https://api.elfa.ai/v2/data/x", Some("3"), OCT);
        assert!(!meter.allows(0, OCT), "10 of 10");
        let saved = serde_json::to_value(meter.snapshot()).unwrap();

        let restarted = CreditMeter::new("https://api.elfa.ai", 10);
        restarted.restore(Some(&saved), 0, OCT);
        assert!(!restarted.allows(0, OCT), "the cap holds across a restart");
        assert_eq!(restarted.snapshot().carried, 7);

        let next = CreditMeter::new("https://api.elfa.ai", 10);
        next.restore(Some(&saved), 0, NOV);
        assert!(next.allows(0, NOV), "a new month starts at zero");
        assert!(
            restarted.allows(0, NOV),
            "the month rolls over in a running process"
        );
        assert_eq!(restarted.snapshot().spent, 0);
    }
}
