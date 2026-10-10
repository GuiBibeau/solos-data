//! Elfa call episodes. `/v3/calls/episodes?from&to` answers every episode *active* in the
//! window, oldest `openedAt` first, so tens of thousands opened months before `from` come
//! ahead of anything new and a page-capped pull from the last `to` never reaches the present.
//! The lane pages in `openedAt` descending order instead, as segments that survive across
//! cycles: each cycle first pulls the head (newest first, down to an hour below the newest
//! `openedAt` stored, to catch late-indexed episodes), then spends the rest of its page budget
//! on the pending segments, resuming each from its stored cursor. The first run's head drains
//! the whole window from the configured start: that is the one-off catch-up. Rows merge on id,
//! the newest copy winning, so an episode seen again with its close replaces the open one.

use super::elfa::{ElfaLane, Stream, flatten, group_by_day, progress_key, write_day};
use super::periods::date_of_ms;
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use serde_json::{Value, json};

/// How far below the newest stored `openedAt` the head reaches, seconds.
pub const HEAD_OVERLAP_S: i64 = 3600;

/// A descending pull over one fixed window, resumable from its cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Window start, seconds.
    pub from: i64,
    /// Window end, seconds.
    pub to: i64,
    /// Where the next page starts; `None` for the first page.
    pub cursor: Option<String>,
    /// The segment ends at the first episode opened before this; `None` drains the window.
    pub stop_below: Option<i64>,
}

/// The stream's progress: the newest `openedAt` stored and the segments still to page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// Newest `openedAt` received, seconds.
    pub newest_opened_at: Option<i64>,
    /// Unfinished segments, in the order they were left.
    pub pending: Vec<Segment>,
}

impl State {
    /// The state in a progress value; the old `{lastTo}` shape reads as a fresh start.
    #[must_use]
    pub fn from_progress(value: Option<&Value>) -> State {
        let Some(value) = value else {
            return State::default();
        };
        let pending = value
            .get("pending")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|s| {
                Some(Segment {
                    from: s.get("from")?.as_i64()?,
                    to: s.get("to")?.as_i64()?,
                    cursor: s.get("cursor").and_then(Value::as_str).map(str::to_owned),
                    stop_below: s.get("stopBelow").and_then(Value::as_i64),
                })
            })
            .collect();
        State {
            newest_opened_at: value.get("newestOpenedAt").and_then(Value::as_i64),
            pending,
        }
    }

    /// The progress value.
    #[must_use]
    pub fn to_progress(&self) -> Value {
        let pending: Vec<Value> = self
            .pending
            .iter()
            .map(|s| json!({ "from": s.from, "to": s.to, "cursor": s.cursor, "stopBelow": s.stop_below }))
            .collect();
        json!({ "newestOpenedAt": self.newest_opened_at, "pending": pending, "updatedAt": now() })
    }

    /// This cycle's segments: the head first, then the pending ones.
    #[must_use]
    pub fn plan(&self, start_s: i64, now_s: i64) -> Vec<Segment> {
        let to = now_s - 60;
        let head = match self.newest_opened_at {
            Some(newest) => Segment {
                from: (newest - HEAD_OVERLAP_S).max(start_s),
                to,
                cursor: None,
                stop_below: Some(newest - HEAD_OVERLAP_S),
            },
            None => Segment {
                from: start_s,
                to,
                cursor: None,
                stop_below: None,
            },
        };
        let mut plan = Vec::with_capacity(1 + self.pending.len());
        if head.from <= head.to {
            plan.push(head);
        }
        plan.extend(self.pending.iter().cloned());
        plan
    }
}

/// The cursor to continue a segment with after a page, or `None` when the segment is done:
/// the answer has no more, or the page reached an episode opened before `stop_below`.
#[must_use]
pub fn next_cursor(
    stop_below: Option<i64>,
    opened: &[i64],
    has_more: bool,
    cursor: Option<String>,
) -> Option<String> {
    let reached = stop_below.is_some_and(|stop| opened.iter().any(|&o| o < stop));
    if reached || !has_more {
        return None;
    }
    cursor
}

/// One cycle of the episodes stream within `max_pages` requests.
pub async fn pull(
    ctx: &Ctx,
    lane: &ElfaLane,
    start_s: i64,
    now_s: i64,
    max_pages: usize,
) -> Result<Outcome, StoreError> {
    let key = progress_key(Stream::Episodes);
    let lookup = key.clone();
    let stored = ctx
        .db
        .run(move |store| super::ledger::get_progress(store, &lookup))
        .await?;
    let state = State::from_progress(stored.as_ref());
    let received_at_ms = chrono::Utc::now().timestamp_millis();
    let mut next = State {
        newest_opened_at: state.newest_opened_at,
        pending: Vec::new(),
    };
    let (mut rows, mut pages) = (Vec::new(), 0usize);
    let mut failure: Option<StoreError> = None;
    for mut segment in state.plan(start_s, now_s) {
        loop {
            if failure.is_some() || pages >= max_pages || ctx.stopping() {
                // A segment without a cursor is a head that has not paged yet: the next
                // cycle's head covers it, so it is not kept.
                if segment.cursor.is_some() {
                    next.pending.push(segment);
                }
                break;
            }
            let page = match fetch(ctx, lane, &segment).await {
                Ok(page) => page,
                Err(error) => {
                    // Keep what was paged and the segment where it stopped; retry next cycle.
                    failure = Some(error);
                    continue;
                }
            };
            pages += 1;
            let opened: Vec<i64> = page.0.iter().filter_map(opened_at).collect();
            if let Some(&max) = opened.iter().max() {
                next.newest_opened_at = Some(next.newest_opened_at.map_or(max, |n| n.max(max)));
            }
            rows.extend(
                page.0
                    .iter()
                    .filter_map(|r| flatten(Stream::Episodes, r, received_at_ms)),
            );
            match next_cursor(segment.stop_below, &opened, page.1, page.2) {
                Some(cursor) => segment.cursor = Some(cursor),
                None => break,
            }
        }
    }
    let mut outcome = Outcome {
        requests: pages as u64,
        ..Outcome::default()
    };
    for (day, day_rows) in group_by_day(rows) {
        let complete = day.start < date_of_ms(now_s * 1000);
        let count = write_day(ctx, Stream::Episodes, day, &day_rows, complete, None, &key).await?;
        outcome.files += 1;
        outcome.rows += u64::try_from(day_rows.len()).unwrap_or(0);
        log(
            "augment_file",
            Obj::new()
                .with("source", "elfa")
                .with("dataset", "episodes")
                .with("symbol", "ALL")
                .with("period", day.label())
                .with("rows", count)
                .with("added", day_rows.len()),
        );
    }
    log(
        "elfa_episodes_cycle",
        Obj::new()
            .with("requests", pages)
            .with("rows", outcome.rows)
            .with("newestOpenedAt", next.newest_opened_at)
            .with("pending", next.pending.len()),
    );
    let value = next.to_progress();
    ctx.db
        .run(move |store| super::ledger::set_progress(store, &key, &value))
        .await?;
    match failure {
        Some(error) => Err(error),
        None => Ok(outcome),
    }
}

fn opened_at(row: &Value) -> Option<i64> {
    let n = row.get("openedAt")?.as_i64()?;
    Some(if n > 100_000_000_000 { n / 1000 } else { n })
}

async fn fetch(
    ctx: &Ctx,
    lane: &ElfaLane,
    segment: &Segment,
) -> Result<(Vec<Value>, bool, Option<String>), StoreError> {
    let mut pairs: Vec<(&str, String)> = vec![
        ("from", segment.from.to_string()),
        ("to", segment.to.to_string()),
        ("limit", Stream::Episodes.limit().to_string()),
        ("order", "desc".into()),
    ];
    if let Some(c) = &segment.cursor {
        pairs.push(("cursor", c.clone()));
    }
    let answer = lane.get(&ctx.http, Stream::Episodes.path(), &pairs).await?;
    let page = answer
        .get("episodes")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| StoreError::Check("episodes answer has no episodes".into()))?;
    let has_more = answer
        .get("hasMore")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let cursor = answer
        .get("nextCursor")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok((page, has_more, cursor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_run_drains_the_window_from_the_start() {
        let plan = State::default().plan(1_000, 10_060);
        assert_eq!(
            plan,
            [Segment {
                from: 1_000,
                to: 10_000,
                cursor: None,
                stop_below: None
            }]
        );
    }

    #[test]
    fn head_reaches_an_hour_below_the_newest_then_resumes_pending() {
        let pending = Segment {
            from: 1_000,
            to: 9_000,
            cursor: Some("c7".into()),
            stop_below: None,
        };
        let state = State {
            newest_opened_at: Some(9_000),
            pending: vec![pending.clone()],
        };
        let plan = state.plan(1_000, 20_060);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].from, 9_000 - HEAD_OVERLAP_S);
        assert_eq!(plan[0].to, 20_000);
        assert_eq!(plan[0].stop_below, Some(9_000 - HEAD_OVERLAP_S));
        assert_eq!(plan[0].cursor, None);
        assert_eq!(plan[1], pending);
        // The head never starts before the configured start.
        let early = State {
            newest_opened_at: Some(1_500),
            pending: vec![],
        };
        assert_eq!(early.plan(1_000, 20_060)[0].from, 1_000);
    }

    #[test]
    fn a_segment_ends_at_its_stop_or_when_the_answer_is_exhausted() {
        let c = || Some("next".to_owned());
        assert_eq!(next_cursor(None, &[50, 40], true, c()), c());
        assert_eq!(next_cursor(None, &[50, 40], false, c()), None);
        assert_eq!(next_cursor(Some(45), &[50, 40], true, c()), None);
        assert_eq!(next_cursor(Some(30), &[50, 40], true, c()), c());
        assert_eq!(next_cursor(Some(30), &[50, 40], true, None), None);
        assert_eq!(next_cursor(Some(30), &[], true, c()), c());
    }

    #[test]
    fn progress_round_trips_and_the_old_shape_restarts() {
        let state = State {
            newest_opened_at: Some(1_791_617_748),
            pending: vec![Segment {
                from: 1,
                to: 2,
                cursor: Some("abc".into()),
                stop_below: Some(0),
            }],
        };
        assert_eq!(State::from_progress(Some(&state.to_progress())), state);
        let old = json!({ "lastTo": 1_790_000_000, "updatedAt": "x" });
        assert_eq!(State::from_progress(Some(&old)), State::default());
        assert_eq!(State::from_progress(None), State::default());
    }
}
