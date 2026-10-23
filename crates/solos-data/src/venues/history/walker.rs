//! The history manifest: one continuous `getSignaturesForAddress` walk of a program, newest
//! first, cut into slot stretches as the backfill descends (ADR-0013). The live lane walks from
//! the tip for every window; history cannot, so the walker keeps its cursor and the rows it has
//! fetched below the stretch it handed out, and the backfill records the cursor of the oldest
//! row it has used. A restart resumes from that row and refetches what lay below it.
//!
//! An empty page is confirmed from the row above the cursor before it counts as the end of
//! history (the dead cursor of ADR-0008), and an end of history above the floor is an error:
//! every program the backfill reads existed before its floor.

use crate::collector::rpc::Rpc;
use crate::venues::rpc::call;
use crate::venues::walk::PAGE;
use serde_json::{Value, json};
use std::collections::VecDeque;

/// A manifest row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Signature.
    pub signature: String,
    /// Slot.
    pub slot: i64,
    /// Whether the transaction succeeded.
    pub ok: bool,
}

/// Where a resumed walk starts: the oldest row used, and the row above it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cursor {
    /// `before` for the next page.
    pub before: Option<String>,
    /// One row newer than `before`, to confirm an empty page from.
    pub fallback: Option<String>,
}

/// A first walk's start: the two oldest signatures above `top` (from Helius's
/// `getTransactionsForAddress` in `signatures` mode, which filters by slot), so the walk begins
/// just above the stretch it needs instead of at the tip. Every row it lists above `top` is
/// dropped by [`Walker::take`] anyway; with no signature above `top` the walk starts at the tip.
///
/// # Errors
///
/// RPC failure or an answer without a list.
pub async fn seed(rpc: &dyn Rpc, address: &str, top: i64) -> Result<Cursor, String> {
    let options = json!({
        "transactionDetails": "signatures", "sortOrder": "asc", "limit": 2,
        "commitment": "finalized", "filters": { "slot": { "gt": top }, "tokenAccounts": "none" }
    });
    let reply = call(rpc, "getTransactionsForAddress", json!([address, options]))
        .await
        .map_err(|e| e.to_string())?;
    let Some(rows) = reply.result.get("data").and_then(Value::as_array) else {
        return Err("getTransactionsForAddress answered no list".into());
    };
    let signature = |i: usize| {
        rows.get(i)
            .and_then(|r| r.get("signature"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    Ok(Cursor {
        before: signature(0),
        fallback: signature(1),
    })
}

/// The walk.
#[derive(Clone, Debug, Default)]
pub struct Walker {
    address: String,
    /// The fetch cursor (the oldest row fetched).
    fetch: Cursor,
    /// Slot of the oldest row fetched, for the V2 check.
    last_slot: Option<i64>,
    /// Rows fetched and not handed out yet, newest first.
    pending: VecDeque<Entry>,
    /// The cursor of the oldest row handed out.
    used: Cursor,
    ended: bool,
    /// Whether the end of history may come above the floor (an account younger than it).
    allow_end: bool,
    /// Pages fetched.
    pub pages: u64,
    /// Empty pages a fallback cursor resolved.
    pub unresolved: u64,
}

impl Walker {
    /// A walk of `address` from `cursor` (the tip when empty).
    #[must_use]
    pub fn resume(address: &str, cursor: Cursor) -> Walker {
        Walker {
            address: address.to_owned(),
            fetch: cursor.clone(),
            used: cursor,
            ..Walker::default()
        }
    }

    /// This walk, accepting an end of history above the floor (a Pyth feed account created
    /// after it); the rows then simply run out.
    #[must_use]
    pub fn allowing_end(mut self, allow: bool) -> Walker {
        self.allow_end = allow;
        self
    }

    /// Whether the walk has reached the end of the address's history.
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.ended && self.pending.is_empty()
    }

    /// The cursor of the oldest row handed out so far.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        self.used.clone()
    }

    async fn page(&self, rpc: &dyn Rpc, before: Option<&str>) -> Result<Vec<Entry>, String> {
        let mut options = json!({ "commitment": "finalized", "limit": PAGE });
        if let Some(before) = before {
            options["before"] = Value::String(before.to_owned());
        }
        let reply = call(
            rpc,
            "getSignaturesForAddress",
            json!([self.address, options]),
        )
        .await
        .map_err(|e| e.to_string())?;
        let Some(rows) = reply.result.as_array() else {
            return Err("getSignaturesForAddress answered no list".into());
        };
        rows.iter()
            .map(|row| {
                let (Some(signature), Some(slot)) = (
                    row.get("signature").and_then(Value::as_str),
                    row.get("slot").and_then(Value::as_i64),
                ) else {
                    return Err("manifest entry without signature or slot".to_owned());
                };
                Ok(Entry {
                    signature: signature.to_owned(),
                    slot,
                    ok: row.get("err").is_none_or(Value::is_null),
                })
            })
            .collect()
    }

    /// Fetch the next page into `pending`.
    async fn fetch_page(&mut self, rpc: &dyn Rpc) -> Result<(), String> {
        let mut page = self.page(rpc, self.fetch.before.as_deref()).await?;
        self.pages += 1;
        let mut full = page.len() >= PAGE;
        if page.is_empty()
            && let (Some(dead), Some(fallback)) =
                (self.fetch.before.clone(), self.fetch.fallback.clone())
        {
            let retry = self.page(rpc, Some(&fallback)).await?;
            self.pages += 1;
            // A resolvable cursor lists `dead` first; what follows it is the history below.
            if let Some(position) = retry.iter().position(|e| e.signature == dead) {
                full = retry.len() >= PAGE;
                page = retry[position + 1..].to_vec();
                if !page.is_empty() {
                    self.unresolved += 1;
                }
            } else if !retry.is_empty() {
                return Err("fallback page does not list the cursor".into());
            }
        }
        let mut previous = self.last_slot.unwrap_or(i64::MAX);
        for entry in &page {
            if entry.slot > previous {
                return Err("V2: slots increased across the cursor".into());
            }
            if self.fetch.before.as_deref() == Some(entry.signature.as_str()) {
                return Err("V2: the cursor repeated".into());
            }
            previous = entry.slot;
        }
        if !full {
            self.ended = true;
        }
        if let Some(oldest) = page.last() {
            self.fetch = Cursor {
                before: Some(oldest.signature.clone()),
                fallback: page
                    .len()
                    .checked_sub(2)
                    .and_then(|i| page.get(i))
                    .map(|e| e.signature.clone())
                    .or_else(|| self.fetch.before.clone()),
            };
            self.last_slot = Some(oldest.slot);
        }
        self.pending.extend(page);
        Ok(())
    }

    /// Every row with a slot in `[from, to]`, newest first. Rows above `to` are dropped (the
    /// first stretch of a walk from the tip); rows below `from` stay for the next stretch.
    /// Stretches must descend.
    ///
    /// # Errors
    ///
    /// RPC failure, a V2 violation, or the end of history at or above `from` (unless the walk
    /// allows it).
    pub async fn take(&mut self, rpc: &dyn Rpc, from: i64, to: i64) -> Result<Vec<Entry>, String> {
        while self.pending.back().is_none_or(|e| e.slot >= from) {
            if self.ended {
                if self.allow_end {
                    break;
                }
                return Err(format!(
                    "the signature walk ended above slot {from} (the provider lost the cursor or the program is younger than the floor)"
                ));
            }
            self.fetch_page(rpc).await?;
        }
        let mut out = Vec::new();
        while let Some(front) = self.pending.front() {
            if front.slot < from {
                break;
            }
            let entry = self.pending.pop_front().expect("front");
            if entry.slot <= to {
                out.push(entry);
            }
        }
        if let Some(oldest) = out.last() {
            let above = out
                .len()
                .checked_sub(2)
                .and_then(|i| out.get(i))
                .map(|e| e.signature.clone())
                .or_else(|| self.used.before.clone());
            self.used = Cursor {
                before: Some(oldest.signature.clone()),
                fallback: above,
            };
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::config::Lane;
    use crate::collector::rpc::{RpcFailure, RpcFuture, RpcResponse};
    use std::sync::Mutex;

    /// Signatures `s<slot>-<i>`, newest first, served in pages of `PAGE` after `before`; a
    /// cursor in `dead` answers an empty page.
    struct Chain {
        rows: Vec<Entry>,
        dead: Vec<String>,
        calls: Mutex<u32>,
    }

    impl Rpc for Chain {
        fn call<'a>(&'a self, _m: &'a str, params: Value, _l: Lane) -> RpcFuture<'a> {
            *self.calls.lock().unwrap() += 1;
            let before = params[1].get("before").and_then(Value::as_str);
            let start = match before {
                None => 0,
                Some(b) if self.dead.iter().any(|d| d == b) => self.rows.len(),
                Some(b) => self.rows.iter().position(|e| e.signature == b).unwrap() + 1,
            };
            let page: Vec<Value> = self.rows[start..]
                .iter()
                .take(PAGE)
                .map(|e| json!({ "signature": e.signature, "slot": e.slot, "err": if e.ok { Value::Null } else { json!("x") } }))
                .collect();
            let result = Value::Array(page);
            let raw = result.to_string();
            Box::pin(async move { Ok::<_, RpcFailure>(RpcResponse { result, raw }) })
        }
        fn provider_name(&self) -> &str {
            "chain"
        }
    }

    fn chain(slots: std::ops::Range<i64>, per_slot: usize) -> Vec<Entry> {
        let mut rows = Vec::new();
        for slot in slots.rev() {
            for i in 0..per_slot {
                rows.push(Entry {
                    signature: format!("s{slot}-{i}"),
                    slot,
                    ok: i != 1,
                });
            }
        }
        rows
    }

    #[test]
    fn stretches_cover_the_walk_once_and_a_resumed_walk_continues() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        // 1,000 slots of three rows: 3,000 rows, three full pages and an empty one.
        let rpc = Chain {
            rows: chain(0..1_000, 3),
            dead: Vec::new(),
            calls: Mutex::new(0),
        };
        let mut walker = Walker::resume("P", Cursor::default());
        let top = rt.block_on(walker.take(&rpc, 900, 949)).unwrap();
        assert_eq!(top.len(), 150, "rows above `to` are dropped");
        assert_eq!(top[0].slot, 949);
        let next = rt.block_on(walker.take(&rpc, 500, 899)).unwrap();
        assert_eq!(
            (next.len(), next[0].slot, next.last().unwrap().slot),
            (1_200, 899, 500)
        );
        let cursor = walker.cursor();
        assert_eq!(cursor.before.as_deref(), Some("s500-2"));
        assert_eq!(cursor.fallback.as_deref(), Some("s500-1"));
        // A restart from the recorded cursor hands out exactly the rows below it.
        let mut resumed = Walker::resume("P", cursor);
        let below = rt.block_on(resumed.take(&rpc, 1, 499)).unwrap();
        assert_eq!(below.len(), 1_497);
        assert_eq!(below[0].signature, "s499-0");
        // The program's history ends at slot 0: a stretch reaching it is an error, not a success.
        assert!(rt.block_on(resumed.take(&rpc, 0, 0)).is_err());
        // A walk that may end early hands out what is left and reports the end.
        let mut young = Walker::resume("P", Cursor::default()).allowing_end(true);
        let all = rt.block_on(young.take(&rpc, -500, 999)).unwrap();
        assert_eq!(all.len(), 3_000);
        assert!(young.exhausted());
        assert!(
            rt.block_on(young.take(&rpc, -900, -501))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn an_empty_page_is_confirmed_from_the_row_above_the_cursor() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let rows = chain(0..1_000, 2);
        // The provider cannot resolve `s500-1` as a cursor, while history continues below it.
        let rpc = Chain {
            rows,
            dead: vec!["s500-1".into()],
            calls: Mutex::new(0),
        };
        let mut walker = Walker::resume(
            "P",
            Cursor {
                before: Some("s500-1".into()),
                fallback: Some("s500-0".into()),
            },
        );
        let below = rt.block_on(walker.take(&rpc, 1, 499)).unwrap();
        assert_eq!(below.len(), 998);
        assert_eq!(walker.unresolved, 1);
        // Without a fallback the dead cursor is an early end of history: refused.
        let mut lost = Walker::resume(
            "P",
            Cursor {
                before: Some("s500-1".into()),
                fallback: None,
            },
        );
        assert!(rt.block_on(lost.take(&rpc, 1, 499)).is_err());
    }
}
