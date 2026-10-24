//! Which instructions of a transaction are invocations of a spot program, and the events each
//! emitted (ADR-0014). An `emit_cpi!` event is a self-CPI whose data starts with Anchor's
//! `EVENT_IX_TAG`: it belongs to its parent invocation. An `emit!` event is a `Program data:`
//! log line and a Raydium AMM v4 event a `Program log: ray_log:` line: they belong to the
//! invocation on top of the log's invoke stack. The k-th `invoke` line of a program is its k-th
//! instruction in execution order (top level and CPI alike), so log lines map to instructions
//! without guessing. After `Log truncated` nothing more is attributed; the swaps whose event was
//! cut are counted as missing.

use super::dex::{Dex, Programs};
use super::tx::SpotTx;
use crate::venues::idl::EVENT_IX_TAG;
use base64::{Engine, engine::general_purpose::STANDARD};

/// An event's bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    /// An Anchor event: discriminator then body.
    Event(Vec<u8>),
    /// A Raydium AMM v4 `ray_log` struct (log type byte first).
    RayLog(Vec<u8>),
}

/// One invocation of a spot program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Its position in [`SpotTx::ixs`].
    pub at: usize,
    /// The program.
    pub dex: Dex,
    /// The events it emitted, in order.
    pub events: Vec<Payload>,
}

/// The invocations of a transaction and whether its log was truncated.
#[derive(Clone, Debug, Default)]
pub struct Framed {
    /// Invocations in execution order.
    pub frames: Vec<Frame>,
    /// Whether the log ended with `Log truncated`.
    pub truncated: bool,
}

/// Split a transaction into invocations of `programs` with their events.
#[must_use]
pub fn frames(tx: &SpotTx, programs: &Programs) -> Framed {
    let dex_of_key: Vec<Option<Dex>> = tx.keys.iter().map(|k| programs.of(k)).collect();
    let dex_at = |p: usize| {
        tx.ixs
            .get(p)
            .and_then(|ix| dex_of_key.get(usize::from(ix.program)).copied().flatten())
    };
    let mut out = Framed::default();
    let mut frame_at: Vec<Option<usize>> = vec![None; tx.ixs.len()];
    let mut invocations: Vec<Vec<usize>> = vec![Vec::new(); super::dex::ALL.len()];
    // The latest instruction at each depth, so a CPI finds its parent.
    let mut stack: Vec<usize> = Vec::new();
    for (p, ix) in tx.ixs.iter().enumerate() {
        let depth = usize::from(ix.depth.max(1));
        stack.truncate(depth - 1);
        let parent = if depth > 1 {
            stack.last().copied()
        } else {
            None
        };
        stack.push(p);
        let Some(dex) = dex_at(p) else {
            continue;
        };
        invocations[dex as usize].push(p);
        if ix.data.starts_with(&EVENT_IX_TAG)
            && let Some(parent) = parent
            && dex_at(parent) == Some(dex)
            && let Some(f) = frame_at[parent]
        {
            out.frames[f]
                .events
                .push(Payload::Event(ix.data[EVENT_IX_TAG.len()..].to_vec()));
            continue;
        }
        frame_at[p] = Some(out.frames.len());
        out.frames.push(Frame {
            at: p,
            dex,
            events: Vec::new(),
        });
    }
    out.truncated = attach_logs(tx, programs, &invocations, &frame_at, &mut out.frames);
    out
}

fn decode_chunks(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for chunk in text.split(' ') {
        if let Ok(decoded) = STANDARD.decode(chunk.trim()) {
            bytes.extend(decoded);
        }
    }
    bytes
}

/// Attribute `Program data:` and `ray_log:` lines; returns whether the log was truncated.
fn attach_logs(
    tx: &SpotTx,
    programs: &Programs,
    invocations: &[Vec<usize>],
    frame_at: &[Option<usize>],
    frames: &mut [Frame],
) -> bool {
    let mut seen = vec![0usize; invocations.len()];
    // (program id, the frame it is when it is a spot program's invocation)
    let mut stack: Vec<(&str, Option<usize>)> = Vec::new();
    for line in &tx.logs {
        if line == "Log truncated" {
            return true;
        }
        let top = stack.last().and_then(|(_, f)| *f);
        if let Some(rest) = line.strip_prefix("Program data: ") {
            if let Some(f) = top
                && frames[f].dex.logs_events()
            {
                frames[f].events.push(Payload::Event(decode_chunks(rest)));
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("Program log: ray_log: ") {
            if let Some(f) = top
                && frames[f].dex == Dex::RaydiumAmm
                && let Ok(bytes) = STANDARD.decode(rest.trim())
            {
                frames[f].events.push(Payload::RayLog(bytes));
            }
            continue;
        }
        let Some(rest) = line.strip_prefix("Program ") else {
            continue;
        };
        let mut words = rest.split(' ');
        let (Some(id), Some(verb)) = (words.next(), words.next()) else {
            continue;
        };
        if verb == "invoke" {
            let frame = programs.of_id(id).and_then(|dex| {
                let k = seen[dex as usize];
                seen[dex as usize] += 1;
                invocations[dex as usize].get(k).and_then(|&p| frame_at[p])
            });
            stack.push((id, frame));
        } else if (verb == "success" || verb.starts_with("failed"))
            && stack.last().is_some_and(|(top, _)| *top == id)
        {
            stack.pop();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spot::tx::{Ix, Key};

    fn tx_with(keys: Vec<Key>, ixs: Vec<Ix>, logs: &[&str]) -> SpotTx {
        SpotTx {
            keys,
            ixs,
            logs: logs.iter().map(|l| (*l).to_owned()).collect(),
            ..SpotTx::default()
        }
    }

    fn ix(program: u8, depth: u8, top: u16, data: Vec<u8>) -> Ix {
        Ix {
            program,
            accounts: Vec::new(),
            data,
            depth,
            top,
        }
    }

    #[test]
    fn self_cpi_events_join_their_parent_and_log_events_their_invoke_frame() {
        let pump = Dex::PumpSwap.key();
        let whirl = Dex::Whirlpool.key();
        let router = [9u8; 32];
        let mut event = EVENT_IX_TAG.to_vec();
        event.extend([1, 2, 3]);
        let txn = tx_with(
            vec![[0; 32], router, pump, whirl],
            vec![
                ix(1, 1, 0, vec![0]), // router
                ix(2, 2, 0, vec![5]), // pumpswap swap (CPI)
                ix(2, 3, 0, event),   // its event self-CPI
                ix(3, 2, 0, vec![6]), // whirlpool swap (CPI)
                ix(3, 1, 1, vec![7]), // whirlpool at the top level
            ],
            &[
                "Program 9 invoke [1]",
                "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [2]",
                "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [3]",
                "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success",
                "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success",
                "Program whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc invoke [2]",
                "Program data: AQI=",
                "Program whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc success",
                "Program 9 success",
                "Program whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc invoke [1]",
                "Log truncated",
            ],
        );
        let framed = frames(&txn, &Programs::all());
        assert!(framed.truncated);
        assert_eq!(framed.frames.len(), 3);
        assert_eq!(framed.frames[0].dex, Dex::PumpSwap);
        assert_eq!(framed.frames[0].events, vec![Payload::Event(vec![1, 2, 3])]);
        assert_eq!(framed.frames[1].at, 3);
        assert_eq!(framed.frames[1].events, vec![Payload::Event(vec![1, 2])]);
        assert!(framed.frames[2].events.is_empty());
    }

    #[test]
    fn a_ray_log_belongs_to_the_amm_frame_only() {
        let amm = Dex::RaydiumAmm.key();
        let txn = tx_with(
            vec![[0; 32], amm],
            vec![ix(1, 1, 0, vec![9])],
            &[
                "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 invoke [1]",
                "Program log: ray_log: Aw==",
                "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success",
                "Program log: ray_log: BA==",
            ],
        );
        let framed = frames(&txn, &Programs::all());
        assert_eq!(framed.frames[0].events, vec![Payload::RayLog(vec![3])]);
    }
}
