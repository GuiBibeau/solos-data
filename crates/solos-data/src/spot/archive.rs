//! The spot lane's archive stream (ADR-0014, the machinery of ADR-0008): every transaction of a
//! slot range from the Old Faithful archive through Jetstreamer. A transaction that names a spot
//! program (static key or loaded through a lookup table) is decoded in the callback into rows,
//! so only rows are held, never transactions. Every block's time and executed-transaction count
//! and every slot's delivered-transaction count are kept for the block completeness check; the
//! signatures of transactions in the manifest windows are kept for the sampled manifest check.
//! Without the `archive` feature the stream reports that it is unavailable.

use super::dex::Dex;
#[cfg(not(feature = "archive"))]
use super::dex::Programs;
use super::rows::Rows;
use std::collections::HashMap;

/// A block as the archive gave it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Block {
    /// Block time.
    pub time: Option<i64>,
    /// Transactions its entries list.
    pub executed: u64,
}

/// A real transaction captured for the fixtures: the RPC shape and the rows it gave.
#[derive(Clone, Debug)]
pub struct Captured {
    /// Slot.
    pub slot: u64,
    /// Index in the block.
    pub index: u32,
    /// Signature.
    pub signature: String,
    /// Base64 wire (re-serialized).
    pub tx_b64: String,
    /// The RPC `meta` JSON.
    pub meta_json: String,
    /// What it covers (program, instruction, events), to pick varied examples.
    pub cover: Vec<String>,
}

/// What a range stream returned.
#[derive(Debug, Default)]
pub struct Stream {
    /// Rows of the successful transactions naming a spot program, and per-program counts.
    pub rows: Rows,
    /// Blocks by slot.
    pub blocks: HashMap<u64, Block>,
    /// Transactions delivered per slot (votes included).
    pub delivered: HashMap<u64, u64>,
    /// `(program, slot, signature)` of successful transactions naming a program inside a
    /// manifest window.
    pub sampled: Vec<(Dex, u64, String)>,
    /// Captured fixtures.
    pub captured: Vec<Captured>,
}

/// What the stream keeps besides rows.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Manifest windows `[from, to]`: signatures of transactions in them are kept.
    pub windows: Vec<(u64, u64)>,
    /// Fixtures to capture per coverage key (0: none).
    pub capture: usize,
}

/// Whether this binary can stream the archive.
#[must_use]
pub const fn available() -> bool {
    cfg!(feature = "archive")
}

/// Stream `[from, to]`.
///
/// # Errors
///
/// Always: this binary was built without the `archive` feature.
#[cfg(not(feature = "archive"))]
pub async fn stream(
    _from: u64,
    _to: u64,
    _programs: &Programs,
    _options: &Options,
) -> Result<Stream, String> {
    Err("archive unavailable: this binary was built without the archive feature".into())
}

#[cfg(feature = "archive")]
pub use enabled::stream;

#[cfg(feature = "archive")]
mod enabled {
    use super::{Block, Captured, Options, Stream};
    use crate::spot::dex::{Dex, Programs};
    use crate::spot::frames::frames;
    use crate::spot::rows::{Rows, named};
    use crate::spot::tx::{Balance, Ix, Key, SpotTx, merge_balances};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use futures_util::FutureExt;
    use jetstreamer_firehose::firehose::{BlockData, TransactionData, firehose};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    /// One firehose thread's share.
    struct Local {
        rows: Rows,
        blocks: HashMap<u64, Block>,
        delivered: HashMap<u64, u64>,
        sampled: Vec<(Dex, u64, String)>,
        captured: Vec<Captured>,
        covered: HashMap<String, usize>,
    }

    impl Default for Local {
        fn default() -> Local {
            Local {
                rows: Rows::new(),
                blocks: HashMap::new(),
                delivered: HashMap::new(),
                sampled: Vec::new(),
                captured: Vec::new(),
                covered: HashMap::new(),
            }
        }
    }

    fn shared(e: impl std::fmt::Display) -> jetstreamer_firehose::SharedError {
        Box::new(std::io::Error::other(e.to_string()))
    }

    fn balances(
        list: Option<&Vec<solana_transaction_status::TransactionTokenBalance>>,
    ) -> Vec<Balance> {
        list.into_iter()
            .flatten()
            .map(|b| Balance {
                account: b.account_index,
                mint: b.mint.clone(),
                decimals: b.ui_token_amount.decimals,
                owner: (!b.owner.is_empty()).then(|| b.owner.clone()),
                pre: b.ui_token_amount.amount.parse().ok(),
            })
            .collect()
    }

    /// The decoders' view of a native transaction.
    fn spot_tx(tx: &TransactionData, keys: Vec<Key>) -> SpotTx {
        let meta = &tx.transaction_status_meta;
        let inner = meta.inner_instructions.as_deref().unwrap_or_default();
        let mut ixs = Vec::new();
        for (i, ix) in tx.transaction.message.instructions().iter().enumerate() {
            let top = u16::try_from(i).unwrap_or(u16::MAX);
            ixs.push(Ix {
                program: ix.program_id_index,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
                depth: 1,
                top,
            });
            for group in inner.iter().filter(|g| usize::from(g.index) == i) {
                for item in &group.instructions {
                    ixs.push(Ix {
                        program: item.instruction.program_id_index,
                        accounts: item.instruction.accounts.clone(),
                        data: item.instruction.data.clone(),
                        depth: item
                            .stack_height
                            .and_then(|h| u8::try_from(h).ok())
                            .unwrap_or(2),
                        top,
                    });
                }
            }
        }
        SpotTx {
            slot: tx.slot,
            index: u32::try_from(tx.transaction_slot_index).unwrap_or(u32::MAX),
            signature: tx.signature.as_ref().to_vec(),
            signers: tx.transaction.signatures.len(),
            keys,
            ixs,
            logs: meta.log_messages.clone().unwrap_or_default(),
            balances: merge_balances(
                balances(meta.pre_token_balances.as_ref()),
                balances(meta.post_token_balances.as_ref()),
            ),
        }
    }

    /// What a transaction covers, for picking varied fixtures: per invocation, the program,
    /// the instruction's first data bytes, the events' discriminators and what the decoder made
    /// of it (a trade, a gap, or nothing).
    fn cover(tx: &SpotTx, programs: &Programs) -> Vec<String> {
        let framed = frames(tx, programs);
        let decoded = crate::spot::dex::decode_tx(tx, &framed.frames);
        let mut out: Vec<String> = framed
            .frames
            .iter()
            .zip(&decoded)
            .map(|(f, d)| {
                let data = &tx.ixs[f.at].data;
                let head = hex::encode(&data[..data.len().min(f.dex.head_len())]);
                let mut events: Vec<String> = f
                    .events
                    .iter()
                    .map(|p| match p {
                        crate::spot::frames::Payload::Event(b) => hex::encode(&b[..b.len().min(8)]),
                        crate::spot::frames::Payload::RayLog(b) => {
                            format!("ray{}", b.first().copied().unwrap_or(255))
                        }
                    })
                    .collect();
                events.sort();
                events.dedup();
                let c = &d.out.counts;
                events.push(
                    if c.unresolved > 0 {
                        "unresolved"
                    } else if c.missing_event > 0 {
                        "missing"
                    } else if c.undecoded > 0 {
                        "undecoded"
                    } else if c.swaps > 0 {
                        "trade"
                    } else {
                        "none"
                    }
                    .into(),
                );
                format!("{}:{head}:{}", f.dex.name(), events.join("+"))
            })
            .collect();
        if framed.truncated {
            out.push("truncated".into());
        }
        out.sort();
        out.dedup();
        out
    }

    fn capture(
        tx: &TransactionData,
        spot: &SpotTx,
        cover: Vec<String>,
    ) -> Result<Captured, jetstreamer_firehose::SharedError> {
        let wire = wincode::serialize(&tx.transaction).map_err(shared)?;
        let ui = solana_transaction_status::UiTransactionStatusMeta::from(
            tx.transaction_status_meta.clone(),
        );
        Ok(Captured {
            slot: spot.slot,
            index: spot.index,
            signature: tx.signature.to_string(),
            tx_b64: STANDARD.encode(&wire),
            meta_json: serde_json::to_string(&ui).map_err(shared)?,
            cover,
        })
    }

    fn on_transaction(
        local: &mut Local,
        programs: &Programs,
        program_keys: &[Key],
        options: &Options,
        tx: &TransactionData,
    ) -> Result<(), jetstreamer_firehose::SharedError> {
        *local.delivered.entry(tx.slot).or_default() += 1;
        if tx.is_vote {
            return Ok(());
        }
        let loaded = &tx.transaction_status_meta.loaded_addresses;
        let statics = tx.transaction.message.static_account_keys();
        let names_one = statics
            .iter()
            .chain(loaded.writable.iter())
            .chain(loaded.readonly.iter())
            .any(|k| program_keys.iter().any(|p| p == k.as_ref()));
        if !names_one {
            return Ok(());
        }
        let keys: Vec<Key> = statics
            .iter()
            .chain(loaded.writable.iter())
            .chain(loaded.readonly.iter())
            .map(|k| k.to_bytes())
            .collect();
        let dexes = named(&keys, programs);
        if tx.transaction_status_meta.status.is_err() {
            for dex in dexes {
                local.rows.failed(dex);
            }
            return Ok(());
        }
        if options
            .windows
            .iter()
            .any(|(from, to)| (*from..=*to).contains(&tx.slot))
        {
            let signature = tx.signature.to_string();
            for dex in &dexes {
                local.sampled.push((*dex, tx.slot, signature.clone()));
            }
        }
        let spot = spot_tx(tx, keys);
        local.rows.add_tx(&spot, programs, &dexes);
        if options.capture > 0 {
            let keys = cover(&spot, programs);
            if keys
                .iter()
                .any(|k| local.covered.get(k).copied().unwrap_or(0) < options.capture)
            {
                for k in &keys {
                    *local.covered.entry(k.clone()).or_default() += 1;
                }
                local.captured.push(capture(tx, &spot, keys)?);
            }
        }
        Ok(())
    }

    /// Stream `[from, to]`.
    ///
    /// # Errors
    ///
    /// A stream failure (the caller retries the range whole).
    pub async fn stream(
        from: u64,
        to: u64,
        programs: &Programs,
        options: &Options,
    ) -> Result<Stream, String> {
        let threads = std::env::var("JETSTREAMER_THREADS")
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or_else(|| {
                jetstreamer_firehose::system::optimal_firehose_thread_count() as u64
            });
        let locals: Arc<Vec<Mutex<Local>>> = Arc::new(
            (0..threads.max(1) as usize + 1)
                .map(|_| Mutex::new(Local::default()))
                .collect(),
        );
        let programs = Arc::new(programs.clone());
        let options = Arc::new(options.clone());
        let program_keys: Arc<Vec<Key>> =
            Arc::new(programs.list().iter().map(|d| d.key()).collect());
        let (l_tx, l_block) = (Arc::clone(&locals), Arc::clone(&locals));
        let on_tx = move |thread: usize, tx: TransactionData| {
            let locals = Arc::clone(&l_tx);
            let programs = Arc::clone(&programs);
            let options = Arc::clone(&options);
            let program_keys = Arc::clone(&program_keys);
            async move {
                let slot = thread.min(locals.len() - 1);
                let mut local = locals[slot].lock().expect("spot local");
                on_transaction(&mut local, &programs, &program_keys, &options, &tx)
            }
            .boxed()
        };
        let on_block = move |thread: usize, block: BlockData| {
            let locals = Arc::clone(&l_block);
            async move {
                if let BlockData::Block {
                    slot,
                    block_time,
                    executed_transaction_count,
                    ..
                } = block
                {
                    let i = thread.min(locals.len() - 1);
                    locals[i].lock().expect("spot local").blocks.insert(
                        slot,
                        Block {
                            time: block_time,
                            executed: executed_transaction_count,
                        },
                    );
                }
                Ok(())
            }
            .boxed()
        };
        let outcome = firehose(
            threads,
            false,
            false,
            None,
            from..to + 1,
            Some(on_block),
            Some(on_tx),
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::EntryData,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::RewardsData,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::HandlerFn<
                    jetstreamer_firehose::firehose::FirehoseErrorContext,
                >,
            >,
            None::<
                jetstreamer_firehose::firehose::StatsTracking<
                    jetstreamer_firehose::firehose::HandlerFn<
                        jetstreamer_firehose::firehose::Stats,
                    >,
                >,
            >,
            None,
        )
        .await;
        if let Err((error, slot)) = outcome {
            return Err(crate::jsonout::safe_error(&format!(
                "archive stream failed at slot {slot}: {error:?}"
            )));
        }
        let mut out = Stream {
            rows: Rows::new(),
            ..Stream::default()
        };
        let locals = Arc::try_unwrap(locals).map_err(|_| "archive stream still holds its state")?;
        let mut seen: HashSet<String> = HashSet::new();
        for local in locals {
            let local = local.into_inner().map_err(|_| "a stream thread panicked")?;
            out.rows.append(local.rows);
            out.blocks.extend(local.blocks);
            for (slot, n) in local.delivered {
                *out.delivered.entry(slot).or_default() += n;
            }
            out.sampled.extend(local.sampled);
            for c in local.captured {
                if seen.insert(c.signature.clone()) {
                    out.captured.push(c);
                }
            }
        }
        Ok(out)
    }
}
