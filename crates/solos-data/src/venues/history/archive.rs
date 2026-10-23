//! The history's archive stream (ADR-0013, the machinery of ADR-0008): every successful
//! transaction of a slot range that names one of the watched addresses (static keys or loaded
//! through a lookup table), from the Old Faithful archive through Jetstreamer, as the RPC shape
//! the venues lane decodes (`tx_b64` re-serialized by the SDK, `meta_json` from
//! `UiTransactionStatusMeta`, the block time from the block). Without the `archive` feature the
//! stream reports that it is unavailable.

use crate::venues::walk::Tx;

/// One streamed transaction and the watched addresses it names (indexes into the list).
#[derive(Clone, Debug)]
pub struct Streamed {
    /// The transaction.
    pub tx: Tx,
    /// Indexes of the watched addresses it names.
    pub touches: Vec<usize>,
}

/// What a range stream returned.
#[derive(Debug, Default)]
pub struct Stream {
    /// Successful transactions naming a watched address.
    pub txs: Vec<Streamed>,
    /// Failed transactions naming one (not kept).
    pub failed: u64,
    /// Blocks seen.
    pub blocks: u64,
}

/// Whether this binary can stream the archive.
#[must_use]
pub const fn available() -> bool {
    cfg!(feature = "archive")
}

/// Stream `[from, to]` watching `addresses`.
///
/// # Errors
///
/// Always: this binary was built without the `archive` feature.
#[cfg(not(feature = "archive"))]
pub async fn stream(_from: i64, _to: i64, _addresses: &[String]) -> Result<Stream, String> {
    Err("archive unavailable: this binary was built without the archive feature".into())
}

#[cfg(feature = "archive")]
pub use enabled::stream;

#[cfg(feature = "archive")]
mod enabled {
    use super::{Stream, Streamed};
    use crate::venues::walk::Tx;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use futures_util::FutureExt;
    use jetstreamer_firehose::firehose::{BlockData, TransactionData, firehose};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Collected {
        txs: Vec<Streamed>,
        failed: u64,
        times: HashMap<u64, Option<i64>>,
    }

    fn shared(e: impl std::fmt::Display) -> jetstreamer_firehose::SharedError {
        Box::new(std::io::Error::other(e.to_string()))
    }

    /// Stream `[from, to]` watching `addresses`.
    ///
    /// # Errors
    ///
    /// A stream failure (the range is then retried whole by the caller).
    pub async fn stream(from: i64, to: i64, addresses: &[String]) -> Result<Stream, String> {
        let watched: Arc<HashMap<[u8; 32], usize>> = Arc::new(
            addresses
                .iter()
                .enumerate()
                .filter_map(|(i, address)| {
                    let bytes = bs58::decode(address).into_vec().ok()?;
                    Some((<[u8; 32]>::try_from(bytes.as_slice()).ok()?, i))
                })
                .collect(),
        );
        if watched.len() != addresses.len() {
            return Err("a watched address is not base58 of 32 bytes".into());
        }
        let collected = Arc::new(Mutex::new(Collected::default()));
        let (c_tx, c_block) = (Arc::clone(&collected), Arc::clone(&collected));
        let on_tx = move |_thread: usize, tx: TransactionData| {
            let collected = Arc::clone(&c_tx);
            let watched = Arc::clone(&watched);
            async move {
                let message = &tx.transaction.message;
                let loaded = &tx.transaction_status_meta.loaded_addresses;
                let mut touches: Vec<usize> = message
                    .static_account_keys()
                    .iter()
                    .chain(loaded.writable.iter())
                    .chain(loaded.readonly.iter())
                    .filter_map(|address| watched.get(address.as_ref()).copied())
                    .collect();
                if touches.is_empty() {
                    return Ok(());
                }
                if tx.transaction_status_meta.status.is_err() {
                    collected.lock().expect("collected").failed += 1;
                    return Ok(());
                }
                touches.sort_unstable();
                touches.dedup();
                let wire = wincode::serialize(&tx.transaction).map_err(shared)?;
                let ui = solana_transaction_status::UiTransactionStatusMeta::from(
                    tx.transaction_status_meta.clone(),
                );
                let meta_json = serde_json::to_string(&ui).map_err(shared)?;
                let streamed = Streamed {
                    tx: Tx {
                        signature: tx.signature.to_string(),
                        slot: i64::try_from(tx.slot).unwrap_or(i64::MAX),
                        block_time: None,
                        tx_b64: STANDARD.encode(&wire),
                        meta_json,
                        via: "archive",
                    },
                    touches,
                };
                collected.lock().expect("collected").txs.push(streamed);
                Ok(())
            }
            .boxed()
        };
        let on_block = move |_thread: usize, block: BlockData| {
            let collected = Arc::clone(&c_block);
            async move {
                if let BlockData::Block {
                    slot, block_time, ..
                } = block
                {
                    collected
                        .lock()
                        .expect("collected")
                        .times
                        .insert(slot, block_time);
                }
                Ok(())
            }
            .boxed()
        };
        let threads = std::env::var("JETSTREAMER_THREADS")
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or_else(|| {
                jetstreamer_firehose::system::optimal_firehose_thread_count() as u64
            });
        let range = u64::try_from(from).unwrap_or(0)..u64::try_from(to + 1).unwrap_or(0);
        let outcome = firehose(
            threads,
            false,
            false,
            None,
            range,
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
        let Collected {
            mut txs,
            failed,
            times,
        } = std::mem::take(&mut *collected.lock().expect("collected"));
        for streamed in &mut txs {
            streamed.tx.block_time = times
                .get(&u64::try_from(streamed.tx.slot).unwrap_or(0))
                .copied()
                .flatten();
        }
        Ok(Stream {
            txs,
            failed,
            blocks: times.len() as u64,
        })
    }
}
