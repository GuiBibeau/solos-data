//! Pyth prices from transactions, for history (ADR-0013): every price feed account update the
//! Pyth push oracle applied. An `update_price_feed` instruction carries the signed Pyth message
//! in its data; the push oracle posts it to the account through the receiver only when its
//! `publish_time` is newer than the account's, so an instruction counts as applied when it has
//! a receiver CPI below it. An applied update is the account state the live poll reads
//! (`PriceUpdateV2`, verification `Full`: the push oracle posts through `post_update`, which
//! requires a fully verified VAA), with `posted_slot` the transaction's slot.
//!
//! Instruction data (Anchor, Borsh): discriminator `sha256("global:update_price_feed")[..8]`,
//! `PostUpdateParams { merkle_price_update: MerklePriceUpdate { message: Vec<u8>, proof:
//! Vec<[u8; 20]> }, treasury_id: u8 }`, `shard_id: u16`, `feed_id: [u8; 32]`. The message is
//! `pythnet_sdk`'s wire format, big-endian: variant `0u8` (`PriceFeedMessage`), `feed_id`,
//! `price: i64`, `conf: u64`, `exponent: i32`, `publish_time: i64`, `prev_publish_time: i64`,
//! `ema_price: i64`, `ema_conf: u64` (85 bytes). Accounts: payer, receiver, encoded VAA,
//! config, treasury, price feed account, system program.

use super::config::Feed;
use super::events::{Ix, TxMeta, flatten};
use super::idl::{DecodeError, Reader, discriminator};
use super::pyth::{RECEIVER, Update};
use serde_json::{Value, json};

/// The Pyth push oracle program (shard accounts of sponsored feeds).
pub const PUSH_ORACLE: &str = "pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT";

/// Index of the price feed account in `update_price_feed`.
const FEED_ACCOUNT: usize = 5;

/// One `update_price_feed` instruction.
#[derive(Debug, Clone, PartialEq)]
pub struct Posted {
    /// The message, as the account holds it once applied.
    pub update: Update,
    /// The shard argument.
    pub shard: u16,
    /// The feed id argument, hex.
    pub feed_arg: String,
}

fn be<const N: usize>(bytes: &[u8], at: &mut usize) -> Result<[u8; N], DecodeError> {
    let slice = bytes.get(*at..*at + N).ok_or(DecodeError::Short(*at))?;
    *at += N;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

/// Decode a `pythnet_sdk` price feed message (big-endian wire).
///
/// # Errors
///
/// Another message type, a short message, or bytes past its end.
pub fn message(bytes: &[u8]) -> Result<Update, DecodeError> {
    let mut at = 0;
    match be::<1>(bytes, &mut at)?[0] {
        0 => {}
        tag => return Err(DecodeError::Variant("Message".into(), tag)),
    }
    let feed_id = hex::encode(be::<32>(bytes, &mut at)?);
    let update = Update {
        verification: "Full".into(),
        feed_id,
        price: i64::from_be_bytes(be(bytes, &mut at)?),
        conf: u64::from_be_bytes(be(bytes, &mut at)?),
        exponent: i32::from_be_bytes(be(bytes, &mut at)?),
        publish_time: i64::from_be_bytes(be(bytes, &mut at)?),
        prev_publish_time: i64::from_be_bytes(be(bytes, &mut at)?),
        ema_price: i64::from_be_bytes(be(bytes, &mut at)?),
        ema_conf: u64::from_be_bytes(be(bytes, &mut at)?),
        posted_slot: 0,
    };
    match bytes.len() - at {
        0 => Ok(update),
        n => Err(DecodeError::Trailing(n)),
    }
}

/// Decode `update_price_feed` instruction data; `None` for another instruction.
///
/// # Errors
///
/// A short or malformed instruction or message.
pub fn instruction(data: &[u8]) -> Option<Result<Posted, DecodeError>> {
    if data.get(..8)? != discriminator("global", "update_price_feed") {
        return None;
    }
    let parse = || {
        let mut r = Reader::new(&data[8..]);
        let len = r.u32()? as usize;
        let message_bytes = r.take(len)?;
        let proof = r.u32()? as usize;
        r.take(proof.checked_mul(20).ok_or(DecodeError::Short(0))?)?;
        r.u8()?;
        let shard = u16::from_le_bytes([r.u8()?, r.u8()?]);
        let feed_arg = hex::encode(r.take(32)?);
        if r.rest() != 0 {
            return Err(DecodeError::Trailing(r.rest()));
        }
        Ok(Posted {
            update: message(message_bytes)?,
            shard,
            feed_arg,
        })
    };
    Some(parse())
}

/// What one transaction's push oracle instructions came to.
#[derive(Debug, Default, Clone, Copy)]
pub struct PythTx {
    /// `update_price_feed` instructions.
    pub updates: u64,
    /// Updates the oracle applied (rows).
    pub applied: u64,
    /// Updates not newer than the account's (no receiver CPI; no row).
    pub skipped: u64,
    /// Instructions or messages that did not decode.
    pub undecoded: u64,
    /// A message whose feed id differs from the instruction's (no row).
    pub mismatched: u64,
    /// Applied updates of accounts outside the configured set (another shard or feed in the
    /// same transaction; no row).
    pub other: u64,
}

impl PythTx {
    /// Add another count.
    pub fn add(&mut self, o: &PythTx) {
        self.updates += o.updates;
        self.applied += o.applied;
        self.skipped += o.skipped;
        self.undecoded += o.undecoded;
        self.mismatched += o.mismatched;
        self.other += o.other;
    }
}

fn applied(all: &[Ix], ix: &Ix) -> bool {
    let prefix = format!("{}.", ix.path);
    all.iter()
        .any(|other| other.program == RECEIVER && other.path.starts_with(&prefix))
}

/// The `prices` rows of one transaction: one per applied update of an account in `accounts`
/// (the configured shard-0 feed accounts), with its signature.
///
/// # Errors
///
/// When the wire or its instruction indices do not resolve.
pub fn rows(
    tx_base64: &str,
    meta: &Value,
    tx: &TxMeta<'_>,
    feeds: &[Feed],
    accounts: &std::collections::HashSet<String>,
) -> Result<(Vec<Value>, PythTx), String> {
    let all = flatten(tx_base64, meta)?;
    let mut out = Vec::new();
    let mut stats = PythTx::default();
    for ix in all.iter().filter(|ix| ix.program == PUSH_ORACLE) {
        let Some(decoded) = instruction(&ix.data) else {
            continue;
        };
        stats.updates += 1;
        let posted = match decoded {
            Ok(posted) => posted,
            Err(_) => {
                stats.undecoded += 1;
                continue;
            }
        };
        if posted.update.feed_id != posted.feed_arg {
            stats.mismatched += 1;
            continue;
        }
        if !applied(&all, ix) {
            stats.skipped += 1;
            continue;
        }
        let account = ix.accounts.get(FEED_ACCOUNT);
        if !account.is_some_and(|a| accounts.contains(a)) {
            stats.other += 1;
            continue;
        }
        stats.applied += 1;
        let mut update = posted.update;
        update.posted_slot = u64::try_from(tx.slot).unwrap_or(0);
        let symbol = feeds
            .iter()
            .find(|f| f.feed_id == update.feed_id)
            .map(|f| f.symbol.clone());
        let scale = 10f64.powi(update.exponent);
        out.push(json!({
            "at_ms": tx.block_time.map(|t| t * 1000), "context_slot": tx.slot, "symbol": symbol,
            "feed_id": update.feed_id, "account": account,
            "price": update.price, "conf": update.conf, "exponent": update.exponent,
            "publish_time": update.publish_time, "prev_publish_time": update.prev_publish_time,
            "ema_price": update.ema_price, "ema_conf": update.ema_conf,
            "posted_slot": update.posted_slot, "verification": update.verification,
            "price_f": update.price as f64 * scale, "conf_f": update.conf as f64 * scale,
            "signature": tx.signature, "shard": posted.shard,
        }));
    }
    Ok((out, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `PriceFeedMessage` in the wire format.
    pub(crate) fn wire(feed: [u8; 32], price: i64, publish: i64) -> Vec<u8> {
        let mut m = vec![0u8];
        m.extend(feed);
        m.extend(price.to_be_bytes());
        m.extend(1_234u64.to_be_bytes());
        m.extend((-8i32).to_be_bytes());
        m.extend(publish.to_be_bytes());
        m.extend((publish - 1).to_be_bytes());
        m.extend((price - 5).to_be_bytes());
        m.extend(1_000u64.to_be_bytes());
        m
    }

    fn ix_data(message: &[u8], feed: [u8; 32]) -> Vec<u8> {
        let mut d = discriminator("global", "update_price_feed").to_vec();
        d.extend(u32::try_from(message.len()).unwrap().to_le_bytes());
        d.extend(message);
        d.extend(2u32.to_le_bytes());
        d.extend([9u8; 40]);
        d.push(0);
        d.extend(0u16.to_le_bytes());
        d.extend(feed);
        d
    }

    #[test]
    fn decodes_the_instruction_and_its_big_endian_message() {
        let feed = [0xef; 32];
        let posted = instruction(&ix_data(&wire(feed, 15_012_345_678, 1_760_000_000), feed))
            .unwrap()
            .unwrap();
        assert_eq!(posted.update.feed_id, "ef".repeat(32));
        assert_eq!(posted.feed_arg, posted.update.feed_id);
        assert_eq!(posted.update.price, 15_012_345_678);
        assert_eq!(posted.update.conf, 1_234);
        assert_eq!(posted.update.exponent, -8);
        assert_eq!(posted.update.publish_time, 1_760_000_000);
        assert_eq!(posted.update.prev_publish_time, 1_759_999_999);
        assert_eq!(posted.update.ema_price, 15_012_345_673);
        assert_eq!(posted.shard, 0);
        assert!(instruction(&[0u8; 12]).is_none());
        let mut short = ix_data(&wire(feed, 1, 2), feed);
        short.pop();
        assert!(instruction(&short).unwrap().is_err());
        let mut twap = wire(feed, 1, 2);
        twap[0] = 1;
        assert!(message(&twap).is_err());
    }
}
