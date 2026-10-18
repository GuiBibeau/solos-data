//! Pyth prices from the sponsored price feed accounts on Solana: `PriceUpdateV2` accounts of the
//! Pyth Solana Receiver, kept current by the Pyth push oracle (shard 0). Each poll reads every
//! configured feed's account; a row is written when its `publish_time` moved (ADR-0012).
//!
//! Layout (Borsh, `pyth-solana-receiver-sdk` `PriceUpdateV2`): 8-byte discriminator
//! `sha256("account:PriceUpdateV2")[..8]`, `write_authority: Pubkey`, `verification_level`
//! (`Partial { num_signatures: u8 }` = tag 0 + u8, `Full` = tag 1), `price_message`
//! (`feed_id: [u8; 32]`, `price: i64`, `conf: u64`, `exponent: i32`, `publish_time: i64`,
//! `prev_publish_time: i64`, `ema_price: i64`, `ema_conf: u64`), `posted_slot: u64`.

use super::config::Feed;
use super::idl::{DecodeError, Reader, discriminator};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;

/// The Pyth Solana Receiver program, owner of every price update account.
pub const RECEIVER: &str = "rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ";

/// A decoded price update.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    /// `Full` or `Partial(<signatures>)`.
    pub verification: String,
    /// Feed id, hex.
    pub feed_id: String,
    /// Price in units of 10^exponent.
    pub price: i64,
    /// Confidence in the same units.
    pub conf: u64,
    /// Exponent.
    pub exponent: i32,
    /// Publish time, seconds.
    pub publish_time: i64,
    /// Previous publish time, seconds.
    pub prev_publish_time: i64,
    /// EMA price.
    pub ema_price: i64,
    /// EMA confidence.
    pub ema_conf: u64,
    /// The slot the update was posted at.
    pub posted_slot: u64,
}

/// Decode a `PriceUpdateV2` account.
///
/// # Errors
///
/// A wrong discriminator, an unknown verification level, or a short account.
pub fn decode(data: &[u8]) -> Result<Update, DecodeError> {
    if data.get(..8) != Some(&discriminator("account", "PriceUpdateV2")[..]) {
        return Err(DecodeError::Unsupported(
            "not a PriceUpdateV2 account".into(),
        ));
    }
    let mut r = Reader::new(&data[8..]);
    r.take(32)?;
    let verification = match r.u8()? {
        0 => format!("Partial({})", r.u8()?),
        1 => "Full".to_owned(),
        tag => return Err(DecodeError::Variant("VerificationLevel".into(), tag)),
    };
    Ok(Update {
        verification,
        feed_id: hex::encode(r.take(32)?),
        price: r.i64()?,
        conf: r.u64()?,
        exponent: r.i32()?,
        publish_time: r.i64()?,
        prev_publish_time: r.i64()?,
        ema_price: r.i64()?,
        ema_conf: r.u64()?,
        posted_slot: r.u64()?,
    })
}

/// The last `publish_time` written per feed.
#[derive(Default)]
pub struct Last {
    inner: Mutex<HashMap<String, i64>>,
}

impl Last {
    /// Whether `update` is newer than the last row of its feed, recording it.
    pub fn newer(&self, update: &Update) -> bool {
        let mut guard = self.inner.lock().expect("pyth last");
        let last = guard.entry(update.feed_id.clone()).or_insert(i64::MIN);
        if update.publish_time > *last {
            *last = update.publish_time;
            true
        } else {
            false
        }
    }
}

/// One `prices` row.
#[must_use]
pub fn row(feed: &Feed, account: &str, update: &Update, at_ms: i64, slot: i64) -> Value {
    let scale = 10f64.powi(update.exponent);
    json!({
        "at_ms": at_ms, "context_slot": slot, "symbol": feed.symbol, "feed_id": update.feed_id,
        "account": account, "price": update.price, "conf": update.conf, "exponent": update.exponent,
        "publish_time": update.publish_time, "prev_publish_time": update.prev_publish_time,
        "ema_price": update.ema_price, "ema_conf": update.ema_conf, "posted_slot": update.posted_slot,
        "verification": update.verification, "price_f": update.price as f64 * scale,
        "conf_f": update.conf as f64 * scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(level: &[u8], publish: i64) -> Vec<u8> {
        let mut data = discriminator("account", "PriceUpdateV2").to_vec();
        data.extend([7u8; 32]);
        data.extend(level);
        data.extend([0xef; 32]);
        data.extend(15_012_345_678i64.to_le_bytes());
        data.extend(1_234_567u64.to_le_bytes());
        data.extend((-8i32).to_le_bytes());
        data.extend(publish.to_le_bytes());
        data.extend((publish - 1).to_le_bytes());
        data.extend(15_000_000_000i64.to_le_bytes());
        data.extend(1_000_000u64.to_le_bytes());
        data.extend(400_000_000u64.to_le_bytes());
        data
    }

    #[test]
    fn decodes_both_verification_levels() {
        let full = decode(&account(&[1], 1_760_000_000)).unwrap();
        assert_eq!(full.verification, "Full");
        assert_eq!(full.feed_id, "ef".repeat(32));
        assert_eq!(full.price, 15_012_345_678);
        assert_eq!(full.exponent, -8);
        assert_eq!(full.posted_slot, 400_000_000);
        let partial = decode(&account(&[0, 5], 1_760_000_000)).unwrap();
        assert_eq!(partial.verification, "Partial(5)");
        assert_eq!(partial.publish_time, 1_760_000_000);
        assert!(decode(&account(&[2], 0)).is_err());
        assert!(decode(&[0u8; 40]).is_err());
        let feed = Feed {
            symbol: "SOL".into(),
            feed_id: "ef".repeat(32),
            account: "A".into(),
            pyth: None,
            sponsored: true,
        };
        let row = row(&feed, "A", &full, 1, 2);
        assert!((row["price_f"].as_f64().unwrap() - 150.12345678).abs() < 1e-9);
    }

    #[test]
    fn only_newer_publish_times_are_rows() {
        let last = Last::default();
        let update = decode(&account(&[1], 100)).unwrap();
        assert!(last.newer(&update));
        assert!(!last.newer(&update));
        assert!(last.newer(&decode(&account(&[1], 101)).unwrap()));
    }
}
