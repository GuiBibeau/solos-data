//! Rows for the six decoded tables from one transaction's decoded groups. A port of
//! `decode/normalize.ts`, including its `taker_side` comparison against the capitalised `Bid`
//! while the SDK emits lowercase sides (kept for parity; a correction is a separate change).

use super::schema::{TABLES, VERSION};
use crate::jsonout::{Obj, now};
use phoenix_codec::DecodedGroup;
use serde_json::Value;

/// One raw transaction row as the source files carry it. Values keep the JSON rendering of the
/// DuckDB row (`slot` and `block_time` are decimal strings) so the content hash matches the
/// TypeScript decoder's.
#[derive(Clone, Debug)]
pub struct RawTransaction {
    /// Base58 signature.
    pub signature: String,
    /// Slot, as rendered.
    pub slot: Value,
    /// Block time, as rendered, or null.
    pub block_time: Value,
    /// Index in the block, or null.
    pub tx_index: Value,
    /// Whether the slot had one program transaction, or null.
    pub single_in_slot: Value,
    /// Base64 wire, or null.
    pub tx_b64: Option<String>,
    /// RPC meta JSON text, or null.
    pub meta_json: Option<String>,
    /// Terminal fetch error, or null.
    pub terminal_error: Option<String>,
    /// `err` as rendered: null, or JSON text.
    pub err: Value,
}

impl RawTransaction {
    /// The slot as text.
    #[must_use]
    pub fn slot_text(&self) -> String {
        match &self.slot {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    /// The content hash the decoder keys revisions by:
    /// `sha256(JSON.stringify([version, tx_b64, meta_json, tx_index, single_in_slot, terminal_error, err]))`.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let array = Value::Array(vec![
            Value::String(VERSION.into()),
            self.tx_b64.clone().map_or(Value::Null, Value::String),
            self.meta_json.clone().map_or(Value::Null, Value::String),
            self.tx_index.clone(),
            self.single_in_slot.clone(),
            self.terminal_error
                .clone()
                .map_or(Value::Null, Value::String),
            self.err.clone(),
        ]);
        crate::fsutil::sha256_hex(array.to_string().as_bytes())
    }
}

/// Rows per table, in [`TABLES`] order.
#[derive(Clone, Debug, Default)]
pub struct Rows {
    tables: [Vec<Obj>; 6],
}

impl Rows {
    /// Rows of one table.
    #[must_use]
    pub fn get(&self, table: &str) -> &[Obj] {
        &self.tables[index(table)]
    }

    /// Mutable rows of one table.
    pub fn get_mut(&mut self, table: &str) -> &mut Vec<Obj> {
        &mut self.tables[index(table)]
    }

    /// Append another transaction's rows.
    pub fn extend(&mut self, other: Rows) {
        for (mine, theirs) in self.tables.iter_mut().zip(other.tables) {
            mine.extend(theirs);
        }
    }

    /// Whether any table has rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.iter().all(Vec::is_empty)
    }
}

fn index(table: &str) -> usize {
    TABLES
        .iter()
        .position(|t| *t == table)
        .unwrap_or_else(|| panic!("unknown decoded table {table}"))
}

fn pubkey(value: Option<&Value>) -> Value {
    let Some(Value::Array(items)) = value else {
        return Value::Null;
    };
    if items.len() != 32 {
        return Value::Null;
    }
    let bytes: Option<Vec<u8>> = items.iter().map(byte).collect();
    bytes.map_or(Value::Null, |bytes| {
        Value::String(bs58::encode(bytes).into_string())
    })
}

fn byte(value: &Value) -> Option<u8> {
    match value {
        Value::Number(n) => n.as_u64().and_then(|n| u8::try_from(n).ok()),
        Value::String(s) => s.parse::<f64>().ok().map(|f| f as u8),
        _ => None,
    }
}

fn symbol(value: Option<&Value>) -> Value {
    let Some(Value::Array(items)) = value.and_then(|v| v.get("symbol_bytes")) else {
        return Value::Null;
    };
    let bytes: Vec<u8> = items.iter().filter_map(byte).collect();
    Value::String(
        String::from_utf8_lossy(&bytes)
            .trim_end_matches('\0')
            .to_owned(),
    )
}

/// `a ?? b`: the first present, non-null value.
fn coalesce<'a>(values: impl IntoIterator<Item = Option<&'a Value>>) -> Value {
    values
        .into_iter()
        .flatten()
        .find(|v| !v.is_null())
        .cloned()
        .unwrap_or(Value::Null)
}

/// Build the rows of one transaction. `Err` means an event's slot context disagrees with the
/// transaction, which the service turns into a validation quarantine.
pub fn normalize(
    tx: &RawTransaction,
    hash: &str,
    groups: &[DecodedGroup],
    source_file: &str,
) -> Result<Rows, String> {
    let mut rows = Rows::default();
    let meta: Value = tx
        .meta_json
        .as_deref()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or_else(|| Value::Object(Default::default()));
    let err_ok = tx.err.is_null() || tx.err.as_str() == Some("null");
    let committed = meta.get("err") == Some(&Value::Null) && err_ok && tx.terminal_error.is_none();
    let mut errors = 0u64;
    let slot_text = tx.slot_text();
    for group in groups {
        for (i, error) in group.errors.iter().enumerate() {
            errors += 1;
            rows.get_mut("decode_errors").push(
                Obj::new()
                    .with("error_id", format!("{}/{}/{}", tx.signature, group.path, i))
                    .with("signature", tx.signature.clone())
                    .with("source_hash", hash)
                    .with("slot", tx.slot.clone())
                    .with("instruction_path", group.path.clone())
                    .with("error", error.error.clone())
                    .with("bytes_base64", error.bytes.clone())
                    .with("decoder_version", VERSION),
            );
        }
        let mut header = Value::Object(Default::default());
        for (ordinal, event) in group.events.iter().enumerate() {
            let Some((kind, body)) = event.as_object().and_then(|o| o.iter().next()) else {
                continue;
            };
            if kind == "Header" {
                header = body.clone();
            }
            if kind == "SlotContext" && body.get("slot").map(value_text) != Some(slot_text.clone())
            {
                return Err("event slot disagrees with transaction".into());
            }
            let row = Obj::new()
                .with(
                    "event_id",
                    format!("{}/{}/{}", tx.signature, group.path, ordinal),
                )
                .with("signature", tx.signature.clone())
                .with("source_hash", hash)
                .with("slot", tx.slot.clone())
                .with("tx_index", tx.tx_index.clone())
                .with("single_in_slot", tx.single_in_slot.clone())
                .with("block_time", tx.block_time.clone())
                .with("instruction_path", group.path.clone())
                .with("event_ordinal", ordinal)
                .with("event_type", kind.clone())
                .with("committed", committed)
                .with("attribution", group.attribution.clone())
                .with(
                    "asset_symbol",
                    symbol(Some(&coalesce([
                        body.get("asset_symbol"),
                        header.get("asset_symbol"),
                    ]))),
                )
                .with(
                    "asset_id",
                    coalesce([body.get("asset_id"), header.get("asset_id")]),
                )
                .with(
                    "trader",
                    pubkey(Some(&coalesce([
                        body.get("trader"),
                        header.get("trader_account"),
                    ]))),
                )
                .with("signer", pubkey(header.get("signer")))
                .with("sequence_number", coalesce([header.get("sequence_number")]))
                .with("tick_size", coalesce([header.get("tick_size")]))
                .with(
                    "base_lot_decimals",
                    coalesce([header.get("base_lot_decimals")]),
                )
                .with(
                    "quote_lot_decimals",
                    coalesce([header.get("quote_lot_decimals")]),
                )
                .with("decoder_version", VERSION)
                .with("event_json", event.to_string());
            rows.get_mut("events").push(row.clone());
            // Attempts remain in events; analytic tables contain only executed events whose
            // instruction ownership was established from stack heights.
            if !committed || group.attribution != "stack_height" {
                continue;
            }
            if kind == "OrderFilled" || kind == "SplineFilled" {
                let side = body.get("side").cloned().unwrap_or(Value::Null);
                let taker = if side.as_str() == Some("Bid") {
                    "Ask"
                } else {
                    "Bid"
                };
                let mut fill = row.clone();
                fill.set("maker", pubkey(body.get("maker")));
                fill.set("maker_side", side);
                fill.set("taker_side", taker);
                fill.set("price_ticks", coalesce([body.get("price")]));
                fill.set("base_lots", coalesce([body.get("base_lots_filled")]));
                fill.set("quote_lots", coalesce([body.get("quote_lots_filled")]));
                fill.set(
                    "maker_fee_rate_micro",
                    coalesce([body.get("maker_fee_rate")]),
                );
                fill.set(
                    "order_sequence_number",
                    coalesce([body.get("order_sequence_number")]),
                );
                fill.set(
                    "spline_sequence_number",
                    coalesce([body.get("spline_sequence_number")]),
                );
                fill.set(
                    "quantity_remaining",
                    coalesce([body.get("quantity_remaining")]),
                );
                rows.get_mut("fills").push(fill);
            }
            if [
                "OrderPlaced",
                "OrderModified",
                "OrderRejected",
                "OrderResidualDiscarded",
            ]
            .contains(&kind.as_str())
            {
                let mut order = row.clone();
                order.set(
                    "order_sequence_number",
                    coalesce([body.get("order_sequence_number")]),
                );
                order.set("price_ticks", coalesce([body.get("price")]));
                order.set(
                    "quantity_signed",
                    coalesce([body.get("quantity"), body.get("base_lots_released")]),
                );
                let client = match body.get("client_order_id") {
                    Some(Value::Array(items)) => Value::String(hex::encode(
                        items.iter().filter_map(byte).collect::<Vec<u8>>(),
                    )),
                    Some(other) if !other.is_null() && other != &Value::Bool(false) => {
                        Value::String(String::new())
                    }
                    _ => Value::Null,
                };
                order.set("client_order_id", client);
                order.set(
                    "modification_reason",
                    body.get("reason")
                        .map_or(Value::Null, |reason| Value::String(reason.to_string())),
                );
                rows.get_mut("order_events").push(order);
            }
            if kind == "TraderFundingSettled" {
                let mut funding = row.clone();
                funding.set(
                    "funding_payment_quote_lots",
                    coalesce([body.get("funding_payment")]),
                );
                funding.set(
                    "new_collateral_quote_lots",
                    coalesce([body.get("new_collateral_balance")]),
                );
                funding.set(
                    "cumulative_funding_snapshot",
                    coalesce([body.get("cumulative_funding_snapshot")]),
                );
                rows.get_mut("funding_events").push(funding);
            }
        }
    }
    let status = if errors > 0 {
        "quarantined"
    } else if groups.iter().any(|g| g.attribution != "stack_height") {
        "unattributed"
    } else {
        "decoded"
    };
    let event_count = rows.get("events").len();
    rows.get_mut("decoded_transactions").push(
        Obj::new()
            .with("signature", tx.signature.clone())
            .with("source_hash", hash)
            .with("slot", tx.slot.clone())
            .with("tx_index", tx.tx_index.clone())
            .with("single_in_slot", tx.single_in_slot.clone())
            .with("block_time", tx.block_time.clone())
            .with("committed", committed)
            .with("status", status)
            .with("event_count", event_count)
            .with("error_count", errors)
            .with("source_file", source_file)
            .with("decoded_at", now())
            .with("decoder_version", VERSION),
    );
    Ok(rows)
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A quarantine group for a transaction whose instructions could not be extracted or whose
/// events failed validation.
#[must_use]
pub fn quarantine(path: &str, error: &str, bytes: &str) -> DecodedGroup {
    DecodedGroup {
        path: path.into(),
        attribution: "unknown".into(),
        events: Vec::new(),
        errors: vec![phoenix_codec::DecodeError {
            error: error.into(),
            bytes: bytes.into(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_matches_javascript_stringify() {
        // sha256 of '["phoenix-rise-events-0.6.12/schema-1","AA==","{}",0,false,null,"null"]'
        let tx = RawTransaction {
            signature: "s".into(),
            slot: Value::String("100".into()),
            block_time: Value::String("100".into()),
            tx_index: Value::from(0),
            single_in_slot: Value::Bool(false),
            tx_b64: Some("AA==".into()),
            meta_json: Some("{}".into()),
            terminal_error: None,
            err: Value::String("null".into()),
        };
        let expected = crate::fsutil::sha256_hex(
            br#"["phoenix-rise-events-0.6.12/schema-1","AA==","{}",0,false,null,"null"]"#,
        );
        assert_eq!(tx.content_hash(), expected);
    }

    #[test]
    fn symbols_and_pubkeys_render() {
        let symbol_value = serde_json::json!({"symbol_bytes": ["83","79","76","0","0"]});
        assert_eq!(symbol(Some(&symbol_value)), Value::String("SOL".into()));
        let key = Value::Array(vec![Value::String("0".into()); 32]);
        assert_eq!(
            pubkey(Some(&key)),
            Value::String("11111111111111111111111111111111".into())
        );
        assert_eq!(pubkey(Some(&Value::Array(vec![]))), Value::Null);
    }
}
