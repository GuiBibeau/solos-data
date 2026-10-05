//! The decoded tables: version tag, table names, column definitions and the checkpoint DDL,
//! character for character the TypeScript `decode/schema.ts`.

/// Codec and schema version stamped on every row.
pub const VERSION: &str = "phoenix-rise-events-0.6.12/schema-1";

/// Published tables in definition order.
pub const TABLES: [&str; 6] = [
    "decoded_transactions",
    "events",
    "fills",
    "order_events",
    "funding_events",
    "decode_errors",
];

const COMMON: &str = "event_id VARCHAR, signature VARCHAR, source_hash VARCHAR, slot BIGINT,
  tx_index INTEGER, single_in_slot BOOLEAN, block_time BIGINT, instruction_path VARCHAR,
  event_ordinal INTEGER, event_type VARCHAR, committed BOOLEAN, attribution VARCHAR,
  asset_symbol VARCHAR, asset_id UINTEGER, trader VARCHAR, signer VARCHAR,
  sequence_number UBIGINT, tick_size UBIGINT, base_lot_decimals SMALLINT, quote_lot_decimals SMALLINT,
  decoder_version VARCHAR, event_json VARCHAR";

/// Column definitions of one table.
#[must_use]
pub fn definition(table: &str) -> String {
    match table {
        "decoded_transactions" => "signature VARCHAR, source_hash VARCHAR, slot BIGINT, tx_index INTEGER,
    single_in_slot BOOLEAN, block_time BIGINT, committed BOOLEAN, status VARCHAR,
    event_count INTEGER, error_count INTEGER, source_file VARCHAR, decoded_at VARCHAR, decoder_version VARCHAR"
            .to_owned(),
        "events" => COMMON.to_owned(),
        "fills" => format!(
            "{COMMON}, maker VARCHAR, maker_side VARCHAR, taker_side VARCHAR,
    price_ticks UBIGINT, base_lots UBIGINT, quote_lots UBIGINT, maker_fee_rate_micro BIGINT,
    order_sequence_number UBIGINT, spline_sequence_number UBIGINT, quantity_remaining UBIGINT"
        ),
        "order_events" => format!(
            "{COMMON}, order_sequence_number UBIGINT, price_ticks UBIGINT,
    quantity_signed BIGINT, client_order_id VARCHAR, modification_reason VARCHAR"
        ),
        "funding_events" => format!(
            "{COMMON}, funding_payment_quote_lots BIGINT, new_collateral_quote_lots BIGINT,
    cumulative_funding_snapshot BIGINT"
        ),
        "decode_errors" => "error_id VARCHAR, signature VARCHAR, source_hash VARCHAR, slot BIGINT,
    instruction_path VARCHAR, error VARCHAR, bytes_base64 VARCHAR, decoder_version VARCHAR"
            .to_owned(),
        other => panic!("unknown decoded table {other}"),
    }
}

/// `[{column: TYPE, ...}]` for `json_transform`, derived from the definition like the TypeScript.
#[must_use]
pub fn type_map(table: &str) -> String {
    // Column order matters: `INSERT ... SELECT unnest(json_transform(...))` maps struct fields to
    // table columns by position, so the map is written by hand in definition order.
    let fields: Vec<String> = definition(table)
        .split(',')
        .filter_map(|column| {
            let mut parts = column.split_whitespace();
            Some(format!("\"{}\":\"{}\"", parts.next()?, parts.next()?))
        })
        .collect();
    format!("[{{{}}}]", fields.join(","))
}

/// The decoder checkpoint schema.
#[must_use]
pub fn schema() -> String {
    let temp: Vec<String> = TABLES
        .iter()
        .map(|table| format!("CREATE TEMP TABLE {table}({});", definition(table)))
        .collect();
    format!(
        "
CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);
CREATE TABLE IF NOT EXISTS processed(signature VARCHAR PRIMARY KEY, source_hash VARCHAR, publication_at VARCHAR);
ALTER TABLE processed ADD COLUMN IF NOT EXISTS publication_at VARCHAR;
CREATE TABLE IF NOT EXISTS sources(source_hash VARCHAR PRIMARY KEY, path VARCHAR, row_offset BIGINT);
CREATE TABLE IF NOT EXISTS files(path VARCHAR PRIMARY KEY, table_name VARCHAR, row_count BIGINT,
  sha256 VARCHAR, batch_id BIGINT, created_at VARCHAR);
CREATE TABLE IF NOT EXISTS retired_files(path VARCHAR PRIMARY KEY,replacement_path VARCHAR,retired_at VARCHAR);
CREATE TABLE IF NOT EXISTS garbage_removed(path VARCHAR PRIMARY KEY,removed_at VARCHAR);
CREATE TEMP TABLE processed_batch(signature VARCHAR, source_hash VARCHAR, publication_at VARCHAR);
{}",
        temp.join("\n")
    )
}

/// The schema as a `'static str` for the store (leaked once per process).
#[must_use]
pub fn schema_static() -> &'static str {
    use std::sync::OnceLock;
    static SCHEMA: OnceLock<&'static str> = OnceLock::new();
    SCHEMA.get_or_init(|| Box::leak(schema().into_boxed_str()))
}

/// The dedupe key of a table in the reader and the compactor.
#[must_use]
pub fn key_column(table: &str) -> &'static str {
    match table {
        "decoded_transactions" => "signature",
        "decode_errors" => "error_id",
        _ => "event_id",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_map_lists_every_column() {
        let text = type_map("fills");
        let map: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(map[0]["price_ticks"], "UBIGINT");
        assert_eq!(map[0]["event_id"], "VARCHAR");
        assert_eq!(map[0].as_object().unwrap().len(), 32);
        assert!(
            text.starts_with("[{\"event_id\":\"VARCHAR\",\"signature\""),
            "definition order is preserved: {text}"
        );
    }
}
