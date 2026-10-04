export const version = 'phoenix-rise-events-0.6.12/schema-1';
export const tables = ['decoded_transactions', 'events', 'fills', 'order_events', 'funding_events', 'decode_errors'] as const;
export const common = `event_id VARCHAR, signature VARCHAR, source_hash VARCHAR, slot BIGINT,
  tx_index INTEGER, single_in_slot BOOLEAN, block_time BIGINT, instruction_path VARCHAR,
  event_ordinal INTEGER, event_type VARCHAR, committed BOOLEAN, attribution VARCHAR,
  asset_symbol VARCHAR, asset_id UINTEGER, trader VARCHAR, signer VARCHAR,
  sequence_number UBIGINT, tick_size UBIGINT, base_lot_decimals SMALLINT, quote_lot_decimals SMALLINT,
  decoder_version VARCHAR, event_json VARCHAR`;
export const definitions: Record<typeof tables[number], string> = {
  decoded_transactions: `signature VARCHAR, source_hash VARCHAR, slot BIGINT, tx_index INTEGER,
    single_in_slot BOOLEAN, block_time BIGINT, committed BOOLEAN, status VARCHAR,
    event_count INTEGER, error_count INTEGER, source_file VARCHAR, decoded_at VARCHAR, decoder_version VARCHAR`,
  events: common,
  fills: `${common}, maker VARCHAR, maker_side VARCHAR, taker_side VARCHAR,
    price_ticks UBIGINT, base_lots UBIGINT, quote_lots UBIGINT, maker_fee_rate_micro BIGINT,
    order_sequence_number UBIGINT, spline_sequence_number UBIGINT, quantity_remaining UBIGINT`,
  order_events: `${common}, order_sequence_number UBIGINT, price_ticks UBIGINT,
    quantity_signed BIGINT, client_order_id VARCHAR, modification_reason VARCHAR`,
  funding_events: `${common}, funding_payment_quote_lots BIGINT, new_collateral_quote_lots BIGINT,
    cumulative_funding_snapshot BIGINT`,
  decode_errors: `error_id VARCHAR, signature VARCHAR, source_hash VARCHAR, slot BIGINT,
    instruction_path VARCHAR, error VARCHAR, bytes_base64 VARCHAR, decoder_version VARCHAR`,
};
export const schema = `
CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);
CREATE TABLE IF NOT EXISTS processed(signature VARCHAR PRIMARY KEY, source_hash VARCHAR, publication_at VARCHAR);
ALTER TABLE processed ADD COLUMN IF NOT EXISTS publication_at VARCHAR;
CREATE TABLE IF NOT EXISTS sources(source_hash VARCHAR PRIMARY KEY, path VARCHAR, row_offset BIGINT);
CREATE TABLE IF NOT EXISTS files(path VARCHAR PRIMARY KEY, table_name VARCHAR, row_count BIGINT,
  sha256 VARCHAR, batch_id BIGINT, created_at VARCHAR);
CREATE TEMP TABLE processed_batch(signature VARCHAR, source_hash VARCHAR, publication_at VARCHAR);
${tables.map(table => `CREATE TEMP TABLE ${table}(${definitions[table]});`).join('\n')}`;
