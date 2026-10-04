export const schema = `
CREATE TABLE IF NOT EXISTS kv (name VARCHAR PRIMARY KEY, value JSON);
CREATE TABLE IF NOT EXISTS signatures (
 signature VARCHAR PRIMARY KEY, slot BIGINT, block_time BIGINT, err JSON,
 source_addresses VARCHAR[], mode VARCHAR, cycle_id VARCHAR, walked_at VARCHAR
);
CREATE INDEX IF NOT EXISTS signatures_slot ON signatures(slot);
CREATE TABLE IF NOT EXISTS transactions (
 signature VARCHAR PRIMARY KEY, slot BIGINT, block_time BIGINT, tx_index INTEGER,
 single_in_slot BOOLEAN, err JSON, fee BIGINT, compute_units_consumed BIGINT,
 tx_b64 VARCHAR, meta_json VARCHAR, raw_rpc_json VARCHAR, mode VARCHAR,
 provider VARCHAR, fetched_at VARCHAR, terminal_error VARCHAR
);
CREATE INDEX IF NOT EXISTS transactions_slot ON transactions(slot);
CREATE TABLE IF NOT EXISTS slot_order (
 signature VARCHAR PRIMARY KEY, slot BIGINT, tx_index INTEGER, block_signature_count INTEGER
);
CREATE TABLE IF NOT EXISTS rpc_pages (
 page_id VARCHAR PRIMARY KEY, method VARCHAR, provider VARCHAR, raw_json VARCHAR, fetched_at VARCHAR
);
ALTER TABLE rpc_pages ADD COLUMN IF NOT EXISTS slot BIGINT;
CREATE TABLE IF NOT EXISTS cycles (
 cycle_id VARCHAR PRIMARY KEY, started_at VARCHAR, finished_at VARCHAR,
 slot_from BIGINT, slot_to BIGINT, status VARCHAR, validation_report JSON
);
CREATE TABLE IF NOT EXISTS files (
 path VARCHAR PRIMARY KEY, table_name VARCHAR, epoch BIGINT, row_count BIGINT,
 sha256 VARCHAR, created_at VARCHAR, status VARCHAR DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS published_ranges (
 slot_from BIGINT, slot_to BIGINT, PRIMARY KEY(slot_from, slot_to)
);
INSERT OR IGNORE INTO published_ranges
 SELECT DISTINCT TRY_CAST(regexp_extract(path, '/([0-9]+)-([0-9]+)-[^/]+[.]parquet$', 1) AS BIGINT),
 TRY_CAST(regexp_extract(path, '/([0-9]+)-([0-9]+)-[^/]+[.]parquet$', 2) AS BIGINT)
 FROM files WHERE table_name='transactions' AND regexp_matches(path, '/[0-9]+-[0-9]+-[^/]+[.]parquet$');
CREATE TABLE IF NOT EXISTS addresses (
 address VARCHAR PRIMARY KEY, kind VARCHAR, symbol VARCHAR, status VARCHAR,
 first_seen_slot BIGINT, last_seen_slot BIGINT
);
CREATE TABLE IF NOT EXISTS program_versions (
 signature VARCHAR PRIMARY KEY, slot BIGINT, kind VARCHAR, observed_at VARCHAR
);
CREATE TABLE IF NOT EXISTS errors (
 unit_type VARCHAR, unit_id VARCHAR, provider VARCHAR, error VARCHAR, occurred_at VARCHAR
);
CREATE TABLE IF NOT EXISTS account_snapshots (
 snapshot_id VARCHAR, page_context_slot BIGINT, pubkey VARCHAR, owner VARCHAR,
 lamports UBIGINT, data_b64 VARCHAR, data_len BIGINT,
 PRIMARY KEY(snapshot_id, pubkey)
);
CREATE OR REPLACE VIEW dataset_watermark AS
 SELECT t.* FROM transactions t
 WHERE EXISTS (SELECT 1 FROM published_ranges p WHERE t.slot BETWEEN p.slot_from AND p.slot_to);
`;
