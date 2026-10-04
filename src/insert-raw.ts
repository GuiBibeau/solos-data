import type { DuckDBConnection } from '@duckdb/node-api';
import { json } from './config.ts';

/** Call under the single writer lock. PK constraints remain the final identity gate. */
export async function insertRaw(connection: DuckDBConnection, input: any[], from: number, to: number) {
  const unique = new Map<string, any>();
  for (const row of input) if (!unique.has(row.signature)) unique.set(row.signature,row);
  if (!unique.size) return;
  // DuckDB's ON CONFLICT builds a join over the entire existing table. Limit the
  // anti-join to this manifest window, then use a normal constraint-checked insert.
  await connection.run(`INSERT INTO transactions
    SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'block_time')::BIGINT,
    NULL, NULL, value->'err', (value->>'fee')::BIGINT, (value->>'compute_units_consumed')::BIGINT,
    value->>'tx_b64', value->>'meta_json', value->>'raw_rpc_json', value->>'mode',
    value->>'provider', value->>'fetched_at', NULL FROM json_each(?::JSON)
    WHERE value->>'signature' NOT IN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?)`,
  [json([...unique.values()]),from,to]);
}
