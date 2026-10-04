import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../src/store.ts';
import { schema } from '../src/decode/schema.ts';
import { sourceRows } from '../src/decode/source.ts';
import { sqlString, fileHash } from '../src/writer.ts';

test('deep decoder resume uses bounded payload memory and preserves descending cursor order', async () => {
  const root = await mkdtemp(join(tmpdir(), 'decode-scale-'));
  let store = await Store.open(root, schema);
  try {
    const path = join(root, 'transactions.parquet');
    // Deliberately unsorted physical rows; old OFFSET cursors refer to logical order.
    await store.exec(`COPY (SELECT md5(i::VARCHAR)||md5(i::VARCHAR) AS signature, (i*499979)%1000000 AS slot,
      i AS block_time, 1 AS tx_index, false AS single_in_slot, 'null' AS err, 5000 AS fee,
      100 AS compute_units_consumed, repeat(md5(i::VARCHAR), 30) AS tx_b64,
      repeat(md5(i::VARCHAR), 200) AS meta_json, '{}' AS raw_rpc_json, 'tail' AS mode,
      'fixture' AS provider, 'fixture' AS fetched_at, NULL::VARCHAR AS terminal_error
      FROM range(1000000) r(i))
      TO ${sqlString(path)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048)`);
    const file = { path, table_name:'transactions', row_count:1000000,
      sha256:await fileHash(path), created_at:'fixture', epoch:0 };
    const expected = await store.rows(`SELECT signature, slot FROM read_parquet(${sqlString(path)})
      ORDER BY slot DESC, signature DESC LIMIT 25 OFFSET 800000`);
    await store.close(); store = await Store.open(root,schema);
    await store.exec("SET memory_limit='384MB'");
    await store.exec('SET threads=1');
    const rows = await sourceRows(store, root, file as any, 800000, 25, new Set());
    assert.deepEqual(rows.map(row => ({ signature:row.signature, slot:row.slot })), expected);
    assert.ok(rows.every(row => row.meta_json!.length === 6400 && row.previous_hash === null));
    assert.equal(new Set(rows.map(row => row.signature)).size, 25);
  } finally { await store.close(); await rm(root, { recursive:true, force:true }); }
});
