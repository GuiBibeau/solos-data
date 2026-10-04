import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../src/store.ts';
import { schema } from '../src/decode/schema.ts';
import { sourceRows, nextSource } from '../src/decode/source.ts';
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

test('live transaction ranges take priority over newly published history and legacy compacted files', async () => {
  const root = await mkdtemp(join(tmpdir(),'decode-priority-'));
  const store = await Store.open(root,schema);
  try {
    const legacy = join(root,'compact.parquet');
    await store.exec(`COPY (SELECT 200 AS slot) TO ${sqlString(legacy)} (FORMAT PARQUET)`);
    const file = (path:string,hash:string,at:string) => ({path,sha256:hash,created_at:at,table_name:'transactions',row_count:1});
    await writeFile(join(root,'catalog.json'),JSON.stringify({at:'fixture',files:[
      file('staging/100-150-history.parquet','history','2026-10-04T04:00:00Z'),
      file('staging/300-350-live.parquet','live','2026-10-04T02:00:00Z'),
      file(legacy,'legacy','2026-10-04T05:00:00Z'),
    ]}));
    assert.equal((await nextSource(store,root))!.file.sha256,'live');
    await store.exec('INSERT INTO sources VALUES (?, ?, ?)', ['live','live',1]);
    assert.equal((await nextSource(store,root))!.file.sha256,'legacy');
    await store.exec('INSERT INTO sources VALUES (?, ?, ?)', ['legacy',legacy,1]);
    assert.equal((await nextSource(store,root))!.file.sha256,'history');
  } finally { await store.close(); await rm(root,{recursive:true,force:true}); }
});
