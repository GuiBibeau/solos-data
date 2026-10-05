import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm, mkdir, writeFile, rename } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { Codec } from '../src/decode/codec.ts';
import { extractGroups, advancePath, groupInstructions, program } from '../src/decode/instructions.ts';
import { normalize } from '../src/decode/normalize.ts';
import { Store } from '../src/store.ts';
import { schema } from '../src/decode/schema.ts';
import { publish, recover } from '../src/decode/publish.ts';
import { queryDecoded } from '../src/decode/reader.ts';
import { runDecoder } from '../src/decode/service.ts';
import { fileHash } from '../src/writer.ts';
import { rawFixture } from './decode-fixtures.ts';

const fixtures = JSON.parse(await readFile(new URL('data/phoenix-events.json', import.meta.url), 'utf8')).transactions;
const binary = process.env.SOLOS_DATA_CODEC ?? resolve('target/release/solos-data-phoenix-codec');

test('official Phoenix golden events, instruction attribution, and failed transaction isolation', async () => {
  const codec = new Codec(binary);
  try {
    for (const fixture of fixtures) {
      const tx = rawFixture(fixture);
      const groups = await codec.decode(extractGroups(tx.tx_b64, JSON.parse(tx.meta_json)));
      assert.equal(groups.reduce((sum,g) => sum + g.events.length, 0), fixture.eventsCount);
      assert.ok(groups.every(g => !g.errors.length && g.attribution === 'stack_height'));
      const rows = normalize(tx, 'h', groups, 'source');
      assert.equal(rows.events.length, fixture.eventsCount);
      const failed = normalize({ ...tx, err: '{}', meta_json: JSON.stringify({ err: {} }) }, 'h', groups, 'source');
      assert.equal(failed.fills.length, 0); assert.equal(failed.order_events.length, 0); assert.equal(failed.funding_events.length, 0);
      assert.ok(failed.events.every(event => event.committed === false));
    }
    const group = extractGroups(rawFixture(fixtures[0]).tx_b64, JSON.parse(rawFixture(fixtures[0]).meta_json))[0];
    group.logs[1] = Buffer.from('8de6d6f209d1cfaa0000000001000000ff', 'hex').toString('base64');
    const [bad] = await codec.decode([group]);
    assert.equal(bad.events.length, 0); assert.ok(bad.errors.length);
  } finally { await codec.close(); }
});

test('nested CPI parents and missing stack heights are explicit', () => {
  assert.deepEqual(advancePath([], 2), [0]);
  assert.deepEqual(advancePath([0], 3), [0,0]);
  assert.deepEqual(advancePath([0,0], 2), [1]);
  const log = Buffer.from('8de6d6f209d1cfaa0000000000000000', 'hex');
  const [group] = groupInstructions([
    { programId: program, path: [3,0], data: Buffer.alloc(8), attribution: 'stack_height' },
    { programId: program, path: [3,0,0], data: log, attribution: 'stack_height' },
  ]);
  assert.equal(group.path, '3.0'); assert.equal(group.logs.length, 1);
  const [orphan] = groupInstructions([{ programId: program, path: [3,0], data: log, attribution: 'unknown' }]);
  assert.equal(orphan.attribution, 'unknown');
});

test('atomic publication survives restart, relocation and corrected revisions without duplicate fills', async () => {
  const root = await mkdtemp(join(tmpdir(), 'phoenix-decode-'));
  let store = await Store.open(root, schema);
  const codec = new Codec(binary);
  try {
    const tx = rawFixture(fixtures[0]);
    const groups = await codec.decode(extractGroups(tx.tx_b64, JSON.parse(tx.meta_json)));
    const rows = normalize(tx, 'hash1', groups, 'source');
    assert.ok(rows.fills.length > 0);
    rows.fills[0].price_ticks = '18446744073709551615';
    await publish(store, rows, { hash:'source', path:'raw', offset:1 });
    const result = await queryDecoded(root, 'SELECT count(*) AS n, max(price_ticks)::VARCHAR AS price FROM fills');
    assert.equal(Number(result.rows[0].n), rows.fills.length); assert.equal(result.rows[0].price, '18446744073709551615');
    await writeFile(join(root, 'orphan.parquet'), 'incomplete');
    await store.close(); store = await Store.open(root, schema); await recover(store);
    assert.equal((await store.rows('SELECT row_offset FROM sources'))[0].row_offset, '1');
    await assert.rejects(readFile(join(root, 'orphan.parquet')));
    const correction = normalize({ ...tx, err:'{}', meta_json:JSON.stringify({ err:{} }) }, 'hash2', groups, 'source2');
    await publish(store, correction, { hash:'source2', path:'raw2', offset:1 });
    assert.equal(Number((await queryDecoded(root, 'SELECT count(*) AS n FROM fills')).rows[0].n), 0);
    await store.close();
    const moved = root + '-moved'; await rename(root, moved);
    assert.equal(Number((await queryDecoded(moved, 'SELECT count(*) AS n FROM decoded_transactions')).rows[0].n), 1);
    await rm(moved, { recursive:true, force:true });
    store = undefined as any;
  } finally { await codec.close(); if (store) await store.close(); await rm(root, { recursive:true, force:true }); }
});

test('decoder consumes raw publications once across restarts and rebases the raw catalog', async () => {
  const root = await mkdtemp(join(tmpdir(), 'phoenix-consumer-'));
  const rawDir = join(root, 'raw'); const dataDir = join(root, 'decoded');
  const fixture = rawFixture(fixtures[0]);
  await mkdir(join(rawDir,'staging'), { recursive:true });
  const store = await Store.open(join(root, 'fixture'));
  try {
    const path = join(rawDir, 'staging', 'tx.parquet');
    const data = JSON.stringify([fixture]);
    await store.exec(`COPY (SELECT value->>'signature' AS signature, (value->>'slot')::BIGINT AS slot,
      (value->>'block_time')::BIGINT AS block_time, 0 AS tx_index, false AS single_in_slot,
      value->>'tx_b64' AS tx_b64, value->>'meta_json' AS meta_json, NULL::VARCHAR AS terminal_error,
      'null' AS err FROM json_each(?::JSON)) TO '${path}' (FORMAT PARQUET)`, [data]);
    await writeFile(join(rawDir, 'catalog.json'), JSON.stringify({ at:'fixture', files:[{ path:'/old/root/staging/tx.parquet',
      table_name:'transactions', row_count:1, sha256:await fileHash(path), created_at:'2026-10-04T03:00:00Z' }] }));
    const config = { rawDir, dataDir, codecPath:binary, batchSize:100, pollMs:100 };
    await runDecoder(config, true); await runDecoder(config, true);
    assert.equal(Number((await queryDecoded(dataDir, 'SELECT count(*) AS n FROM decoded_transactions')).rows[0].n), 1);
    assert.equal(Number((await queryDecoded(dataDir, 'SELECT count(*) AS n FROM events')).rows[0].n), fixtures[0].eventsCount);
    // Newest-first traversal must never undo a newer correction with old files.
    const older = join(rawDir, 'staging', 'older.parquet');
    await store.exec(`COPY (SELECT * REPLACE(7 AS tx_index) FROM read_parquet('${path}')) TO '${older}' (FORMAT PARQUET)`);
    await writeFile(join(rawDir, 'catalog.json'), JSON.stringify({ at:'fixture2', files:[{ path:older,
      table_name:'transactions', row_count:1, sha256:await fileHash(older), created_at:'2026-10-04T02:00:00Z' }] }));
    await runDecoder(config, true);
    assert.equal((await queryDecoded(dataDir, 'SELECT tx_index FROM decoded_transactions')).rows[0].tx_index, 0);
  } finally { await store.close(); await rm(root, { recursive:true, force:true }); }
});
