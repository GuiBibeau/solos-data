import test from 'node:test';
import assert from 'node:assert/strict';
import { compact } from '../src/compactor.ts';
import { orderRange } from '../src/ordering.ts';
import { publishRange } from '../src/writer.ts';
import { fixture, insertTx, FixtureRpc, sig } from './fixtures.ts';
import { walkPage } from '../src/walker.ts';
import { makeWalk } from '../src/pipeline.ts';
import { insertRaw } from '../src/insert-raw.ts';

test('overlapping raw insert jobs dedupe atomically and reject an identity in another slot', async () => {
  const f = await fixture();
  const row = { signature:'a',slot:100,block_time:100,err:null,fee:5000,compute_units_consumed:10,
    tx_b64:'AA==',meta_json:'{}',raw_rpc_json:'{}',mode:'tail',provider:'fixture',fetched_at:'fixture' };
  try {
    await Promise.all([f.store.transaction(c => insertRaw(c,[row,row],100,100)),
      f.store.transaction(c => insertRaw(c,[row],100,100))]);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'1');
    await assert.rejects(f.store.transaction(c => insertRaw(c,[{...row,slot:101}],101,101)),/key|constraint/i);
    assert.equal((await f.store.rows('SELECT slot FROM transactions'))[0].slot,'100');
  } finally { await f.close(); }
});

test('ordering repairs only transactions in the validated slot range', async () => {
  const f = await fixture();
  try {
    await walkPage(new FixtureRpc([[sig('a',100), sig('b',100)]]), f.store, 'manifest', makeWalk('p','tail','c',0,100));
    await insertTx(f.store,'a',100); await insertTx(f.store,'b',101);
    await orderRange(new FixtureRpc([], {100:['a','b']}), f.store, 'tail',100,100);
    assert.equal((await f.store.rows("SELECT tx_index FROM transactions WHERE signature='b'"))[0].tx_index, null);
  } finally { await f.close(); }
});

test('bounded compaction leaves an oversized base intact and compacts the newest prefix', async () => {
  const f = await fixture();
  try {
    await insertTx(f.store,'a',100);
    await publishRange(f.store,f.root,100,100);
    const [base] = await f.store.rows("SELECT path FROM files WHERE table_name='transactions'");
    // Catalog size is deliberately inflated to exercise the scheduling boundary.
    await f.store.exec('UPDATE files SET row_count=300000 WHERE path=?',[base.path]);
    for (let i=0;i<10;i++) {
      await f.store.exec('UPDATE transactions SET tx_index=?',[i]);
      await publishRange(f.store,f.root,100,100);
    }
    await compact(f.store,f.root);
    const files = await f.store.rows("SELECT path FROM files WHERE status='active' AND table_name='transactions'");
    assert.equal(files.length,2);
    assert.ok(files.some(file => file.path === base.path));
    const merged = files.find(file => file.path !== base.path)!;
    assert.equal((await f.store.rows('SELECT tx_index FROM read_parquet(?)',[merged.path]))[0].tx_index,9);
  } finally { await f.close(); }
});

test('compaction never promotes older files above an excluded newest revision', async () => {
  const f = await fixture();
  try {
    await insertTx(f.store,'a',100);
    for (let i=0;i<11;i++) {
      await f.store.exec('UPDATE transactions SET tx_index=?',[i]);
      await publishRange(f.store,f.root,100,100);
    }
    const [latest] = await f.store.rows(`SELECT path FROM files WHERE table_name='transactions'
      ORDER BY created_at DESC, path DESC LIMIT 1`);
    await f.store.exec('UPDATE files SET row_count=300000 WHERE path=?',[latest.path]);
    await compact(f.store,f.root);
    assert.equal((await f.store.rows("SELECT count(*) AS n FROM files WHERE status='active' AND table_name='transactions'"))[0].n,'11');
  } finally { await f.close(); }
});
