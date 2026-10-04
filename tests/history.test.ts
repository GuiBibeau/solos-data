import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { readFile, rm } from 'node:fs/promises';
import { backfillStep } from '../src/history.ts';
import { makeWalk } from '../src/pipeline.ts';
import { walkPage } from '../src/walker.ts';
import { verifyFiles, recoverFiles, publishRange } from '../src/writer.ts';
import { writeCatalog, mergeCoverage } from '../src/catalog.ts';
import { queryDataset } from '../src/reader.ts';
import { Store } from '../src/store.ts';
import type { Provider } from '../src/rpc.ts';
import { fixture, FixtureRpc, insertTx, sig } from './fixtures.ts';

test('recent batches reuse a partial manifest and publish before EOF; failed/restarted batches hold coverage', async () => {
  const f = await fixture();
  let reopened: Store | undefined;
  try {
    f.config.backfillChunkSlots = 2;
    await f.store.set('H0', { signature: 'a', slot: 100 });
    await f.store.set('W', { signature: 'live', slot: 200 });
    await f.store.set('backfill', { phase: 'fetching', next: 1, ceiling: 100 });
    const rows = [sig('b', 100), sig('a', 100), sig('c', 99), sig('e', 98), sig('d', 98), sig('f', 97), sig('old', 90)];
    await walkPage(new FixtureRpc([rows]), f.store, 'walk/backfill', makeWalk(f.config.programId, 'backfill', 'old', 0, 100));
    await f.store.set('walk/programdata', { ...makeWalk('programdata', 'backfill', 'data', 0, 100), done: true });
    for (const row of rows) await insertTx(f.store, row.signature, row.slot);
    const rpc = new FixtureRpc([], { 100: ['other', 'a', 'b'], 98: ['d', 'e'] });
    const first = await backfillStep(rpc as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal(first.oldestPublishedSlot, 99);
    assert.equal(first.next, 98);
    assert.equal(first.direction, 'newest-first');
    assert.equal(first.publishedTransactions, 3);
    assert.equal((await f.store.get('walk/backfill')).done, false);
    assert.equal(rpc.calls.filter(call => call.method === 'getSignaturesForAddress').length, 0);
    assert.equal((await f.store.get('backfill/oldest-first')).next, 1);
    assert.equal((await f.store.get('W')).slot, 200);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM dataset_watermark'))[0].n, '3');
    await writeCatalog(f.store, f.root);
    const query = await queryDataset(f.root, 'SELECT count(*) AS n, min(slot) AS oldest FROM transactions');
    assert.deepEqual(query.rows, [{ n: '3', oldest: '99' }]);
    assert.deepEqual(query.coverage, [{ from: 99, to: 100 }]);
    rpc.failBlock = true;
    await assert.rejects(backfillStep(rpc as unknown as Provider, f.store, f.config, 'programdata'), /block failure/);
    assert.equal((await f.store.get('backfill')).next, 98);
    // An old deployment has file registrations but no published-range registry.
    await f.store.exec('DELETE FROM published_ranges');
    await f.store.close();
    reopened = await Store.open(f.root);
    await recoverFiles(reopened, f.root);
    assert.equal((await reopened.rows('SELECT count(*) AS n FROM dataset_watermark'))[0].n, '3');
    rpc.failBlock = false;
    const second = await backfillStep(rpc as unknown as Provider, reopened, f.config, 'programdata');
    assert.equal(second.oldestPublishedSlot, 97);
    assert.equal(second.next, 96);
    const catalog = await writeCatalog(reopened, f.root);
    assert.deepEqual(catalog.coverage, [{ from: 97, to: 100 }]);
    assert.deepEqual((await queryDataset(f.root, 'SELECT count(*) AS n FROM transactions')).rows, [{ n: '6' }]);
    assert.equal((await verifyFiles(reopened)).ok, true);
    assert.equal((await reopened.get('W')).slot, 200);
  } finally {
    if (reopened) { await reopened.close(); await rm(f.root, { recursive: true, force: true }); }
    else await f.close();
  }
});

test('reverse batches strictly cross split-slot pages and stop at the launch boundary', async () => {
  const f = await fixture();
  try {
    f.config.backfillChunkSlots = 2;
    await f.store.set('H0', { signature: 'a', slot: 100 });
    for (const [signature, slot] of [['a', 100], ['b', 99], ['c', 99], ['old', 98]] as const) await insertTx(f.store, signature, slot);
    const rpc = new FixtureRpc([[sig('a', 100), sig('b', 99)], [sig('c', 99), sig('old', 98)], [], []], { 99: ['b', 'c'] });
    const first = await backfillStep(rpc as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal(first.oldestPublishedSlot, 99);
    assert.equal(first.phase, 'fetching');
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM slot_order WHERE slot=99'))[0].n, '2');
    const second = await backfillStep(rpc as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal(second.phase, 'complete');
    assert.equal(second.oldestPublishedSlot, 98);
    assert.equal(await f.store.get('S_start'), 98);
    const requests = rpc.calls.length;
    await backfillStep(rpc as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal(rpc.calls.length, requests);
  } finally { await f.close(); }
});

test('published reader dedupes overlap revisions and leaves disjoint coverage explicit', async () => {
  const f = await fixture();
  try {
    await walkPage(new FixtureRpc([[sig('a', 100)]]), f.store, 'one', makeWalk('program', 'tail', 'one', 0, 100));
    await insertTx(f.store, 'a', 100);
    await publishRange(f.store, f.root, 100, 100);
    await f.store.exec('UPDATE transactions SET tx_index=7, single_in_slot=false');
    await publishRange(f.store, f.root, 100, 100);
    await publishRange(f.store, f.root, 200, 200);
    await writeCatalog(f.store, f.root);
    assert.deepEqual((await queryDataset(f.root, 'SELECT signature, tx_index FROM transactions')).rows, [{ signature: 'a', tx_index: 7 }]);
    assert.deepEqual(JSON.parse(await readFile(join(f.root, 'catalog.json'), 'utf8')).coverage,
      [{ from: 100, to: 100 }, { from: 200, to: 200 }]);
    assert.deepEqual(mergeCoverage([{ from: 3, to: 4 }, { from: 1, to: 3 }, { from: 10, to: 12 }]),
      [{ from: 1, to: 4 }, { from: 10, to: 12 }]);
  } finally { await f.close(); }
});
