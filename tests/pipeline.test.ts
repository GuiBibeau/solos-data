import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { checkPage, walkPage, walkToEnd } from '../src/walker.ts';
import { makeWalk, tailCycle } from '../src/pipeline.ts';
import { joinOrdering, orderRange } from '../src/ordering.ts';
import { validateRange, assertPublishable, chunkEnd } from '../src/validation.ts';
import { publishRange, recoverFiles, verifyFiles } from '../src/writer.ts';
import { compact } from '../src/compactor.ts';
import type { Provider } from '../src/rpc.ts';
import { fixture, FixtureRpc, insertTx, sig } from './fixtures.ts';

test('cursor and manifest commit together; restart resumes before cursor; overlap dedupes sources', async () => {
  const f = await fixture();
  try {
    const rpc = new FixtureRpc([[sig('a', 102), sig('b', 102)], [sig('c', 101)], []]);
    await walkPage(rpc, f.store, 'walk', makeWalk('program', 'tail', 'cycle', 0, 102));
    const stored = await f.store.get('walk');
    assert.equal(stored.before, 'b');
    await walkToEnd(rpc, f.store, 'walk', makeWalk('program', 'tail', 'ignored', 0, 102));
    assert.equal(rpc.calls[1].params[1].before, 'b');
    await walkPage(new FixtureRpc([[sig('a', 102)]]), f.store, 'other', makeWalk('market', 'backfill', 'overlap', 0, 102));
    const rows = await f.store.rows('SELECT signature, source_addresses FROM signatures ORDER BY signature');
    assert.equal(rows.length, 3);
    assert.deepEqual(new Set(rows[0].source_addresses), new Set(['program', 'market']));
    await assert.rejects(walkPage(new FixtureRpc([[sig('bad', 103)]]), f.store, 'walk', { ...stored, done: false }), /slots increased/);
    assert.equal((await f.store.get('walk')).before, 'c');
  } finally { await f.close(); }
});

test('same-slot boundary fully crosses floor; block indexes control ordering', async () => {
  const f = await fixture();
  try {
    const rpc = new FixtureRpc([[sig('b', 100)], [sig('a', 100), sig('old', 99)]], { 100: ['other', 'a', 'b'] });
    await walkToEnd(rpc, f.store, 'boundary', makeWalk('program', 'tail', 'boundary', 100, 100));
    assert.equal((await f.store.rows('SELECT * FROM signatures')).length, 2);
    await insertTx(f.store, 'a', 100); await insertTx(f.store, 'b', 100);
    await orderRange(rpc, f.store, 'tail', 100, 100);
    const rows = await f.store.rows('SELECT signature, tx_index FROM transactions ORDER BY tx_index');
    assert.deepEqual(rows, [{ signature: 'a', tx_index: 1 }, { signature: 'b', tx_index: 2 }]);
    assert.equal((await validateRange(f.store, 100, 100)).ok, true);
    assert.throws(() => joinOrdering(100, ['missing'], ['other']), /missing/);
  } finally { await f.close(); }
});

test('failed cycle holds watermark; retry publishes files before advancing; restart removes orphans', async () => {
  const f = await fixture();
  try {
    await f.store.set('H0', { signature: 'a', slot: 100 });
    const rpc = new FixtureRpc([[sig('b', 102), sig('a', 102)], [], []], { 102: ['a', 'b'] });
    await insertTx(f.store, 'a', 102); await insertTx(f.store, 'b', 102);
    rpc.failBlock = true;
    await assert.rejects(tailCycle(rpc as unknown as Provider, f.store, f.config, 'programdata'));
    assert.equal(await f.store.get('W'), undefined);
    rpc.failBlock = false;
    await tailCycle(rpc as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal((await f.store.get('W')).slot, 102);
    assert.ok((await verifyFiles(f.store)).files >= 3);
    const orphan = join(f.root, 'orphan.parquet');
    await writeFile(orphan, 'not registered');
    await writeFile(join(f.root, 'interrupted.tmp'), 'incomplete');
    await recoverFiles(f.store, f.root);
    await assert.rejects(readFile(orphan));
    assert.equal((await verifyFiles(f.store)).ok, true);
  } finally { await f.close(); }
});

test('missing fetch cannot be published; catch-up chunks are bounded', async () => {
  const f = await fixture();
  try {
    await walkPage(new FixtureRpc([[sig('a', 100)]]), f.store, 'one', makeWalk('program', 'tail', 'one', 0, 100));
    assert.throws(() => assertPublishable({ ok: false }), /watermark/);
    assert.equal((await validateRange(f.store, 0, 100)).ok, false);
    await insertTx(f.store, 'a', 101);
    await f.store.exec('UPDATE transactions SET single_in_slot=true');
    assert.equal((await validateRange(f.store, 0, 100)).ok, false, 'a matching signature in the wrong slot cannot satisfy coverage');
    assert.equal(chunkEnd(100, 50000, 20000), 20099);
    assert.equal(chunkEnd(49999, 50000, 20000), 50000);
    assert.throws(() => checkPage([sig('a', 100), sig('a', 100)], makeWalk('p', 'tail', 'c', 0, 100)), /repeated/);
  } finally { await f.close(); }
});

test('a lagging provider node cannot move an existing watermark backward', async () => {
  const f = await fixture();
  try {
    await f.store.set('W', { signature: 'existing', slot: 200 });
    await tailCycle(new FixtureRpc([]) as unknown as Provider, f.store, f.config, 'programdata');
    assert.equal((await f.store.get('W')).slot, 200);
    assert.equal(await f.store.get('tail-active'), undefined);
  } finally { await f.close(); }
});

test('compaction dedupes publications and picks corrected ordering; hash corruption fails V7', async () => {
  const f = await fixture();
  try {
    await walkPage(new FixtureRpc([[sig('a', 100)]]), f.store, 'one', makeWalk('program', 'tail', 'one', 0, 100));
    await insertTx(f.store, 'a', 100);
    for (let index = 0; index < 10; index++) {
      await f.store.exec('UPDATE transactions SET tx_index=?, single_in_slot=false', [index]);
      await publishRange(f.store, f.root, 100, 100);
    }
    await compact(f.store, f.root);
    const files = await f.store.rows("SELECT * FROM files WHERE status='active' AND table_name='transactions'");
    assert.equal(files.length, 1);
    const rows = await f.store.rows('SELECT signature, tx_index FROM read_parquet(?)', [files[0].path]);
    assert.deepEqual(rows, [{ signature: 'a', tx_index: 9 }]);
    await writeFile(files[0].path, 'corrupt');
    await assert.rejects(verifyFiles(f.store), /hash mismatch/);
  } finally { await f.close(); }
});
