import test from 'node:test';
import assert from 'node:assert/strict';
import { getBase58Decoder } from '@solana/kit';
import { bulkFetchRange, wireSignature } from '../src/bulk-fetcher.ts';
import { fetchRange } from '../src/fetcher.ts';
import { makeWalk } from '../src/pipeline.ts';
import { walkPage } from '../src/walker.ts';
import { publishRange, verifyFiles } from '../src/writer.ts';
import type { Provider } from '../src/rpc.ts';
import { fixture, FixtureRpc, sig, insertTx } from './fixtures.ts';

function wire(version: 'legacy' | 0 | 1, byte = 7) {
  const signature = Buffer.alloc(64, byte);
  const legacyMessage = Buffer.concat([Buffer.from([1, 0, 0, 1]), Buffer.alloc(64), Buffer.from([0])]);
  if (version === 'legacy') return Buffer.concat([Buffer.from([1]), signature, legacyMessage]).toString('base64');
  if (version === 0) return Buffer.concat([Buffer.from([1]), signature, Buffer.from([128]), legacyMessage, Buffer.from([0])]).toString('base64');
  return Buffer.concat([Buffer.from([129, 1, 0, 0]), Buffer.alloc(4 + 32), Buffer.from([0, 1]), Buffer.alloc(32), signature]).toString('base64');
}

test('wire signature extraction handles legacy, v0 and tail-signature v1 envelopes', () => {
  const expected = getBase58Decoder().decode(Buffer.alloc(64, 7));
  for (const version of ['legacy', 0, 1] as const) assert.equal(wireSignature(wire(version)), expected);
  assert.throws(() => wireSignature('AA=='));
});

test('bulk fetch skips hydrated overlap slots while validation retains the full manifest', async () => {
  const f = await fixture();
  try {
    const old = wireSignature(wire(1));
    const recent = wireSignature(wire(1, 8));
    await walkPage(new FixtureRpc([[sig(recent, 100), sig(old, 90)]]), f.store, 'manifest',
      makeWalk(f.config.programId, 'tail', 'cycle', 0, 100));
    await insertTx(f.store, old, 90);
    let calls = 0;
    const provider = { provider: 'fixture', rawPages: new WeakMap(), async call(method: string, params: any[]) {
      calls++;
      assert.equal(method, 'getTransactionsForAddress');
      assert.deepEqual(params[1].filters.slot, { gte: 100, lte: 100 });
      return { data: [{ slot: 100, blockTime: 100, transaction: [wire(1, 8), 'base64'],
        meta: { err: null, fee: 5000 } }], paginationToken: null };
    } } as unknown as Provider;
    await fetchRange(provider, f.store, f.config, 'tail', 90, 100);
    await fetchRange(provider, f.store, f.config, 'tail', 90, 100);
    assert.equal(calls, 1);
    assert.equal((await f.store.rows('SELECT * FROM transactions')).length, 2);
    assert.equal((await f.store.rows('SELECT * FROM signatures')).length, 2);
  } finally { await f.close(); }
});

test('bulk pages checkpoint raw response and cursor with deduped transactions; restart resumes token', async () => {
  const f = await fixture();
  try {
    const signature = wireSignature(wire(1));
    await walkPage(new FixtureRpc([[sig(signature, 100)]]), f.store, 'manifest', makeWalk(f.config.programId, 'tail', 'cycle', 0, 100));
    const first = { data: [{ slot: 100, blockTime: 100, transaction: [wire(1), 'base64'], version: 1,
      meta: { err: { InstructionError: [0, 'Custom'] }, fee: 5000, computeUnitsConsumed: 10 } }], paginationToken: 'next' };
    const raw = new WeakMap<object, string>([[first, JSON.stringify(first)]]);
    let attempts = 0;
    const provider = { provider: 'fixture', rawPages: raw, async call(_method: string, params: any[]) {
      attempts++;
      assert.equal(params[1].maxSupportedTransactionVersion, 1);
      if (attempts === 1) return first;
      if (attempts === 2) throw new Error('crash after first durable page');
      assert.equal(params[1].paginationToken, 'next');
      return { data: [], paginationToken: null };
    } } as unknown as Provider;
    await assert.rejects(bulkFetchRange(provider, f.store, f.config, 'tail', 100, 100), /crash/);
    const rows = await f.store.rows('SELECT * FROM transactions');
    assert.equal(rows.length, 1);
    assert.ok(rows[0].err);
    assert.equal((await f.store.get('bulk/tail/100/100')).token, 'next');
    await bulkFetchRange(provider, f.store, f.config, 'tail', 100, 100);
    assert.equal((await f.store.rows('SELECT * FROM transactions')).length, 1);
    await publishRange(f.store, f.root, 100, 100);
    assert.equal((await verifyFiles(f.store)).files, 3);
    assert.equal((await f.store.rows('SELECT * FROM rpc_pages')).length, 2);
  } finally { await f.close(); }
});

test('parallel bulk windows drain on failure and a narrower retry fills every manifest gap', async () => {
  const f = await fixture();
  try {
    f.config.bulkFetchWindowSlots = 2;
    const tx = (slot: number) => ({ slot, blockTime: slot, transaction: [wire(1, slot), 'base64'], meta: { err: null, fee: 5000 } });
    const signature = (slot: number) => wireSignature(wire(1, slot));
    await walkPage(new FixtureRpc([[103, 102, 101, 100].map(slot => sig(signature(slot), slot))]), f.store,
      'manifest', makeWalk(f.config.programId, 'backfill', 'cycle', 0, 103));
    let fail = true;
    let otherFinished = false;
    const provider = { provider: 'fixture', rawPages: new WeakMap(), async call(method: string, params: any[]) {
      assert.equal(method, 'getTransactionsForAddress');
      const { gte, lte } = params[1].filters.slot;
      if (params[1].paginationToken) throw new Error('interrupted first window');
      if (fail && gte === 100) return { data: [tx(100)], paginationToken: 'next' };
      if (gte === 102) { await new Promise(resolve => setTimeout(resolve, 20)); otherFinished = true; }
      return { data: Array.from({ length: lte - gte + 1 }, (_, i) => tx(gte + i)), paginationToken: null };
    } } as unknown as Provider;
    await assert.rejects(fetchRange(provider, f.store, f.config, 'backfill', 100, 103), /interrupted/);
    assert.equal(otherFinished, true);
    assert.equal((await f.store.rows('SELECT * FROM transactions')).length, 3);
    assert.equal((await f.store.get('bulk/backfill/102/103')).done, true);
    fail = false;
    await fetchRange(provider, f.store, f.config, 'backfill', 100, 103);
    assert.equal((await f.store.rows('SELECT * FROM transactions')).length, 4);
    assert.equal((await f.store.rows(`SELECT * FROM signatures s LEFT JOIN transactions t USING(signature)
      WHERE t.signature IS NULL`)).length, 0);
  } finally { await f.close(); }
});
