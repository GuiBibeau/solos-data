import test from 'node:test';
import assert from 'node:assert/strict';
import { orderRange } from '../src/ordering.ts';
import { makeWalk } from '../src/pipeline.ts';
import { walkPage } from '../src/walker.ts';
import { validateRange } from '../src/validation.ts';
import { fixture, FixtureRpc, insertTx, sig } from './fixtures.ts';

test('batched ordering preserves finalized indexes, bounded restart and cached repairs', async () => {
  const f = await fixture();
  try {
    const signatures = [];
    const blocks: Record<number, string[]> = {};
    for (let slot = 165; slot >= 100; slot--) {
      signatures.push(sig(`a-${slot}`, slot));
      await insertTx(f.store, `a-${slot}`, slot);
      if (slot !== 101) {
        signatures.push(sig(`b-${slot}`, slot));
        await insertTx(f.store, `b-${slot}`, slot);
        blocks[slot] = ['unrelated', `b-${slot}`, `a-${slot}`];
      }
    }
    await walkPage(new FixtureRpc([signatures]), f.store, 'manifest', makeWalk('program', 'backfill', 'batch', 0, 165));
    const failing = new FixtureRpc([], { ...blocks, 164: [] });
    await assert.rejects(orderRange(failing, f.store, 'backfill', 100, 165), /missing/);
    assert.equal((await validateRange(f.store, 100, 163)).ok, true);
    assert.equal((await validateRange(f.store, 164, 165)).ok, false);
    // Cached block data must repair transactions whose ordering write was not retained.
    await f.store.exec('UPDATE transactions SET tx_index=NULL WHERE slot=100');
    const retry = new FixtureRpc([], blocks);
    await orderRange(retry, f.store, 'backfill', 100, 165);
    assert.deepEqual(retry.calls.map(call => call.params[0]).sort(), [164, 165]);
    assert.equal((await validateRange(f.store, 100, 165)).ok, true);
    assert.deepEqual(await f.store.rows('SELECT signature, tx_index FROM transactions WHERE slot=100 ORDER BY tx_index'),
      [{ signature: 'b-100', tx_index: 1 }, { signature: 'a-100', tx_index: 2 }]);
    assert.deepEqual(await f.store.rows('SELECT tx_index, single_in_slot FROM transactions WHERE slot=101'),
      [{ tx_index: null, single_in_slot: true }]);
  } finally { await f.close(); }
});
