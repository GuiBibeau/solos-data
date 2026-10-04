import { json } from './config.ts';
import { parallel } from './fetcher.ts';
import type { Rpc } from './rpc.ts';
import type { Store } from './store.ts';
import type { Lane } from './config.ts';

export function joinOrdering(slot: number, signatures: string[], block: string[]) {
  const indexes = new Map(block.map((signature, index) => [signature, index]));
  return signatures.map(signature => {
    const index = indexes.get(signature);
    if (index === undefined) throw new Error(`V6: signature missing from finalized block ${slot}`);
    return { signature, slot, tx_index: index, block_signature_count: block.length };
  });
}

export async function orderRange(rpc: Rpc, store: Store, lane: Lane, from: number, to: number) {
  const slots = await store.rows(`SELECT slot, list(signature ORDER BY signature) AS signatures
    FROM signatures WHERE slot BETWEEN ? AND ? GROUP BY slot ORDER BY slot`, [from, to]);
  const cache = new Map<number, any[]>();
  for (const item of await store.rows('SELECT * FROM slot_order WHERE slot BETWEEN ? AND ?', [from, to])) {
    const slot = Number(item.slot);
    const entries = cache.get(slot) ?? [];
    entries.push(item); cache.set(slot, entries);
  }
  // Bound memory and replay on failure; avoid one read/commit/fsync for every slot.
  for (let offset = 0; offset < slots.length; offset += 64) {
    const batch = slots.slice(offset, offset + 64);
    const ordered: ReturnType<typeof joinOrdering> = [];
    const singles: number[] = [];
    await parallel(batch, 32, async row => {
      const slot = Number(row.slot);
      const signatures = row.signatures as string[];
      if (signatures.length === 1) {
        singles.push(slot);
        return;
      }
      const cached = cache.get(slot) ?? [];
      if (cached.length === signatures.length && signatures.every(signature => cached.some(item => item.signature === signature))) {
        ordered.push(...cached.map(item => ({ signature: item.signature, slot, tx_index: Number(item.tx_index),
          block_signature_count: Number(item.block_signature_count) })));
      } else {
        const result = await rpc.call('getBlock', [slot, {
          transactionDetails: 'signatures', rewards: false, maxSupportedTransactionVersion: 1, commitment: 'finalized',
        }], lane);
        if (!Array.isArray(result?.signatures)) throw new Error(`V6: finalized block ${slot} unavailable`);
        ordered.push(...joinOrdering(slot, signatures, result.signatures));
      }
    });
    await store.transaction(async connection => {
      await connection.run('DELETE FROM slot_order WHERE slot BETWEEN ? AND ?',
        [Number(batch[0].slot),Number(batch.at(-1)!.slot)]);
      if (ordered.length) await connection.run(`INSERT INTO slot_order
        SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'tx_index')::INTEGER,
        (value->>'block_signature_count')::INTEGER FROM json_each(?::JSON)`, [json(ordered)]);
      await connection.run(`UPDATE transactions SET tx_index=o.tx_index, single_in_slot=false
        FROM slot_order o WHERE transactions.signature=o.signature AND o.slot BETWEEN ? AND ?
        AND transactions.slot BETWEEN ? AND ?
        AND (transactions.tx_index IS DISTINCT FROM o.tx_index OR transactions.single_in_slot IS DISTINCT FROM false)`,
      [Number(batch[0].slot), Number(batch.at(-1)!.slot), Number(batch[0].slot), Number(batch.at(-1)!.slot)]);
      if (singles.length) await connection.run(`UPDATE transactions SET tx_index=NULL, single_in_slot=true
        WHERE slot IN (SELECT value::BIGINT FROM json_each(?::JSON))`, [json(singles)]);
    });
  }
}
