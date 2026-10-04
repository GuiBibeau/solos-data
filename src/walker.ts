import { json, now, type Lane } from './config.ts';
import type { Rpc } from './rpc.ts';
import type { Store } from './store.ts';

export interface Signature {
  signature: string; slot: number; blockTime: number | null; err: unknown;
}
export interface Walk {
  address: string; mode: Lane; cycleId: string; before?: string; until?: string;
  lastSlot?: number; floor: number; ceiling: number; done: boolean; pages: number;
  oldestSlot?: number; newest?: Signature;
}

export function checkPage(page: Signature[], walk: Walk) {
  let previous = walk.lastSlot ?? Infinity;
  const seen = new Set<string>();
  for (const row of page) {
    if (!Number.isSafeInteger(row.slot) || row.slot < 0 || !row.signature) throw new Error('Invalid signature row');
    if (row.slot > previous) throw new Error('V2: slots increased across cursor');
    if (seen.has(row.signature) || row.signature === walk.before) throw new Error('V2: repeated cursor/signature');
    previous = row.slot;
    seen.add(row.signature);
  }
}

export async function walkPage(rpc: Rpc, store: Store, key: string, walk: Walk): Promise<Walk> {
  if (walk.done) return walk;
  const options = { limit: 1000, commitment: 'finalized', before: walk.before, until: walk.until };
  const page = await rpc.call<Signature[]>('getSignaturesForAddress', [walk.address, options], walk.mode);
  if (!Array.isArray(page)) throw new Error('V2: invalid page');
  checkPage(page, walk);
  const oldest = page.at(-1);
  const next: Walk = {
    ...walk, before: oldest?.signature ?? walk.before, lastSlot: oldest?.slot ?? walk.lastSlot,
    oldestSlot: oldest?.slot ?? walk.oldestSlot, newest: walk.newest ?? page[0],
    done: page.length === 0 || (oldest !== undefined && oldest.slot < walk.floor), pages: walk.pages + 1,
  };
  const selected = page.filter(row => row.slot >= walk.floor && row.slot <= walk.ceiling);
  await store.transaction(async connection => {
    if (selected.length) {
      const from = selected.at(-1)!.slot; const to = selected[0].slot;
      await connection.run(`UPDATE signatures SET source_addresses=list_append(source_addresses, ?)
        WHERE slot BETWEEN ? AND ? AND NOT list_contains(source_addresses, ?)
        AND signature IN (SELECT value->>'signature' FROM json_each(?::JSON))`,
      [walk.address,from,to,walk.address,json(selected)]);
      await connection.run(`
      INSERT INTO signatures
      SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'blockTime')::BIGINT,
        value->'err', [?]::VARCHAR[], ?, ?, ? FROM json_each(?::JSON)
      WHERE value->>'signature' NOT IN (SELECT signature FROM signatures WHERE slot BETWEEN ? AND ?)`,
      [walk.address, walk.mode, walk.cycleId, now(), json(selected),from,to]);
    }
    await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', [key, json(next)]);
  });
  return next;
}

export async function walkToEnd(rpc: Rpc, store: Store, key: string, initial: Walk) {
  let walk = await store.get<Walk>(key) ?? initial;
  while (!walk.done) walk = await walkPage(rpc, store, key, walk);
  return walk;
}
