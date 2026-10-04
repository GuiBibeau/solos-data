import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadConfig } from '../src/config.ts';
import { Store } from '../src/store.ts';
import type { Rpc } from '../src/rpc.ts';

export const sig = (signature: string, slot: number) => ({ signature, slot, blockTime: slot, err: null });
export class FixtureRpc implements Rpc {
  calls: { method: string; params: any[] }[] = [];
  pages: ReturnType<typeof sig>[][];
  blocks: Record<number, string[]>;
  failBlock = false;
  constructor(pages: ReturnType<typeof sig>[][], blocks: Record<number, string[]> = {}) {
    this.pages = pages; this.blocks = blocks;
  }
  async call<T = any>(method: string, params: any[]): Promise<T> {
    this.calls.push({ method, params });
    if (method === 'getSlot') return 102 as T;
    if (method === 'getSignaturesForAddress') return (this.pages.shift() ?? []) as T;
    if (method === 'getBlock') {
      if (this.failBlock) throw new Error('fixture block failure');
      return { signatures: this.blocks[params[0]] } as T;
    }
    throw new Error(`Unexpected fixture method ${method}`);
  }
}
export async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'solos-data-'));
  const store = await Store.open(root);
  const config = await loadConfig();
  config.dataDir = root;
  return { root, store, config, async close() { await store.close(); await rm(root, { recursive: true, force: true }); } };
}
export async function insertTx(store: Store, signature: string, slot: number) {
  await store.exec(`INSERT INTO transactions VALUES (?, ?, ?, NULL, NULL, 'null', 5000, 10,
    'AA==', '{}', '{}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)`, [signature, slot, slot]);
}
