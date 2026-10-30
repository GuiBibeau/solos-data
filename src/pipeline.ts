import { log, now } from './config.ts';
import type { Store } from './store.ts';
import type { Walk } from './walker.ts';

export interface Watermark { signature: string; slot: number; completedAt: string }

export async function recordVersions(store: Store, programData: string, from: number, to: number) {
  const added = await store.rows(`SELECT s.signature, s.slot FROM signatures s
    LEFT JOIN program_versions p USING(signature) WHERE p.signature IS NULL
    AND list_contains(s.source_addresses, ?) AND s.slot BETWEEN ? AND ?`, [programData, from, to]);
  for (const row of added) {
    await store.exec('INSERT OR IGNORE INTO program_versions VALUES (?, ?, ?, ?)',
      [row.signature, Number(row.slot), 'programdata_transaction_unclassified', now()]);
    log('program_version_attention', { signature: row.signature, slot: row.slot });
  }
}

export function makeWalk(address: string, mode: 'tail' | 'backfill', cycleId: string, floor: number, ceiling: number): Walk {
  return { address, mode, cycleId, floor, ceiling, pages: 0, done: false };
}
