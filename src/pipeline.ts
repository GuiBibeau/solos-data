import { randomUUID } from 'node:crypto';
import { json, log, now, type Config } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';
import { fetchRange } from './fetcher.ts';
import { orderRange } from './ordering.ts';
import { assertPublishable, chunkEnd, validateRange } from './validation.ts';
import { publishRange } from './writer.ts';
import { walkToEnd, type Walk } from './walker.ts';

export interface Watermark { signature: string; slot: number; completedAt: string }
export interface TailCycle {
  id: string; floor: number; ceiling: number; next: number; programData: string;
}

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

export async function tailCycle(provider: Provider, store: Store, config: Config, programData: string) {
  let active = await store.get<TailCycle>('tail-active');
  if (!active) {
    const previous = await store.get<Watermark>('W');
    const W = previous ?? await store.get<Watermark>('H0');
    if (!W) throw new Error('H0 is missing');
    const head = await provider.call<number>('getSlot', [{ commitment: 'finalized' }]);
    if (previous && head < previous.slot) {
      log('provider_head_behind_watermark', { head, watermark: previous.slot }); return;
    }
    active = { id: randomUUID(), floor: Math.max(0, W.slot - config.overlapSlots), ceiling: head,
      next: Math.max(0, W.slot - config.overlapSlots), programData };
    await store.transaction(async connection => {
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', ['tail-active', json(active)]);
      await connection.run('INSERT INTO cycles VALUES (?, ?, NULL, ?, ?, ?, NULL)',
        [active!.id, now(), active!.floor, head, 'walking']);
    });
  }
  for (const addr of [config.programId, active.programData]) {
    const walk = makeWalk(addr, 'tail', active.id, active.floor, active.ceiling);
    const [anchor] = await store.rows(`SELECT signature FROM signatures WHERE slot<?
      AND list_contains(source_addresses, ?) ORDER BY slot DESC, signature LIMIT 1`, [active.floor, addr]);
    if (anchor) walk.until = anchor.signature;
    await walkToEnd(provider, store, `walk/tail/${active.id}/${addr}`, walk);
  }
  await recordVersions(store, active.programData, active.floor, active.ceiling);
  while (active.next <= active.ceiling) {
    const to = chunkEnd(active.next, active.ceiling, config.maxSlotsPerChunk);
    await fetchRange(provider, store, config, 'tail', active.next, to);
    await orderRange(provider, store, 'tail', active.next, to);
    const report = await validateRange(store, active.next, to);
    assertPublishable(report);
    const [latest] = await store.rows(`SELECT signature FROM signatures WHERE slot<=?
      AND list_contains(source_addresses, ?) ORDER BY slot DESC, signature LIMIT 1`, [to, config.programId]);
    const W: Watermark = { signature: latest?.signature ?? '', slot: to, completedAt: now() };
    await publishRange(store, config.dataDir, active.next, to, { key: 'W', value: W });
    active.next = to + 1;
    // A crash before this checkpoint safely republishes an overlap; W is already durable.
    await store.set('tail-active', active);
    await store.exec('UPDATE cycles SET validation_report=?::JSON WHERE cycle_id=?', [json(report), active.id]);
    log('tail_chunk', { ...report, watermark: W.slot });
  }
  await store.transaction(async connection => {
    await connection.run("UPDATE cycles SET finished_at=?, status='ok' WHERE cycle_id=?", [now(), active!.id]);
    await connection.run('DELETE FROM kv WHERE name=? OR name LIKE ?', ['tail-active', `walk/tail/${active!.id}/%`]);
  });
}
