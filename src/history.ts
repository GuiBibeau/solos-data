import { log, now, type Config } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';
import { makeWalk, recordVersions, type Watermark } from './pipeline.ts';
import { walkPage, type Walk } from './walker.ts';
import { fetchRange } from './fetcher.ts';
import { orderRange } from './ordering.ts';
import { assertPublishable, validateRange } from './validation.ts';
import { publishRange } from './writer.ts';

export interface BackfillProgress {
  direction: 'newest-first'; phase: 'fetching' | 'complete'; next: number; ceiling: number;
  completedChunks: number; publishedTransactions: number;
  oldestPublishedSlot?: number; newestPublishedSlot?: number; completedAt?: string;
  timingSeconds?: Record<string, number>;
}

async function progress(store: Store) {
  const previous = await store.get<BackfillProgress>('backfill');
  if (previous?.direction === 'newest-first') return previous;
  const H0 = await store.get<Watermark>('H0');
  if (!H0) throw new Error('H0 is missing');
  // Preserve old checkpoints and all collected rows; changing direction is additive.
  if (previous) await store.set('backfill/oldest-first', previous);
  const state: BackfillProgress = { direction: 'newest-first', phase: 'fetching', next: H0.slot,
    ceiling: H0.slot, completedChunks: 0, publishedTransactions: 0 };
  await store.set('backfill', state);
  return state;
}

async function manifestThrough(provider: Provider, store: Store, key: string, initial: Walk, floor: number) {
  let walk = await store.get<Walk>(key) ?? initial;
  // Cross the slot boundary strictly: a page can split a slot's signature set.
  while (!walk.done && (walk.lastSlot === undefined || walk.lastSlot >= floor)) {
    walk = await walkPage(provider, store, key, walk);
  }
  return walk;
}

/** Publish one recent-to-old batch; publication atomically checkpoints the next lower slot. */
export async function backfillStep(provider: Provider, store: Store, config: Config, programData: string) {
  const state = await progress(store);
  if (state.phase === 'complete') return state;
  const started = performance.now();
  const to = state.next;
  let from = Math.max(0, to - config.backfillChunkSlots + 1);
  const walks: Walk[] = [];
  for (const [key, address] of [['walk/backfill', config.programId], ['walk/programdata', programData]]) {
    walks.push(await manifestThrough(provider, store, key,
      makeWalk(address, 'backfill', key, 0, state.ceiling), from));
  }
  const ended = walks.every(walk => walk.done);
  const oldest = Math.min(...walks.map(walk => walk.oldestSlot ?? Infinity));
  if (ended && walks[0].oldestSlot === undefined) throw new Error('History walk returned no program transactions');
  if (ended) from = Math.max(from, oldest);
  if (ended) await store.set('S_start', walks[0].oldestSlot);
  if (from > to) {
    const complete: BackfillProgress = { ...state, phase: 'complete' };
    await store.set('backfill', complete);
    return complete;
  }
  await recordVersions(store, programData, from, to);
  const fetchStarted = performance.now();
  await fetchRange(provider, store, config, 'backfill', from, to);
  const orderStarted = performance.now();
  await orderRange(provider, store, 'backfill', from, to);
  const validationStarted = performance.now();
  const report = await validateRange(store, from, to);
  assertPublishable(report);
  const next: BackfillProgress = { ...state, next: from - 1,
    phase: ended && from <= oldest ? 'complete' : 'fetching',
    oldestPublishedSlot: from, newestPublishedSlot: state.ceiling, completedAt: now(),
    completedChunks: state.completedChunks + 1,
    publishedTransactions: state.publishedTransactions + report.fetched,
    timingSeconds: { manifest: (fetchStarted - started) / 1000, fetch: (orderStarted - fetchStarted) / 1000,
      ordering: (validationStarted - orderStarted) / 1000, validation: (performance.now() - validationStarted) / 1000 } };
  await publishRange(store, config.dataDir, from, to, { key: 'backfill', value: next });
  log('backfill_chunk', { ...report, direction: next.direction, oldestPublishedSlot: from,
    timingSeconds: next.timingSeconds, durationSeconds: (performance.now() - started) / 1000 });
  return next;
}

export async function backfill(provider: Provider, store: Store, config: Config, programData: string) {
  while ((await backfillStep(provider, store, config, programData)).phase !== 'complete') {}
  log('backfill_collection_complete', { direction: 'newest-first', independentValidation: 'pending' });
}
