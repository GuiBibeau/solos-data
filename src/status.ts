import { writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { json, now } from './config.ts';
import type { Store } from './store.ts';
import type { Provider } from './rpc.ts';
import { writeCatalog } from './catalog.ts';

export async function status(store: Store, provider?: Provider) {
  const [counts] = await store.rows(`SELECT
    (SELECT count(*) FROM signatures) AS signatures,
    (SELECT count(*) FROM transactions) AS transactions,
    (SELECT count(*) FROM files WHERE status='active') AS active_files,
    (SELECT count(*) FROM errors) AS errors,
    (SELECT min(slot) FROM signatures) AS oldest_slot,
    (SELECT max(slot) FROM signatures) AS newest_slot`);
  return { at: now(), ...counts, H0: await store.get('H0'), watermark: await store.get('W'),
    backfill: await store.get('backfill'), walk: await store.get('walk/backfill'),
    retention:await store.get('retention'),checkpointRepack:await store.get('checkpoint-repack'),maintenance:await store.get('maintenance'),checkpointCounts:counts,
    tailCycle: await store.get('tail-active'), tailHealth: await store.get('tail-health'),
    exchange: await store.get('exchange'), metrics: provider?.counters,
    storageTimings: store.timings,
    effectiveCuPerSecond: provider?.limiter.rate, maximumCuPerSecond: provider?.limiter.maximumRate,
    concurrency: provider?.limiter.windows, lastError: await store.get('last-error'),
    acceptance: 'collecting; independent validation and sealing pending' };
}

export async function writeStatus(store: Store, root: string, provider: Provider) {
  await writeCatalog(store, root);
  const body = json(await status(store, provider));
  await writeFile(join(root, 'status.json.tmp'), body + '\n');
  await rename(join(root, 'status.json.tmp'), join(root, 'status.json'));
}
