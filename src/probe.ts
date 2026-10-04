import { join } from 'node:path';
import { writeFile } from 'node:fs/promises';
import { json, now, type Config } from './config.ts';
import { confirmMainnet, refreshExchange } from './exchange.ts';
import { makeWalk } from './pipeline.ts';
import { walkPage } from './walker.ts';
import { fetchTransaction, parallel } from './fetcher.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';

export async function probe(provider: Provider, store: Store, config: Config) {
  config.maxRetries = 2;
  await confirmMainnet(provider);
  const exchange = await refreshExchange(provider, store, config);
  const head = await provider.call<number>('getSlot', [{ commitment: 'finalized' }]);
  const cutoff = Math.floor(Date.now() / 1000) - 86400;
  let walk = makeWalk(config.programId, 'backfill', 'probe', 0, head);
  let bounded = false;
  while (!walk.done && walk.pages < config.probePageLimit) {
    walk = await walkPage(provider, store, 'walk/probe', walk);
    const [oldest] = await store.rows('SELECT min(block_time) AS time FROM signatures');
    if (oldest.time !== null && Number(oldest.time) <= cutoff) { bounded = true; break; }
  }
  const sample = await store.rows(`SELECT * FROM signatures USING SAMPLE reservoir(${config.probeTransactionLimit} ROWS) REPEATABLE(42)`);
  let unavailable = 0;
  const failures: Record<string, number> = {};
  await parallel(sample, config.concurrency.backfill, async row => {
    try { await fetchTransaction(provider, store, row, config, 'backfill'); }
    catch (error) {
      unavailable++;
      const reason = error instanceof Error ? error.message : 'Unknown';
      failures[reason] = (failures[reason] ?? 0) + 1;
    }
  });
  const [stats] = await store.rows(`SELECT count(*) AS signatures, min(slot) AS from_slot,
    max(slot) AS to_slot, min(block_time) AS from_time, max(block_time) AS to_time FROM signatures`);
  const [tx] = await store.rows(`SELECT count(*) AS sampled, avg(length(tx_b64)*0.75+length(meta_json)) AS mean_bytes,
    count(*) FILTER (WHERE err::VARCHAR<>'null') AS failed FROM transactions`);
  const [multi] = await store.rows('SELECT count(*) AS n FROM (SELECT slot FROM signatures GROUP BY slot HAVING count(*)>1)');
  const span = Math.max(1, Number(stats.to_time) - Number(stats.from_time));
  const estimatedPerDay = Number(stats.signatures) / span * 86400;
  const report = { at: now(), exchange, head, pages: walk.pages, coveredOneDay: bounded,
    pageLimited: !bounded && !walk.done, stats, sample: tx, multiSlots: Number(multi.n),
    unavailableSamples: unavailable, failures, complete: unavailable === 0,
    estimatedTxPerDay: Math.round(estimatedPerDay), estimatedTransactionCuPerDay: Math.round(estimatedPerDay * 40),
    estimatedMeanCuPerSecond: estimatedPerDay * 40 / 86400,
    metrics: provider.counters, D1: 'pending; block ordering retained',
    missing: ['same-slot until probe', 'CPI/lookup-table cross-check', 'authenticated fills', 'cross-provider', 'WebSocket deltas'],
    note: 'Sizing extrapolates a bounded recent sample; it is not a complete M0 day.' };
  await writeFile(join(config.dataDir, 'probe.json'), json(report) + '\n');
  return report;
}
