import { json, log, now, sleep, type Config } from './config.ts';
import { refreshExchange, confirmMainnet } from './exchange.ts';
import type { Provider } from './rpc.ts';
import { RpcFailure } from './rpc.ts';
import type { Store } from './store.ts';
import { tailCycle } from './pipeline.ts';
import { backfill } from './history.ts';
import { compact } from './compactor.ts';
import { recoverFiles } from './writer.ts';
import { writeStatus } from './status.ts';

export function safeError(error: unknown): string {
  const message = error instanceof Error ? error.message : 'Unknown failure';
  return message.replace(/https?:\/\/[^\s"']+/g, '[redacted-url]').slice(0, 300);
}

export async function run(provider: Provider, store: Store, config: Config) {
  await recoverFiles(store, config.dataDir);
  await confirmMainnet(provider);
  let exchange = await refreshExchange(provider, store, config);
  if (!await store.get('H0')) {
    const recent = await provider.call('getSignaturesForAddress', [config.programId, { limit: 1, commitment: 'finalized' }]);
    if (!recent[0]) throw new Error('Program has no finalized signatures');
    await store.set('H0', { signature: recent[0].signature, slot: recent[0].slot, recordedAt: now() });
  }
  let stopped = false;
  const stop = () => { stopped = true; provider.shutdown.abort(); log('shutdown_requested'); };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  let lastRefresh = Date.now();
  let lastCompact = Date.now();
  let errorStreak = 0;
  async function recordError(lane: string, error: unknown) {
    const detail = { lane, error: safeError(error), at: now() };
    await store.set('last-error', detail);
    await store.exec('INSERT INTO errors VALUES (?, ?, ?, ?, ?)', ['service', lane, provider.provider, detail.error, detail.at]);
    log('collector_error', detail);
  }
  const history = (async () => {
    if (!config.backfillEnabled) return;
    while (!stopped) {
      try { await backfill(provider, store, config, exchange.programData); return; }
      catch (error) {
        if (stopped) return;
        await recordError('backfill', error);
        if (error instanceof RpcFailure && [401, 403].includes(error.code)) { stopped = true; return; }
        await sleep(30000);
      }
    }
  })();
  const follower = (async () => {
    while (!stopped) {
      const started = Date.now();
      try {
        if (started - lastRefresh > config.exchangeRefreshSeconds * 1000) {
          exchange = await refreshExchange(provider, store, config); lastRefresh = started;
        }
        await tailCycle(provider, store, config, exchange.programData);
        errorStreak = 0;
        if (started - lastCompact > config.compactionIntervalSeconds * 1000) {
          await compact(store, config.dataDir); lastCompact = Date.now();
        }
      } catch (error) { if (stopped) break; errorStreak++; await recordError('tail', error); }
      await store.set('tail-health', { errorStreak, cycleDurationSeconds: (Date.now() - started) / 1000 });
      const delay = Math.max(1000, config.tailIntervalSeconds * 1000 - (Date.now() - started));
      await sleep(delay);
    }
  })();
  const observer = (async () => {
    while (!stopped) { await writeStatus(store, config.dataDir, provider); await sleep(10000); }
  })();
  log('collector_started', { program: config.programId, markets: exchange.markets, backfill: config.backfillEnabled });
  const outcomes = await Promise.allSettled([history, follower, observer]);
  for (const outcome of outcomes) if (outcome.status === 'rejected') throw outcome.reason;
  await writeStatus(store, config.dataDir, provider);
  process.off('SIGTERM', stop);
  process.off('SIGINT', stop);
  log('collector_stopped', { metrics: json(provider.counters) });
}
