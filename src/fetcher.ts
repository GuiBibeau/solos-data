import { json, now, sleep, type Config, type Lane } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';
import { bulkFetchRange, wireSignature } from './bulk-fetcher.ts';

export async function parallel<T>(items: T[], concurrency: number, job: (item: T) => Promise<void>) {
  let cursor = 0;
  // Wait for every in-flight write even when one worker fails.
  const outcomes = await Promise.allSettled(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (cursor < items.length) await job(items[cursor++]);
  }));
  const failure = outcomes.find(item => item.status === 'rejected');
  if (failure?.status === 'rejected') throw failure.reason;
}

export async function fetchTransaction(provider: Provider, store: Store, row: any, config: Config, lane: Lane) {
  let result: any = null;
  for (let attempt = 0; attempt < config.maxRetries; attempt++) {
    result = await provider.call('getTransaction', [row.signature, {
      encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion, commitment: 'finalized',
    }], lane);
    if (result !== null) break;
    if (attempt + 1 < config.maxRetries) await sleep(Math.random() * Math.min(32000, 500 * 2 ** attempt));
  }
  if (result === null) {
    await store.exec('INSERT INTO errors VALUES (?, ?, ?, ?, ?)', ['transaction', row.signature, provider.provider, 'null_after_retries', now()]);
    throw new Error('Transaction unavailable after retries; range remains unpublished');
  }
  if (result.slot !== Number(row.slot)) throw new Error('Transaction slot does not match manifest');
  if (!Array.isArray(result.transaction) || result.transaction[1] !== 'base64' || !result.meta) throw new Error('Invalid raw transaction response');
  if (wireSignature(result.transaction[0]) !== row.signature) throw new Error('Wire signature does not match manifest');
  const raw = provider.rawTransactions.get(row.signature);
  await store.exec(`INSERT INTO transactions VALUES (?, ?, ?, NULL, NULL, ?::JSON, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
    ON CONFLICT(signature) DO NOTHING`, [row.signature, row.slot, result.blockTime,
    json(result.meta.err), result.meta.fee, result.meta.computeUnitsConsumed ?? null,
    result.transaction[0], json(result.meta), raw ?? json(result), row.mode, provider.provider, now()]);
  provider.rawTransactions.delete(row.signature);
}

export async function fetchRange(provider: Provider, store: Store, config: Config, lane: Lane, from: number, to: number) {
  const [missing] = await store.rows(`SELECT count(*) AS n, min(s.slot) AS first_slot, max(s.slot) AS last_slot
    FROM signatures s LEFT JOIN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?) t USING(signature)
    WHERE t.signature IS NULL AND s.slot BETWEEN ? AND ?`, [from, to, from, to]);
  // Tail overlap stays in the manifest and validation, but fetched slots need no replay.
  if (config.bulkFetchEnabled && Number(missing.n)>0) {
    const windows = [];
    const last = Number(missing.last_slot);
    for (let first = Number(missing.first_slot); first <= last; first += config.bulkFetchWindowSlots) {
      windows.push({ from: first, to: Math.min(last, first + config.bulkFetchWindowSlots - 1) });
    }
    // Tokens remain sequential inside each window; independent slot bounds may overlap in flight.
    await parallel(windows, Math.min(config.bulkFetchConcurrency, config.concurrency[lane]), window =>
      bulkFetchRange(provider, store, config, lane, window.from, window.to));
  }
  for (;;) {
    const pending = await store.rows(`SELECT s.* FROM signatures s
      LEFT JOIN (SELECT signature FROM transactions WHERE slot BETWEEN ? AND ?) t USING(signature)
      WHERE t.signature IS NULL AND s.slot BETWEEN ? AND ? ORDER BY s.slot ASC, s.signature LIMIT ?`,
    [from, to, from, to, config.fetchBatchSize]);
    if (!pending.length) return;
    await parallel(pending, config.concurrency[lane], row => fetchTransaction(provider, store, row, config, lane));
  }
}
