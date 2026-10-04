import { getSignatureFromTransaction, getTransactionDecoder } from '@solana/kit';
import { randomUUID } from 'node:crypto';
import { json, now, type Config, type Lane } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';
import { insertRaw } from './insert-raw.ts';

export function wireSignature(base64: string) {
  const bytes = Buffer.from(base64, 'base64');
  return getSignatureFromTransaction(getTransactionDecoder().decode(bytes));
}

/** Acceleration only: the standard manifest still defines fetch completeness. */
export async function bulkFetchRange(provider: Provider, store: Store, config: Config, lane: Lane, from: number, to: number) {
  const key = `bulk/${lane}/${from}/${to}`;
  let state = await store.get<{ token?: string; done: boolean }>(key) ?? { done: false };
  if (state.done) return;
  // This range's standard manifest is already complete. Load it once, not once per page.
  const manifestRows = await store.rows('SELECT signature, slot, mode FROM signatures WHERE slot BETWEEN ? AND ?', [from, to]);
  const manifests = new Map(manifestRows.map(row => [row.signature, row]));
  let first = true;
  let pending: { id:string; raw:string; at:string; slot:number; rows:any[] }[] = [];
  while (!state.done) {
    const result = await provider.call('getTransactionsForAddress', [config.programId, {
      commitment: 'finalized', transactionDetails: 'full', sortOrder: 'asc', limit: 100,
      encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion, paginationToken: state.token,
      filters: { slot: { gte: from, lte: to }, status: 'any', tokenAccounts: 'none' },
    }], lane);
    if (!Array.isArray(result?.data)) throw new Error('Invalid bulk transaction page');
    const pageId = randomUUID();
    const rows: any[] = [];
    const entries = result.data;
    const decoded = entries.map((tx: any) => {
      if (tx.slot < from || tx.slot > to) throw new Error('Bulk provider ignored slot bounds');
      if (!Array.isArray(tx.transaction) || tx.transaction[1] !== 'base64' || !tx.meta) throw new Error('Invalid bulk wire transaction');
      return { tx, signature: wireSignature(tx.transaction[0]) };
    });
    for (let index = 0; index < entries.length; index++) {
      const { tx, signature } = decoded[index];
      const manifest = manifests.get(signature);
      if (!manifest) {
        // Do not silently treat a discrepancy between two provider indexes as independent proof.
        await store.exec('INSERT INTO errors VALUES (?, ?, ?, ?, ?)', ['index_gap', signature, provider.provider, 'bulk_signature_absent_from_manifest', now()]);
        throw new Error('Bulk signature is absent from the completed standard manifest');
      }
      if (Number(manifest.slot) !== tx.slot) throw new Error('Bulk slot does not match manifest');
      rows.push({ signature, slot: tx.slot, block_time: tx.blockTime, err: tx.meta.err,
        fee: tx.meta.fee, compute_units_consumed: tx.meta.computeUnitsConsumed ?? null,
        tx_b64: tx.transaction[0], meta_json: json(tx.meta), mode: manifest.mode,
        raw_rpc_json: json({ pageId, arrayIndex: index }), provider: provider.provider, fetched_at: now() });
    }
    if (result.paginationToken && result.paginationToken === state.token) throw new Error('Bulk cursor did not advance');
    state = { token: result.paginationToken ?? undefined, done: !result.paginationToken };
    pending.push({ id:pageId,raw:provider.rawPages.get(result) ?? json(result),at:now(),slot:entries[0]?.slot ?? from,rows });
    // Establish an immediate durable anchor; subsequent bounded batches amortize
    // commits. On interruption, uncommitted pages replay from the stored token.
    if (!first && !state.done && pending.length < config.bulkCommitPages) continue;
    await store.transaction(async connection => {
      for (const page of pending) await connection.run('INSERT INTO rpc_pages VALUES (?, ?, ?, ?, ?, ?)',
        [page.id, 'getTransactionsForAddress', provider.provider, page.raw, page.at, page.slot]);
      await insertRaw(connection, pending.flatMap(page => page.rows), from, to);
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', [key, json(state)]);
    });
    first = false; pending = [];
  }
}
