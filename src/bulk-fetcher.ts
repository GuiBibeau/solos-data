import { getSignatureFromTransaction, getTransactionDecoder } from '@solana/kit';
import { randomUUID } from 'node:crypto';
import { json, now, type Config, type Lane } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';

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
    await store.transaction(async connection => {
      await connection.run('INSERT INTO rpc_pages VALUES (?, ?, ?, ?, ?, ?)',
        [pageId, 'getTransactionsForAddress', provider.provider, provider.rawPages.get(result) ?? json(result), now(), entries[0]?.slot ?? from]);
      if (rows.length) await connection.run(`INSERT INTO transactions
        SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'block_time')::BIGINT,
        NULL, NULL, value->'err', (value->>'fee')::BIGINT, (value->>'compute_units_consumed')::BIGINT,
        value->>'tx_b64', value->>'meta_json', value->>'raw_rpc_json', value->>'mode',
        value->>'provider', value->>'fetched_at', NULL FROM json_each(?::JSON)
        ON CONFLICT(signature) DO NOTHING`, [json(rows)]);
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', [key, json(state)]);
    });
  }
}
