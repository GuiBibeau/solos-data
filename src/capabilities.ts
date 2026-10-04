import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { json, now, type Config } from './config.ts';
import type { Provider } from './rpc.ts';
import { confirmMainnet } from './exchange.ts';
import { safeError } from './service.ts';
import { wireSignature } from './bulk-fetcher.ts';

/** Small, repeatable CLI probe of current Alchemy extensions and launch boundary. */
export async function capabilities(provider: Provider, config: Config) {
  await confirmMainnet(provider);
  const report: Record<string, unknown> = { at: now() };
  for (const details of ['signatures', 'full']) {
    try {
      const result = await provider.call('getTransactionsForAddress', [config.programId, {
        commitment: 'finalized', transactionDetails: details, sortOrder: 'asc', limit: 3,
        encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
      }]);
      const entries = result.data ?? result.transactions ?? [];
      report[details] = { keys: Object.keys(result), count: entries.length,
        pagination: Boolean(result.paginationToken),
        entries: entries.map((entry: any) => ({
          keys: Object.keys(entry), signature: entry.signature, slot: entry.slot,
          blockTime: entry.blockTime, transactionIndex: entry.transactionIndex,
          transactionEncoding: Array.isArray(entry.transaction) ? entry.transaction[1] : typeof entry.transaction,
          hasMeta: entry.meta != null,
        })) };
      if (details === 'signatures' && entries[0]?.signature) {
        const transaction = await provider.call('getTransaction', [entries[0].signature, {
          commitment: 'finalized', encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        }]);
        report.oldestStandardFetch = { available: transaction !== null, slot: transaction?.slot,
          blockTime: transaction?.blockTime, hasMeta: Boolean(transaction?.meta) };
        const parsed = await provider.call('getTransaction', [entries[0].signature, {
          commitment: 'finalized', encoding: 'jsonParsed', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
        }]);
        const instructions = parsed?.transaction?.message?.instructions ?? [];
        report.launchBoundary = {
          programAccountCreated: instructions.some((instruction: any) => instruction.parsed?.type === 'createAccount'
            && instruction.parsed?.info?.newAccount === config.programId),
          instructions: instructions.map((instruction: any) => ({ program: instruction.program, type: instruction.parsed?.type })),
        };
      }
      if (details === 'full') report.bulkWireSignatures = entries.map((entry: any) => wireSignature(entry.transaction[0]));
    } catch (error) { report[details] = { error: safeError(error) }; }
  }
  try {
    const recent = await provider.call('getTransactionsForAddress', [config.programId, {
      commitment: 'finalized', transactionDetails: 'full', sortOrder: 'desc', limit: 10,
      encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion, filters: { status: 'any', tokenAccounts: 'none' },
    }]);
    const first = recent.data?.[0];
    const bounded = first ? await provider.call('getTransactionsForAddress', [config.programId, {
      commitment: 'finalized', transactionDetails: 'full', sortOrder: 'asc', limit: 100,
      encoding: 'base64', maxSupportedTransactionVersion: config.maxSupportedTransactionVersion,
      filters: { slot: { gte: first.slot, lte: first.slot }, status: 'any', tokenAccounts: 'none' },
    }]) : null;
    report.recentBulk = { count: recent.data?.length, slot: first?.slot, hasMeta: Boolean(first?.meta),
      boundedCount: bounded?.data?.length, boundsHonored: bounded?.data?.every((tx: any) => tx.slot === first.slot),
      versions: [...new Set(recent.data?.map((tx: any) => tx.version))],
      wireSignaturesChecked: recent.data?.map((tx: any) => wireSignature(tx.transaction[0])).length };
  } catch (error) { report.recentBulk = { error: safeError(error) }; }
  report.metrics = provider.counters;
  await writeFile(join(config.dataDir, 'capabilities.json'), json(report) + '\n');
  return report;
}
