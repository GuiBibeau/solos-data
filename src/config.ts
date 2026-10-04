import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

export type Lane = 'tail' | 'backfill';
export interface Config {
  programId: string; exchangeUrl: string; dataDir: string;
  cuPerSecond: number; utilization: number; tailShare: number;
  concurrency: Record<Lane, number>; cuWeights: Record<string, number>;
  tailIntervalSeconds: number; overlapSlots: number; maxSlotsPerChunk: number;
  fetchBatchSize: number; freshnessSloSeconds: number; backfillEnabled: boolean;
  exchangeRefreshSeconds: number; compactionIntervalSeconds: number;
  maxRetries: number; probeTransactionLimit: number; probePageLimit: number;
  bulkFetchEnabled: boolean;
  bulkFetchWindowSlots: number; bulkFetchConcurrency: number;
  maxSupportedTransactionVersion: number;
  backfillChunkSlots: number;
}

export async function loadConfig(path = 'config/phoenix.json'): Promise<Config> {
  const config: Config = JSON.parse(await readFile(path, 'utf8'));
  config.backfillChunkSlots ??= 1000;
  config.bulkFetchWindowSlots ??= 128;
  config.bulkFetchConcurrency ??= 8;
  config.dataDir = resolve(process.env.SOLOS_DATA_DIR ?? config.dataDir);
  if (process.env.SOLOS_DATA_CU_PER_SECOND) config.cuPerSecond = Number(process.env.SOLOS_DATA_CU_PER_SECOND);
  if (config.cuPerSecond <= 0 || !Number.isFinite(config.cuPerSecond)) throw new Error('Invalid CU/s');
  if (config.tailShare <= 0 || config.tailShare >= 1) throw new Error('Invalid tail share');
  if (config.utilization <= 0 || config.utilization > 1) throw new Error('Invalid utilization');
  for (const key of ['overlapSlots', 'maxSlotsPerChunk', 'backfillChunkSlots', 'fetchBatchSize', 'maxRetries',
    'bulkFetchWindowSlots', 'bulkFetchConcurrency'] as const) {
    if (!Number.isInteger(config[key]) || config[key] <= 0) throw new Error(`Invalid ${key}`);
  }
  return config;
}

export function providerUrl(name = 'SOLANA_RPC_URL'): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  const parsed = new URL(value);
  if (parsed.protocol !== 'https:' && parsed.hostname !== '127.0.0.1') throw new Error('RPC requires HTTPS');
  return value;
}

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
export const now = () => new Date().toISOString();
export const json = (value: unknown) => JSON.stringify(value, (_, v) => typeof v === 'bigint' ? Number(v) : v);
export const log = (event: string, fields: Record<string, unknown> = {}) =>
  console.log(json({ at: now(), event, ...fields }));
