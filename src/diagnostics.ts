import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import type { Catalog } from './catalog.ts';
import type { Store } from './store.ts';
import { sqlString } from './writer.ts';

/** Offline performance reproduction. Never prints transaction data or source paths. */
export async function benchmarkDecoder(root: string, mode: string) {
  const catalog: Catalog = JSON.parse(await readFile(join(root, 'catalog.json'), 'utf8'));
  const file = catalog.files.filter(f => f.table_name === 'transactions')
    .sort((a,b) => Number(b.row_count) - Number(a.row_count))[0];
  if (!file) throw new Error('no published transactions');
  const instance = await DuckDBInstance.create(':memory:', { threads:'4', memory_limit:'4GB' });
  const connection = await instance.connect();
  try {
    const path = sqlString(file.path);
    const offset = Math.floor(Number(file.row_count) * 0.75);
    const started = performance.now();
    const keys = mode === 'legacy' ? undefined : (await connection.runAndReadAll(`SELECT file_row_number FROM
      (SELECT file_row_number, row_number() OVER (ORDER BY slot DESC, signature DESC) AS ordinal
      FROM read_parquet(${path}, file_row_number=true)) WHERE ordinal>${offset} AND ordinal<=${offset+250}`))
      .getRowObjectsJson().map(row => Number(row.file_row_number));
    const rows = await connection.runAndReadAll(keys ? `SELECT * FROM read_parquet(${path}, file_row_number=true)
      WHERE file_row_number IN (${keys.join(',')}) ORDER BY slot DESC, signature DESC` :
      `SELECT * FROM read_parquet(${path}) ORDER BY slot DESC, signature DESC LIMIT 250 OFFSET ${offset}`);
    return { mode, sourceRows:Number(file.row_count), offset, batchRows:rows.getRowObjectsJson().length,
      seconds:(performance.now()-started)/1000 };
  } finally { connection.closeSync(); instance.closeSync(); }
}

/** Stop the writer first. Each update is rolled back, including on failure. */
export async function benchmarkOrdering(store: Store, mode: string) {
  const progress = await store.get('backfill');
  const to = Number(progress.next) + 1000;
  const from = to - 63;
  return store.exclusive(async connection => {
    await connection.run('BEGIN');
    try {
      const started = performance.now();
      const result = await connection.runAndReadAll(`EXPLAIN ANALYZE UPDATE transactions SET tx_index=o.tx_index,
        single_in_slot=false FROM slot_order o WHERE transactions.signature=o.signature
        AND o.slot BETWEEN ${from} AND ${to} ${mode === 'bounded' ? `AND transactions.slot BETWEEN ${from} AND ${to}` : ''}`);
      return { mode, from, to, seconds:(performance.now()-started)/1000,
        plan:result.getRowObjectsJson().map(row => row.explain_value) };
    } finally { await connection.run('ROLLBACK'); }
  });
}

export async function benchmarkIngest(store: Store, mode: string) {
  const progress = await store.get('backfill');
  return store.exclusive(async connection => {
    await connection.run('BEGIN');
    try {
      const started = performance.now();
      const result = await connection.runAndReadAll(`EXPLAIN ANALYZE INSERT INTO transactions
        SELECT * FROM transactions WHERE slot BETWEEN ${Number(progress.next)+1} AND ${Number(progress.next)+1000}
        ${mode === 'bounded' ? `AND signature NOT IN (SELECT signature FROM transactions
          WHERE slot BETWEEN ${Number(progress.next)+1} AND ${Number(progress.next)+1000}) LIMIT 100` :
          'LIMIT 100 ON CONFLICT(signature) DO NOTHING'}`);
      return { seconds:(performance.now()-started)/1000, plan:result.getRowObjectsJson().map(row => row.explain_value) };
    } finally { await connection.run('ROLLBACK'); }
  });
}
