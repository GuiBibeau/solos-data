import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, open, rename, readdir, rm } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { json, now } from './config.ts';
import type { Store } from './store.ts';
import { requireStorageSpace } from './storage-space.ts';

export const tables = ['signatures', 'transactions', 'slot_order', 'program_versions', 'rpc_pages'] as const;
export const sqlString = (value: string) => `'${value.replaceAll("'", "''")}'`;

export async function fileHash(path: string) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}

export async function syncPath(path: string) {
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

export async function recoverFiles(store: Store, root: string) {
  const registered = new Set((await store.rows<{ path: string }>('SELECT path FROM files')).map(row => row.path));
  // After a move, registrations point at the old root; cleanup would delete every published file.
  if ([...registered].some(path => relative(root, path).startsWith('..'))) {
    throw new Error('Registered files are outside the data root; run relocate before resuming');
  }
  async function visit(directory: string) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.name.endsWith('.tmp') || (entry.name.endsWith('.parquet') && !registered.has(path))) await rm(path);
    }
  }
  await visit(root);
}

export async function publishRange(store: Store, root: string, from: number, to: number,
  checkpoint?: { key: string; value: unknown; cycleId?: string }) {
  await requireStorageSpace(root);
  await store.exclusive(async connection => {
    const files: { path: string; table: string; epoch: number; count: number; hash: string }[] = [];
    for (const table of tables) {
      const reader = await connection.runAndReadAll(`SELECT slot // 432000 AS epoch, count(*) AS n
        FROM ${table} WHERE slot BETWEEN ? AND ? GROUP BY epoch`, [from, to]);
      for (const item of reader.getRowObjectsJson()) {
        const epoch = Number(item.epoch);
        const count = Number(item.n);
        const directory = join(root, 'staging', table, `epoch=${epoch}`);
        await mkdir(directory, { recursive: true });
        const path = join(directory, `${from}-${to}-${randomUUID()}.parquet`);
        const projection = table === 'rpc_pages' ? 'page_id AS signature, * EXCLUDE(page_id)' : '*';
        const query = `SELECT ${projection} FROM ${table} WHERE slot BETWEEN ${from} AND ${to}
          AND slot // 432000 = ${epoch} ORDER BY slot, signature`;
        await connection.run(`COPY (${query}) TO ${sqlString(path + '.tmp')} (FORMAT PARQUET, COMPRESSION ZSTD)`);
        await syncPath(path + '.tmp');
        await rename(path + '.tmp', path);
        await syncPath(directory);
        const actual = await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
        if (Number(actual.getRowObjectsJson()[0].n) !== count) throw new Error('V7: exported row count mismatch');
        files.push({ path, table, epoch, count, hash: await fileHash(path) });
      }
    }
    await connection.run('BEGIN');
    try {
      for (const file of files) await connection.run('INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)',
        [file.path, file.table, file.epoch, file.count, file.hash, now(), 'active']);
      await connection.run('INSERT OR IGNORE INTO published_ranges VALUES (?, ?)', [from, to]);
      if (checkpoint) {
        await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', [checkpoint.key, json(checkpoint.value)]);
        if (checkpoint.cycleId) await connection.run("UPDATE cycles SET finished_at=?, status='ok' WHERE cycle_id=?",
          [now(), checkpoint.cycleId]);
      }
      await connection.run('COMMIT');
    } catch (error) { await connection.run('ROLLBACK'); throw error; }
  });
}

export async function verifyFiles(store: Store) {
  const files = await store.rows('SELECT * FROM files WHERE status=?', ['active']);
  for (const file of files) {
    if (await fileHash(file.path) !== file.sha256) throw new Error('V7: file hash mismatch');
    const [count] = await store.rows(`SELECT count(*) AS n FROM read_parquet(${sqlString(file.path)})`);
    if (Number(count.n) !== Number(file.row_count)) throw new Error('V7: file count mismatch');
  }
  return { files: files.length, ok: true };
}
