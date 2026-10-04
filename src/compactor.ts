import { mkdir, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { now } from './config.ts';
import type { Store } from './store.ts';
import { fileHash, sqlString, syncPath } from './writer.ts';

/** Consolidates only registered revisions; the newest publication wins per signature. */
export async function compact(store: Store, root: string) {
  const groups = await store.rows(`SELECT table_name, epoch FROM files WHERE status='active'
    GROUP BY table_name, epoch HAVING count(*)>=10`);
  for (const group of groups) await store.exclusive(async connection => {
    const reader = await connection.runAndReadAll("SELECT path FROM files WHERE status='active' AND table_name=? AND epoch=?",
      [group.table_name, Number(group.epoch)]);
    const paths = reader.getRowObjectsJson().map(row => String(row.path));
    const pathList = `[${paths.map(sqlString).join(',')}]`;
    const query = `SELECT p.* EXCLUDE(filename) FROM read_parquet(${pathList}, filename=true) p
      JOIN files f ON f.path=p.filename QUALIFY row_number() OVER
      (PARTITION BY p.signature ORDER BY f.created_at DESC, p.filename DESC)=1`;
    const directory = join(root, group.table_name, `epoch=${group.epoch}`, 'open');
    await mkdir(directory, { recursive: true });
    const path = join(directory, `compact-${randomUUID()}.parquet`);
    await connection.run(`COPY (${query} ORDER BY slot, signature) TO ${sqlString(path + '.tmp')} (FORMAT PARQUET, COMPRESSION ZSTD)`);
    await syncPath(path + '.tmp');
    await rename(path + '.tmp', path);
    await syncPath(directory);
    const count = await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
    const expected = await connection.runAndReadAll(`SELECT count(*) AS n FROM (${query})`);
    const n = Number(count.getRowObjectsJson()[0].n);
    if (n !== Number(expected.getRowObjectsJson()[0].n)) throw new Error('V7: compaction mismatch');
    const hash = await fileHash(path);
    await connection.run('BEGIN');
    try {
      for (const old of paths) await connection.run("UPDATE files SET status='superseded' WHERE path=?", [old]);
      await connection.run('INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)',
        [path, group.table_name, Number(group.epoch), n, hash, now(), 'active']);
      await connection.run('COMMIT');
    } catch (error) { await connection.run('ROLLBACK'); throw error; }
    // Superseded files remain recoverable. Garbage collection is an explicit future operation.
  });
}
