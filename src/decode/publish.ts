import { mkdir, rename, writeFile, readdir, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { now } from '../config.ts';
import { fileHash, sqlString, syncPath } from '../writer.ts';
import type { Store } from '../store.ts';
import { definitions, tables, version } from './schema.ts';
import type { Rows } from './normalize.ts';

export interface SourceProgress {
  hash: string; path: string; offset: number; at?: string;
  seen?: { signature: string; source_hash: string }[];
}
export async function publish(store: Store, rows: Rows, source: SourceProgress) {
  const batch = Number(await store.get('batch') ?? 0) + 1;
  // Files are durable before the transaction that registers them and progress.
  await store.exclusive(async connection => {
    const files: any[] = [];
    for (const table of tables) {
      await connection.run(`DELETE FROM ${table}`);
      if (!rows[table].length) continue;
      await connection.run(`INSERT INTO ${table} SELECT unnest(json_transform(?::JSON, ?::JSON), recursive:=true)`,
        [JSON.stringify(rows[table]), JSON.stringify([Object.fromEntries(definitions[table].split(',').map(column => {
          const [name, type] = column.trim().split(/\s+/); return [name, type];
        }))])]);
      const epochs = await connection.runAndReadAll(`SELECT DISTINCT slot // 432000 AS epoch FROM ${table}`);
      for (const item of epochs.getRowObjectsJson()) {
        const epoch = Number(item.epoch);
        const relative = `tables/${table}/epoch=${epoch}/${String(batch).padStart(12,'0')}.parquet`;
        const path = join(store.root, relative);
        await mkdir(dirname(path), { recursive: true });
        await connection.run(`COPY (SELECT * FROM ${table} WHERE slot // 432000=${epoch}
          ORDER BY slot, signature) TO ${sqlString(path + '.tmp')} (FORMAT PARQUET, COMPRESSION ZSTD)`);
        await syncPath(path + '.tmp'); await rename(path + '.tmp', path); await syncPath(dirname(path));
        const count = await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
        const expected = rows[table].filter(row => Math.floor(Number(row.slot) / 432000) === epoch).length;
        if (Number(count.getRowObjectsJson()[0].n) !== expected) throw new Error('decoded file row count mismatch');
        files.push({ path: relative, table, count: expected, hash: await fileHash(path) });
      }
    }
    await connection.run('BEGIN');
    try {
      for (const file of files) await connection.run('INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)',
        [file.path, file.table, file.count, file.hash, batch, now()]);
      const seen = source.seen ?? rows.decoded_transactions;
      if (seen.length) {
        const keys = seen.map(row => sqlString(String(row.signature))).join(',');
        await connection.run('DELETE FROM processed_batch');
        await connection.run(`INSERT INTO processed_batch SELECT value->>'signature', value->>'source_hash', ?
          FROM json_each(?::JSON)`, [source.at ?? '',JSON.stringify(seen)]);
        await connection.run(`UPDATE processed SET source_hash=b.source_hash, publication_at=b.publication_at
          FROM processed_batch b WHERE processed.signature=b.signature AND processed.signature IN (${keys})
          AND coalesce(processed.publication_at,'')<=b.publication_at`);
        await connection.run(`INSERT INTO processed SELECT * FROM processed_batch
          WHERE signature NOT IN (SELECT signature FROM processed WHERE signature IN (${keys}))`);
      }
      await connection.run('INSERT OR REPLACE INTO sources VALUES (?, ?, ?)', [source.hash, source.path, source.offset]);
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', ['batch', String(batch)]);
      await connection.run('COMMIT');
    } catch (error) { await connection.run('ROLLBACK'); throw error; }
  });
  if (tables.some(table => rows[table].length)) await writeCatalog(store);
}

export async function writeCatalog(store: Store) {
  const files = await store.rows('SELECT * FROM files ORDER BY batch_id');
  const catalog = { at: now(), schemaVersion: 1, decoderVersion: version, files,
    acceptance: 'decoded events; source range validation/sealing and state reconstruction pending' };
  const path = join(store.root, 'catalog.json');
  await writeFile(path + '.tmp', JSON.stringify(catalog) + '\n');
  await syncPath(path + '.tmp'); await rename(path + '.tmp', path); await syncPath(store.root);
}

export async function recover(store: Store) {
  const files = new Set((await store.rows('SELECT path FROM files')).map(row => join(store.root, row.path)));
  const walk = async (root: string) => {
    for (const item of await readdir(root, { withFileTypes: true })) {
      const path = join(root, item.name);
      if (item.isDirectory()) await walk(path);
      else if (item.name.endsWith('.tmp') || (item.name.endsWith('.parquet') && !files.has(path))) await rm(path);
    }
  };
  await walk(store.root); await writeCatalog(store);
}
