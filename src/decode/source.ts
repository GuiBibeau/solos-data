import { readFile } from 'node:fs/promises';
import { resolve, relative, join, isAbsolute } from 'node:path';
import type { Store } from '../store.ts';
import type { Catalog, CatalogFile } from '../catalog.ts';
import { sqlString, fileHash } from '../writer.ts';
import type { RawTransaction } from './normalize.ts';

/** Legacy raw catalogs use absolute paths. Rebase only their known staging tree. */
export function sourcePath(root: string, path: string) {
  const marker = ['/staging/', '/transactions/'].find(marker => path.includes(marker));
  const suffix = isAbsolute(path) && marker ? path.slice(path.indexOf(marker) + 1) : path;
  const result = resolve(root, suffix);
  if (relative(resolve(root), result).startsWith('..')) throw new Error('source path outside raw root');
  return result;
}

export async function nextSource(store: Store, rawRoot: string) {
  const catalog: Catalog = JSON.parse(await readFile(join(rawRoot, 'catalog.json'), 'utf8'));
  const progress = new Map((await store.rows('SELECT * FROM sources')).map(row => [row.source_hash, Number(row.row_offset)]));
  const file = catalog.files.filter(file => file.table_name === 'transactions' &&
    (progress.get(file.sha256) ?? 0) < Number(file.row_count))
    .sort((a,b) => b.created_at.localeCompare(a.created_at) || b.path.localeCompare(a.path))[0];
  return file ? { file, offset: progress.get(file.sha256) ?? 0, catalogAt: catalog.at } : undefined;
}

export async function sourceRows(store: Store, rawRoot: string, file: CatalogFile, offset: number, limit: number,
  verified: Set<string>) {
  const path = sourcePath(rawRoot, file.path);
  if (!verified.has(file.sha256)) {
    if (await fileHash(path) !== file.sha256) throw new Error('raw source checksum mismatch');
    verified.add(file.sha256);
  }
  // Keep legacy logical OFFSET ordering, but never sort wire/meta payloads for the entire file.
  const keys = await store.rows(`SELECT file_row_number FROM (
    SELECT file_row_number, row_number() OVER (ORDER BY slot DESC, signature DESC) AS ordinal
    FROM read_parquet(${sqlString(path)}, file_row_number=true))
    WHERE ordinal>? AND ordinal<=?`, [offset,offset+limit]);
  if (!keys.length && offset < Number(file.row_count)) throw new Error('raw source row count mismatch');
  if (!keys.length) return [];
  const raw = await store.rows<RawTransaction>(`SELECT * EXCLUDE(file_row_number)
    FROM read_parquet(${sqlString(path)}, file_row_number=true)
    WHERE file_row_number IN (${keys.map(row => Number(row.file_row_number)).join(',')})
    ORDER BY slot DESC, signature DESC`);
  // Point lookups avoid building a hash table for all previously decoded signatures.
  const previous = new Map((await store.rows(`SELECT * FROM processed WHERE signature IN
    (${raw.map(row => sqlString(row.signature)).join(',')})`))
    .map(row => [row.signature, row]));
  const rows = raw.map(row => ({ ...row, previous_hash:previous.get(row.signature)?.source_hash ?? null,
    previous_at:previous.get(row.signature)?.publication_at ?? null }));
  if (!rows.length && offset < Number(file.row_count)) throw new Error('raw source row count mismatch');
  return rows;
}
