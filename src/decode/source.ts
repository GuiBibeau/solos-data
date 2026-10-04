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
  const candidates = [];
  for (const file of catalog.files.filter(file => file.table_name === 'transactions' &&
    (progress.get(file.sha256) ?? 0) < Number(file.row_count))) {
    candidates.push({ file,slot:await sourcePriority(store,rawRoot,file) });
  }
  const file = candidates.sort((a,b) => b.slot-a.slot || b.file.created_at.localeCompare(a.file.created_at)
    || b.file.path.localeCompare(a.file.path))[0]?.file;
  return file ? { file, offset: progress.get(file.sha256) ?? 0, catalogAt: catalog.at } : undefined;
}

async function sourcePriority(store: Store, root: string, file: CatalogFile) {
  const range = file.path.match(/(?:^|\/)(\d+)-(\d+)-[^/]+\.parquet$/);
  if (range) return Number(range[2]);
  // Older compacted files have no range in their name. Cache an immutable
  // source's actual newest slot so newly written history cannot displace live data.
  const key = `source-priority/${file.sha256}`;
  let slot = await store.get<number>(key);
  if (slot === undefined) {
    const [bounds] = await store.rows(`SELECT max(slot) AS newest FROM read_parquet(${sqlString(sourcePath(root,file.path))})`);
    slot = Number(bounds.newest ?? 0); await store.set(key,slot);
  }
  return slot;
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
