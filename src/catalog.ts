import { writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { json, now } from './config.ts';
import type { Store } from './store.ts';

export interface PublishedRange { from: number; to: number }
export interface CatalogFile {
  path: string; table_name: string; epoch: number; row_count: number;
  sha256: string; created_at: string;
}
export interface Catalog {
  at: string; coverage: PublishedRange[]; files: CatalogFile[];
  acceptance: string;
}

export function mergeCoverage(ranges: PublishedRange[]) {
  const merged: PublishedRange[] = [];
  for (const range of [...ranges].sort((a, b) => a.from - b.from)) {
    const last = merged.at(-1);
    if (last && range.from <= last.to + 1) last.to = Math.max(last.to, range.to);
    else merged.push({ ...range });
  }
  return merged;
}

export async function writeCatalog(store: Store, root: string) {
  // Snapshot registrations and coverage under the same writer lock.
  const catalog = await store.exclusive(async connection => {
    const files = await connection.runAndReadAll("SELECT * EXCLUDE(status) FROM files WHERE status='active'");
    const ranges = await connection.runAndReadAll('SELECT slot_from, slot_to FROM published_ranges');
    return { at: now(), files: files.getRowObjectsJson() as unknown as CatalogFile[],
      coverage: mergeCoverage(ranges.getRowObjectsJson().map(row => ({ from: Number(row.slot_from), to: Number(row.slot_to) }))),
      acceptance: 'published with fetch/order checks; independent validation and sealing pending' } satisfies Catalog;
  });
  await writeFile(join(root, 'catalog.json.tmp'), json(catalog) + '\n');
  await rename(join(root, 'catalog.json.tmp'), join(root, 'catalog.json'));
  return catalog;
}
