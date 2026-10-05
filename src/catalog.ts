import { writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { json, now } from './config.ts';
import type { Store } from './store.ts';
import { syncPath } from './writer.ts';

export interface PublishedRange { from: number; to: number }
export interface CatalogFile {
  path: string; table_name: string; epoch: number; row_count: number;
  sha256: string; created_at: string;
  parents?: {sha256:string;row_count:number}[];
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
  return store.exclusive(async connection => {
    const files = await connection.runAndReadAll("SELECT * EXCLUDE(status) FROM files WHERE status='active'");
    const ranges = await connection.runAndReadAll('SELECT slot_from, slot_to FROM published_ranges');
    const inputs=(await connection.runAndReadAll(`SELECT i.* FROM compaction_inputs i JOIN files f ON f.path=i.path
      WHERE f.status='active'`)).getRowObjectsJson();
    const lineage=new Map<string,{sha256:string;row_count:number}[]>();
    for(const row of inputs) {
      const parents=lineage.get(String(row.path))??[];
      parents.push({sha256:String(row.source_hash),row_count:Number(row.row_count)});lineage.set(String(row.path),parents);
    }
    const published=files.getRowObjectsJson() as unknown as CatalogFile[];
    for(const file of published) {
      const parents=lineage.get(file.path);
      if(parents?.length) file.parents=parents;
    }
    const catalog={ at: now(), files: published,
      coverage: mergeCoverage(ranges.getRowObjectsJson().map(row => ({ from: Number(row.slot_from), to: Number(row.slot_to) }))),
      acceptance: 'published with fetch/order checks; independent validation and sealing pending' } satisfies Catalog;
    await writeFile(join(root, 'catalog.json.tmp'), json(catalog) + '\n');
    await syncPath(join(root,'catalog.json.tmp'));
    await rename(join(root, 'catalog.json.tmp'), join(root, 'catalog.json'));
    await syncPath(root);
    return catalog;
  });
}
