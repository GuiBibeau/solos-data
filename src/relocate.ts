import { relative, resolve } from 'node:path';
import { sqlString, tables, fileHash } from './writer.ts';
import { writeCatalog } from './catalog.ts';
import type { Store } from './store.ts';

/** Offline only: DuckDB's writer lock prevents rebasing an active collector. */
export async function relocate(store: Store) {
  const files = await store.rows('SELECT path, sha256, row_count FROM files');
  const replacements: { old: string; path: string }[] = [];
  for (const file of files) {
    let path = file.path;
    if (relative(store.root, path).startsWith('..')) {
      const marker = ['/staging/', ...tables.map(table => `/${table}/`)].find(marker => path.includes(marker));
      if (!marker) throw new Error('unrecognized registered path; relocation held');
      path = resolve(store.root, path.slice(path.indexOf(marker) + 1));
    }
    if (await fileHash(path) !== file.sha256) throw new Error('relocation checksum mismatch');
    const [count] = await store.rows(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
    if (String(count.n) !== String(file.row_count)) throw new Error('relocation row count mismatch');
    replacements.push({ old:file.path, path });
  }
  await store.transaction(async connection => {
    for (const item of replacements) await connection.run('UPDATE files SET path=? WHERE path=?', [item.path, item.old]);
  });
  await writeCatalog(store, store.root);
  return { ok:true, checkedFiles:files.length, relocated:replacements.filter(item => item.old !== item.path).length };
}
