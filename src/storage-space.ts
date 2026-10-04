import { statfs } from 'node:fs/promises';

/** Keep room for DuckDB recovery; a full disk must not turn into silent data loss. */
export async function requireStorageSpace(root: string) {
  const stats = await statfs(root);
  if (stats.bavail * stats.bsize < 20 * 1024 ** 3) throw new Error('Storage below 20 GiB reserve; collection paused');
}
