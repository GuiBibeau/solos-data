import { join } from 'node:path';
import type { Store } from '../store.ts';
import { fileHash,sqlString } from '../writer.ts';

export async function verifyDecoded(store:Store) {
  const files=await store.rows('SELECT * FROM files');
  for(const file of files) {
    const path=join(store.root,file.path);
    if(await fileHash(path)!==file.sha256) throw new Error('decoded file hash mismatch');
    const [count]=await store.rows(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
    if(Number(count.n)!==Number(file.row_count)) throw new Error('decoded file count mismatch');
  }
  return {files:files.length,ok:true};
}
