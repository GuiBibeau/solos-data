import { rm,stat } from 'node:fs/promises';
import type { Store } from './store.ts';
import { archiveRelation,registerBounds,checkpointDigest } from './archive.ts';
import { writeCatalog } from './catalog.ts';
import { hasReaders } from './read-lease.ts';
import { fileHash,sqlString } from './writer.ts';

/** One-time migration of old superseded files that predate replacement lineage. */
export async function collectLegacy(store:Store,root:string) {
  await registerBounds(store);await writeCatalog(store,root);
  if(await hasReaders(root)) return {removedFiles:0,removedBytes:0,retainedFiles:0};
  return store.exclusive(async connection=>{
    const files=(await connection.runAndReadAll(`SELECT * FROM files WHERE status='superseded'
      AND path NOT IN (SELECT path FROM retired_files)`)).getRowObjectsJson();
    const verified=new Set<string>();
    let removedFiles=0,removedBytes=0,retainedFiles=0;
    for(const f of files) {
      const path=String(f.path),table=String(f.table_name);
      const bytes=await stat(path).then(s=>s.size).catch(e=>{if(e.code==='ENOENT')return 0;throw e;});
      if(!bytes) {await connection.run("UPDATE files SET status='deleted' WHERE path=?",[path]);continue;}
      if(await fileHash(path)!==f.sha256) throw new Error('legacy source checksum mismatch');
      const b=(await connection.runAndReadAll(`SELECT min(slot) AS first,max(slot) AS last FROM read_parquet(${sqlString(path)})`)).getRowObjectsJson()[0];
      const archive=await archiveRelation(connection,table,Number(b.first),Number(b.last),verified,true);
      if(!archive) {retainedFiles++;continue;}
      const digest=await checkpointDigest(connection,table,true);
      const n=(await connection.runAndReadAll(`SELECT count(*) AS n FROM (SELECT signature,${digest} AS digest
        FROM read_parquet(${sqlString(path)}) EXCEPT (${archive}))`)).getRowObjectsJson()[0];
      if(Number(n.n)) {retainedFiles++;continue;}
      await rm(path);await connection.run("UPDATE files SET status='deleted' WHERE path=?",[path]);
      removedFiles++;removedBytes+=bytes;
    }
    return {removedFiles,removedBytes,retainedFiles};
  });
}
