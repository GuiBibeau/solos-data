import { rm,stat } from 'node:fs/promises';
import { resolve,relative } from 'node:path';
import type { Store } from './store.ts';
import { hasReaders } from './read-lease.ts';
import { writeCatalog } from './catalog.ts';
import { fileHash,sqlString } from './writer.ts';

/** A registered replacement and a durable catalog must precede any unlink. */
export async function collectRetired(store: Store,root: string,minimumAgeSeconds=600,
  publishCatalog:(store:Store,root:string)=>Promise<unknown>=writeCatalog) {
  await publishCatalog(store,root);
  if(await hasReaders(root)) return {removedFiles:0,removedBytes:0,readers:true};
  return store.exclusive(async connection=>{
    const retired=(await connection.runAndReadAll('SELECT * FROM retired_files')).getRowObjectsJson();
    const links=new Map(retired.map(f=>[String(f.path),String(f.replacement_path)]));
    const removed=new Set((await connection.runAndReadAll('SELECT path FROM garbage_removed')).getRowObjectsJson().map(f=>String(f.path)));
    const active=new Map((await connection.runAndReadAll('SELECT * FROM files')).getRowObjectsJson()
      .filter(f=>f.status===undefined || f.status==='active').map(f=>[String(f.path),f]));
    const verified=new Set<string>();
    let removedFiles=0,removedBytes=0;
    const inside=(path:string)=>{
      const full=resolve(root,path),suffix=relative(resolve(root),full);
      if(suffix==='..'||suffix.startsWith('../')) throw new Error('retired file outside data root');
      return full;
    };
    for(const f of retired) {
      if(removed.has(String(f.path))) continue;
      if(Date.now()-Date.parse(String(f.retired_at))<minimumAgeSeconds*1000) continue;
      let replacement=String(f.replacement_path);
      const seen=new Set<string>([String(f.path)]);
      while(!active.has(replacement) && links.has(replacement)) {
        if(seen.has(replacement)) throw new Error('retired replacement cycle');
        seen.add(replacement); replacement=links.get(replacement)!;
      }
      const target=active.get(replacement);
      if(!target) continue;
      if(!verified.has(replacement)) {
        const path=inside(replacement);
        if(await fileHash(path)!==target.sha256) throw new Error('replacement checksum mismatch');
        const n=await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
        if(Number(n.getRowObjectsJson()[0].n)!==Number(target.row_count)) throw new Error('replacement row count mismatch');
        verified.add(replacement);
      }
      const path=inside(String(f.path));
      const bytes=await stat(path).then(s=>s.size).catch(e=>{if(e.code==='ENOENT')return 0;throw e;});
      if(!bytes) {await connection.run('INSERT OR IGNORE INTO garbage_removed VALUES (?, ?)',[String(f.path),new Date().toISOString()]);continue;}
      await rm(path,{force:true}); removedFiles++; removedBytes+=bytes;
      await connection.run('INSERT OR IGNORE INTO garbage_removed VALUES (?, ?)',[String(f.path),new Date().toISOString()]);
      // Keep the small replacement link: descendants can still resolve through it after restart.
    }
    return {removedFiles,removedBytes,readers:false};
  });
}
