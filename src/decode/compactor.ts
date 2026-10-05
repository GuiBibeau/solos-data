import { mkdir,rename,stat } from 'node:fs/promises';
import { join,dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { Store } from '../store.ts';
import { fileHash,sqlString,syncPath } from '../writer.ts';
import { now } from '../config.ts';
import { writeCatalog } from './publish.ts';

/** A newest prefix per table/epoch preserves revision precedence against excluded files. */
export async function compactDecoded(store: Store) {
  const groups=await store.rows(`SELECT table_name,regexp_extract(path,'epoch=([0-9]+)',1) AS epoch
    FROM files WHERE regexp_matches(path,'epoch=[0-9]+') GROUP BY table_name,epoch HAVING count(*)>=32`);
  let merges=0;
  for(const group of groups) await store.exclusive(async connection=>{
    const input=(await connection.runAndReadAll(`SELECT * FROM files WHERE table_name=?
      AND regexp_extract(path,'epoch=([0-9]+)',1)=? ORDER BY batch_id DESC,path DESC`,[group.table_name,group.epoch])).getRowObjectsJson();
    const files:typeof input=[];
    let bytes=0,rows=0;
    for(const file of input) {
      const size=(await stat(join(store.root,String(file.path)))).size;
      if(bytes+size>128*1024**2 || rows+Number(file.row_count)>500000) break;
      bytes+=size;rows+=Number(file.row_count);files.push(file);
    }
    if(files.length<32) return;
    const paths=`[${files.map(f=>sqlString(join(store.root,String(f.path)))).join(',')}]`;
    const registrations=JSON.stringify(files.map(f=>({path:join(store.root,String(f.path)),batch:Number(f.batch_id)})));
    const key=group.table_name==='decoded_transactions' ? 'signature' : group.table_name==='decode_errors' ? 'error_id' : 'event_id';
    const query=`SELECT p.* EXCLUDE(filename,epoch) FROM read_parquet(${paths},filename=true,union_by_name=true) p
      JOIN (SELECT value->>'path' AS path,(value->>'batch')::BIGINT AS batch FROM json_each(${sqlString(registrations)}::JSON)) r
      ON r.path=p.filename QUALIFY row_number() OVER(PARTITION BY p.${key} ORDER BY r.batch DESC,p.filename DESC)=1`;
    const relative=`tables/${group.table_name}/epoch=${group.epoch}/compact-${randomUUID()}.parquet`;
    const path=join(store.root,relative);
    await mkdir(dirname(path),{recursive:true});
    await connection.run(`COPY (${query} ORDER BY slot,signature) TO ${sqlString(path+'.tmp')} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    await syncPath(path+'.tmp');await rename(path+'.tmp',path);await syncPath(dirname(path));
    const count=await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(path)})`);
    const expected=await connection.runAndReadAll(`SELECT count(*) AS n FROM (${query})`);
    const n=Number(count.getRowObjectsJson()[0].n);
    if(n!==Number(expected.getRowObjectsJson()[0].n)) throw new Error('decoded compaction row count mismatch');
    const hash=await fileHash(path);
    await connection.run('BEGIN');
    try {
      for(const file of files) {
        await connection.run('DELETE FROM files WHERE path=?',[String(file.path)]);
        await connection.run('INSERT INTO retired_files VALUES (?, ?, ?)',[String(file.path),relative,now()]);
      }
      await connection.run('INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)',
        [relative,String(group.table_name),n,hash,Math.max(...files.map(f=>Number(f.batch_id))),now()]);
      await connection.run('COMMIT');merges++;
    } catch(error) {await connection.run('ROLLBACK');throw error;}
  });
  if(merges) await writeCatalog(store);
  return {merges};
}
