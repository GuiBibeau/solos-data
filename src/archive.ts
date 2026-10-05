import type { DuckDBConnection } from '@duckdb/node-api';
import { fileHash, sqlString } from './writer.ts';
import type { Store } from './store.ts';

export async function registerBounds(store: Store) {
  const missing=await store.rows(`SELECT f.path FROM files f LEFT JOIN file_bounds b USING(path)
    WHERE f.status='active' AND b.path IS NULL`);
  for(const file of missing) {
    const [bounds]=await store.rows(`SELECT min(slot) AS first,max(slot) AS last FROM read_parquet(${sqlString(file.path)})`);
    await store.exec('INSERT OR IGNORE INTO file_bounds VALUES (?, ?, ?)',[file.path,Number(bounds.first),Number(bounds.last)]);
  }
}

export async function checkpointDigest(connection:DuckDBConnection,table:string,archive=false) {
  const columns=(await connection.runAndReadAll(`SELECT name FROM pragma_table_info('${table}') ORDER BY cid`)).getRowObjectsJson();
  const fields=columns.map(c=>`${c.name}:=${archive && table==='rpc_pages' && c.name==='page_id' ? 'signature' : c.name}::VARCHAR`).join(',');
  return `sha256(to_json(struct_pack(${fields})))`;
}

export async function archiveRelation(connection: DuckDBConnection, table: string, from: number, to: number,
  verified: Set<string>,digests=false) {
  const result=await connection.runAndReadAll(`SELECT f.* FROM files f JOIN file_bounds b USING(path)
    WHERE f.status='active' AND f.table_name=? AND b.slot_from<=? AND b.slot_to>=?`,[table,to,from]);
  const files=result.getRowObjectsJson();
  for(const f of files) if(!verified.has(String(f.sha256))) {
    if(await fileHash(String(f.path))!==f.sha256) throw new Error('archive checksum mismatch');
    const count=await connection.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${sqlString(String(f.path))})`);
    if(Number(count.getRowObjectsJson()[0].n)!==Number(f.row_count)) throw new Error('archive row count mismatch');
    verified.add(String(f.sha256));
  }
  if(!files.length) return undefined;
  const paths=`[${files.map(f=>sqlString(String(f.path))).join(',')}]`;
  if(digests) {
    const digest=await checkpointDigest(connection,table,true);
    // Compute hashes before the revision window: its working set has only keys/hashes,
    // never the transaction or RPC payload strings for the entire historical range.
    return `SELECT p.signature,p.digest FROM (SELECT signature,${digest} AS digest,filename
      FROM read_parquet(${paths},filename=true,union_by_name=true) WHERE slot BETWEEN ${from} AND ${to}) p
      JOIN files f ON f.path=p.filename QUALIFY row_number() OVER
      (PARTITION BY p.signature ORDER BY f.created_at DESC,p.filename DESC)=1`;
  }
  const columns=(await connection.runAndReadAll(`SELECT name FROM pragma_table_info('${table}') ORDER BY cid`)).getRowObjectsJson();
  const projection=columns.map(c=>table==='rpc_pages' && c.name==='page_id' ? 'p.signature AS page_id' : `p.${c.name}`).join(',');
  const canonical=`SELECT ${projection} FROM read_parquet(${paths},filename=true,union_by_name=true) p
    JOIN files f ON f.path=p.filename WHERE p.slot BETWEEN ${from} AND ${to}
    QUALIFY row_number() OVER(PARTITION BY p.signature ORDER BY f.created_at DESC,p.filename DESC)=1`;
  return canonical;
}
