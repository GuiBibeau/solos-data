import { stat,rm,rename } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { sqlString,syncPath } from './writer.ts';

/** Offline only. Verified replacement is atomically renamed over the closed checkpoint. */
export async function repackCheckpoint(root:string) {
  const source=join(root,'checkpoint.duckdb'),candidate=join(root,'checkpoint.repack.duckdb');
  const bytesBefore=(await stat(source)).size;
  const instance=await DuckDBInstance.create(':memory:',{threads:'4',memory_limit:process.env.SOLOS_DATA_DB_MEMORY??'4GB'});
  const connection=await instance.connect();
  try {
    // A read-write attachment holds DuckDB's process lock throughout verification.
    await connection.run(`ATTACH ${sqlString(source)} AS original`);
    await connection.run('CHECKPOINT original');
    await rm(candidate,{force:true});await rm(candidate+'.wal',{force:true});
    await connection.run(`ATTACH ${sqlString(candidate)} AS packed`);
    await connection.run('COPY FROM DATABASE original TO packed');
    const tables=(await connection.runAndReadAll("SELECT table_name FROM duckdb_tables() WHERE database_name='original' AND NOT temporary")).getRowObjectsJson();
    for(const table of tables) {
      const name=String(table.table_name).replaceAll('"','""');
      const mismatch=await connection.runAndReadAll(`SELECT count(*) AS n FROM (
        (SELECT sha256(to_json(t)) FROM original."${name}" t EXCEPT ALL SELECT sha256(to_json(t)) FROM packed."${name}" t) UNION ALL
        (SELECT sha256(to_json(t)) FROM packed."${name}" t EXCEPT ALL SELECT sha256(to_json(t)) FROM original."${name}" t))`);
      if(Number(mismatch.getRowObjectsJson()[0].n)) throw new Error('repacked checkpoint data mismatch');
    }
    await connection.run('CHECKPOINT packed');
  } finally {connection.closeSync();instance.closeSync();}
  if((await stat(source+'.wal').catch(e=>{if(e.code==='ENOENT')return {size:0};throw e;})).size)
    throw new Error('checkpoint WAL remains; refusing replacement');
  await syncPath(candidate);
  const bytesAfter=(await stat(candidate)).size;
  await rename(candidate,source);await syncPath(root);
  return {ok:true,bytesBefore,bytesAfter,reclaimedBytes:bytesBefore-bytesAfter};
}
