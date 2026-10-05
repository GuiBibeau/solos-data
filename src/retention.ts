import type { Store } from './store.ts';
import { writeCatalog,mergeCoverage } from './catalog.ts';
import { archiveRelation, registerBounds,checkpointDigest } from './archive.ts';
import { tables } from './writer.ts';

/** Delete only checkpoint copies; every source field remains in the verified raw archive. */
export async function prunePublished(store: Store,root: string,hotSlots: number,maxSlots=16000) {
  const W=await store.get('W');
  const tail=await store.get('tail-active');
  if(!W) return {ranges:0,removedRows:0};
  const cutoff=Math.min(Number(W.slot)-hotSlots,tail ? Number(tail.floor)-1 : Infinity);
  await registerBounds(store);
  await writeCatalog(store,root);
  const coverage=mergeCoverage((await store.rows('SELECT slot_from,slot_to FROM published_ranges'))
    .map(r=>({from:Number(r.slot_from),to:Math.min(Number(r.slot_to),cutoff)})).filter(r=>r.from<=r.to));
  const ranges:{first:number;last:number}[]=[];
  const cursor=Number(await store.get('retention-cursor')??0);
  const candidates=[...coverage.map(c=>({...c,from:Math.max(c.from,cursor)})).filter(c=>c.from<=c.to),...coverage];
  for(const covered of candidates) {
    const [hot]=await store.rows('SELECT min(slot) AS first FROM transactions WHERE slot BETWEEN ? AND ?',[covered.from,covered.to]);
    if(hot.first===null) continue;
    ranges.push({first:Number(hot.first),last:Math.min(covered.to,Number(hot.first)+maxSlots-1)});
    break;
  }
  const verified=new Set<string>();
  let removedRows=0;
  for(const range of ranges) await store.exclusive(async connection=>{
    await connection.run('BEGIN');
    try {
      for(const table of tables.filter(t=>t!=='program_versions')) {
        const from=Number(range.first),to=Number(range.last);
        const count=await connection.runAndReadAll(`SELECT count(*) AS n FROM ${table} WHERE slot BETWEEN ? AND ?`,[from,to]);
        const n=Number(count.getRowObjectsJson()[0].n);
        if(!n) continue;
        const archive=await archiveRelation(connection,table,from,to,verified,true);
        if(!archive) throw new Error('unarchived checkpoint rows');
        const digest=await checkpointDigest(connection,table);
        const key=table==='rpc_pages' ? 'page_id' : 'signature';
        const missing=await connection.runAndReadAll(`SELECT count(*) AS n FROM (
          SELECT ${key} AS signature,${digest} AS digest FROM ${table}
          WHERE slot BETWEEN ${from} AND ${to} EXCEPT (${archive}))`);
        if(Number(missing.getRowObjectsJson()[0].n)) throw new Error('archive does not match checkpoint rows');
        await connection.run(`DELETE FROM ${table} WHERE slot BETWEEN ? AND ?`,[from,to]);
        removedRows+=n;
      }
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)',
        [`retention/${range.first}-${range.last}`,JSON.stringify({at:new Date().toISOString(),archiveVerified:true})]);
      await connection.run('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)',['retention-cursor',String(range.last+1)]);
      await connection.run('COMMIT');
    } catch(error) { await connection.run('ROLLBACK'); throw error; }
  });
  const value={at:new Date().toISOString(),hotSlots,cutoff,ranges:ranges.length,removedRows,
    policy:'raw history retained in Parquet; only published checkpoint copies trimmed'};
  await store.set('retention',value);
  return value;
}
