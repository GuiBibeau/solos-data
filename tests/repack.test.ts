import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture,insertTx } from './fixtures.ts';
import { Store } from '../src/store.ts';
import { reclaimCheckpoint } from '../src/checkpoint-maintenance.ts';
import { repackCheckpoint } from '../src/repack.ts';

test('offline checkpoint repack preserves every row, cursor, view and primary key',async()=>{
  const f=await fixture();
  try {
    await insertTx(f.store,'kept',10);await f.store.set('backfill',{next:9});
    await f.store.exec("INSERT INTO published_ranges VALUES (10,10)");
    await f.store.close();
    assert.equal((await repackCheckpoint(f.root)).ok,true);
    f.store=await Store.open(f.root);
    assert.deepEqual(await f.store.get('backfill'),{next:9});
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM dataset_watermark'))[0].n,'1');
    await assert.rejects(insertTx(f.store,'kept',20),/key|constraint/i);
    assert.equal((await f.store.rows('SELECT meta_json FROM transactions'))[0].meta_json,'{}');
  } finally {await f.store.close(); const {rm}=await import('node:fs/promises');await rm(f.root,{recursive:true,force:true});}
});


test('writer rewrite queues concurrent database users and reopens its connection',async()=>{
  const f=await fixture();
  try {
    await insertTx(f.store,'before',10);
    const rewrite=f.store.repack();
    const queued=insertTx(f.store,'after',11);
    assert.equal((await rewrite).ok,true);await queued;
    assert.deepEqual((await f.store.rows('SELECT signature FROM transactions ORDER BY slot')).map(r=>r.signature),['before','after']);
  } finally {await f.close();}
});


test('automatic checkpoint reclamation keeps data and records a durable size baseline',async()=>{
  const f=await fixture();
  try {
    await insertTx(f.store,'kept',10);
    assert.equal((await reclaimCheckpoint(f.store,1))?.ok,true);
    assert.ok((await f.store.get('checkpoint-repack')).bytesAfter>0);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'1');
    assert.equal(await reclaimCheckpoint(f.store,16*1024**3),undefined);
  } finally {await f.close();}
});
