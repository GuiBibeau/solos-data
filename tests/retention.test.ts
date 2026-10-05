import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { fixture, insertTx, FixtureRpc, sig } from './fixtures.ts';
import { makeWalk } from '../src/pipeline.ts';
import { walkPage } from '../src/walker.ts';
import { publishRange } from '../src/writer.ts';
import { writeCatalog } from '../src/catalog.ts';
import { prunePublished } from '../src/retention.ts';
import { queryDataset } from '../src/reader.ts';
import { Store } from '../src/store.ts';

test('checkpoint trimming retains full archive, unpublished rows, tail overlap and walk cursor', async () => {
  const f = await fixture();
  try {
    await walkPage(new FixtureRpc([[sig('live',1000),sig('pending',90),sig('old',80)]]),
      f.store,'walk/backfill',makeWalk('p','backfill','c',0,1000));
    for (const [id,slot] of [['old',80],['pending',90],['live',1000]] as const) await insertTx(f.store,id,slot);
    await publishRange(f.store,f.root,80,80);
    await publishRange(f.store,f.root,1000,1000);
    await f.store.set('W',{slot:1000});
    const cursor = await f.store.get('walk/backfill');
    await prunePublished(f.store,f.root,200);
    assert.deepEqual((await f.store.rows('SELECT signature FROM transactions ORDER BY signature')).map(r=>r.signature),['live','pending']);
    assert.deepEqual(await f.store.get('walk/backfill'),cursor);
    assert.deepEqual((await queryDataset(f.root,'SELECT signature FROM transactions ORDER BY signature')).rows.map(r=>r.signature),['live','old']);
    assert.equal((await f.store.rows("SELECT count(*) AS n FROM signatures WHERE signature='pending'"))[0].n,'1');
    await prunePublished(f.store,f.root,200);
    assert.deepEqual((await queryDataset(f.root,'SELECT signature FROM transactions ORDER BY signature')).rows.map(r=>r.signature),['live','old']);
  } finally { await f.close(); }
});

test('large archive trimming compares payload hashes within a bounded memory budget',async()=>{
  const f=await fixture();
  try {
    await f.store.exec(`INSERT INTO transactions SELECT md5(i::VARCHAR)||md5(i::VARCHAR),i+100,i,NULL,NULL,'null',5000,10,
      repeat(md5(i::VARCHAR),2),repeat(md5(i::VARCHAR),50),'{}','backfill','fixture','fixture',NULL FROM range(300000) r(i)`);
    await publishRange(f.store,f.root,100,300099);await f.store.set('W',{slot:400000});
    await f.store.close();f.store=await Store.open(f.root);
    await f.store.exec("SET memory_limit='384MB'");await f.store.exec('SET threads=1');
    const result=await prunePublished(f.store,f.root,200,1000000000);
    assert.equal(result.removedRows,300000);
    assert.equal((await queryDataset(f.root,'SELECT count(*) AS n FROM transactions')).rows[0].n,'300000');
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'0');
  } finally {await f.store.close();const {rm}=await import('node:fs/promises');await rm(f.root,{recursive:true,force:true});}
});

test('checkpoint trimming rejects corrupt archives and changed unpublished payloads', async () => {
  const f = await fixture();
  try {
    await insertTx(f.store,'old',80); await publishRange(f.store,f.root,80,80);
    await f.store.set('W',{slot:1000});
    await f.store.exec("UPDATE transactions SET meta_json='changed' WHERE signature='old'");
    await assert.rejects(prunePublished(f.store,f.root,200),/archive.*match|unarchived/i);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'1');
    await f.store.exec("UPDATE transactions SET meta_json='{}'");
    const [file] = await f.store.rows("SELECT path FROM files WHERE table_name='transactions'");
    const bytes = await readFile(file.path); await writeFile(file.path,'corrupt');
    await assert.rejects(prunePublished(f.store,f.root,200),/checksum/i);
    await writeFile(file.path,bytes);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'1');
    await prunePublished(f.store,f.root,200);
    assert.equal((await f.store.rows('SELECT count(*) AS n FROM transactions'))[0].n,'0');
    const catalog=JSON.parse(await readFile(f.root+'/catalog.json','utf8'));
    assert.ok(catalog.coverage.some((r:any)=>r.from===80 && r.to===80));
  } finally { await f.close(); }
});


test('continuous older backfill arrivals do not starve other cold checkpoint ranges',async()=>{
  const f=await fixture();
  try {
    await f.store.set('W',{slot:1000000});
    for(const slot of [10,50000,100000]) {
      await insertTx(f.store,`initial-${slot}`,slot);await publishRange(f.store,f.root,slot,slot);
    }
    await prunePublished(f.store,f.root,10000,10);
    for(const slot of [9,8]) {
      await insertTx(f.store,`older-${slot}`,slot);await publishRange(f.store,f.root,slot,slot);
      await prunePublished(f.store,f.root,10000,10);
    }
    assert.equal((await f.store.rows("SELECT count(*) AS n FROM transactions WHERE signature LIKE 'initial-%'"))[0].n,'0');
    assert.equal((await queryDataset(f.root,'SELECT count(*) AS n FROM transactions')).rows[0].n,'5');
  } finally {await f.close();}
});
