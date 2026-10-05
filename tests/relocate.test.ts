import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rename, rm } from 'node:fs/promises';
import { fixture, insertTx } from './fixtures.ts';
import { Store } from '../src/store.ts';
import { publishRange } from '../src/writer.ts';
import { relocate } from '../src/relocate.ts';
import { queryDataset } from '../src/reader.ts';
import { compact } from '../src/compactor.ts';
import { collectRetired } from '../src/garbage-collector.ts';

test('raw collector file registrations rebase after a move, before resuming or querying', async () => {
  const f = await fixture(); const moved = f.root + '-moved';
  try {
    await insertTx(f.store, 'sig', 100);
    await publishRange(f.store, f.root, 100, 100);
    await f.store.close(); await rename(f.root, moved);
    const store = await Store.open(moved);
    try { assert.equal((await relocate(store)).relocated, 1); }
    finally { await store.close(); }
    assert.equal(Number((await queryDataset(moved, 'SELECT count(*) AS n FROM transactions')).rows[0].n), 1);
  } finally { await rm(f.root, { recursive:true, force:true }); await rm(moved, { recursive:true, force:true }); }
});

test('relocation rebases archive bounds and removed-file lineage after cleanup',async()=>{
  const f=await fixture(),moved=f.root+'-moved';
  try {
    await insertTx(f.store,'sig',100);
    for(let i=0;i<10;i++) await publishRange(f.store,f.root,100,100);
    await compact(f.store,f.root);await collectRetired(f.store,f.root,0);
    await f.store.close();await rename(f.root,moved);
    const store=await Store.open(moved);
    try {
      await relocate(store);
      for(const row of await store.rows('SELECT path FROM file_bounds UNION SELECT path FROM retired_files')) assert.ok(row.path.startsWith(moved+'/'));
      assert.equal((await collectRetired(store,moved,0)).removedFiles,0);
    } finally {await store.close();}
    assert.equal((await queryDataset(moved,'SELECT count(*) AS n FROM transactions')).rows[0].n,'1');
  } finally {await rm(f.root,{recursive:true,force:true});await rm(moved,{recursive:true,force:true});}
});
