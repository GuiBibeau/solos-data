import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm,access } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../src/store.ts';
import { schema } from '../src/decode/schema.ts';
import { emptyRows } from '../src/decode/normalize.ts';
import { publish,recover,writeCatalog } from '../src/decode/publish.ts';
import { compactDecoded } from '../src/decode/compactor.ts';
import { queryDecoded } from '../src/decode/reader.ts';
import { collectRetired } from '../src/garbage-collector.ts';

test('decoded compaction and cleanup retain history and corrections after restart',async()=>{
  const root=await mkdtemp(join(tmpdir(),'decoded-compact-'));
  let store=await Store.open(root,schema);
  try {
    for(let i=0;i<40;i++) {
      const rows=emptyRows();
      rows.decoded_transactions.push({signature:`s-${i%20}`,source_hash:`h-${i}`,slot:100+i%20,
        tx_index:0,single_in_slot:false,block_time:100,committed:true,status:'decoded',event_count:1,
        error_count:0,source_file:'fixture',decoded_at:'fixture',decoder_version:'fixture'});
      rows.fills.push({signature:`s-${i%20}`,source_hash:`h-${i}`,slot:100+i%20,event_id:`e-${i%20}`} as any);
      await publish(store,rows,{hash:`file-${i}`,path:'raw',offset:1,at:String(i).padStart(4,'0')});
    }
    const before=await queryDecoded(root,'SELECT signature,source_hash FROM fills ORDER BY signature');
    const originals=await store.rows('SELECT path FROM files');
    await compactDecoded(store);
    assert.ok((await store.rows('SELECT path FROM files')).length<originals.length);
    assert.deepEqual((await queryDecoded(root,'SELECT signature,source_hash FROM fills ORDER BY signature')).rows,before.rows);
    assert.ok((await collectRetired(store,root,0,writeCatalog)).removedFiles>0);
    await store.close();store=await Store.open(root,schema);await recover(store);
    assert.deepEqual((await queryDecoded(root,'SELECT signature,source_hash FROM fills ORDER BY signature')).rows,before.rows);
    const rows=emptyRows();
    rows.decoded_transactions.push({signature:'s-0',source_hash:'failed',slot:100,tx_index:0,
      single_in_slot:false,block_time:100,committed:false,status:'decoded',event_count:0,error_count:0,
      source_file:'fixture',decoded_at:'fixture',decoder_version:'fixture'});
    await publish(store,rows,{hash:'correction',path:'raw',offset:1,at:'9999'});
    assert.equal((await queryDecoded(root,"SELECT count(*) AS n FROM fills WHERE signature='s-0'")).rows[0].n,'0');
    assert.equal((await queryDecoded(root,'SELECT count(*) AS n FROM decoded_transactions')).rows[0].n,'20');
    for(const f of await store.rows('SELECT path FROM files')) await access(join(root,f.path));
  } finally {await store.close();await rm(root,{recursive:true,force:true});}
});
