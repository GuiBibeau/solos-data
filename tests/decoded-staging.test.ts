import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../src/store.ts';
import { schema } from '../src/decode/schema.ts';
import { emptyRows } from '../src/decode/normalize.ts';
import { publish } from '../src/decode/publish.ts';

test('repeated decoded batches release previous temporary payload storage',async()=>{
  const root=await mkdtemp(join(tmpdir(),'decoded-staging-'));
  const store=await Store.open(root,schema);
  try {
    for(let batch=0;batch<40;batch++) {
      const rows=emptyRows();
      for(let i=0;i<500;i++) rows.events.push({signature:`s-${batch}-${i}`,source_hash:'fixture',slot:100,
        event_id:`e-${i}`,event_json:'payload-'.repeat(1024)} as any);
      await publish(store,rows,{hash:`file-${batch}`,path:'raw',offset:500});
    }
    const [memory]=await store.rows(`SELECT sum(memory_usage_bytes+temporary_storage_bytes) AS bytes
      FROM duckdb_memory() WHERE tag='IN_MEMORY_TABLE'`);
    assert.ok(Number(memory.bytes)<16*1024**2,`staging retained ${memory.bytes} bytes`);
    assert.equal((await store.rows('SELECT count(*) AS n FROM events'))[0].n,'500');
  } finally {await store.close();await rm(root,{recursive:true,force:true});}
});
