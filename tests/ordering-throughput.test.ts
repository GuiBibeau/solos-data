import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, FixtureRpc } from './fixtures.ts';
import { orderRange } from '../src/ordering.ts';
import { validateRange } from '../src/validation.ts';

test('ordering a 256-slot range uses one durable commit and retains block index checks',async()=>{
  const f=await fixture();
  try {
    await f.store.exec(`INSERT INTO signatures SELECT 'a-'||i,i,i,'null',['p'],'backfill','c','fixture' FROM range(256) r(i)`);
    await f.store.exec(`INSERT INTO signatures SELECT 'b-'||i,i,i,'null',['p'],'backfill','c','fixture' FROM range(256) r(i)`);
    await f.store.exec(`INSERT INTO transactions SELECT signature,slot,slot,NULL,NULL,'null',5000,10,
      'AA==','{}','{}','backfill','fixture','fixture',NULL FROM signatures`);
    const blocks=Object.fromEntries(Array.from({length:256},(_,i)=>[i,['unrelated',`b-${i}`,`a-${i}`]]));
    const before=f.store.timings['COMMIT:']?.calls??0;
    await orderRange(new FixtureRpc([],blocks),f.store,'backfill',0,255);
    assert.equal(f.store.timings['COMMIT:'].calls-before,1);
    assert.equal((await validateRange(f.store,0,255)).ok,true);
  } finally { await f.close(); }
});
