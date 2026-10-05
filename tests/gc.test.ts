import test from 'node:test';
import assert from 'node:assert/strict';
import { access, readFile, writeFile } from 'node:fs/promises';
import { fixture, insertTx } from './fixtures.ts';
import { publishRange } from '../src/writer.ts';
import { compact } from '../src/compactor.ts';
import { collectRetired } from '../src/garbage-collector.ts';
import { withReadLease } from '../src/read-lease.ts';
import { queryDataset } from '../src/reader.ts';
import { collectLegacy } from '../src/legacy-gc.ts';

test('retired-file cleanup preserves an active reader and checks replacement integrity', async () => {
  const f=await fixture();
  try {
    await insertTx(f.store,'old',80);
    for(let i=0;i<10;i++) await publishRange(f.store,f.root,80,80);
    const originals=await f.store.rows("SELECT path FROM files WHERE table_name='transactions'");
    await compact(f.store,f.root);
    await withReadLease(f.root,async()=>{
      assert.equal((await collectRetired(f.store,f.root,0)).removedFiles,0);
      for(const p of originals) await access(p.path);
    });
    const [replacement]=await f.store.rows("SELECT path FROM files WHERE status='active' AND table_name='transactions'");
    const bytes=await readFile(replacement.path); await writeFile(replacement.path,'corrupt');
    await assert.rejects(collectRetired(f.store,f.root,0),/checksum/i);
    for(const p of originals) await access(p.path);
    await writeFile(replacement.path,bytes);
    assert.ok((await collectRetired(f.store,f.root,0)).removedFiles>=10);
    for(const p of originals) await assert.rejects(access(p.path));
    assert.deepEqual((await queryDataset(f.root,'SELECT signature FROM transactions')).rows,[{signature:'old'}]);
  } finally { await f.close(); }
});

test('legacy cleanup deletes only files whose complete payloads still exist in the canonical archive',async()=>{
  const f=await fixture();
  try {
    await insertTx(f.store,'old',80);
    for(let i=0;i<10;i++) await publishRange(f.store,f.root,80,80);
    await compact(f.store,f.root);
    await f.store.exec('DELETE FROM retired_files');
    const originals=await f.store.rows("SELECT path FROM files WHERE status='superseded'");
    await f.store.exec("UPDATE transactions SET meta_json='changed'");
    for(let i=0;i<10;i++) await publishRange(f.store,f.root,80,80);
    await compact(f.store,f.root);
    assert.ok((await collectLegacy(f.store,f.root)).retainedFiles>0);
    for(const f of originals) await access(f.path);
    await f.store.exec("UPDATE transactions SET meta_json='{}'");
    for(let i=0;i<10;i++) await publishRange(f.store,f.root,80,80);
    await compact(f.store,f.root);
    assert.ok((await collectLegacy(f.store,f.root)).removedFiles>0);
    for(const f of originals) await assert.rejects(access(f.path));
  } finally {await f.close();}
});
