import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Store } from '../src/store.ts';

test('SIGKILL before a checkpoint recovers committed manifest/progress and rolls back unfinished work', async () => {
  const root = await mkdtemp(join(tmpdir(),'collector-crash-'));
  const child = spawn(process.execPath,[fileURLToPath(new URL('crash-writer.fixture.ts',import.meta.url)),root],
    { stdio:['ignore','pipe','pipe'] });
  try {
    await new Promise<void>((resolve,reject) => {
      const fail = () => { clearTimeout(timeout); reject(new Error('fixture writer did not become ready')); };
      const timeout = setTimeout(fail,10000);
      child.once('exit',fail); child.once('error',fail);
      child.stdout.once('data',() => {
        clearTimeout(timeout); child.off('exit',fail); child.off('error',fail); resolve();
      });
    });
    assert.ok((await stat(join(root,'checkpoint.duckdb.wal'))).size>0);
    const exited = once(child,'exit'); child.kill('SIGKILL'); await exited;
    const store = await Store.open(root);
    try {
      assert.equal(await store.get('durable'),true);
      assert.equal(await store.get('unfinished'),undefined);
      assert.equal((await store.rows('SELECT signature FROM signatures'))[0].signature,'durable');
    } finally { await store.close(); }
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child,'exit'); child.kill('SIGKILL'); await exited;
    }
    await rm(root,{recursive:true,force:true});
  }
});
