import { stat } from 'node:fs/promises';
import { join } from 'node:path';
import type { Store } from './store.ts';

/** Checkpoint index churn can grow the file even when archived rows have been trimmed. */
export async function reclaimCheckpoint(store:Store,minimumBytes=16*1024**3) {
  const previous=await store.get('checkpoint-repack');
  const size=(await stat(join(store.root,'checkpoint.duckdb'))).size;
  if(size<Math.max(minimumBytes,Number(previous?.bytesAfter??0)*2)) return;
  const started=performance.now();
  const result=await store.repack();
  const value={at:new Date().toISOString(),seconds:(performance.now()-started)/1000,...result};
  await store.set('checkpoint-repack',value);
  return value;
}
