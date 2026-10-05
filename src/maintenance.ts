import type { Store } from './store.ts';
import { log,type Config } from './config.ts';
import { compact } from './compactor.ts';
import { collectRetired } from './garbage-collector.ts';
import { prunePublished } from './retention.ts';
import { collectLegacy } from './legacy-gc.ts';
import { reclaimCheckpoint } from './checkpoint-maintenance.ts';

export async function maintain(store: Store,config: Config,all=false,legacy=false) {
  await compact(store,config.dataDir);
  log('checkpoint_trim_started',{all});
  let trimmedRows=0,ranges=0;
  for(;;) {
    const result=await prunePublished(store,config.dataDir,config.checkpointHotSlots,all ? 1000000000 : 16000);
    trimmedRows+=result.removedRows;ranges+=result.ranges;
    if(result.ranges) log('checkpoint_trim_progress',{removedRows:result.removedRows,ranges:result.ranges});
    if(!all || result.ranges===0) break;
  }
  const garbage=await collectRetired(store,config.dataDir,config.garbageGraceSeconds);
  if(legacy) log('legacy_cleanup_started');
  const legacyResult=legacy ? await collectLegacy(store,config.dataDir) : undefined;
  await store.exec('CHECKPOINT');
  const checkpoint=await reclaimCheckpoint(store);
  const result={at:new Date().toISOString(),trimmedRows,ranges,...garbage,legacy:legacyResult,checkpoint};
  await store.set('maintenance',result);
  return result;
}
