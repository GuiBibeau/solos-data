import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { json, loadConfig, log, providerUrl } from './config.ts';
import { Limiter } from './limiter.ts';
import { Provider } from './rpc.ts';
import { Store } from './store.ts';
import { run, safeError } from './service.ts';
import { probe } from './probe.ts';
import { verifyFiles } from './writer.ts';
import { capabilities } from './capabilities.ts';
import { queryDataset } from './reader.ts';
import { relocate } from './relocate.ts';
import { benchmarkDecoder, benchmarkOrdering, benchmarkIngest } from './diagnostics.ts';

async function main() {
  const command = process.argv[2] ?? 'help';
  if (command === 'help' || command === '--help') {
    console.log(json({ commands: {
      run: 'Supervised finalized-only collector (tail + backfill)',
      probe: 'Bounded live sizing; isolated in dataDir/probe',
      capabilities: 'Probe Alchemy extensions and oldest indexed program transactions',
      status: 'Read supervisor status snapshot without opening the live writer',
      catalog: 'Read published file/coverage catalog while collection runs',
      query: 'Query published Parquet data while collection runs: --sql SELECT ... or --sql-file path',
      'verify-storage': 'Offline hash/count verification; stop the service first',
      relocate: 'Offline rebase and verify raw file registrations after moving the data root; stop the service first',
      'benchmark-decoder': 'Offline published-file resume benchmark: --mode legacy|bounded',
      'benchmark-ordering': 'Offline checkpoint benchmark with rolled-back writes: --mode legacy|bounded; stop writer first',
      'benchmark-ingest': 'Offline duplicate-page insert with rolled-back writes; stop writer first',
    }, config: '--config path or SOLOS_DATA_CONFIG; credentials: SOLANA_RPC_URL only' })); return;
  }
  const index = process.argv.indexOf('--config');
  const config = await loadConfig(index === -1 ? process.env.SOLOS_DATA_CONFIG : process.argv[index + 1]);
  const modeIndex = process.argv.indexOf('--mode');
  const mode = modeIndex === -1 ? 'bounded' : process.argv[modeIndex + 1];
  if (command.startsWith('benchmark-') && !['legacy', 'bounded'].includes(mode)) throw new Error('invalid benchmark mode');
  if (command === 'benchmark-decoder') { console.log(json(await benchmarkDecoder(config.dataDir, mode))); return; }
  if (['status', 'catalog'].includes(command)) {
    console.log(await readFile(join(config.dataDir, command + '.json'), 'utf8')); return;
  }
  if (command === 'query') {
    const fileIndex = process.argv.indexOf('--sql-file');
    const sqlIndex = process.argv.indexOf('--sql');
    const sql = fileIndex === -1 ? process.argv[sqlIndex + 1] : await readFile(process.argv[fileIndex + 1], 'utf8');
    if (fileIndex === -1 && sqlIndex === -1) throw new Error('query requires --sql or --sql-file');
    console.log(json(await queryDataset(config.dataDir, sql))); return;
  }
  if (command === 'probe') config.dataDir = join(config.dataDir, 'probe');
  if (!['run', 'probe', 'capabilities', 'verify-storage', 'relocate', 'benchmark-ordering', 'benchmark-ingest'].includes(command)) throw new Error('Unknown command');
  const store = await Store.open(config.dataDir);
  try {
    if (command === 'benchmark-ordering') { console.log(json(await benchmarkOrdering(store, mode))); return; }
    if (command === 'benchmark-ingest') { console.log(json(await benchmarkIngest(store,mode))); return; }
    if (command === 'verify-storage') { console.log(json(await verifyFiles(store))); return; }
    if (command === 'relocate') { console.log(json(await relocate(store))); return; }
    const limiter = new Limiter(config.cuPerSecond * config.utilization, config.tailShare, config.concurrency);
    const provider = new Provider(providerUrl(), config, limiter);
    if (command === 'capabilities') { console.log(json(await capabilities(provider, config))); return; }
    if (command === 'probe') console.log(json(await probe(provider, store, config)));
    else await run(provider, store, config);
  } finally { await store.close(); }
}

main().catch(error => { log('fatal', { error: safeError(error) }); process.exitCode = 1; });
