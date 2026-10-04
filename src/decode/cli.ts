import { readFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { runDecoder } from './service.ts';
import { queryDecoded } from './reader.ts';
import { safeError } from '../service.ts';

async function main() {
  const command = process.argv[2] ?? 'help';
  if (['help', '--help'].includes(command)) {
    console.log(JSON.stringify({ commands: ['once', 'watch', 'status', 'query --sql SELECT ...'],
      config: '--config config/decoded.json; no RPC credential required' })); return;
  }
  const index = process.argv.indexOf('--config');
  const config = JSON.parse(await readFile(index === -1 ? 'config/decoded.json' : process.argv[index + 1], 'utf8'));
  config.rawDir = resolve(process.env.SOLOS_DATA_RAW_DIR ?? config.rawDir);
  config.dataDir = resolve(process.env.SOLOS_DATA_DECODED_DIR ?? config.dataDir);
  config.codecPath = resolve(process.env.SOLOS_DATA_CODEC ?? config.codecPath);
  if (!Number.isInteger(config.batchSize) || config.batchSize < 1 || config.batchSize > 10000) throw new Error('invalid batchSize');
  if (!Number.isInteger(config.pollMs) || config.pollMs < 100) throw new Error('invalid pollMs');
  if (config.rawDir === config.dataDir) throw new Error('raw and decoded roots must differ');
  if (command === 'status') { console.log(await readFile(join(config.dataDir, 'status.json'), 'utf8')); return; }
  if (command === 'query') {
    const sqlIndex = process.argv.indexOf('--sql');
    if (sqlIndex === -1) throw new Error('query requires --sql');
    console.log(JSON.stringify(await queryDecoded(config.dataDir, process.argv[sqlIndex + 1]))); return;
  }
  if (!['once', 'watch'].includes(command)) throw new Error('unknown decoder command');
  await runDecoder(config, command === 'once');
}
main().catch(error => { console.error(JSON.stringify({ at: new Date().toISOString(), event: 'decoder_fatal',
  error: safeError(error) })); process.exitCode = 1; });
