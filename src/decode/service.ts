import { createHash } from 'node:crypto';
import { writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { now, sleep, log } from '../config.ts';
import { Store } from '../store.ts';
import { schema, tables, version } from './schema.ts';
import { Codec, type DecodedGroup } from './codec.ts';
import { extractGroups } from './instructions.ts';
import { emptyRows, normalize } from './normalize.ts';
import { nextSource, sourceRows } from './source.ts';
import { publish, recover } from './publish.ts';

export interface DecoderConfig { rawDir: string; dataDir: string; codecPath: string; batchSize: number; pollMs: number }
export async function runDecoder(config: DecoderConfig, once = false) {
  const store = await Store.open(config.dataDir, schema);
  const codec = new Codec(config.codecPath);
  const verified = new Set<string>();
  let stopping = false;
  const stop = () => { stopping = true; };
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
  try {
    await recover(store);
    while (!stopping) {
      const source = await nextSource(store, config.rawDir);
      if (!source) {
        await snapshot(store, { idle: true });
        if (once) break;
        await sleep(config.pollMs); continue;
      }
      const raw = await sourceRows(store, config.rawDir, source.file, source.offset, config.batchSize, verified);
      const rows = emptyRows();
      const seen: { signature: string; source_hash: string }[] = [];
      for (const tx of raw) {
        const hash = createHash('sha256').update(JSON.stringify([version, tx.tx_b64, tx.meta_json,
          tx.tx_index, tx.single_in_slot, tx.terminal_error, tx.err])).digest('hex');
        if (tx.previous_at && tx.previous_at > source.file.created_at) continue;
        seen.push({ signature:tx.signature, source_hash:hash });
        if (hash === tx.previous_hash) continue;
        let decoded: DecodedGroup[];
        try {
          if (!tx.tx_b64 || !tx.meta_json || tx.terminal_error) throw new Error('missing raw transaction or terminal fetch error');
          const groups = extractGroups(tx.tx_b64, JSON.parse(tx.meta_json));
          decoded = await codec.decode(groups);
        } catch {
          if (codec.dead) throw codec.dead;
          decoded = [{ path: 'extraction', logs: [], attribution: 'unknown', events: [],
            errors: [{ error: 'transaction instruction extraction failed', bytes: tx.tx_b64 ?? '' }] }];
        }
        let normalized;
        try { normalized = normalize(tx, hash, decoded, source.file.sha256); }
        catch {
          normalized = normalize(tx, hash, [{ path: 'validation', logs: [], attribution: 'unknown', events: [],
            errors: [{ error: 'event context validation failed', bytes: tx.tx_b64 ?? '' }] }], source.file.sha256);
        }
        for (const table of tables) rows[table].push(...normalized[table]);
      }
      await publish(store, rows, { hash: source.file.sha256, path: source.file.path, offset: source.offset + raw.length,
        at:source.file.created_at, seen });
      await snapshot(store, { idle: false, sourceCatalogAt: source.catalogAt, batchTransactions: rows.decoded_transactions.length,
        batchEvents: rows.events.length, batchFills: rows.fills.length,
        batchQuarantined: rows.decoded_transactions.filter(row => row.status === 'quarantined').length });
      if (once) break;
    }
  } finally {
    process.off('SIGTERM', stop); process.off('SIGINT', stop);
    await codec.close(); await store.close();
  }
}

async function snapshot(store: Store, progress: Record<string, unknown>) {
  const [counts] = await store.rows(`SELECT count(*) AS transactions_processed FROM processed`);
  const files = await store.rows('SELECT table_name, sum(row_count) AS published_rows, count(*) AS files FROM files GROUP BY table_name');
  const value = { at: now(), decoderVersion: version, ...counts, ...progress, tables: files,
    storageTimings:store.timings };
  await writeFile(join(store.root, 'status.json.tmp'), JSON.stringify(value) + '\n');
  await rename(join(store.root, 'status.json.tmp'), join(store.root, 'status.json'));
  if (!progress.idle) log('decoded_batch', { ...progress, ...counts });
}
