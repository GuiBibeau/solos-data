import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { sqlString } from '../writer.ts';
import { definitions, tables } from './schema.ts';
import { sourcePath } from './source.ts';

/** Read-only SQL over registered Parquet, with latest transaction revisions. */
export async function queryDecoded(root: string, sql: string) {
  if (!/^\s*(select|with)\b/i.test(sql) || sql.includes(';')) throw new Error('query accepts one SELECT or WITH statement');
  const catalog = JSON.parse(await readFile(join(root, 'catalog.json'), 'utf8'));
  const instance = await DuckDBInstance.create(':memory:', { threads: '4', memory_limit: '4GB' });
  const connection = await instance.connect();
  try {
    for (const table of tables) {
      const files = catalog.files.filter((file: any) => file.table_name === table);
      if (!files.length) { await connection.run(`CREATE TABLE ${table}(${definitions[table]})`); continue; }
      const paths = files.map((file: any) => sourcePath(root, file.path));
      await connection.run(`CREATE TABLE ${table}_registrations AS SELECT value->>'path' AS path,
        (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?::JSON)`,
        [JSON.stringify(files.map((file: any, i: number) => ({ ...file, path: paths[i] })))]);
      const base = `SELECT p.* EXCLUDE(filename), r.batch_id FROM read_parquet([${paths.map(sqlString).join(',')}],
        filename=true, union_by_name=true) p JOIN ${table}_registrations r ON r.path=p.filename`;
      const key = table === 'decoded_transactions' ? 'signature' : table === 'decode_errors' ? 'error_id' : 'event_id';
      const relation = table === 'decoded_transactions' ? `(${base}) p` : `(${base}) p
        JOIN decoded_transactions d ON d.signature=p.signature AND d.source_hash=p.source_hash`;
      await connection.run(`CREATE VIEW ${table} AS SELECT p.* EXCLUDE(batch_id) FROM ${relation}
        QUALIFY row_number() OVER(PARTITION BY p.${key} ORDER BY p.batch_id DESC)=1`);
    }
    const result = await connection.runAndReadAll(sql);
    return { catalogAt: catalog.at, decoderVersion: catalog.decoderVersion,
      acceptance: catalog.acceptance, rows: result.getRowObjectsJson() };
  } finally { connection.closeSync(); instance.closeSync(); }
}
