import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { json } from './config.ts';
import { sqlString, tables } from './writer.ts';
import type { Catalog } from './catalog.ts';

/** Open registered immutable Parquet files in an independent in-memory database. */
export async function queryDataset(root: string, sql: string) {
  if (!/^\s*(select|with)\b/i.test(sql) || sql.includes(';')) throw new Error('query accepts one SELECT or WITH statement');
  const catalog: Catalog = JSON.parse(await readFile(join(root, 'catalog.json'), 'utf8'));
  const instance = await DuckDBInstance.create(':memory:', { threads: '4', memory_limit: '4GB' });
  const connection = await instance.connect();
  try {
    await connection.run(`CREATE TABLE registrations AS SELECT value->>'path' AS path,
      value->>'created_at' AS created_at FROM json_each(?::JSON)`, [json(catalog.files)]);
    await connection.run(`CREATE VIEW coverage AS SELECT (value->>'from')::BIGINT AS slot_from,
      (value->>'to')::BIGINT AS slot_to FROM json_each(${sqlString(json(catalog.coverage))}::JSON)`);
    const availableTables: string[] = [];
    for (const table of tables) {
      const paths = catalog.files.filter(file => file.table_name === table).map(file => file.path);
      if (!paths.length) continue;
      await connection.run(`CREATE VIEW ${table} AS SELECT p.* EXCLUDE(filename)
        FROM read_parquet([${paths.map(sqlString).join(',')}], filename=true, union_by_name=true) p
        JOIN registrations r ON r.path=p.filename QUALIFY row_number() OVER
        (PARTITION BY p.signature ORDER BY r.created_at DESC, p.filename DESC)=1`);
      availableTables.push(table);
    }
    const result = await connection.runAndReadAll(sql);
    return { catalogAt: catalog.at, coverage: catalog.coverage, availableTables,
      acceptance: catalog.acceptance, rows: result.getRowObjectsJson() };
  } finally { connection.closeSync(); instance.closeSync(); }
}
