import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance, type DuckDBConnection, type DuckDBValue } from '@duckdb/node-api';
import { json } from './config.ts';
import { schema } from './schema.ts';
import { requireStorageSpace } from './storage-space.ts';

export class Store {
  connection: DuckDBConnection;
  instance: DuckDBInstance;
  root = '';
  queue: Promise<unknown> = Promise.resolve();
  timings: Record<string, { calls:number; seconds:number; maxSeconds:number }> = {};

  constructor(instance: DuckDBInstance, connection: DuckDBConnection) {
    this.instance = instance;
    this.connection = connection;
    const run = connection.run.bind(connection);
    connection.run = async (...args: Parameters<typeof connection.run>) => {
      const started = performance.now();
      const verb = args[0].trim().split(/\s+/)[0].toUpperCase();
      const table = args[0].match(/(?:INTO|UPDATE|FROM)\s+([a-z_]+)/i)?.[1].toLowerCase() ?? '';
      const label = `${verb}:${table}`;
      try { return await run(...args); }
      finally {
        const seconds = (performance.now()-started)/1000;
        const timing = this.timings[label] ??= { calls:0,seconds:0,maxSeconds:0 };
        timing.calls++; timing.seconds += seconds; timing.maxSeconds = Math.max(timing.maxSeconds,seconds);
      }
    };
  }

  static async open(dataDir: string, ddl = schema) {
    await mkdir(dataDir, { recursive: true });
    const memory = process.env.SOLOS_DATA_DB_MEMORY ?? '4GB';
    const instance = await DuckDBInstance.create(join(dataDir, 'checkpoint.duckdb'), { threads: '4', memory_limit: memory,
      checkpoint_threshold:process.env.SOLOS_DATA_DB_CHECKPOINT ?? '256MB' });
    const store = new Store(instance, await instance.connect());
    store.root = dataDir;
    await store.exec(ddl);
    return store;
  }

  exclusive<T>(job: (connection: DuckDBConnection) => Promise<T>): Promise<T> {
    const result = this.queue.then(() => job(this.connection));
    this.queue = result.catch(() => {});
    return result;
  }

  async exec(sql: string, values: DuckDBValue[] = []) {
    return this.exclusive(connection => connection.run(sql, values));
  }

  async rows<T = Record<string, any>>(sql: string, values: DuckDBValue[] = []): Promise<T[]> {
    return this.exclusive(async connection => {
      const reader = await connection.runAndReadAll(sql, values);
      return reader.getRowObjectsJson() as T[];
    });
  }

  async transaction(job: (connection: DuckDBConnection) => Promise<void>) {
    await requireStorageSpace(this.root);
    return this.exclusive(async connection => {
      await connection.run('BEGIN');
      try { await job(connection); await connection.run('COMMIT'); }
      catch (error) { await connection.run('ROLLBACK'); throw error; }
    });
  }

  async get<T = any>(name: string): Promise<T | undefined> {
    const [row] = await this.rows<{ value: string }>('SELECT value::VARCHAR AS value FROM kv WHERE name=?', [name]);
    return row ? JSON.parse(row.value) : undefined;
  }

  async set(name: string, value: unknown) {
    await this.exec('INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)', [name, json(value)]);
  }

  async close() {
    await this.queue;
    this.connection.closeSync();
    this.instance.closeSync();
  }
}
