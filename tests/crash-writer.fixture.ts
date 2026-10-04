import { Store } from '../src/store.ts';

const store = await Store.open(process.argv[2]);
await store.transaction(async connection => {
  await connection.run("INSERT INTO kv VALUES ('durable','true')");
  await connection.run("INSERT INTO signatures VALUES ('durable',100,100,'null',[],'tail','c','fixture')");
});
await store.connection.run('BEGIN');
await store.connection.run("INSERT INTO kv VALUES ('unfinished','true')");
process.stdout.write('ready\n');
setInterval(() => {},1000);
