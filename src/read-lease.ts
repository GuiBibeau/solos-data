import { mkdir, writeFile, readdir, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

/** Acquire before reading a catalog. GC publishes its new catalog before checking leases. */
export async function withReadLease<T>(root: string, read: () => Promise<T>): Promise<T> {
  const directory=join(root,'.readers');
  await mkdir(directory,{recursive:true});
  const path=join(directory,`${process.pid}-${randomUUID()}.json`);
  await writeFile(path,JSON.stringify({pid:process.pid}),{flag:'wx',mode:0o600});
  try { return await read(); } finally { await rm(path,{force:true}); }
}

export async function hasReaders(root: string) {
  const directory=join(root,'.readers');
  await mkdir(directory,{recursive:true});
  for(const name of await readdir(directory)) {
    const path=join(directory,name);
    try {
      const {pid}=JSON.parse(await readFile(path,'utf8'));
      if(!Number.isInteger(pid) || pid<1) return true;
      try { process.kill(pid,0); return true; }
      catch(error) { if((error as NodeJS.ErrnoException).code!=='ESRCH') return true; }
      await rm(path,{force:true});
    } catch(error) { if((error as NodeJS.ErrnoException).code!=='ENOENT') return true; }
  }
  return false;
}
