import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import { once } from 'node:events';
import type { Group } from './instructions.ts';

export interface DecodedGroup extends Group { events: Record<string, any>[]; errors: { error: string; bytes: string }[] }
export class Codec {
  child: ChildProcessWithoutNullStreams;
  pending?: { resolve: (groups: DecodedGroup[]) => void; reject: (error: Error) => void };
  dead?: Error;
  constructor(path: string) {
    this.child = spawn(path, [], { stdio: ['pipe', 'pipe', 'pipe'] });
    this.child.stderr.resume();
    createInterface({ input: this.child.stdout }).on('line', line => {
      const pending = this.pending; this.pending = undefined;
      if (!pending) return;
      try {
        const response = JSON.parse(line);
        if (response.fatal) throw new Error('codec rejected instruction payload');
        pending.resolve(response.groups);
      } catch (error) { pending.reject(error as Error); }
    });
    const fail = () => {
      this.dead = new Error('Phoenix codec process stopped');
      this.pending?.reject(this.dead); this.pending = undefined;
    };
    this.child.on('error', fail); this.child.on('exit', fail);
  }
  async decode(groups: Group[]): Promise<DecodedGroup[]> {
    if (!groups.length) return [];
    if (this.dead) throw this.dead;
    if (this.pending) throw new Error('codec requests must be serialized');
    const response = new Promise<DecodedGroup[]>((resolve, reject) => { this.pending = { resolve, reject }; });
    if (!this.child.stdin.write(JSON.stringify(groups) + '\n')) await once(this.child.stdin, 'drain');
    const timeout = setTimeout(() => this.child.kill(), 30_000);
    try { return await response; } finally { clearTimeout(timeout); }
  }
  async close() {
    if (this.dead) return;
    const closed = once(this.child, 'exit'); this.child.stdin.end(); await closed;
  }
}
