import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, execFileSync, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..');

async function runCli(args: string[], env: Record<string, string>) {
  const child = spawn(process.execPath, ['src/cli.ts', ...args], {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk: Buffer) => { stdout += chunk; });
  child.stderr.on('data', (chunk: Buffer) => { stderr += chunk; });
  const code = await new Promise<number | null>(resolve => child.once('exit', resolve));
  return { code, stdout, stderr };
}

/** Hold an exclusive `flock` on `lockPath` in a helper process until it is killed. */
async function holdLock(lockPath: string): Promise<ChildProcess> {
  const holder = spawn('flock', ['-x', lockPath, 'sleep', '30'], { stdio: 'ignore' });
  const deadline = Date.now() + 5_000;
  for (;;) {
    try {
      execFileSync('flock', ['-n', '-x', lockPath, 'true'], { stdio: 'ignore' });
    } catch {
      return holder; // the helper holds the lock
    }
    if (Date.now() > deadline) {
      holder.kill('SIGKILL');
      throw new Error('lock holder never acquired the lock');
    }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

test('a collector command refuses while the checkpoint lock is held and never opens the database', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ts-lock-'));
  const holder = await holdLock(join(root, 'checkpoint.duckdb.lock'));
  try {
    const { code, stderr } = await runCli(['maintain'], {
      SOLOS_DATA_DIR: root,
      SOLOS_DATA_CONFIG: join(repoRoot, 'config/phoenix.json'),
    });
    assert.notEqual(code, 0, 'the locked command must exit non-zero');
    assert.match(stderr, /checkpoint already owned/i, `unexpected refusal message: ${stderr}`);
    assert.equal(
      existsSync(join(root, 'checkpoint.duckdb')),
      false,
      'the locked command must not open checkpoint.duckdb',
    );
  } finally {
    holder.kill('SIGKILL');
    await rm(root, { recursive: true, force: true });
  }
});
