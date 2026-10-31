import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
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

test('the TypeScript live collector entrypoint is retired', async () => {
  // The single-owner guarantee lives in the Rust binary (ADR-0016). The lockless TypeScript
  // writer must not be startable: neither `npm start` nor the `run` command may exist.
  const pkg = JSON.parse(await readFile(join(repoRoot, 'package.json'), 'utf8'));
  assert.equal(pkg.scripts?.start, undefined, 'npm start must not run the lockless TypeScript collector');

  const root = await mkdtemp(join(tmpdir(), 'retired-collector-'));
  try {
    const { code, stdout, stderr } = await runCli(['run'], { SOLOS_DATA_DIR: root });
    assert.notEqual(code, 0, 'the retired `run` command must exit non-zero');
    assert.match(stdout + stderr, /Unknown command/);
    assert.equal(existsSync(join(root, 'checkpoint.duckdb')), false,
      'the retired `run` command must not open a checkpoint');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
