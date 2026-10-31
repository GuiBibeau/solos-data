import { closeSync, mkdirSync, openSync } from 'node:fs';
import { join } from 'node:path';
import { spawn } from 'node:child_process';

const HELD = 'SOLOS_DATA_CHECKPOINT_LOCK_HELD';
/** Exit code `flock -E` reports when the lock is already held (util-linux sysexits EX_TEMPFAIL). */
const CONFLICT = 75;
const FORWARDED: readonly NodeJS.Signals[] = ['SIGINT', 'SIGTERM', 'SIGHUP'];

/** Create `<root>/checkpoint.duckdb.lock` (0600) if it does not exist, as Rust's `acquire_lock` does. */
function ensureLockFile(root: string): string {
  mkdirSync(root, { recursive: true });
  const path = join(root, 'checkpoint.duckdb.lock');
  let fd: number | undefined;
  try {
    fd = openSync(path, 'wx', 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException)?.code !== 'EEXIST') throw error;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
  return path;
}

/**
 * Run the rest of this process under `flock -n -x <root>/checkpoint.duckdb.lock`, the same lock
 * file and exclusive non-blocking semantics as Rust's `DbLock` (ADR-0016). The wrapper re-execs
 * the same CLI and waits for it, forwarding termination signals; it never resolves, so the caller
 * does not continue past this call. When another process holds the lock, `flock` exits with
 * [`CONFLICT`] before Node runs and the wrapper prints `checkpoint already owned`, so the command
 * refuses and never opens the checkpoint. The marker environment variable breaks the re-exec loop:
 * the inner process holds the lock for its whole lifetime and the kernel releases it on exit.
 * Commands that only read status files or query published Parquet do not call this.
 */
export function execUnderCheckpointLock(root: string): Promise<void> {
  if (process.env[HELD] === '1') return Promise.resolve();
  const lockPath = ensureLockFile(root);
  return new Promise<void>(() => {
    const child = spawn(
      'flock',
      ['-n', '-x', '-E', String(CONFLICT), lockPath, process.execPath, process.argv[1], ...process.argv.slice(2)],
      { stdio: 'inherit', env: { ...process.env, [HELD]: '1' } },
    );
    const forward = (signal: NodeJS.Signals) => { child.kill(signal); };
    for (const signal of FORWARDED) process.on(signal, forward);
    child.on('exit', (code, signal) => {
      for (const signal of FORWARDED) process.off(signal, forward);
      if (signal) process.kill(process.pid, signal);
      else if (code === CONFLICT) {
        process.stderr.write(`checkpoint already owned (${lockPath})\n`);
        process.exit(1);
      } else process.exit(code ?? 1);
    });
  });
}
