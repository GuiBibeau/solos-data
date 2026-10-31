import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const ciOk = resolve(here, '../scripts/ci-ok.sh');

const ALLOWED_SKIPS = 'archive-check: pull request touched no archive inputs\n';

type Needs = Record<string, { result: string }>;

function runCiOk(needs: Needs, env: Record<string, string> = {}) {
  const result = spawnSync(ciOk, [], {
    encoding: 'utf8',
    env: {
      ...process.env,
      NEEDS: JSON.stringify(needs),
      ALLOWED_SKIPS,
      IS_PR: 'false',
      ARCHIVE_CHANGED: '',
      ...env,
    },
  });
  return { status: result.status, output: `${result.stdout}\n${result.stderr}` };
}

test('ci-ok passes when every required job succeeded', () => {
  const { status, output } = runCiOk({
    changes: { result: 'success' },
    fmt: { result: 'success' },
    test: { result: 'success' },
  });
  assert.equal(status, 0);
  assert.match(output, /ci-ok: all required jobs passed/);
});

test('ci-ok fails and names a job that did not succeed', () => {
  const { status, output } = runCiOk({
    changes: { result: 'success' },
    fmt: { result: 'failure' },
  });
  assert.notEqual(status, 0);
  assert.match(output, /required job 'fmt' did not succeed: failure/);
});

test('ci-ok allows a skipped archive-check on a PR with no archive inputs', () => {
  const { status, output } = runCiOk(
    { changes: { result: 'success' }, 'archive-check': { result: 'skipped' } },
    { IS_PR: 'true', ARCHIVE_CHANGED: '' },
  );
  assert.equal(status, 0);
  assert.match(output, /archive-check intentionally skipped/);
});

test('ci-ok fails a skipped archive-check that touched archive inputs', () => {
  const { status, output } = runCiOk(
    { changes: { result: 'success' }, 'archive-check': { result: 'skipped' } },
    { IS_PR: 'true', ARCHIVE_CHANGED: 'true' },
  );
  assert.notEqual(status, 0);
  assert.match(output, /required job 'archive-check' did not succeed: skipped/);
});
