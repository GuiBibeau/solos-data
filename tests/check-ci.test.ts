import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const checkCi = resolve(here, '../scripts/check-ci.mjs');

function runCheck(fixture: string) {
  const result = spawnSync(process.execPath, [checkCi, resolve(here, 'data', fixture)], {
    encoding: 'utf8',
  });
  return { status: result.status, output: `${result.stdout}\n${result.stderr}` };
}

test('check-ci fails and names a job missing from ci-ok.needs', () => {
  const { status, output } = runCheck('ci-missing-fmt.yml');
  assert.notEqual(status, 0);
  assert.match(output, /fmt/);
  assert.match(output, /add "fmt" to ci-ok\.needs/);
});

test('check-ci fails and names an extra job with no ci-ok entry', () => {
  const { status, output } = runCheck('ci-extra-job.yml');
  assert.notEqual(status, 0);
  assert.match(output, /extra/);
  assert.match(output, /add "extra" to ci-ok\.needs/);
});

test('check-ci passes on a consistent workflow', () => {
  const { status } = runCheck('ci-consistent.yml');
  assert.equal(status, 0);
});

test('check-ci fails when ci-ok is missing if: always()', () => {
  const { status, output } = runCheck('ci-missing-always.yml');
  assert.notEqual(status, 0);
  assert.match(output, /ci-ok must set "if: always\(\)" so it reports a result even when a job it needs fails/);
});

test('check-ci fails when the skip map names a job that does not exist', () => {
  const { status, output } = runCheck('ci-skip-unknown-job.yml');
  assert.notEqual(status, 0);
  assert.match(output, /ci-ok skip map names "archive-check", but there is no such job/);
});
