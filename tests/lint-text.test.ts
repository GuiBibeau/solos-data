import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const script = join(repoRoot, 'scripts', 'lint-text.mjs');
const EM = '\u2014';

function run(files: string[], env: Record<string, string> = {}) {
  const result = spawnSync(process.execPath, [script, ...files], {
    cwd: repoRoot,
    encoding: 'utf8',
    env: { ...process.env, ...env },
  });
  return { status: result.status ?? 1, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function fixtureDir() {
  return mkdtempSync(join(tmpdir(), 'solos-lint-'));
}

function write(dir: string, name: string, content: string) {
  const path = join(dir, name);
  writeFileSync(path, content, 'utf8');
  return path;
}

function assertViolation(stdout: string, file: string, lineCol: string, rule: string) {
  assert.ok(stdout.includes(`${file}:${lineCol}: ${rule}:`), `expected ${file}:${lineCol}: ${rule}: in ${stdout}`);
  assert.ok(stdout.includes(`lint-text: FAIL`), `expected FAIL summary in ${stdout}`);
}

test('an em-dash in Markdown reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'prose.md', `a ${EM} b\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:3', 'em-dash');
    assert.ok(r.stdout.includes('replace the em-dash with a comma, a colon, parentheses or two sentences'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('an em-dash in a Rust comment reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'lib.rs', `// ${EM} x\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:4', 'em-dash');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a tailnet name reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const tailnet = ['example-host', 'ts', 'net'].join('.');
    const file = write(dir, 'hosts.md', `host = ${tailnet}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:8', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a 192.168 address reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const ip = ['192', '168', '0', '1'].join('.');
    const file = write(dir, 'ip.txt', `ip = ${ip}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:6', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a home path reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const home = ['', 'home', 'alice', ''].join('/');
    const file = write(dir, 'path.txt', `path = ${home}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:8', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a deny-file pattern reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const token = ['internal', 'secret', 'host'].join('-');
    const denyFile = write(dir, 'deny.txt', `${token}\n`);
    const file = write(dir, 'note.txt', `use ${token} here\n`);
    const r = run([file], { SOLOS_LINT_DENY_FILE: denyFile });
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:5', 'private-host');
    assert.ok(r.stdout.includes('deny-file pattern'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lint-allow escape is honored, printed, and the file passes', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'escape.rs', `// ${EM} lint-allow: em-dash intentional\n`);
    const r = run([file]);
    assert.equal(r.status, 0);
    assert.ok(r.stdout.includes(`${file}:1: lint-allow: em-dash intentional`));
    assert.ok(r.stdout.includes('lint-text: PASS 1 files'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lint-allow inside a hash comment is honored', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'script.sh', `# ${EM} lint-allow: em-dash shell\n`);
    const r = run([file]);
    assert.equal(r.status, 0);
    assert.ok(r.stdout.includes(`${file}:1: lint-allow: em-dash shell`));
    assert.ok(r.stdout.includes('lint-text: PASS 1 files'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lint-allow inside a string literal is not honored', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'lib.rs', `"${EM} // lint-allow: em-dash in a string"\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:2', 'em-dash');
    assert.ok(!r.stdout.includes('lint-allow: em-dash'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lint-allow in prose is not honored', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'prose.md', `plain ${EM} lint-allow: em-dash text\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:7', 'em-dash');
    assert.ok(!r.stdout.includes('lint-allow: em-dash'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a deny-file pattern matches case-insensitively', () => {
  const dir = fixtureDir();
  try {
    const denyFile = write(dir, 'deny.txt', 'INTERNAL-SECRET-HOST\n');
    const file = write(dir, 'note.txt', 'use internal-secret-host here\n');
    const r = run([file], { SOLOS_LINT_DENY_FILE: denyFile });
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:5', 'private-host');
    assert.ok(r.stdout.includes('deny-file pattern'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a 10/8 address reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const ip = ['10', '0', '0', '1'].join('.');
    const file = write(dir, 'ip.txt', `ip = ${ip}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:6', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a 172.16/12 address reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const ip = ['172', '16', '0', '1'].join('.');
    const file = write(dir, 'ip.txt', `ip = ${ip}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:6', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a 172.32.0.1 address is outside 172.16/12 and passes', () => {
  const dir = fixtureDir();
  try {
    const ip = ['172', '32', '0', '1'].join('.');
    const file = write(dir, 'ip.txt', `ip = ${ip}\n`);
    const r = run([file]);
    assert.equal(r.status, 0);
    assert.ok(r.stdout.includes('lint-text: PASS 1 files'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a /Users home path reports file:line:col and exits 1', () => {
  const dir = fixtureDir();
  try {
    const home = ['', 'Users', 'alice', ''].join('/');
    const file = write(dir, 'path.txt', `path = ${home}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:8', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a home path without a trailing slash reports and exits 1', () => {
  const dir = fixtureDir();
  try {
    const cmd = ['cd ', 'home', 'debian'].join('/');
    const file = write(dir, 'cmd.sh', `${cmd}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:4', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a tilde home path reports and exits 1', () => {
  const dir = fixtureDir();
  try {
    const cmd = ['cd ~', 'research'].join('/');
    const file = write(dir, 'cmd.sh', `${cmd}\n`);
    const r = run([file]);
    assert.equal(r.status, 1);
    assertViolation(r.stdout, file, '1:4', 'private-host');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a binary file is skipped and not scanned', () => {
  const dir = fixtureDir();
  try {
    const file = write(dir, 'blob.bin', `a${EM}\u0000b\n`);
    const r = run([file]);
    assert.equal(r.status, 0);
    assert.ok(r.stdout.includes('lint-text: PASS 0 files'));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('excluded paths are skipped without being read', () => {
  const r = run(['vendor/foo.md', 'config/idl/x.md', 'tests/data/y.md', 'Cargo.lock', 'package-lock.json']);
  assert.equal(r.status, 0);
  assert.ok(r.stdout.includes('lint-text: PASS 0 files'));
});

test('a missing deny file is a clean error and exits 1', () => {
  const dir = fixtureDir();
  try {
    const denyPath = join(dir, 'nope.txt');
    const file = write(dir, 'note.txt', 'ok\n');
    const r = run([file], { SOLOS_LINT_DENY_FILE: denyPath });
    assert.equal(r.status, 1);
    assert.ok(r.stderr.includes('lint-text: ERROR'), `expected lint-text: ERROR in ${r.stderr}`);
    assert.ok(r.stderr.includes(`cannot read deny file '${denyPath}'`), `expected deny file message in ${r.stderr}`);
    assert.ok(r.stderr.includes('ENOENT'), `expected ENOENT in ${r.stderr}`);
    assert.ok(!r.stderr.includes('at readFileSync'), `no stack trace expected in ${r.stderr}`);
    assert.ok(!r.stderr.includes('node:internal'), `no node internals expected in ${r.stderr}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a missing file is a clean error and exits 1', () => {
  const dir = fixtureDir();
  try {
    const missing = join(dir, 'gone.md');
    const r = run([missing]);
    assert.equal(r.status, 1);
    assert.ok(r.stderr.includes(`lint-text: ERROR cannot read '${missing}'`), `expected error in ${r.stderr}`);
    assert.ok(r.stderr.includes('ENOENT'), `expected ENOENT in ${r.stderr}`);
    assert.ok(!r.stderr.includes('at readFileSync'), `no stack trace expected in ${r.stderr}`);
    assert.ok(r.stdout.includes('lint-text: FAIL'), `expected FAIL summary in ${r.stdout}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
