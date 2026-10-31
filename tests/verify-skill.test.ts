import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { dangerousEnvNames, subprocessEnv } from '../.cursor/skills/verify-solos-data/env.mjs';

test('verify-solos-data doctor detects every journey-redirecting or credential env var', () => {
  assert.deepEqual(dangerousEnvNames(undefined), []);
  assert.deepEqual(dangerousEnvNames({}), []);
  assert.deepEqual(dangerousEnvNames({ SOLOS_DATA_DIR: '', SOLOS_DATA_DECODED_DIR: '/real/decoded' }), [
    'SOLOS_DATA_DECODED_DIR',
  ]);
  const dangerous = [
    'SOLOS_DATA_DIR',
    'SOLOS_DATA_RAW_DIR',
    'SOLOS_DATA_DECODED_DIR',
    'SOLOS_DATA_CONFIG',
    'SOLOS_DATA_AUGMENT_DIR',
    'SOLOS_DATA_DB_CHECKPOINT',
    'SOLOS_DATA_FAILPOINT_MARKER',
    'SOLANA_RPC_URL',
    'HELIUS_RPC_ENDPOINT',
    'PROVIDER_URL',
    'ELFA_API_KEY',
    'JETSTREAMER_THREADS',
    'JETSTREAMER_ENDPOINT',
  ];
  assert.deepEqual(
    dangerousEnvNames(Object.fromEntries(dangerous.map((name) => [name, 'x']))).sort(),
    [...dangerous].sort(),
  );
  // Empty values are not exported by a shell and are ignored.
  assert.deepEqual(dangerousEnvNames({ SOLANA_RPC_URL: '', JETSTREAMER_THREADS: '4' }), [
    'JETSTREAMER_THREADS',
  ]);
});

test('verify-solos-data subprocess env is an allowlist, proven by a real child process', () => {
  const env = {
    // The names the allowlist keeps.
    PATH: '/usr/bin:/bin',
    HOME: '/opt/verify-home',
    TMPDIR: '/tmp',
    LANG: 'C.UTF-8',
    RUSTUP_HOME: '/opt/rustup',
    CARGO_TARGET_DIR: '/tmp/target',
    CARGO_PROFILE_DEV_DEBUG: '0',
    // Names the allowlist must drop.
    SOLOS_DATA_DIR: '/real/raw',
    SOLOS_DATA_CONFIG: '/real/config.json',
    SOLOS_DATA_AUGMENT_DIR: '/real/augment',
    SOLOS_DATA_DB_CHECKPOINT: '1GB',
    SOLOS_DATA_FAILPOINT_MARKER: '/tmp/failpoint-marker',
    SOLANA_RPC_URL: 'https://secret.example',
    SOME_RPC_ENDPOINT: 'https://secret.example',
    PROVIDER_URL: 'https://secret.example',
    ELFA_API_KEY: 'secret-key',
    JETSTREAMER_THREADS: '16',
    JETSTREAMER_ENDPOINT: 'https://secret.example',
    CC: 'clang',
    NODE_OPTIONS: '--throw-deprecation',
  };
  const res = spawnSync(
    process.execPath,
    ['-e', 'process.stdout.write(JSON.stringify(Object.keys(process.env).sort()))'],
    { env: subprocessEnv(env), encoding: 'utf8' },
  );
  assert.equal(res.status, 0, res.stderr);
  const names = JSON.parse(res.stdout);

  // The allowlist survives.
  for (const name of ['PATH', 'HOME', 'TMPDIR', 'LANG', 'RUSTUP_HOME', 'CARGO_TARGET_DIR']) {
    assert.ok(names.includes(name), `${name} must survive the allowlist`);
  }
  // Nothing that could redirect a journey or leak a credential survives.
  for (const name of names) {
    assert.doesNotMatch(name, /^SOLOS_DATA_|_RPC_|_API_KEY$|^JETSTREAMER_/, `${name} must not survive`);
  }
  assert.ok(!names.includes('PROVIDER_URL'), 'PROVIDER_URL must not survive');
  assert.ok(!names.includes('SOLOS_DATA_FAILPOINT_MARKER'), 'SOLOS_DATA_FAILPOINT_MARKER must not survive');
  assert.ok(!names.includes('CC'), 'CC must not survive the allowlist');
  assert.ok(!names.includes('NODE_OPTIONS'), 'NODE_OPTIONS must not survive');
});
