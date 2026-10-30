import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  DATA_DIR_OVERRIDES,
  dataDirOverridesIn,
  subprocessEnv,
} from '../.cursor/skills/verify-solos-data/env.mjs';

test('verify-solos-data doctor detects data-directory overrides', () => {
  assert.deepEqual(DATA_DIR_OVERRIDES, [
    'SOLOS_DATA_DIR',
    'SOLOS_DATA_RAW_DIR',
    'SOLOS_DATA_DECODED_DIR',
  ]);
  assert.deepEqual(dataDirOverridesIn(undefined), []);
  assert.deepEqual(dataDirOverridesIn({}), []);
  assert.deepEqual(
    dataDirOverridesIn({ SOLOS_DATA_DIR: '', SOLOS_DATA_DECODED_DIR: '/real/decoded' }),
    ['SOLOS_DATA_DECODED_DIR'],
  );
  assert.deepEqual(
    dataDirOverridesIn({
      SOLOS_DATA_DIR: '/real/raw',
      SOLOS_DATA_RAW_DIR: '/real/raw',
      SOLOS_DATA_DECODED_DIR: '/real/decoded',
    }),
    ['SOLOS_DATA_DIR', 'SOLOS_DATA_RAW_DIR', 'SOLOS_DATA_DECODED_DIR'],
  );
});

test('verify-solos-data subprocess environment is scrubbed of data-directory overrides', () => {
  const env = {
    SOLOS_DATA_DIR: '/real/raw',
    SOLOS_DATA_RAW_DIR: '/real/raw',
    SOLOS_DATA_DECODED_DIR: '/real/decoded',
    SOLOS_DATA_QUERY_MEMORY: '1GB',
    CARGO_TARGET_DIR: '/tmp/target',
    CC: 'gcc',
  };
  const cleaned = subprocessEnv(env);
  for (const name of DATA_DIR_OVERRIDES) {
    assert.equal(name in cleaned, false, `${name} must be removed`);
  }
  assert.equal(cleaned.SOLOS_DATA_QUERY_MEMORY, '1GB');
  assert.equal(cleaned.CARGO_TARGET_DIR, '/tmp/target');
  assert.equal(cleaned.CC, 'gcc');
});
