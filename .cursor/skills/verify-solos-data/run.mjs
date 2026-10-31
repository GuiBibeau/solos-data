#!/usr/bin/env node
// Offline verification skill: drives the real `solos-data` binary through four offline journeys
// and leaves reviewable evidence in target/verify-evidence/. Prints PASS or FAIL.
//
// Doctor: no `.env`, no provider/RPC/data-directory environment variables, a fresh temp run
// directory, and the binary and DuckDB versions. Journeys: fixture raw decodes to Parquet
// through the production decoder (checked against a golden row count and content hash), a query
// over the fixture catalog, a watermark/range check that refuses on a gap fixture and leaves the
// watermark unchanged, and a spot fixture publish through the durable write and watermark.

import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dangerousEnvNames, subprocessEnv } from './env.mjs';

const skillDir = dirname(fileURLToPath(import.meta.url));
const repo = join(skillDir, '..', '..', '..');
process.chdir(repo);

// Everything lives under the repository's ignored target/ directory, whatever CARGO_TARGET_DIR
// the surrounding environment set.
const targetDir = join(repo, 'target');
process.env.CARGO_TARGET_DIR = targetDir;
const bin = join(targetDir, 'debug', 'solos-data');
const evidenceDir = join(targetDir, 'verify-evidence');
mkdirSync(evidenceDir, { recursive: true });

const doctor = {};
const journeys = [];
const fails = [];

function sh(cmd, args, options = {}) {
  // Subprocesses run with the build allowlist (env.mjs): every data-directory, provider and
  // archive override is dropped, so no journey can be redirected away from its fixtures.
  return spawnSync(cmd, args, { encoding: 'utf8', env: subprocessEnv(), ...options });
}

function binRun(args) {
  const res = sh(bin, args, { maxBuffer: 64 * 1024 * 1024 });
  return {
    exitCode: res.status ?? -1,
    stdout: (res.stdout ?? '').trim(),
    stderr: (res.stderr ?? '').trim(),
  };
}

function journey(name, args, pass) {
  const res = binRun(args);
  const ok = pass(res);
  journeys.push({
    name,
    ok,
    command: `${bin} ${args.join(' ')}`,
    exitCode: res.exitCode,
    stdout: res.stdout,
    stderr: res.stderr,
  });
  if (!ok) fails.push(name);
  process.stdout.write(`${ok ? 'PASS' : 'FAIL'}  ${name} (exit ${res.exitCode})\n`);
  return res;
}

function fail(name, detail) {
  fails.push(name);
  process.stderr.write(`FAIL  ${name}: ${detail}\n`);
}

function reportJson(res) {
  try {
    return JSON.parse(res.stdout);
  } catch {
    return null;
  }
}

// --------------------------------------------------------------------------------------------
// Doctor. The .env and environment-variable checks are fatal: they refuse before the binary is
// built and before any journey runs, so a poisoned shell can never reach the fixtures.

doctor.noEnvFile = !existsSync(join(repo, '.env'));
if (!doctor.noEnvFile) fail('doctor: no .env', `${repo}/.env exists`);

doctor.dangerousEnv = dangerousEnvNames(process.env);
if (doctor.dangerousEnv.length > 0) {
  fail('doctor: no provider/data-directory env vars', `set: ${doctor.dangerousEnv.join(', ')}`);
}

if (!doctor.noEnvFile || doctor.dangerousEnv.length > 0) {
  process.stderr.write(
    'verify-solos-data: refusing to start (unset the listed variables or remove the .env, then ' +
      're-run) so the journeys stay on their fixtures.\n',
  );
  process.exit(1);
}

process.stdout.write('verify-solos-data: building the binary...\n');
const build = sh('cargo', ['build', '--locked', '--bin', 'solos-data'], {
  stdio: 'inherit',
});
doctor.buildOk = build.status === 0;
if (!doctor.buildOk) {
  process.stderr.write(
    `verify-solos-data: refusing to start: cargo build exited ${build.status ?? 'signal'}\n`,
  );
  process.exit(1);
}

const versionRes = binRun(['dev', 'version']);
if (versionRes.exitCode === 0) {
  try {
    const version = JSON.parse(versionRes.stdout);
    doctor.binary = version.binary;
    doctor.duckdb = version.duckdb;
  } catch {
    fail('doctor: versions', `unparseable: ${versionRes.stdout}`);
  }
} else {
  fail('doctor: versions', `exit ${versionRes.exitCode}: ${versionRes.stderr}`);
}

const gitSha = (sh('git', ['rev-parse', 'HEAD']).stdout ?? '').trim();
const runDir = mkdtempSync(join(evidenceDir, 'run.'));
doctor.runDir = runDir;

// --------------------------------------------------------------------------------------------
// Journey 1: fixture raw decodes to Parquet, checked against a golden row count and hash.
// The fixture raw source is materialised by the same encoding the decoder tests use and consumed
// by the production decoder (catalog -> source rows -> decode -> durable publish + watermark).

const fixtureDir = join(runDir, 'fixture');
const decodedDir = join(runDir, 'decoded');
journey('decoder fixture build', ['dev', 'make-decoder-fixture', '--out', fixtureDir], (r) => r.exitCode === 0);

const decodedConfig = {
  rawDir: join(fixtureDir, 'raw'),
  dataDir: decodedDir,
  batchSize: 1000,
  maxBatchSources: 32,
  pollMs: 100,
  checkpointIntervalSeconds: 900,
  compactionIntervalSeconds: 60,
  compactionBudgetSeconds: 15,
  compactionSettledBatches: 100,
};
const decodedConfigPath = join(runDir, 'decoded.json');
writeFileSync(decodedConfigPath, JSON.stringify(decodedConfig));

journey('decoder once (real pipeline)', ['decoder', 'once', '--config', decodedConfigPath], (r) => r.exitCode === 0);

const goldenPath = join(skillDir, 'golden', 'decoder.json');
const golden = JSON.parse(readFileSync(goldenPath, 'utf8'));
journey('decoder golden row count + hash', ['dev', 'decoder-verify', '--root', decodedDir, '--golden', goldenPath], (r) => r.exitCode === 0);

// --------------------------------------------------------------------------------------------
// Journey 2: a query over the fixture catalog (reads catalog.json to find the Parquet files).

journey('query over fixture catalog', ['decoder', 'query', '--config', decodedConfigPath, '--sql', 'SELECT count(*) AS n FROM fills'], (r) => {
  if (r.exitCode !== 0) return false;
  try {
    return JSON.parse(r.stdout).rows?.[0]?.n === golden.rows.fills;
  } catch {
    return false;
  }
});

// --------------------------------------------------------------------------------------------
// Journey 3: the watermark/range check refuses on a gap fixture (and passes a complete one),
// and leaves the backfill watermark unchanged.

const rangeGood = join(runDir, 'range-good');
const rangeGap = join(runDir, 'range-gap');
journey('range fixture (complete)', ['dev', 'make-range-fixture', '--out', rangeGood, '--chunk', '10', '--next', '5000'], (r) => r.exitCode === 0);
journey('range check passes a complete range', ['collector', 'validate-next-backfill', '--config', join(rangeGood, 'config.json')], (r) => r.exitCode === 0);
journey('range fixture (gap)', ['dev', 'make-range-fixture', '--out', rangeGap, '--chunk', '10', '--next', '5000', '--gap'], (r) => r.exitCode === 0);

const gapConfig = join(rangeGap, 'config.json');
const gapBefore = reportJson(binRun(['dev', 'collector-watermark', '--config', gapConfig]))?.next;
journey('range check refuses a gap fixture', ['collector', 'validate-next-backfill', '--config', gapConfig], (r) => r.exitCode !== 0 && r.stdout.includes('watermark will not advance'));
journey('gap leaves the watermark unchanged', ['dev', 'collector-watermark', '--config', gapConfig], (r) => r.exitCode === 0 && reportJson(r)?.next === gapBefore);

// --------------------------------------------------------------------------------------------
// Journey 4: spot fixture decode through the durable write and the watermark, then the spot
// lane's own check over the published files.

const spotDir = join(runDir, 'spot');
journey('spot fixture publish (durable write + watermark)', ['dev', 'spot-fixture-publish', '--root', spotDir], (r) => {
  if (r.exitCode !== 0) return false;
  const report = reportJson(r);
  return report?.ok === true && report.transactions > 0 && report.swaps > 0 && report.liquidity > 0;
});
journey('spot check passes the published lane', ['spot', 'check', '--config', join(spotDir, 'config.json')], (r) => r.exitCode === 0);

// --------------------------------------------------------------------------------------------
// Throughput baseline from the real decoder's status snapshot.

let throughput = null;
const statusPath = join(decodedDir, 'status.json');
if (existsSync(statusPath)) {
  const status = JSON.parse(readFileSync(statusPath, 'utf8'));
  const transactions = Number(status.batchTransactions ?? status.batchRaw ?? 0);
  const loopSeconds = Number(status.loopSeconds ?? 0);
  throughput = {
    transactions,
    loopSeconds,
    transactionsPerSecond: loopSeconds > 0 ? Number((transactions / loopSeconds).toFixed(2)) : null,
  };
}

// --------------------------------------------------------------------------------------------
// Evidence.

const evidence = {
  at: new Date().toISOString(),
  result: fails.length === 0 ? 'PASS' : 'FAIL',
  gitSha,
  binary: doctor.binary ?? null,
  duckdb: doctor.duckdb ?? null,
  doctor: {
    noEnvFile: doctor.noEnvFile,
    dangerousEnv: doctor.dangerousEnv ?? [],
    buildOk: doctor.buildOk,
    runDir,
  },
  journeys,
  decoderThroughput: throughput,
};

writeFileSync(join(evidenceDir, 'evidence.json'), `${JSON.stringify(evidence, null, 2)}\n`);
const commands = journeys
  .map((j) => `$ ${j.command}\n  exit ${j.exitCode} ${j.ok ? 'ok' : 'FAILED'}`)
  .join('\n');
writeFileSync(join(evidenceDir, 'commands.log'), `${commands}\n`);

// --------------------------------------------------------------------------------------------
// Cleanup: remove the scratch run directories, never the evidence. Proof artifacts survive.

for (const name of readdirSync(evidenceDir)) {
  if (name.startsWith('run.')) {
    rmSync(join(evidenceDir, name), { recursive: true, force: true });
  }
}

process.stdout.write(`\nverify-solos-data: ${evidence.result}  (evidence in ${evidenceDir})\n`);
process.exitCode = evidence.result === 'PASS' ? 0 : 1;
