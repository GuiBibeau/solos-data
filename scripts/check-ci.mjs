#!/usr/bin/env node
// Structural check over .github/workflows/ci.yml. It keeps the `ci-ok` aggregator honest:
// every job must appear in ci-ok.needs, ci-ok must run with `if: always()`, and every job the
// aggregator allows to skip must name a real job. Run as `node scripts/check-ci.mjs [path]`;
// without a path it reads .github/workflows/ci.yml.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';

const scriptDir = dirname(fileURLToPath(import.meta.url));

// A run step whose command references a file under scripts/ depends on the checkout: without
// an actions/checkout step the file is not on the runner and the step fails with exit 127.
const REPO_SCRIPT_RUN = /(?:^|[\s;&|>])\.?\/?scripts\/[\w.-]+/;

function runsRepoScript(step) {
  return typeof step?.run === 'string' && REPO_SCRIPT_RUN.test(step.run);
}

function isCheckoutStep(step) {
  return typeof step?.uses === 'string' && step.uses.startsWith('actions/checkout@');
}

// The allowed-skips map lives in the aggregator step's env as "job: condition" lines, so the
// bash loop and this check share one source of truth. Read the names back and fail here, before
// CI runs, when a name does not match any real job.
export function parseSkipMap(job) {
  const entries = new Map();
  for (const step of job?.steps ?? []) {
    const raw = step?.env?.ALLOWED_SKIPS;
    if (raw === undefined) continue;
    for (const line of String(raw).split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const colon = trimmed.indexOf(':');
      if (colon === -1) {
        entries.set(trimmed, '');
        continue;
      }
      entries.set(trimmed.slice(0, colon).trim(), trimmed.slice(colon + 1).trim());
    }
  }
  return entries;
}

export function checkCi(workflow) {
  const errors = [];
  const jobs = workflow?.jobs;
  if (jobs === null || typeof jobs !== 'object' || Array.isArray(jobs)) {
    errors.push('ci.yml has no "jobs" mapping');
    return errors;
  }
  const ciOk = jobs['ci-ok'];
  if (ciOk === undefined) {
    errors.push('ci.yml has no "ci-ok" job');
    return errors;
  }
  const needs = Array.isArray(ciOk.needs)
    ? ciOk.needs
    : typeof ciOk.needs === 'string'
      ? [ciOk.needs]
      : [];
  for (const job of Object.keys(jobs)) {
    if (job === 'ci-ok') continue;
    if (!needs.includes(job)) {
      errors.push(`job "${job}" is not in ci-ok.needs. add "${job}" to ci-ok.needs`);
    }
  }
  if (ciOk.if !== 'always()') {
    errors.push('ci-ok must set "if: always()" so it reports a result even when a job it needs fails');
  }
  const steps = Array.isArray(ciOk.steps) ? ciOk.steps : [];
  const firstRepoScript = steps.findIndex(runsRepoScript);
  if (firstRepoScript !== -1 && !steps.slice(0, firstRepoScript).some(isCheckoutStep)) {
    errors.push('ci-ok runs a repo script but has no actions/checkout step before it. add actions/checkout before the step that runs scripts/ci-ok.sh');
  }
  for (const [skipJob] of parseSkipMap(ciOk)) {
    if (!Object.prototype.hasOwnProperty.call(jobs, skipJob)) {
      errors.push(`ci-ok skip map names "${skipJob}", but there is no such job`);
    }
  }
  return errors;
}

export function main(argv = process.argv.slice(2)) {
  const target = argv[0] ?? resolve(scriptDir, '../.github/workflows/ci.yml');
  let source;
  try {
    source = readFileSync(target, 'utf8');
  } catch (err) {
    console.error(`check-ci: cannot read ${target}: ${err.message}`);
    return 1;
  }
  let workflow;
  try {
    workflow = parse(source);
  } catch (err) {
    console.error(`check-ci: cannot parse ${target}: ${err.message}`);
    return 1;
  }
  const errors = checkCi(workflow);
  if (errors.length === 0) {
    console.log(`check-ci: ${target} is consistent`);
    return 0;
  }
  for (const message of errors) {
    console.error(`check-ci: ${message}`);
  }
  return 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main();
}
