#!/usr/bin/env node
// Lint every tracked text file for em-dashes and private host or path names.
//
// With no arguments it lints every file `git ls-files` lists, except
// vendor/**, config/idl/**, Cargo.lock, package-lock.json, tests/data/** and
// binary files. Pass explicit paths to lint only those files, which is how the
// test suite feeds it fixtures.
//
// A `lint-allow: <rule> <reason>` directive placed in a comment on a line
// suppresses that rule on that line. The script prints every escape it
// honors. Lines of the file named by SOLOS_LINT_DENY_FILE (one regex per
// line, `#` comments and blank lines skipped) become extra private-host
// checks, so machine-specific host names stay out of Git.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const EM_DASH_FIX =
  'replace the em-dash with a comma, a colon, parentheses or two sentences';

const PRIVATE_IPV4 =
  /\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2[0-9]|3[01])(?:\.\d{1,3}){2})\b/g;

const HOME_PATH = /\/(?:home|Users)\/[^/\s]+\//g;

const TAILNET = /\b[A-Za-z0-9][A-Za-z0-9.-]*\.ts\.net\b/g;

const BASE_CHECKS = [
  { rule: 'em-dash', re: /\u2014/g, describe: () => EM_DASH_FIX },
  { rule: 'private-host', re: TAILNET, describe: () => 'remove the tailnet name' },
  { rule: 'private-host', re: PRIVATE_IPV4, describe: () => 'remove the private IPv4 address' },
  { rule: 'private-host', re: HOME_PATH, describe: () => 'remove the home path' },
];

const EXCLUDED = [/^vendor\//, /^config\/idl\//, /^tests\/data\//];
const EXCLUDED_FILES = new Set(['Cargo.lock', 'package-lock.json']);

function isExcluded(path) {
  if (EXCLUDED_FILES.has(path)) return true;
  return EXCLUDED.some((re) => re.test(path));
}

function trackedFiles() {
  const out = execFileSync('git', ['ls-files'], { encoding: 'utf8' });
  return out.split('\n').filter((line) => line !== '');
}

function denyChecks() {
  const denyPath = process.env.SOLOS_LINT_DENY_FILE;
  if (!denyPath) return [];
  const text = readFileSync(denyPath, 'utf8');
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('#'))
    .map((pattern) => ({
      rule: 'private-host',
      re: new RegExp(pattern, 'g'),
      describe: () => `remove text matching the deny-file pattern '${pattern}'`,
    }));
}

function allowedRules(line) {
  const allowed = new Map();
  const re = /lint-allow:\s*(\S+)(?:\s+(.*))?/g;
  let m;
  while ((m = re.exec(line)) !== null) {
    allowed.set(m[1], (m[2] ?? '').trim());
  }
  return allowed;
}

function lineFindings(line, checks) {
  const findings = [];
  for (const check of checks) {
    check.re.lastIndex = 0;
    let m;
    while ((m = check.re.exec(line)) !== null) {
      findings.push({ rule: check.rule, col: m.index + 1, message: check.describe() });
      if (check.re.lastIndex === m.index) check.re.lastIndex += 1;
    }
  }
  return findings;
}

function lintFile(path, checks) {
  const buf = readFileSync(path);
  if (buf.includes(0)) return { violations: [], honors: [], scanned: false };
  const lines = buf.toString('utf8').split('\n');
  const violations = [];
  const honors = [];
  lines.forEach((line, i) => {
    const allowed = allowedRules(line);
    const findings = lineFindings(line, checks);
    for (const [rule, reason] of allowed) {
      if (findings.some((f) => f.rule === rule)) {
        honors.push({ path, line: i + 1, rule, reason });
      }
    }
    for (const f of findings) {
      if (!allowed.has(f.rule)) {
        violations.push({ path, line: i + 1, col: f.col, rule: f.rule, message: f.message });
      }
    }
  });
  return { violations, honors, scanned: true };
}

function main() {
  const checks = BASE_CHECKS.concat(denyChecks());
  const args = process.argv.slice(2);
  const paths = args.length > 0 ? args : trackedFiles();
  const files = paths.filter((p) => !isExcluded(p));
  const violations = [];
  const honors = [];
  let scanned = 0;
  for (const path of files) {
    const r = lintFile(path, checks);
    if (r.scanned) scanned += 1;
    violations.push(...r.violations);
    honors.push(...r.honors);
  }
  for (const h of honors) {
    const reason = h.reason === '' ? '' : ` ${h.reason}`;
    console.log(`${h.path}:${h.line}: lint-allow: ${h.rule}${reason}`);
  }
  for (const v of violations) {
    console.log(`${v.path}:${v.line}:${v.col}: ${v.rule}: ${v.message}`);
  }
  if (violations.length > 0) {
    console.log(`lint-text: FAIL ${violations.length} violations`);
    process.exitCode = 1;
  } else {
    console.log(`lint-text: PASS ${scanned} files`);
  }
}

main();
