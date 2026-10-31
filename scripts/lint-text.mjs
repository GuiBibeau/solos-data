#!/usr/bin/env node
// Lint every tracked text file for em-dashes and private host or path names.
//
// With no arguments it lints every file `git ls-files` lists, except
// vendor/**, config/idl/**, Cargo.lock, package-lock.json, tests/data/** and
// binary files. Pass explicit paths to lint only those files, which is how the
// test suite feeds it fixtures.
//
// A `lint-allow: <rule> <reason>` directive suppresses that rule on that line,
// but only when it sits inside a comment. The script recognizes `//`, `#`,
// `/* ... */` and `<!-- ... -->` comments; a directive in a string literal or
// in prose is ignored. The script prints every escape it honors.
//
// Lines of the file named by SOLOS_LINT_DENY_FILE (one regex per line, `#`
// comments and blank lines skipped) become extra private-host checks, so
// machine-specific host names stay out of Git. A missing or unreadable deny
// file is a hard error, not a skip: the deny checks exist to keep names out of
// Git, so silently dropping them would weaken the lint.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const EM_DASH_FIX =
  'replace the em-dash with a comma, a colon, parentheses or two sentences';

const PRIVATE_IPV4 =
  /\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2[0-9]|3[01])(?:\.\d{1,3}){2})\b/g;

// Home-directory paths: an absolute path under the home or Users directory
// (with or without a trailing slash), or a tilde-prefixed path to a visible
// directory such as a checkout or scratch dir. Dot-directories (.config,
// .local, .ssh) are not matched: they are generic locations, not private
// paths. A bare tilde is not matched: in prose it means "approximately".
const HOME_PATH = /\/(?:home|Users)\/[^/\s]+|~\/[^.\s][^\s]*/g;

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
  let text;
  try {
    text = readFileSync(denyPath, 'utf8');
  } catch (err) {
    throw new Error(`cannot read deny file '${denyPath}': ${err.code ?? 'error'}`);
  }
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('#'))
    .map((pattern) => ({
      rule: 'private-host',
      re: new RegExp(pattern, 'gi'),
      describe: () => `remove text matching the deny-file pattern '${pattern}'`,
    }));
}

// [start, end) spans of `line` that are comments. String literals are skipped,
// so a `//`, `#` or `/*` inside a quoted value does not open a comment.
function commentRanges(line) {
  const spans = [];
  const n = line.length;
  let i = 0;
  while (i < n) {
    const c = line[i];
    if (c === '"' || c === "'" || c === '`') {
      const quote = c;
      i += 1;
      while (i < n) {
        if (line[i] === '\\') {
          i += 2;
          continue;
        }
        if (line[i] === quote) {
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    if (c === '/' && line[i + 1] === '/') {
      spans.push([i, n]);
      return spans;
    }
    if (c === '/' && line[i + 1] === '*') {
      const start = i;
      const end = line.indexOf('*/', i + 2);
      if (end === -1) {
        spans.push([start, n]);
        return spans;
      }
      spans.push([start, end + 2]);
      i = end + 2;
      continue;
    }
    if (c === '<' && line.startsWith('<!--', i)) {
      const start = i;
      const end = line.indexOf('-->', i + 4);
      if (end === -1) {
        spans.push([start, n]);
        return spans;
      }
      spans.push([start, end + 3]);
      i = end + 3;
      continue;
    }
    if (c === '#') {
      spans.push([i, n]);
      return spans;
    }
    i += 1;
  }
  return spans;
}

function allowedRules(line) {
  const spans = commentRanges(line);
  const inComment = (index) => spans.some(([start, end]) => index >= start && index < end);
  const allowed = new Map();
  const re = /lint-allow:\s*(\S+)(?:\s+(.*))?/g;
  let m;
  while ((m = re.exec(line)) !== null) {
    if (inComment(m.index)) {
      allowed.set(m[1], (m[2] ?? '').trim());
    }
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

function lintFile(path, buf, checks) {
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
  let checks;
  try {
    checks = BASE_CHECKS.concat(denyChecks());
  } catch (err) {
    console.error(`lint-text: ERROR ${err.message}`);
    process.exitCode = 1;
    return;
  }

  const args = process.argv.slice(2);
  const paths = args.length > 0 ? args : trackedFiles();
  const files = paths.filter((p) => !isExcluded(p));

  const violations = [];
  const honors = [];
  const errors = [];
  let scanned = 0;

  for (const path of files) {
    let buf;
    try {
      buf = readFileSync(path);
    } catch (err) {
      errors.push(`cannot read '${path}': ${err.code ?? 'error'}`);
      continue;
    }
    const r = lintFile(path, buf, checks);
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
  for (const e of errors) {
    console.error(`lint-text: ERROR ${e}`);
  }

  if (violations.length > 0 || errors.length > 0) {
    const parts = [];
    if (violations.length > 0) parts.push(`${violations.length} violations`);
    if (errors.length > 0) parts.push(`${errors.length} unreadable files`);
    console.log(`lint-text: FAIL ${parts.join(', ')}`);
    process.exitCode = 1;
  } else {
    console.log(`lint-text: PASS ${scanned} files`);
  }
}

main();
