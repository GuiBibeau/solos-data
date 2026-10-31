// Environment isolation for the offline verify skill.
//
// The `solos-data` binary reads many configuration knobs from the environment: the data
// directories (`SOLOS_DATA_DIR`, `SOLOS_DATA_RAW_DIR`, `SOLOS_DATA_DECODED_DIR`), the
// collector config path (`SOLOS_DATA_CONFIG`), the augment root (`SOLOS_DATA_AUGMENT_DIR`),
// DuckDB settings (`SOLOS_DATA_DB_*`), provider URLs and keys (`SOLANA_RPC_URL`,
// `*_RPC_ENDPOINT`, `PROVIDER_URL`, `ELFA_API_KEY`) and the archive (`JETSTREAMER_*`). A
// parent shell that exports any of them would silently redirect the offline journeys away from
// their fixtures, or leak a credential into a child. So every subprocess the skill spawns runs
// with an allowlist: only the names the build genuinely needs survive, and everything else
// (including every data-directory, provider and archive override) is dropped. The doctor
// refuses to start when the invoking shell exported any name that could redirect a journey.

// Whole names that subprocesses may keep.
const ALLOWED_NAMES = new Set(['PATH', 'HOME', 'TMPDIR', 'LANG']);

// Prefixes that subprocesses may keep: cargo needs CARGO_* (including CARGO_TARGET_DIR, which
// the skill sets to the repository's target/ directory) and rustup needs RUSTUP_*.
const ALLOWED_PREFIXES = ['CARGO_', 'RUSTUP_'];

// Failpoint env vars (#72/#73). They already fall under the `SOLOS_DATA_` prefix below, but they
// are named here explicitly so a future allowlist widening can never leak them into a subprocess.
const FAILPOINT_ENV_NAMES = new Set(['SOLOS_DATA_FAILPOINT_MARKER']);

function allowed(name) {
  return (
    !FAILPOINT_ENV_NAMES.has(name) &&
    (ALLOWED_NAMES.has(name) || ALLOWED_PREFIXES.some((prefix) => name.startsWith(prefix)))
  );
}

// A name that could redirect a journey or leak a credential. Used by the doctor and by the
// subprocess allowlist test; the allowlist itself is broader (it keeps only ALLOWED_*).
const DANGEROUS = /^SOLOS_DATA_|(^|_)RPC_(URL|ENDPOINT|HOST)$|^PROVIDER_URL$|_API_KEY$|^JETSTREAMER_/;

/** Names in `env` set to a non-empty value that could redirect a journey or leak a credential. */
export function dangerousEnvNames(env) {
  const names = Object.keys(env ?? {});
  return names.filter(
    (name) =>
      env[name] !== undefined &&
      env[name] !== '' &&
      (DANGEROUS.test(name) || FAILPOINT_ENV_NAMES.has(name)),
  );
}

/** A copy of `env` reduced to the build allowlist, dropping every override and credential. */
export function subprocessEnv(env = process.env) {
  const cleaned = {};
  for (const name of Object.keys(env)) {
    if (allowed(name)) cleaned[name] = env[name];
  }
  return cleaned;
}
