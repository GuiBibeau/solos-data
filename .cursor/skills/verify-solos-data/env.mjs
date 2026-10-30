// Environment isolation for the offline verify skill.
//
// The `solos-data` binary gives data-directory overrides from the environment precedence over a
// config file's `dataDir`/`rawDir` fields (see `load_decoder_config` in
// crates/solos-data/src/cli.rs and `load_config` in crates/solos-data/src/collector/config.rs).
// A parent shell that exports SOLOS_DATA_DIR, SOLOS_DATA_RAW_DIR or SOLOS_DATA_DECODED_DIR would
// therefore silently redirect the offline journeys away from their fixtures and into a real data
// directory. Every subprocess the skill spawns is scrubbed of these overrides, and the doctor
// refuses to start when the invoking shell exported any of them.

export const DATA_DIR_OVERRIDES = [
  'SOLOS_DATA_DIR',
  'SOLOS_DATA_RAW_DIR',
  'SOLOS_DATA_DECODED_DIR',
];

/** Names from DATA_DIR_OVERRIDES that are set to a non-empty value in `env`. */
export function dataDirOverridesIn(env) {
  return DATA_DIR_OVERRIDES.filter(
    (name) => env !== undefined && env !== null && env[name] !== undefined && env[name] !== '',
  );
}

/** A copy of `env` with every data-directory override removed, leaving other variables intact. */
export function subprocessEnv(env = process.env) {
  const cleaned = { ...env };
  for (const name of DATA_DIR_OVERRIDES) {
    delete cleaned[name];
  }
  return cleaned;
}
