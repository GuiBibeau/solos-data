export declare const DATA_DIR_OVERRIDES: readonly string[];

export declare function dataDirOverridesIn(
  env: Record<string, string | undefined> | undefined | null,
): string[];

export declare function subprocessEnv(
  env?: Record<string, string | undefined>,
): Record<string, string | undefined>;
