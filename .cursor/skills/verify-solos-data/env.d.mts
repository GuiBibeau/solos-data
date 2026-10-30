export declare function dangerousEnvNames(
  env: Record<string, string | undefined> | undefined | null,
): string[];

export declare function subprocessEnv(
  env?: Record<string, string | undefined>,
): Record<string, string | undefined>;
