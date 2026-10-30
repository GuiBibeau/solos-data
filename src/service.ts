/** A bounded, URL-redacted error string for the CLI's fatal log line. */
export function safeError(error: unknown): string {
  const message = error instanceof Error ? error.message : 'Unknown failure';
  return message.replace(/https?:\/\/[^\s"']+/g, '[redacted-url]').slice(0, 300);
}
