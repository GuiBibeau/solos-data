import type { Store } from './store.ts';

export async function validateRange(store: Store, from: number, to: number) {
  const [counts] = await store.rows(`SELECT count(*) AS manifest,
    count(t.signature) AS fetched,
    count(*) FILTER (WHERE t.tx_b64 IS NULL) AS missing,
    count(*) FILTER (WHERE t.single_in_slot IS NULL OR (NOT t.single_in_slot AND t.tx_index IS NULL)) AS unordered
    FROM signatures s LEFT JOIN (SELECT signature, tx_b64, single_in_slot, tx_index FROM transactions
      WHERE slot BETWEEN ? AND ?) t USING(signature) WHERE s.slot BETWEEN ? AND ?`, [from, to, from, to]);
  const [coverage] = await store.rows(`SELECT count(*) AS bad FROM (
    SELECT s.slot, count(*) AS n, count(t.tx_index) AS indexed,
      count(DISTINCT t.tx_index) AS distinct_index,
      count(*) FILTER (WHERE t.single_in_slot) AS singles
    FROM signatures s LEFT JOIN (SELECT signature, single_in_slot, tx_index FROM transactions
      WHERE slot BETWEEN ? AND ?) t USING(signature) WHERE s.slot BETWEEN ? AND ?
    GROUP BY s.slot HAVING (n>1 AND (indexed<>n OR distinct_index<>n OR singles>0)) OR (n=1 AND singles<>1)
    )`, [from, to, from, to]);
  const ok = Number(counts.missing) === 0 && Number(counts.unordered) === 0 && Number(coverage.bad) === 0;
  return { ok, V1: Number(counts.missing) === 0, V6: Number(coverage.bad) === 0 && Number(counts.unordered) === 0,
    missing:Number(counts.missing),unordered:Number(counts.unordered),badOrderingSlots:Number(coverage.bad),
    from, to, manifest: Number(counts.manifest), fetched: Number(counts.fetched),
    independentChecks: { V3: 'pending', V4: 'credentials_required', V5: 'provider_required' } };
}

export function chunkEnd(from: number, ceiling: number, size: number) {
  return Math.min(ceiling, from + size - 1);
}

export function assertPublishable(report: { ok: boolean; missing?:number; unordered?:number; badOrderingSlots?:number }) {
  if (!report.ok) throw new Error(`Range validation failed; watermark will not advance (missing=${report.missing??'unknown'}, unordered=${report.unordered??'unknown'}, badSlots=${report.badOrderingSlots??'unknown'})`);
}
