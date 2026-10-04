import { getBase58Decoder } from '@solana/kit';
import type { DecodedGroup } from './codec.ts';
import { version, tables } from './schema.ts';

export interface RawTransaction {
  signature: string; slot: string | number; block_time: string | number | null;
  tx_index: number | null; single_in_slot: boolean | null; tx_b64: string | null;
  meta_json: string | null; terminal_error: string | null; err: unknown;
}
export type Rows = Record<typeof tables[number], Record<string, any>[]>;
export const emptyRows = (): Rows => ({ decoded_transactions: [], events: [], fills: [], order_events: [], funding_events: [], decode_errors: [] });
const pubkey = (value: unknown) => Array.isArray(value) && value.length === 32
  ? getBase58Decoder().decode(Uint8Array.from(value.map(Number))) : null;
const symbol = (value: any) => value?.symbol_bytes ? Buffer.from(value.symbol_bytes.map(Number)).toString('utf8').replace(/\0+$/, '') : null;

export function normalize(tx: RawTransaction, hash: string, groups: DecodedGroup[], sourceFile: string): Rows {
  const rows = emptyRows();
  let meta: any = {};
  try { meta = JSON.parse(tx.meta_json ?? '{}'); } catch { /* An extraction error quarantines this row. */ }
  const committed = meta.err === null && (tx.err === null || tx.err === 'null') && !tx.terminal_error;
  let errors = 0;
  for (const group of groups) {
    for (const [i, error] of group.errors.entries()) {
      errors++;
      rows.decode_errors.push({ error_id: `${tx.signature}/${group.path}/${i}`, signature: tx.signature,
        source_hash: hash, slot: tx.slot, instruction_path: group.path, error: error.error,
        bytes_base64: error.bytes, decoder_version: version });
    }
    let header: any = {};
    for (const [ordinal, event] of group.events.entries()) {
      const [type, body] = Object.entries(event)[0];
      if (type === 'Header') header = body;
      if (type === 'SlotContext' && String(body.slot) !== String(tx.slot)) throw new Error('event slot disagrees with transaction');
      const row = { event_id: `${tx.signature}/${group.path}/${ordinal}`, signature: tx.signature, source_hash: hash,
        slot: tx.slot, tx_index: tx.tx_index, single_in_slot: tx.single_in_slot, block_time: tx.block_time,
        instruction_path: group.path, event_ordinal: ordinal, event_type: type, committed,
        attribution: group.attribution, asset_symbol: symbol(body.asset_symbol ?? header.asset_symbol),
        asset_id: body.asset_id ?? header.asset_id ?? null, trader: pubkey(body.trader ?? header.trader_account),
        signer: pubkey(header.signer), sequence_number: header.sequence_number ?? null,
        tick_size: header.tick_size ?? null, base_lot_decimals: header.base_lot_decimals ?? null,
        quote_lot_decimals: header.quote_lot_decimals ?? null, decoder_version: version, event_json: JSON.stringify(event) };
      rows.events.push(row);
      // Attempts remain in events; analytic tables contain only executed events
      // whose instruction ownership was established from stack heights.
      if (!committed || group.attribution !== 'stack_height') continue;
      if (type === 'OrderFilled' || type === 'SplineFilled') rows.fills.push({ ...row,
        maker: pubkey(body.maker), maker_side: body.side, taker_side: body.side === 'Bid' ? 'Ask' : 'Bid',
        price_ticks: body.price, base_lots: body.base_lots_filled, quote_lots: body.quote_lots_filled,
        maker_fee_rate_micro: body.maker_fee_rate, order_sequence_number: body.order_sequence_number ?? null,
        spline_sequence_number: body.spline_sequence_number ?? null, quantity_remaining: body.quantity_remaining ?? null });
      if (['OrderPlaced', 'OrderModified', 'OrderRejected', 'OrderResidualDiscarded'].includes(type)) rows.order_events.push({ ...row,
        order_sequence_number: body.order_sequence_number ?? null, price_ticks: body.price ?? null,
        quantity_signed: body.quantity ?? body.base_lots_released ?? null,
        client_order_id: body.client_order_id ? Buffer.from(body.client_order_id.map(Number)).toString('hex') : null,
        modification_reason: body.reason === undefined ? null : JSON.stringify(body.reason) });
      if (type === 'TraderFundingSettled') rows.funding_events.push({ ...row,
        funding_payment_quote_lots: body.funding_payment, new_collateral_quote_lots: body.new_collateral_balance,
        cumulative_funding_snapshot: body.cumulative_funding_snapshot });
    }
  }
  rows.decoded_transactions.push({ signature: tx.signature, source_hash: hash, slot: tx.slot,
    tx_index: tx.tx_index, single_in_slot: tx.single_in_slot, block_time: tx.block_time, committed,
    status: errors ? 'quarantined' : groups.some(g => g.attribution !== 'stack_height') ? 'unattributed' : 'decoded',
    event_count: rows.events.length, error_count: errors, source_file: sourceFile,
    decoded_at: new Date().toISOString(), decoder_version: version });
  return rows;
}
