import { getBase58Decoder, getBase58Encoder } from '@solana/kit';
import { program } from '../src/decode/instructions.ts';

const short = (value: number) => {
  const bytes = [];
  do { const byte = value & 127; value >>>= 7; bytes.push(byte | (value ? 128 : 0)); } while (value);
  return Buffer.from(bytes);
};
/** Reconstruct only the published fixture's compiled instructions, offline. */
export function rawFixture(fixture: any) {
  const topCount = Math.max(...fixture.instructions.map((ix: any) => ix.stackPath[0])) + 1;
  const top = Array.from({ length: topCount }, (_, i) => {
    const ix = fixture.instructions.find((ix: any) => ix.stackPath.length === 1 && ix.stackPath[0] === i);
    const bytes = Buffer.from(ix?.dataBase64 ?? '', 'base64');
    return Buffer.concat([Buffer.from([ix ? 1 : 0, 0]), short(bytes.length), bytes]);
  });
  const wire = Buffer.concat([Buffer.from([1]), Buffer.alloc(64), Buffer.from([1,0,1,2]), Buffer.alloc(32),
    Buffer.from(getBase58Encoder().encode(program)), Buffer.alloc(32), short(topCount), ...top]);
  const innerInstructions = Array.from({ length: topCount }, (_, i) => ({ index: i,
    instructions: fixture.instructions.filter((ix: any) => ix.stackPath.length > 1 && ix.stackPath[0] === i)
      .map((ix: any) => ({ programIdIndex: 1, accounts: [], data: getBase58Decoder().decode(Buffer.from(ix.dataBase64, 'base64')),
        stackHeight: ix.stackPath.length })) }));
  return { signature: fixture.signature, slot: fixture.slot, block_time: fixture.blockTime, tx_index: 0,
    single_in_slot: false, tx_b64: wire.toString('base64'), terminal_error: null, err: null,
    meta_json: JSON.stringify({ err: null, innerInstructions }) };
}
