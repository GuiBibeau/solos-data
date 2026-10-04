import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { getTransactionDecoder, getTransactionEncoder, getCompiledTransactionMessageDecoder,
  getCompiledTransactionMessageEncoder } from '@solana/kit';
import { rawFixture } from './decode-fixtures.ts';
import { extractGroups } from '../src/decode/instructions.ts';

test('event instruction extraction resolves legacy, v0 and v1 wire layouts identically', async () => {
  const fixture = JSON.parse(await readFile(new URL('data/phoenix-events.json', import.meta.url),'utf8')).transactions[0];
  const raw = rawFixture(fixture);
  const tx = getTransactionDecoder().decode(Buffer.from(raw.tx_b64, 'base64'));
  const message = getCompiledTransactionMessageDecoder().decode(tx.messageBytes);
  if (message.version !== 'legacy') throw new Error('expected legacy fixture');
  const expected = extractGroups(raw.tx_b64, JSON.parse(raw.meta_json));
  const v0 = { ...message, version:0 as const, addressTableLookups:[] };
  const v1 = { header:message.header, staticAccounts:message.staticAccounts, lifetimeToken:message.lifetimeToken,
    version:1 as const, configMask:0, configValues:[], numInstructions:message.instructions.length,
    numStaticAccounts:message.staticAccounts.length,
    instructionHeaders:message.instructions.map(ix => ({ programAccountIndex:ix.programAddressIndex,
      numInstructionAccounts:ix.accountIndices?.length ?? 0, numInstructionDataBytes:ix.data?.length ?? 0 })),
    instructionPayloads:message.instructions.map(ix => ({ instructionAccountIndices:ix.accountIndices ?? [],
      instructionData:ix.data ?? new Uint8Array() })) };
  for (const variant of [v0,v1]) {
    const messageBytes = getCompiledTransactionMessageEncoder().encode(variant) as typeof tx.messageBytes;
    const wire = getTransactionEncoder().encode({ ...tx, messageBytes });
    assert.deepEqual(extractGroups(Buffer.from(wire).toString('base64'), JSON.parse(raw.meta_json)), expected);
  }
});
