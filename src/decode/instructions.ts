import { getBase58Encoder, getTransactionDecoder, getCompiledTransactionMessageDecoder } from '@solana/kit';

export const program = 'EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih';
const tags = new Set(['8de6d6f209d1cfaa', 'f70786cbb5479947']);
export interface Instruction { programId: string; path: number[]; data: Uint8Array; attribution: string }
export interface Group { path: string; logs: string[]; attribution: string }

/** A CPI sibling increments at its stack depth; descendants start at zero. */
export function advancePath(path: number[], height: number) {
  if (!Number.isInteger(height) || height < 2) throw new Error('invalid CPI stack height');
  const depth = height - 1;
  if (depth > path.length + 1) throw new Error('CPI stack depth skips an ancestor');
  if (depth > path.length) return [...path, 0];
  const next = path.slice(0, depth);
  next[depth - 1]++;
  return next;
}

export function groupInstructions(instructions: Instruction[]): Group[] {
  const groups: Group[] = [];
  for (const ix of instructions) {
    if (ix.programId !== program) continue;
    const path = ix.path.join('.');
    const tag = Buffer.from(ix.data.subarray(0, 8)).toString('hex');
    if (!tags.has(tag)) { groups.push({ path, logs: [], attribution: ix.attribution }); continue; }
    const parent = ix.path.slice(0, -1).join('.');
    let group = groups.findLast(group => group.path === parent);
    if (!group) {
      // Missing stackHeight cannot safely associate nested Phoenix instructions.
      group = { path: `orphan.${path}`, logs: [], attribution: 'unknown' };
      groups.push(group);
    }
    group.logs.push(Buffer.from(ix.data).toString('base64'));
    if (ix.attribution !== 'stack_height') group.attribution = 'unknown';
  }
  return groups.filter(group => group.logs.length > 0);
}

export function extractGroups(txBase64: string, meta: any) {
  const tx = getTransactionDecoder().decode(Buffer.from(txBase64, 'base64'));
  const message = getCompiledTransactionMessageDecoder().decode(tx.messageBytes);
  const addresses = [...message.staticAccounts, ...(meta.loadedAddresses?.writable ?? []),
    ...(meta.loadedAddresses?.readonly ?? [])];
  const top = message.version === 1 ? message.instructionHeaders.map((header, i) => ({
    programAddressIndex: header.programAccountIndex, data: message.instructionPayloads[i].instructionData,
  })) : message.instructions;
  const instructions: Instruction[] = [];
  for (let i = 0; i < top.length; i++) {
    const ix = top[i];
    if (!addresses[ix.programAddressIndex]) throw new Error('unresolved program address');
    instructions.push({ programId: addresses[ix.programAddressIndex], path: [i],
      data: new Uint8Array(ix.data ?? []), attribution: 'stack_height' });
    let innerPath: number[] = [];
    for (const inner of meta.innerInstructions?.find((group: any) => group.index === i)?.instructions ?? []) {
      if (!addresses[inner.programIdIndex] || typeof inner.data !== 'string') throw new Error('unresolved CPI instruction');
      const known = Number.isInteger(inner.stackHeight) && inner.stackHeight >= 2;
      innerPath = advancePath(innerPath, known ? inner.stackHeight : 2);
      instructions.push({ programId: addresses[inner.programIdIndex], path: [i, ...innerPath],
        data: new Uint8Array(getBase58Encoder().encode(inner.data)), attribution: known ? 'stack_height' : 'unknown' });
    }
  }
  return groupInstructions(instructions);
}
