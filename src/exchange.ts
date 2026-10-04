import { address } from '@solana/kit';
import { log, now, type Config } from './config.ts';
import type { Provider } from './rpc.ts';
import type { Store } from './store.ts';

export async function refreshExchange(provider: Provider, store: Store, config: Config) {
  const response = await fetch(config.exchangeUrl, { signal: AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error(`Exchange HTTP ${response.status}`);
  const snapshot = await response.json() as any;
  if (!snapshot.keys?.globalConfig || !Array.isArray(snapshot.markets)) throw new Error('Invalid exchange snapshot');
  const typed = provider.typed();
  const global = await typed.getAccountInfo(address(snapshot.keys.globalConfig), {
    encoding: 'base64', commitment: 'finalized',
  }).send();
  if (global.value?.owner !== config.programId || (snapshot.programId && snapshot.programId !== config.programId)) {
    throw new Error('Exchange program identity mismatch');
  }
  const program = await provider.call('getAccountInfo', [config.programId, { encoding: 'jsonParsed', commitment: 'finalized' }]);
  if (!program.value?.executable) throw new Error('Program is not executable');
  const programData = program.value.data?.parsed?.info?.programData;
  if (!programData) throw new Error('ProgramData address is missing');
  const slot = Number(global.context.slot);
  const entries = [{ address: config.programId, kind: 'program', symbol: '', status: 'active' },
    { address: programData, kind: 'programdata', symbol: '', status: 'active' }];
  for (const market of snapshot.markets) {
    for (const [field, kind] of [['marketPubkey', 'market'], ['splinePubkey', 'spline']]) {
      if (!market[field]) throw new Error('Exchange market address is missing');
      entries.push({ address: market[field], kind, symbol: market.symbol, status: market.marketStatus ?? 'unknown' });
    }
  }
  let added = 0;
  for (const entry of entries) {
    const [exists] = await store.rows('SELECT address FROM addresses WHERE address=?', [entry.address]);
    if (!exists) added++;
    await store.exec(`INSERT INTO addresses VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(address) DO UPDATE SET status=excluded.status, last_seen_slot=excluded.last_seen_slot`,
    [entry.address, entry.kind, entry.symbol, entry.status, slot, slot]);
  }
  await store.set('exchange', { fetchedAt: now(), observedSlot: slot, markets: snapshot.markets.length, programData });
  if (added) log('addresses_added', { count: added, observedSlot: slot });
  return { programData, slot, markets: snapshot.markets.length, identityConfirmed: true };
}

export async function confirmMainnet(provider: Provider) {
  const genesis = await provider.typed().getGenesisHash().send();
  if (genesis !== '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d') throw new Error('RPC is not Solana mainnet');
  return genesis;
}
