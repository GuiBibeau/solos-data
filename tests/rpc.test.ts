import test from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config.ts';
import { Limiter } from '../src/limiter.ts';
import { Provider, RpcFailure } from '../src/rpc.ts';

test('an unhealthy Solana node is retriable but is not a throughput throttle', async () => {
  const original = globalThis.fetch;
  const config = await loadConfig(); config.maxRetries = 1;
  const limiter = new Limiter(700, 0.7, config.concurrency);
  limiter.tokens = 700; limiter.tailTokens = 700;
  const provider = new Provider('https://rpc.invalid/v2/secret', config, limiter);
  globalThis.fetch = async () => new Response(JSON.stringify({ error: { code: -32005, message: 'Node is behind by 100 slots' } }));
  try {
    await assert.rejects(provider.call('getSlot', [{ commitment: 'finalized' }]), RpcFailure);
    assert.equal(limiter.rate, 700);
    assert.equal(provider.counters.throttled, 0);
    assert.equal(provider.counters['error_code_-32005'], 1);
  } finally { globalThis.fetch = original; }
});

test('unsupported transaction versions fail without useless retries or provider message disclosure', async () => {
  const original = globalThis.fetch;
  const config = await loadConfig();
  const limiter = new Limiter(700, 0.7, config.concurrency);
  limiter.tokens = 700; limiter.tailTokens = 700;
  let calls = 0;
  const provider = new Provider('https://rpc.invalid/v2/secret', config, limiter);
  globalThis.fetch = async () => {
    calls++;
    return new Response(JSON.stringify({ error: { code: -32015, message: 'provider https://rpc.invalid/v2/secret says version unsupported' } }));
  };
  try {
    await assert.rejects(provider.call('getTransaction', ['sig', { maxSupportedTransactionVersion: 0 }]), error => {
      assert.ok(error instanceof RpcFailure); assert.ok(!error.message.includes('secret')); return true;
    });
    assert.equal(calls, 1);
  } finally { globalThis.fetch = original; }
});
