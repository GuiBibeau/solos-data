import test from 'node:test';
import assert from 'node:assert/strict';
import { Limiter } from '../src/limiter.ts';
import { safeError } from '../src/service.ts';

test('idle backfill borrowing cannot consume reserved tail tokens when tail is waiting', async () => {
  const limiter = new Limiter(400, 0.7, { tail: 2, backfill: 2 });
  limiter.tokens = 100; limiter.tailTokens = 80; limiter.backfillTokens = 0;
  limiter.waitingTail = 1; limiter.lastTail = performance.now();
  let backfillGranted = false;
  const backfill = limiter.acquire('backfill', 40).then(() => { backfillGranted = true; });
  await limiter.acquire('tail', 40);
  assert.equal(backfillGranted, false);
  assert.equal(limiter.active.tail, 1);
  limiter.release('tail');
  limiter.waitingTail = 0;
  limiter.backfillTokens = 40;
  limiter.tokens = 40;
  await backfill;
  limiter.release('backfill');
});

test('429 reduces throughput and concurrency and honors retry pause', () => {
  const limiter = new Limiter(400, 0.3, { tail: 8, backfill: 4 });
  limiter.active.tail = 1;
  limiter.release('tail', true, 2500);
  assert.equal(limiter.rate, 280);
  assert.equal(limiter.windows.tail, 4);
  assert.ok(limiter.cooldownUntil - performance.now() > 2400);
  assert.ok(!safeError(new Error('provider https://example.com/v2/secret failed')).includes('secret'));
});

test('an idle paid-plan bucket cannot release more than a 100ms burst', () => {
  const limiter = new Limiter(8000, 0.2, { tail: 24, backfill: 32 });
  limiter.last = performance.now() - 10000;
  limiter.refill();
  assert.equal(limiter.tokens, 800);
  assert.equal(limiter.tailTokens, 800);
  assert.equal(limiter.backfillTokens, 800);
});
