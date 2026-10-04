import { sleep, type Lane } from './config.ts';

/** One CU bucket, tail priority, bounded idle borrowing, and AIMD concurrency. */
export class Limiter {
  rate: number;
  tokens: number;
  tailTokens: number;
  backfillTokens: number;
  last = performance.now();
  lastTail = -Infinity;
  waitingTail = 0;
  active = { tail: 0, backfill: 0 };
  windows: Record<Lane, number>;
  ceilings: Record<Lane, number>;
  successes = { tail: 0, backfill: 0 };
  cooldownUntil = 0;
  readonly maximumRate: number;
  readonly share: number;

  constructor(rate: number, share: number, concurrency: Record<Lane, number>) {
    this.maximumRate = rate;
    this.rate = rate;
    this.share = share;
    this.tokens = 0;
    this.tailTokens = 0;
    this.backfillTokens = 0;
    this.ceilings = { ...concurrency };
    this.windows = { ...concurrency };
  }

  refill() {
    const time = performance.now();
    const seconds = (time - this.last) / 1000;
    this.last = time;
    // Smooth requests to a 100ms burst, leaving headroom in provider rolling windows.
    const capacity = Math.max(this.rate / 10, 100);
    this.tokens = Math.min(capacity, this.tokens + seconds * this.rate);
    this.tailTokens = Math.min(capacity, this.tailTokens + seconds * this.rate * this.share);
    this.backfillTokens = Math.min(capacity, this.backfillTokens + seconds * this.rate * (1 - this.share));
  }

  async acquire(lane: Lane, weight: number, signal?: AbortSignal) {
    if (weight > this.maximumRate) throw new Error('CU rate is below a method weight');
    if (lane === 'tail') this.waitingTail++;
    try {
      for (;;) {
        if (signal?.aborted) throw new Error('Shutdown requested');
        this.refill();
        const time = performance.now();
        const idleBorrow = lane === 'backfill' && this.waitingTail === 0 && time - this.lastTail > 1000;
        const laneTokens = lane === 'tail' ? this.tailTokens : this.backfillTokens;
        if (time >= this.cooldownUntil && this.active[lane] < this.windows[lane]
          && this.tokens >= weight && (laneTokens >= weight || idleBorrow)) {
          this.tokens -= weight;
          if (lane === 'tail') { this.tailTokens -= weight; this.lastTail = time; }
          else this.backfillTokens = Math.max(0, this.backfillTokens - weight);
          this.active[lane]++;
          return;
        }
        await sleep(10);
      }
    } finally { if (lane === 'tail') this.waitingTail--; }
  }

  release(lane: Lane, throttled = false, retryMs = 0) {
    this.active[lane]--;
    if (throttled) {
      this.rate = Math.max(40, this.rate * 0.7);
      this.tokens = 0;
      this.windows[lane] = Math.max(1, Math.floor(this.windows[lane] / 2));
      this.cooldownUntil = performance.now() + Math.max(1000, retryMs);
      this.successes[lane] = 0;
    } else if (++this.successes[lane] >= 100) {
      this.windows[lane] = Math.min(this.ceilings[lane], this.windows[lane] + 1);
      this.rate = Math.min(this.maximumRate, this.rate + 10);
      this.successes[lane] = 0;
    }
  }
}
