import { createSolanaRpcFromTransport } from '@solana/kit';
import { json, sleep, type Config, type Lane } from './config.ts';
import { Limiter } from './limiter.ts';

export class RpcFailure extends Error {
  code: number;
  throttled: boolean;
  constructor(method: string, code: number, throttled = code === 429) {
    super(`RPC ${method} failed (code ${code})`);
    this.code = code;
    this.throttled = throttled;
  }
}

export interface Rpc {
  call<T = any>(method: string, params: unknown[], lane?: Lane): Promise<T>;
}

export class Provider implements Rpc {
  url: string;
  config: Config;
  limiter: Limiter;
  provider: string;
  counters: Record<string, number> = { requests: 0, cu: 0, throttled: 0, errors: 0, nulls: 0 };
  sequence = 0;
  shutdown = new AbortController();
  rawTransactions = new Map<string, string>();
  rawPages = new WeakMap<object, string>();

  constructor(url: string, config: Config, limiter: Limiter, provider = 'alchemy') {
    this.url = url;
    this.config = config;
    this.limiter = limiter;
    this.provider = provider;
  }

  typed(lane: Lane = 'tail') {
    return createSolanaRpcFromTransport(async <TResponse>({ payload }: { payload: unknown }): Promise<TResponse> => {
      const request = payload as { id: string; method: string; params: unknown[] };
      return { jsonrpc: '2.0', id: request.id, result: await this.call(request.method, request.params, lane) } as TResponse;
    });
  }

  async call<T = any>(method: string, params: unknown[], lane: Lane = 'tail'): Promise<T> {
    const weight = this.config.cuWeights[method];
    if (!weight) throw new Error(`Read method is not configured: ${method}`);
    for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
      if (this.shutdown.signal.aborted) throw new Error('Shutdown requested');
      const queuedAt = performance.now();
      await this.limiter.acquire(lane, weight, this.shutdown.signal);
      const startedAt = performance.now();
      const prefix = `${lane}_${method}`;
      this.counters[`${prefix}_wait_ms`] = (this.counters[`${prefix}_wait_ms`] ?? 0) + startedAt - queuedAt;
      let throttled = false;
      let retryMs = 0;
      try {
        this.counters.requests++;
        this.counters.cu += weight;
        this.counters[`${lane}_cu`] = (this.counters[`${lane}_cu`] ?? 0) + weight;
        this.counters[`${prefix}_requests`] = (this.counters[`${prefix}_requests`] ?? 0) + 1;
        const response = await fetch(this.url, {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: json({ jsonrpc: '2.0', id: ++this.sequence, method, params }),
          signal: AbortSignal.any([this.shutdown.signal, AbortSignal.timeout(30000)]),
        });
        const retryHeader = response.headers.get('retry-after');
        retryMs = retryHeader ? (/^\d+$/.test(retryHeader) ? Number(retryHeader) * 1000 : Math.max(0, Date.parse(retryHeader) - Date.now())) : 0;
        if (!Number.isFinite(retryMs)) retryMs = 0;
        if (!response.ok) throw new RpcFailure(method, response.status);
        const raw = await response.text();
        const envelope = JSON.parse(raw);
        if (envelope.error) {
          const code = Number(envelope.error.code);
          const limited = code === 429 || (code === -32005 && /throughput|rate.limit|too many/i.test(String(envelope.error.message)));
          throw new RpcFailure(method, code, limited);
        }
        if (!Object.hasOwn(envelope, 'result')) throw new RpcFailure(method, -1);
        if (envelope.result === null) this.counters.nulls++;
        if (method === 'getTransaction' && envelope.result !== null) this.rawTransactions.set(String(params[0]), raw);
        if (method === 'getTransactionsForAddress' && envelope.result) this.rawPages.set(envelope.result, raw);
        return envelope.result;
      } catch (error) {
        if (this.shutdown.signal.aborted) throw new Error('Shutdown requested');
        const code = error instanceof RpcFailure ? error.code : -1;
        throttled = error instanceof RpcFailure && error.throttled;
        this.counters[`error_code_${code}`] = (this.counters[`error_code_${code}`] ?? 0) + 1;
        this.counters.errors++;
        if (throttled) this.counters.throttled++;
        if ([400, 401, 403, -32601, -32602, -32015].includes(code) || attempt + 1 === this.config.maxRetries) throw new RpcFailure(method, code);
      } finally {
        this.counters[`${prefix}_duration_ms`] = (this.counters[`${prefix}_duration_ms`] ?? 0) + performance.now() - startedAt;
        this.limiter.release(lane, throttled, retryMs);
      }
      await sleep(Math.max(retryMs, Math.random() * Math.min(32000, 500 * 2 ** attempt)));
    }
    throw new RpcFailure(method, -1);
  }
}
