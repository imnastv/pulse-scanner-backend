import { EventEmitter } from 'node:events';

const MAX_SIGNALS = 250;
const MAX_PENDING = 750;
const RETRY_WINDOW_MS = 5 * 60 * 1000;
const RETRY_INTERVAL_MS = 20 * 1000;
const RETRY_BATCH_SIZE = 3;

export class SolanaScanner extends EventEmitter {
  constructor({ wsUrl, rpcUrl, programId, minMarketCap = 2500 }) {
    super();
    this.wsUrl = wsUrl;
    this.rpcUrl = rpcUrl;
    this.programId = programId;
    this.minMarketCap = minMarketCap;
    this.ws = null;
    this.reconnectTimer = null;
    this.reconnectAttempt = 0;
    this.signals = [];
    this.pending = new Map();
    this.processingQueue = false;
    this.metrics = { status: 'starting', slots: 0, logs: 0, exactCreates: 0, candidates: 0, queued: 0, queueRechecks: 0, queueExpired: 0, marketCapFiltered: 0, minMarketCap, startedAt: new Date().toISOString(), lastEventAt: null };
  }

  start() {
    this.connect();
    this.queueTimer = setInterval(() => this.processQueue(), 1000);
    this.queueTimer.unref?.();
    return this;
  }

  connect() {
    clearTimeout(this.reconnectTimer);
    this.metrics.status = 'connecting';
    const ws = new WebSocket(this.wsUrl);
    this.ws = ws;
    ws.addEventListener('open', () => {
      this.reconnectAttempt = 0;
      this.metrics.status = this.programId ? 'live' : 'rpc-live-program-id-required';
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'slotSubscribe' }));
      if (this.programId) ws.send(JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'logsSubscribe', params: [{ mentions: [this.programId] }, { commitment: 'processed' }] }));
      this.emit('status', this.metrics);
    });
    ws.addEventListener('message', (event) => this.onMessage(String(event.data)));
    ws.addEventListener('close', () => this.reconnect());
    ws.addEventListener('error', () => ws.close());
  }

  async rpc(method, params) {
    const response = await fetch(this.rpcUrl, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method, params }) });
    const body = await response.json();
    if (!response.ok || body.error) throw new Error(body.error?.message || `RPC ${response.status}`);
    return body.result;
  }

  async marketData(mint) {
    try {
      const response = await fetch(`https://frontend-api-v3.pump.fun/coins-v2/${mint}`, { headers: { accept: 'application/json' } });
      if (response.ok) {
        const coin = await response.json();
        const marketCap = Number(coin.usd_market_cap ?? coin.market_cap ?? coin.data?.usd_market_cap ?? coin.data?.market_cap);
        if (Number.isFinite(marketCap) && marketCap > 0) return { marketCap, priceUsd: Number(coin.usd_market_price ?? coin.price_usd) || null, marketSource: 'pump.fun' };
      }
    } catch {}
    try {
      const response = await fetch(`https://api.dexscreener.com/token-pairs/v1/solana/${mint}`, { headers: { accept: 'application/json' } });
      if (response.ok) {
        const pairs = await response.json();
        const pair = Array.isArray(pairs) ? pairs.filter((x) => Number(x.marketCap ?? x.fdv) > 0).sort((a, b) => Number(b.liquidity?.usd || 0) - Number(a.liquidity?.usd || 0))[0] : null;
        if (pair) return { marketCap: Number(pair.marketCap ?? pair.fdv), priceUsd: Number(pair.priceUsd) || null, liquidityUsd: Number(pair.liquidity?.usd) || null, marketSource: 'dexscreener' };
      }
    } catch {}
    return { marketCap: null, priceUsd: null, marketSource: null };
  }

  async enrich(signature) {
    const transaction = await this.rpc('getTransaction', [signature, { encoding: 'jsonParsed', commitment: 'confirmed', maxSupportedTransactionVersion: 0 }]);
    const balances = transaction?.meta?.postTokenBalances ?? [];
    const mint = balances.find((x) => x?.mint?.endsWith('pump'))?.mint ?? balances.find((x) => x?.mint)?.mint;
    if (!mint) throw new Error('Mint unavailable');
    const [supplyResult, largestResult, marketResult] = await Promise.allSettled([
      this.rpc('getTokenSupply', [mint, { commitment: 'confirmed' }]),
      this.rpc('getTokenLargestAccounts', [mint, { commitment: 'confirmed' }]),
      this.marketData(mint)
    ]);
    const supply = supplyResult.status === 'fulfilled' ? supplyResult.value : null;
    const largest = largestResult.status === 'fulfilled' ? largestResult.value : null;
    const market = marketResult.status === 'fulfilled' ? marketResult.value : { marketCap: null, priceUsd: null, marketSource: null };
    const total = Number(supply?.value?.uiAmountString || 0);
    const accounts = largest?.value ?? [];
    const top = accounts.slice(0, 5).reduce((sum, x) => sum + Number(x.uiAmountString || 0), 0);
    const concentration = total > 0 ? Math.min(100, top / total * 100) : 100;
    const holderDataAvailable = largestResult.status === 'fulfilled' && total > 0 && accounts.length > 0;
    const score = holderDataAvailable ? Math.max(20, Math.min(95, Math.round(92 - concentration * .65 + Math.min(10, accounts.length / 2)))) : 45;
    return { mint, score, risk: holderDataAvailable ? (concentration < 25 ? 'Low' : concentration < 50 ? 'Medium' : 'High') : 'High', top5Concentration: holderDataAvailable ? Number(concentration.toFixed(1)) : null, holderDataAvailable, supply: supply?.value?.uiAmountString ?? '0', holderAccountsSampled: accounts.length, ...market };
  }

  publish(base, enrichment) {
    this.pending.delete(enrichment.mint);
    this.metrics.queued = this.pending.size;
    const concentration = enrichment.holderDataAvailable ? `${enrichment.top5Concentration}%` : 'unavailable (fallback score 45)';
    const signal = { ...base, ...enrichment, status: 'qualified', reason: `Pump.fun mint qualified after ${enrichment.retryCount || 0} rechecks; $${Math.round(enrichment.marketCap).toLocaleString('en-US')} market cap; top-5 concentration ${concentration}` };
    this.signals.unshift(signal);
    this.signals.length = Math.min(this.signals.length, MAX_SIGNALS);
    this.metrics.candidates += 1;
    this.emit('signal', signal);
  }

  queueCandidate(base, enrichment) {
    const key = enrichment.mint || base.signature;
    if (!key || this.pending.has(key) || (enrichment.mint && this.signals.some((signal) => signal.mint === enrichment.mint))) return;
    if (this.pending.size >= MAX_PENDING) {
      const oldest = this.pending.keys().next().value;
      if (oldest) this.pending.delete(oldest);
      this.metrics.queueExpired += 1;
    }
    const now = Date.now();
    this.pending.set(key, { base, enrichment, priority: enrichment.mint ? 110 : 100, createdAt: now, expiresAt: now + RETRY_WINDOW_MS, nextCheckAt: now + 1500, retryCount: 0 });
    this.metrics.queued = this.pending.size;
  }

  async processQueue() {
    if (this.processingQueue || !this.pending.size) return;
    this.processingQueue = true;
    try {
      const now = Date.now();
      const due = [...this.pending]
        .filter(([, value]) => value.nextCheckAt <= now)
        .sort((a, b) => b[1].priority - a[1].priority || a[1].createdAt - b[1].createdAt)
        .slice(0, RETRY_BATCH_SIZE);
      for (const [key, item] of due) {
        if (now >= item.expiresAt) {
          this.pending.delete(key);
          this.metrics.queueExpired += 1;
          this.metrics.marketCapFiltered += 1;
          continue;
        }
        item.retryCount += 1;
        this.metrics.queueRechecks += 1;
        if (!item.enrichment.mint) {
          try {
            const enrichment = await this.enrich(item.base.signature);
            this.pending.delete(key);
            if (this.pending.has(enrichment.mint) || this.signals.some((signal) => signal.mint === enrichment.mint)) continue;
            item.enrichment = { ...enrichment, retryCount: item.retryCount };
            item.priority = 110;
            if (Number.isFinite(enrichment.marketCap) && enrichment.marketCap >= this.minMarketCap) this.publish(item.base, item.enrichment);
            else {
              item.nextCheckAt = Date.now() + RETRY_INTERVAL_MS;
              this.pending.set(enrichment.mint, item);
            }
          } catch {
            item.nextCheckAt = Date.now() + 5000;
          }
          continue;
        }
        const market = await this.marketData(item.enrichment.mint);
        item.nextCheckAt = Date.now() + RETRY_INTERVAL_MS;
        item.enrichment = { ...item.enrichment, ...market, retryCount: item.retryCount };
        if (Number.isFinite(market.marketCap) && market.marketCap >= this.minMarketCap) this.publish(item.base, item.enrichment);
      }
    } finally {
      this.metrics.queued = this.pending.size;
      this.processingQueue = false;
    }
  }

  async onMessage(raw) {
    let message;
    try { message = JSON.parse(raw); } catch { return; }
    if (message.method === 'slotNotification') {
      this.metrics.slots += 1;
      this.metrics.lastEventAt = new Date().toISOString();
      return;
    }
    if (message.method !== 'logsNotification') return;
    this.metrics.logs += 1;
    this.metrics.lastEventAt = new Date().toISOString();
    const result = message.params?.result?.value;
    const logs = result?.logs ?? [];
    const exactCreate = logs.some((line) => /^Program log: Instruction: (Create|CreateV2)$/.test(line));
    if (!exactCreate || result?.err) return;
    this.metrics.exactCreates += 1;
    const base = {
      signature: result.signature,
      detectedAt: new Date().toISOString(),
      status: 'enriched'
    };
    this.queueCandidate(base, {});
  }

  reconnect() {
    if (this.ws !== null) this.ws = null;
    this.metrics.status = 'reconnecting';
    const delay = Math.min(30_000, 1000 * 2 ** this.reconnectAttempt++);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  snapshot() { return { metrics: this.metrics, signals: this.signals }; }
}

