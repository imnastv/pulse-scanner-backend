import { EventEmitter } from 'node:events';

const MAX_SIGNALS = 250;

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
    this.metrics = { status: 'starting', slots: 0, logs: 0, candidates: 0, marketCapFiltered: 0, minMarketCap, startedAt: new Date().toISOString(), lastEventAt: null };
  }

  start() {
    this.connect();
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
    const mint = balances.find((x) => x?.mint)?.mint;
    if (!mint) throw new Error('Mint unavailable');
    const [supply, largest, market] = await Promise.all([
      this.rpc('getTokenSupply', [mint, { commitment: 'confirmed' }]),
      this.rpc('getTokenLargestAccounts', [mint, { commitment: 'confirmed' }]),
      this.marketData(mint)
    ]);
    const total = Number(supply?.value?.uiAmountString || 0);
    const accounts = largest?.value ?? [];
    const top = accounts.slice(0, 5).reduce((sum, x) => sum + Number(x.uiAmountString || 0), 0);
    const concentration = total > 0 ? Math.min(100, top / total * 100) : 100;
    const score = Math.max(20, Math.min(95, Math.round(92 - concentration * .65 + Math.min(10, accounts.length / 2))));
    return { mint, score, risk: concentration < 25 ? 'Low' : concentration < 50 ? 'Medium' : 'High', top5Concentration: Number(concentration.toFixed(1)), supply: supply?.value?.uiAmountString ?? '0', holderAccountsSampled: accounts.length, ...market };
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
    const launchLike = logs.some((line) => /initialize|create|mint/i.test(line));
    if (!launchLike || result?.err) return;
    const base = {
      signature: result.signature,
      detectedAt: new Date().toISOString(),
      status: 'enriched'
    };
    let enrichment;
    try { enrichment = await this.enrich(result.signature); }
    catch (error) { enrichment = { score: 45, risk: 'High', reason: `Enrichment pending: ${error.message}` }; }
    if (!Number.isFinite(enrichment.marketCap) || enrichment.marketCap < this.minMarketCap) {
      this.metrics.marketCapFiltered += 1;
      return;
    }
    const signal = { ...base, ...enrichment, reason: enrichment.reason || `New Pump.fun mint detected; $${Math.round(enrichment.marketCap).toLocaleString('en-US')} market cap; top-5 concentration ${enrichment.top5Concentration}%` };
    this.signals.unshift(signal);
    this.signals.length = Math.min(this.signals.length, MAX_SIGNALS);
    this.metrics.candidates += 1;
    this.emit('signal', signal);
  }

  reconnect() {
    if (this.ws !== null) this.ws = null;
    this.metrics.status = 'reconnecting';
    const delay = Math.min(30_000, 1000 * 2 ** this.reconnectAttempt++);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  snapshot() { return { metrics: this.metrics, signals: this.signals }; }
}

