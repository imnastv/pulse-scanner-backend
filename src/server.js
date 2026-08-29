import http from 'node:http';
import { URL } from 'node:url';
import { SolanaScanner } from './scanner.js';

const config = {
  port: Number(process.env.PORT || 10000),
  rpcUrl: process.env.SOLANA_RPC_URL || 'https://rpc.solanatracker.io/public',
  wsUrl: process.env.SOLANA_WS_URL || 'wss://rpc.solanatracker.io/public',
  programId: process.env.PUMP_PROGRAM_ID || '',
  allowedOrigin: process.env.ALLOWED_ORIGIN || 'https://pulse-pumpfun-scanner.contactnxstv.chatgpt.site',
  botToken: process.env.TELEGRAM_BOT_TOKEN || '',
  chatId: process.env.TELEGRAM_CHAT_ID || ''
  ,apiKey: process.env.API_ACCESS_KEY || '',
  alertThreshold: Number(process.env.ALERT_SCORE_THRESHOLD || 60)
  ,minMarketCap: Number(process.env.MIN_MARKET_CAP_USD || 2500)
};

const scanner = new SolanaScanner(config).start();
const clients = new Set();

const alerted = new Map();
async function sendTelegram(text) {
  if (!config.botToken || !config.chatId) throw new Error('Telegram secrets are not configured');
  const response = await fetch(`https://api.telegram.org/bot${config.botToken}/sendMessage`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ chat_id: config.chatId, text, disable_web_page_preview: true }) });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error(result.description || 'Telegram request failed');
  return { ok: true };
}

scanner.on('signal', async (signal) => {
  for (const client of clients) client.write(`event: signal\ndata: ${JSON.stringify(signal)}\n\n`);

  const verified = signal.holderDataAvailable && signal.score >= config.alertThreshold;
  const fallback = !signal.holderDataAvailable && signal.score === 45;
  if (!signal.mint || alerted.has(signal.mint) || (!verified && !fallback)) return;

  alerted.set(signal.mint, Date.now());
  const title = verified
    ? `✅ VERIFIED HIGH-POTENTIAL — PULSE ${signal.score}/100`
    : '⚠️ UNVERIFIED CANDIDATE — FALLBACK 45/100';
  const concentration = verified ? `${signal.top5Concentration}%` : 'Unavailable — holder RPC temporarily failed';
  const verification = verified ? 'Holder data verified' : 'Unverified fallback — use extra caution';

  try {
    await sendTelegram(`${title}\n\nMint: ${signal.mint}\nMarket cap: ${Math.round(signal.marketCap).toLocaleString('en-US')}\nStatus: ${verification}\nRisk: ${signal.risk}\nTop 5 concentration: ${concentration}\n\nhttps://pump.fun/coin/${signal.mint}`);
  } catch (error) {
    console.error('Telegram alert failed:', error.message);
  }
});

function sendJson(res, status, value) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': config.allowedOrigin, vary: 'Origin' });
  res.end(JSON.stringify(value));
}

async function telegramTest() {
  return sendTelegram('✅ PULSE backend is connected. Live scanner alerts are ready.');
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': config.allowedOrigin, 'access-control-allow-methods': 'GET,POST,OPTIONS', 'access-control-allow-headers': 'content-type' }); return res.end(); }
  if (req.method === 'GET' && url.pathname === '/health') return sendJson(res, 200, { ok: true, scanner: scanner.metrics.status, telegramConfigured: Boolean(config.botToken && config.chatId), programConfigured: Boolean(config.programId), minMarketCap: config.minMarketCap, alertScoreThreshold: config.alertThreshold });
  if (req.method === 'GET' && url.pathname === '/api/signals') return sendJson(res, 200, scanner.snapshot());
  if (req.method === 'GET' && url.pathname === '/api/stream') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', 'access-control-allow-origin': config.allowedOrigin });
    res.write(`event: snapshot\ndata: ${JSON.stringify(scanner.snapshot())}\n\n`);
    clients.add(res); req.on('close', () => clients.delete(res)); return;
  }
  if (req.method === 'POST' && url.pathname === '/api/telegram/test') {
    if (!config.apiKey || req.headers.authorization !== `Bearer ${config.apiKey}`) return sendJson(res, 401, { ok: false, error: 'Unauthorized' });
    try { return sendJson(res, 200, await telegramTest()); } catch (error) { return sendJson(res, 503, { ok: false, error: error.message }); }
  }
  return sendJson(res, 404, { error: 'Not found' });
});

server.listen(config.port, '0.0.0.0', () => console.log(`PULSE backend listening on ${config.port}`));

