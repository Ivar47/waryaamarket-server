/**
 * WaryaaMarket Data Pipeline Server — Fly.io Edition
 * ====================================================
 * NO Redis required. Pure in-memory cache with Map-based
 * TTL store. Optional Upstash Redis via UPSTASH_REDIS_URL.
 *
 * Architecture:
 *   Binance WS ──► Collector ──► store (Map) ──► Express REST ──► Browser
 *                                             └──► WebSocket broadcast
 *
 * Deploy: fly launch → fly deploy (see fly.toml)
 */

import express        from 'express';
import cors           from 'cors';
import { WebSocketServer, WebSocket } from 'ws';
import fetch          from 'node-fetch';
import { createServer } from 'http';

const app  = express();
const PORT = process.env.PORT || 8080;

// ─── CORS ─────────────────────────────────────────────────────────────────────
app.use(cors({
  origin: [
    'https://waryaamarket.com',
    'https://www.waryaamarket.com',
    'https://ivar47.github.io',
    'http://localhost:3000',
    'http://127.0.0.1:5500',
  ]
}));
app.use(express.json());

// ─── IN-MEMORY CACHE (replaces Redis entirely) ────────────────────────────────
// Simple TTL Map — no external dependencies needed
class MemCache {
  constructor() { this._store = new Map(); }
  set(key, value, ttlSeconds = 3600) {
    this._store.set(key, { value, expiresAt: Date.now() + ttlSeconds * 1000 });
  }
  get(key) {
    const item = this._store.get(key);
    if (!item) return null;
    if (Date.now() > item.expiresAt) { this._store.delete(key); return null; }
    return item.value;
  }
  has(key) { return this.get(key) !== null; }
  // Cleanup expired keys every 5 minutes
  startCleanup() {
    setInterval(() => {
      const now = Date.now();
      for (const [k, v] of this._store) {
        if (now > v.expiresAt) this._store.delete(k);
      }
    }, 5 * 60 * 1000);
  }
}

const cache = new MemCache();
cache.startCleanup();

// ─── LIVE DATA STORE (hot path — zero serialization) ─────────────────────────
const store = {
  prices:     [],
  feargreed:  null,
  global:     null,
  gas:        null,
  exchanges:  null,
  categories: null,
  lastUpdate: { prices: 0, feargreed: 0, global: 0, gas: 0, exchanges: 0 }
};

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const COIN_IDS = {
  BTC:'bitcoin',ETH:'ethereum',BNB:'binancecoin',SOL:'solana',XRP:'ripple',
  USDT:'tether',USDC:'usd-coin',ADA:'cardano',AVAX:'avalanche-2',DOGE:'dogecoin',
  TRX:'tron',LINK:'chainlink',DOT:'polkadot',MATIC:'matic-network',SHIB:'shiba-inu',
  LTC:'litecoin',UNI:'uniswap',ATOM:'cosmos',XLM:'stellar',BCH:'bitcoin-cash',
  ETC:'ethereum-classic',NEAR:'near',APT:'aptos',ARB:'arbitrum',OP:'optimism',
  FIL:'filecoin',ALGO:'algorand',VET:'vechain',ICP:'internet-computer',
  HBAR:'hedera-hashgraph',AAVE:'aave',MKR:'maker',SNX:'synthetix-network-token',
  COMP:'compound-governance-token',CRV:'curve-dao-token',LDO:'lido-dao',
  INJ:'injective-protocol',SUI:'sui',SEI:'sei-network',TIA:'celestia',
  PYTH:'pyth-network',JUP:'jupiter',WLD:'worldcoin-wld',IMX:'immutable-x',
  SAND:'the-sandbox',MANA:'decentraland',AXS:'axie-infinity',FLOW:'flow',
  CHZ:'chiliz',ENJ:'enjincoin',GALA:'gala',FTM:'fantom',ONE:'harmony',
  XMR:'monero',ZEC:'zcash',DASH:'dash',EOS:'eos',XTZ:'tezos',
  SUSHI:'sushi',YFI:'yearn-finance',BAL:'balancer',ZRX:'0x',
  GRT:'the-graph',LRC:'loopring','1INCH':'1inch',DYDX:'dydx',
};

const CG_IDS = {
  BTC:'1',ETH:'279',BNB:'825',SOL:'4128',XRP:'44',USDT:'325',USDC:'6319',
  ADA:'975',AVAX:'12559',DOGE:'5',TRX:'1094',LINK:'877',DOT:'12171',
  MATIC:'4713',SHIB:'11939',LTC:'2',UNI:'12504',ATOM:'1481',XLM:'100',
};

function normalizeTicker(t) {
  const sym  = t.s.replace(/USDT$|BUSD$/, '');
  const id   = COIN_IDS[sym] || sym.toLowerCase();
  const cgId = CG_IDS[sym];
  const image = cgId
    ? `https://assets.coingecko.com/coins/images/${cgId}/small/${sym.toLowerCase()}.png`
    : `https://cdn.jsdelivr.net/gh/spothq/cryptocurrency-icons@master/32/color/${sym.toLowerCase()}.png`;
  return {
    id, symbol: sym.toLowerCase(), name: sym,
    current_price:                        parseFloat(t.c) || 0,
    price_change_percentage_24h:          parseFloat(t.P) || 0,
    price_change_percentage_1h_in_currency:  0,
    price_change_percentage_7d_in_currency:  0,
    market_cap:    (parseFloat(t.q) || 0) * (parseFloat(t.c) || 0),
    total_volume:   parseFloat(t.q) || 0,
    market_cap_rank: null,
    circulating_supply: 0,
    image,
    sparkline_in_7d: { price: [] },
    high_24h: parseFloat(t.h) || 0,
    low_24h:  parseFloat(t.l) || 0,
    _ts: Date.now(),
  };
}

async function safeFetch(url, opts = {}) {
  try {
    const r = await fetch(url, { ...opts });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    console.warn(`[Fetch] ${url.slice(0, 80)} — ${e.message}`);
    return null;
  }
}

// ─── BINANCE WEBSOCKET ────────────────────────────────────────────────────────
let binanceWs = null;
let binanceRetry = 2000;
let lastBroadcast = 0;

function connectBinance() {
  console.log('[Binance] Connecting...');
  binanceWs = new WebSocket('wss://stream.binance.com:9443/ws/!ticker@arr');

  binanceWs.on('open', () => {
    console.log('[Binance] Connected ✓');
    binanceRetry = 2000;
  });

  binanceWs.on('message', (raw) => {
    try {
      const tickers = JSON.parse(raw);
      if (!Array.isArray(tickers)) return;
      const filtered = tickers
        .filter(t => t.s.endsWith('USDT') && parseFloat(t.q) > 50000)
        .sort((a, b) => parseFloat(b.q) - parseFloat(a.q))
        .slice(0, 200);
      store.prices = filtered.map(normalizeTicker);
      store.lastUpdate.prices = Date.now();
      // Throttle broadcast to once per second max
      if (Date.now() - lastBroadcast > 1000) {
        broadcastPrices();
        lastBroadcast = Date.now();
      }
    } catch { /* ignore parse errors */ }
  });

  binanceWs.on('error', err => console.error('[Binance] Error:', err.message));
  binanceWs.on('close', () => {
    console.log(`[Binance] Closed. Retry in ${binanceRetry / 1000}s`);
    setTimeout(() => {
      binanceRetry = Math.min(binanceRetry * 1.5, 30000);
      connectBinance();
    }, binanceRetry);
  });
}
connectBinance();

// ─── COLLECTORS ───────────────────────────────────────────────────────────────
async function collectFearGreed() {
  const d = await safeFetch('https://api.alternative.me/fng/?limit=7');
  if (d) { store.feargreed = d; store.lastUpdate.feargreed = Date.now(); }
}

async function collectGlobal() {
  const d = await safeFetch('https://api.coingecko.com/api/v3/global');
  if (d) { store.global = d; store.lastUpdate.global = Date.now(); }
}

async function collectGas() {
  const key = process.env.ETHERSCAN_KEY || '';
  const d   = await safeFetch(
    `https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey=${key}`
  );
  if (d) { store.gas = d; store.lastUpdate.gas = Date.now(); }
}

async function collectExchanges() {
  const d = await safeFetch('https://api.coingecko.com/api/v3/exchanges?per_page=20&page=1');
  if (d) { store.exchanges = d; store.lastUpdate.exchanges = Date.now(); }
}

async function collectCategories() {
  const d = await safeFetch(
    'https://api.coingecko.com/api/v3/coins/categories?order=market_cap_desc'
  );
  if (d) { store.categories = d; }
}

async function collectSparklines() {
  const d = await safeFetch(
    'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=200&page=1&sparkline=true&price_change_percentage=1h,24h,7d'
  );
  if (d && Array.isArray(d)) {
    const map = {};
    d.forEach(c => {
      map[c.symbol.toUpperCase()] = {
        sparkline_in_7d: c.sparkline_in_7d,
        price_change_percentage_1h_in_currency: c.price_change_percentage_1h_in_currency,
        price_change_percentage_7d_in_currency: c.price_change_percentage_7d_in_currency,
        market_cap_rank: c.market_cap_rank,
        circulating_supply: c.circulating_supply,
        image: c.image,
        name: c.name,
      };
    });
    store.prices = store.prices.map(p => ({ ...p, ...(map[p.symbol.toUpperCase()] || {}) }));
    console.log('[Sparklines] Updated');
  }
}

// Initial load + schedule
(async () => {
  await Promise.allSettled([
    collectFearGreed(), collectGlobal(), collectGas(),
    collectExchanges(), collectCategories(), collectSparklines(),
  ]);
  console.log('[Init] All collectors done');
})();

setInterval(collectFearGreed,   10 * 60 * 1000);
setInterval(collectGlobal,       5 * 60 * 1000);
setInterval(collectGas,          2 * 60 * 1000);
setInterval(collectExchanges,   10 * 60 * 1000);
setInterval(collectCategories,  60 * 60 * 1000);
setInterval(collectSparklines,  60 * 60 * 1000);

// ─── BROWSER WEBSOCKET ────────────────────────────────────────────────────────
const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  if (store.prices.length) ws.send(JSON.stringify({ type: 'prices', data: store.prices }));
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function broadcastPrices() {
  if (!clients.size) return;
  const msg = JSON.stringify({ type: 'prices', data: store.prices });
  clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

// ─── REST ENDPOINTS ───────────────────────────────────────────────────────────
function fromStore(key, res) {
  const val = store[key];
  if (val) return res.json(val);
  res.status(503).json({ error: 'Warming up — retry in 10s' });
}

app.get('/api/prices',     (_, res) => fromStore('prices',     res));
app.get('/api/feargreed',  (_, res) => fromStore('feargreed',  res));
app.get('/api/global',     (_, res) => fromStore('global',     res));
app.get('/api/gas',        (_, res) => fromStore('gas',        res));
app.get('/api/exchanges',  (_, res) => fromStore('exchanges',  res));
app.get('/api/categories', (_, res) => fromStore('categories', res));

app.get('/api/category-coins', async (req, res) => {
  const id  = req.query.id || '';
  if (!id) return res.status(400).json({ error: 'Missing id' });
  const key = `cat:${id}`;
  const hit = cache.get(key);
  if (hit) return res.json(hit);
  const data = await safeFetch(
    `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=${id}&order=market_cap_desc&per_page=50&page=1&sparkline=true`
  );
  if (!data) return res.status(503).json({ error: 'Upstream error' });
  cache.set(key, data, 3600);
  res.json(data);
});

app.get('/api/news', async (req, res) => {
  const cat = req.query.categories || 'ALL';
  const key = `news:${cat}`;
  const hit = cache.get(key);
  if (hit) return res.json(hit);
  const data = await safeFetch(
    `https://min-api.cryptocompare.com/data/v2/news/?lang=EN${cat !== 'ALL' ? `&categories=${cat}` : ''}`
  );
  if (!data) return res.status(503).json({ error: 'Upstream error' });
  cache.set(key, data, 180);
  res.json(data);
});

app.get('/api/coin-tickers', async (req, res) => {
  const id  = req.query.id || '';
  if (!id) return res.status(400).json({ error: 'Missing id' });
  const key = `tickers:${id}`;
  const hit = cache.get(key);
  if (hit) return res.json(hit);
  const data = await safeFetch(
    `https://api.coingecko.com/api/v3/coins/${id}/tickers?include_exchange_logo=true&depth=true`
  );
  if (!data) return res.status(503).json({ error: 'Upstream error' });
  cache.set(key, data, 60);
  res.json(data);
});

app.get('/api/bitcoin-stats', async (req, res) => {
  const hit = cache.get('btc-stats');
  if (hit) return res.json(hit);
  const data = await safeFetch('https://blockchain.info/stats?format=json');
  if (!data) return res.status(503).json({ error: 'Upstream error' });
  cache.set('btc-stats', data, 120);
  res.json(data);
});

app.get('/api/dex/search', async (req, res) => {
  const q = encodeURIComponent((req.query.q || '').slice(0, 100));
  const data = await safeFetch(`https://api.dexscreener.com/latest/dex/search?q=${q}`);
  data ? res.json(data) : res.status(503).json({ error: 'Upstream error' });
});

app.get('/api/dex/boosts', async (req, res) => {
  const hit = cache.get('dex:boosts');
  if (hit) return res.json(hit);
  const data = await safeFetch('https://api.dexscreener.com/token-boosts/top/v1');
  if (!data) return res.status(503).json({ error: 'Upstream error' });
  cache.set('dex:boosts', data, 30);
  res.json(data);
});

app.get('/api/dex/tokens', async (req, res) => {
  const addr = req.query.address || '';
  if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) return res.status(400).json({ error: 'Invalid address' });
  const data = await safeFetch(`https://api.dexscreener.com/latest/dex/tokens/${addr}`);
  data ? res.json(data) : res.status(503).json({ error: 'Upstream error' });
});

app.get('/api/geckoterminal/new-pools', async (req, res) => {
  const net = /^[a-z0-9-]+$/.test(req.query.network || '') ? req.query.network : 'eth';
  const data = await safeFetch(
    `https://api.geckoterminal.com/api/v2/networks/${net}/new_pools?page=1`,
    { headers: { Accept: 'application/json;version=20230302' } }
  );
  data ? res.json(data) : res.status(503).json({ error: 'Upstream error' });
});

app.get('/api/geckoterminal/trending', async (req, res) => {
  const net = /^[a-z0-9-]+$/.test(req.query.network || '') ? req.query.network : 'eth';
  const data = await safeFetch(
    `https://api.geckoterminal.com/api/v2/networks/${net}/trending_pools?page=1`,
    { headers: { Accept: 'application/json;version=20230302' } }
  );
  data ? res.json(data) : res.status(503).json({ error: 'Upstream error' });
});

app.get('/api/rugcheck', async (req, res) => {
  const addr    = req.query.address || '';
  const chainId = req.query.chainId || '1';
  if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) return res.status(400).json({ error: 'Invalid address' });
  const [honeypot, dex] = await Promise.allSettled([
    safeFetch(`https://api.honeypot.is/v2/IsHoneypot?address=${addr}&chainID=${chainId}`),
    safeFetch(`https://api.dexscreener.com/latest/dex/tokens/${addr}`),
  ]);
  res.json({
    honeypot: honeypot.status === 'fulfilled' ? honeypot.value : null,
    dex:      dex.status === 'fulfilled' ? dex.value : null,
  });
});

// Signals — stored in memory cache (set via POST /api/admin/signals)
app.get('/api/signals', (_, res) => {
  res.json(cache.get('signals') || []);
});

// Health
app.get('/api/health', (_, res) => {
  res.json({
    status:      'ok',
    platform:    'fly.io',
    prices:       store.prices.length,
    clients:      clients.size,
    uptime:       Math.round(process.uptime()),
    lastPrice:    store.lastUpdate.prices,
    cacheKeys:    cache._store.size,
    memMB:        Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
  });
});

// ─── START ────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 WaryaaMarket [Fly.io] — port ${PORT}`);
  console.log(`   Health:    http://localhost:${PORT}/api/health`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Shutdown] SIGTERM received');
  httpServer.close(() => process.exit(0));
});
