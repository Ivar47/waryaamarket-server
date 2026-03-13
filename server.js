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
import crypto         from 'crypto';

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

  // Helper: parse RSS XML into article objects
  function parseRSS(xml, sourceName) {
    const items = [];
    const itemRegex = /<item>([\s\S]*?)<\/item>/g;
    let m;
    while ((m = itemRegex.exec(xml)) !== null) {
      const block = m[1];
      const get = (tag) => {
        const r = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]></${tag}>|<${tag}[^>]*>([^<]*)</${tag}>`);
        const x = r.exec(block);
        return x ? (x[1] || x[2] || '').trim() : '';
      };
      const title = get('title');
      const link  = get('link') || get('guid');
      const pubDate = get('pubDate');
      const desc = get('description');
      // extract image from media:content or enclosure or description img tag
      const imgMatch = block.match(/url="([^"]+\.(jpg|jpeg|png|webp))"/i)
                    || block.match(/<img[^>]+src="([^"]+)"/i);
      if (title && link) {
        items.push({
          title,
          url: link,
          source: sourceName,
          source_info: { name: sourceName, img: '' },
          published_on: pubDate ? Math.floor(new Date(pubDate).getTime() / 1000) : Math.floor(Date.now() / 1000),
          imageurl: imgMatch ? imgMatch[1] : '',
          categories: 'Crypto'
        });
      }
    }
    return items;
  }

  // Fetch RSS as text (not JSON)
  async function fetchRSS(url, sourceName) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(8000) });
      if (!r.ok) return [];
      const xml = await r.text();
      return parseRSS(xml, sourceName);
    } catch(e) {
      console.warn(`[News] RSS failed: ${sourceName} — ${e.message}`);
      return [];
    }
  }

  // Source 1: CoinDesk RSS (most reliable crypto news RSS)
  // Source 2: Cointelegraph RSS
  // Source 3: Bitcoin Magazine RSS
  const [coindesk, cointelegraph, bitcoinmag] = await Promise.allSettled([
    fetchRSS('https://www.coindesk.com/arc/outboundfeeds/rss/', 'CoinDesk'),
    fetchRSS('https://cointelegraph.com/rss', 'CoinTelegraph'),
    fetchRSS('https://bitcoinmagazine.com/.rss/full/', 'Bitcoin Magazine'),
  ]);

  const articles = [
    ...(coindesk.status === 'fulfilled' ? coindesk.value : []),
    ...(cointelegraph.status === 'fulfilled' ? cointelegraph.value : []),
    ...(bitcoinmag.status === 'fulfilled' ? bitcoinmag.value : []),
  ];

  if (articles.length > 0) {
    // Sort by newest first, deduplicate by title
    const seen = new Set();
    const deduped = articles
      .filter(a => { if (seen.has(a.title)) return false; seen.add(a.title); return true; })
      .sort((a, b) => b.published_on - a.published_on)
      .slice(0, 30);

    const result = { Data: deduped };
    cache.set(key, result, 300); // cache 5 minutes
    return res.json(result);
  }

  // Last resort: CryptoCompare (may still work in some regions)
  const cc = await safeFetch(
    `https://min-api.cryptocompare.com/data/v2/news/?lang=EN${cat !== 'ALL' ? `&categories=${cat}` : ''}`
  );
  if (cc && cc.Data && cc.Data.length > 0) {
    cache.set(key, cc, 180);
    return res.json(cc);
  }

  return res.status(503).json({ error: 'News unavailable', Data: [] });
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

// ─── SIMPLE JWT ───────────────────────────────────────────────────────────────

function signJWT(payload) {
  const secret = process.env.JWT_SECRET || 'waryaa-secret';
  const header  = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body    = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig     = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}

function verifyJWT(token) {
  try {
    const secret = process.env.JWT_SECRET || 'waryaa-secret';
    const [header, body, sig] = token.split('.');
    const expected = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
    if (sig !== expected) return null;
    const payload = JSON.parse(Buffer.from(body, 'base64url').toString());
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) return null;
    return payload;
  } catch { return null; }
}

function requireAdmin(req, res) {
  const auth = req.headers['authorization'] || '';
  const token = auth.replace('Bearer ', '').trim();
  if (!token || !verifyJWT(token)) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

// ─── ADMIN ROUTES ─────────────────────────────────────────────────────────────
const loginAttempts = new Map();

app.post('/api/admin/login', (req, res) => {
  // Rate limit: 5 attempts per minute per IP
  const ip  = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const entry = loginAttempts.get(ip) || { count: 0, start: now };
  if (now - entry.start > 60000) { entry.count = 0; entry.start = now; }
  entry.count++;
  loginAttempts.set(ip, entry);
  if (entry.count > 5) return res.status(429).json({ error: 'Too many attempts. Wait 1 minute.' });

  const { password } = req.body;
  const expected = process.env.ADMIN_PASSWORD;
  if (!password || !expected || password !== expected) {
    return res.status(401).json({ error: 'Incorrect password' });
  }

  const token = signJWT({ admin: true, exp: Math.floor(Date.now() / 1000) + 28800 }); // 8h
  res.json({ token });
});

app.get('/api/admin/signals', (req, res) => {
  if (!requireAdmin(req, res)) return;
  res.json(cache.get('signals') || []);
});

app.post('/api/admin/signals', (req, res) => {
  if (!requireAdmin(req, res)) return;
  // Accept flat body: { name, ticker, entry, target, risk, horizon, notes }
  const { name, ticker, entry, target, risk, horizon, notes } = req.body;
  if (!name || !ticker) return res.status(400).json({ error: 'Missing fields' });
  const signal = { name, ticker, entry, target, risk, horizon, notes };
  signal.id = Date.now().toString();
  signal.ts = Math.floor(Date.now() / 1000);
  const signals = cache.get('signals') || [];
  signals.unshift(signal);
  cache.set('signals', signals.slice(0, 50), 86400 * 7);
  res.json({ ok: true, signal });
});

app.delete('/api/admin/signals/:id', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const signals = (cache.get('signals') || []).filter(s => s.id !== req.params.id);
  cache.set('signals', signals, 86400 * 7);
  res.json({ ok: true });
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
