#!/usr/bin/env node
// shim.mjs — local proxy. All the clever work lives here.
//
//   OpenCode --(plain, localhost)--> shim --(tiny copy/insert recipe)--> Worker --> OpenCode Zen / Go
//
// * Only big POSTs to chat/messages/responses endpoints go through the Worker. Everything else
//   (GET /models, auth checks, ...) goes straight to the upstream, untouched.
// * Targets are allowlisted (default: opencode.ai only).
// * The shim keeps recent request bodies in memory, picks the one sharing the longest prefix with
//   the new body, and sends only a recipe: "copy these byte ranges, insert these bytes".
// * If the Worker no longer has the base (409), it re-sends once in full.
//
// Config (env):
//   WORKER_URL      https://<name>.<acct>.workers.dev/<PATH_SECRET>        (required)
//   SHIM_TOKEN      same value as the Worker secret                        (required)
//   PORT / HOST     default 8787 / 127.0.0.1
//   ALLOW_HOSTS     comma list, default "opencode.ai" ("*.example.com" ok)
//   ALIASES         JSON {"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"}
//   DELTA_PATHS     regex for POST paths that use delta, default (chat/completions|messages|responses)$
//   STORE_MIN       only store bodies >= this many bytes in the Worker's KV (default 32768)
//   BASE_MIN        minimum shared prefix to bother with a base (default 4096)
//   CACHE_MB        memory for remembered bodies (default 256)
//   FALLBACK_DIRECT "0" to disable falling back to a direct upstream call if the Worker is broken
//   ALLOW_INSECURE  "1" allows http:// targets (tests only)

import http from 'node:http';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { once } from 'node:events';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { commonLen, makeRecipe, encodeRecipe, encodeFull } from './diff.mjs';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);
const brotliDecompress = promisify(zlib.brotliDecompress);
const zstdDecompress = promisify(zlib.zstdDecompress);
const MiB = 1048576;
const HOP = new Set([
  'host', 'connection', 'content-length', 'transfer-encoding', 'keep-alive', 'te', 'trailer',
  'upgrade', 'proxy-authorization', 'proxy-authenticate', 'accept-encoding', 'content-encoding', 'expect',
]);
const STRIP_RES = new Set(['connection', 'keep-alive', 'transfer-encoding', 'content-length', 'content-encoding']);

export function configFromEnv(env = process.env) {
  return {
    port: Number(env.PORT) || 8787,
    host: env.HOST || '127.0.0.1',
    workerUrl: (env.WORKER_URL || '').replace(/\/+$/, ''),
    token: env.SHIM_TOKEN || '',
    allowHosts: (env.ALLOW_HOSTS || 'opencode.ai').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean),
    aliases: env.ALIASES
      ? JSON.parse(env.ALIASES)
      : { zen: 'https://opencode.ai/zen', go: 'https://opencode.ai/zen/go' },
    deltaPaths: new RegExp(env.DELTA_PATHS || '/(chat/completions|messages|responses)$'),
    storeMin: Number(env.STORE_MIN) || 32768,
    storeDeltaMin: Number.isFinite(Number(env.STORE_DELTA_MIN)) ? Number(env.STORE_DELTA_MIN) : 64 * 1024,
    storeAgeMs: (Number.isFinite(Number(env.STORE_AGE_MAX)) ? Number(env.STORE_AGE_MAX) : 3600) * 1000,
    baseMin: Number(env.BASE_MIN) || 4096,
    cacheBytes: (Number(env.CACHE_MB) || 256) * MiB,
    cacheCount: 16,
    maxBody: 256 * MiB,
    fallbackDirect: env.FALLBACK_DIRECT !== '0',
    // auto: gzip the fallback only if the incoming request was compressed; 1/0 force it.
    fallbackGzip: env.FALLBACK_GZIP === '1' ? true : env.FALLBACK_GZIP === '0' ? false : 'auto',
    pinBase: env.PIN_BASE !== '0',
    allowInsecure: env.ALLOW_INSECURE === '1',
    quiet: env.QUIET === '1',
    trace: env.SHIM_TRACE === '1',
  };
}

class BodyCache {
  constructor(maxBytes, maxCount) { this.m = new Map(); this.bytes = 0; this.maxBytes = maxBytes; this.maxCount = maxCount; }
  put(hash, buf) {
    if (this.m.has(hash)) { this.bytes -= this.m.get(hash).length; this.m.delete(hash); }
    this.m.set(hash, buf); this.bytes += buf.length;
    while (this.m.size > this.maxCount || this.bytes > this.maxBytes) {
      const [k, v] = this.m.entries().next().value;
      this.m.delete(k); this.bytes -= v.length;
    }
  }
  delete(hash) { const v = this.m.get(hash); if (v) { this.bytes -= v.length; this.m.delete(hash); } }
  entries() { return this.m.entries(); } // oldest -> newest
}

const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const kb = (n) => `${(n / 1024).toFixed(1)}K`;

export function createShim(cfg) {
  const cache = new BodyCache(cfg.cacheBytes, cfg.cacheCount);
  const stats = {
    requests: 0, delta: 0, passthrough: 0, fullUploads: 0, misses: 0, fallbacks: 0,
    origBytes: 0, wireBytes: 0, incomingBytes: 0, compressedRequests: 0, fallbackWireBytes: 0,
    totalMs: 0, lastMs: 0, workerMs: 0, pipeMs: 0, stores: 0,
  };
  // The base hash the Worker currently holds. Reused across turns until it stores a new one.
  let pinned = null;
  const log = (...a) => { if (!cfg.quiet) console.error('[shim]', ...a); };
  const trace = (...a) => { if (cfg.trace) console.error('[trace]', ...a); };

  // ---------- target resolution + allowlist ----------
  function hostAllowed(h) {
    return cfg.allowHosts.some((p) => (p.startsWith('*.') ? h.endsWith(p.slice(1)) : h === p));
  }

  function resolveTarget(rawUrl) {
    const q = rawUrl.indexOf('?');
    const path = q < 0 ? rawUrl : rawUrl.slice(0, q);
    const query = q < 0 ? '' : rawUrl.slice(q);
    let target;
    try {
      if (path.startsWith('/r/')) {
        // /r/https://host/path  (tolerates a collapsed "https:/host")
        const m = /^(https?):\/{1,2}/i.exec(path.slice(3));
        if (!m) return null;
        target = new URL(`${m[1].toLowerCase()}://${path.slice(3 + m[0].length)}${query}`);
      } else {
        const m = /^\/([A-Za-z0-9_-]+)(\/.*)?$/.exec(path);
        if (!m || !Object.hasOwn(cfg.aliases, m[1])) return null;
        target = new URL(`${cfg.aliases[m[1]].replace(/\/+$/, '')}${m[2] || ''}${query}`);
      }
    } catch { return null; }
    const okScheme = target.protocol === 'https:' || (cfg.allowInsecure && target.protocol === 'http:');
    if (!okScheme || target.username || target.password) return null;
    if (target.port && !cfg.allowInsecure) return null;
    if (!hostAllowed(target.hostname)) return null;
    return target;
  }

  // ---------- helpers ----------
  function localHostOk(req) {
    const h = (req.headers.host || '').toLowerCase().replace(/:\d+$/, '');
    return h === '127.0.0.1' || h === 'localhost' || h === '[::1]';
  }

  async function readBody(req) {
    const chunks = []; let n = 0;
    for await (const c of req) {
      n += c.length;
      if (n > cfg.maxBody) throw Object.assign(new Error('request too large'), { status: 413 });
      chunks.push(c);
    }
    req._wireIn = n; // actual bytes OpenCode sent -- what a direct (no-proxy) upload would cost
    let buf = Buffer.concat(chunks);
    const ce = (req.headers['content-encoding'] || 'identity').toLowerCase().trim();
    // Node's http server does not decode request bodies; do it here so the delta path
    // works with compressed requests too. The encoding header is stripped by forwardHeaders.
    if (ce === 'gzip') buf = await gunzip(buf);
    else if (ce === 'br') buf = await brotliDecompress(buf);
    else if (ce === 'zstd') buf = await zstdDecompress(buf);
    else if (ce !== 'identity' && ce !== '') throw Object.assign(new Error(`unsupported content-encoding: ${ce}`), { status: 415 });
    if (buf.length > cfg.maxBody) throw Object.assign(new Error('request too large'), { status: 413 });
    req._decodedEncoding = ce === '' ? 'identity' : ce;
    return buf;
  }

  function forwardHeaders(req) {
    const h = {};
    for (const [k, v] of Object.entries(req.headers)) if (!HOP.has(k)) h[k] = Array.isArray(v) ? v.join(', ') : v;
    return h;
  }

  async function pipeResponse(r, res) {
    const headers = {};
    for (const [k, v] of r.headers) if (!STRIP_RES.has(k) && !k.startsWith('x-delta-')) headers[k] = v;
    if (r.status >= 400) {
      const errBody = (await r.clone().text().catch(() => '')).slice(0, 2000);
      stats.lastError = { status: r.status, body: errBody, at: new Date().toISOString() };
      log(`!! response ${r.status}: ${errBody.slice(0, 240)}`);
    }
    trace('pipe start', r.status, 'ct', r.headers.get('content-type'), 'te', r.headers.get('transfer-encoding'), 'len', r.headers.get('content-length'));
    res.writeHead(r.status, headers);
    res.flushHeaders();
    if (!r.body) { trace('pipe end (no body)'); return res.end(); }
    let bytes = 0, chunks = 0;
    const t0 = Date.now();
    try {
      for await (const chunk of r.body) {
        bytes += chunk.length; chunks++;
        if (!res.write(chunk)) await once(res, 'drain');
      }
      res.end();
      stats.pipeMs += Date.now() - t0;
      trace('pipe end', { bytes, chunks, ms: Date.now() - t0, finished: res.writableFinished });
    } catch (e) {
      trace('pipe error', e.message, { bytes, chunks, ms: Date.now() - t0 });
      res.destroy();
    }
  }

  // ---------- delta planning ----------
  function plan(body) {
    // With pinning, always diff against the base the Worker still holds so one base can serve
    // several turns. Without it, fall back to the best-matching cached body.
    if (cfg.pinBase && pinned) {
      const n = commonLen(pinned.body, 0, body, 0);
      if (n >= cfg.baseMin) {
        const recipe = encodeRecipe(makeRecipe(pinned.body, body), body);
        if (recipe.length < body.length * 0.9) return { baseHash: pinned.hash, recipe, shared: n };
      }
    }
    let best = null, bestHash = null, bestN = 0;
    for (const [h, buf] of cache.entries()) { // oldest -> newest, so ties go to the newest
      if (cfg.pinBase && pinned && h === pinned.hash) continue;
      const n = commonLen(buf, 0, body, 0);
      if (n >= bestN) { best = buf; bestHash = h; bestN = n; }
    }
    if (best && bestN >= cfg.baseMin) {
      const recipe = encodeRecipe(makeRecipe(best, body), body);
      if (recipe.length < body.length * 0.9) return { baseHash: bestHash, recipe, shared: bestN };
    }
    return { baseHash: null, recipe: encodeFull(body), shared: 0 };
  }

  async function sendViaWorker(req, target, body, p, hash, store, signal) {
    let payload = p.recipe, enc = 'identity';
    if (payload.length >= 512) {
      const z = await gzip(payload, { level: 6 });
      if (z.length < payload.length * 0.95) { payload = z; enc = 'gzip'; }
    }
    const headers = {
      ...forwardHeaders(req),
      'content-type': 'application/octet-stream',
      'x-delta-ct': req.headers['content-type'] || 'application/json',
      'x-shim-token': cfg.token,
      'x-delta-target': target.href,
      'x-delta-len': String(body.length),
      'x-delta-enc': enc,
    };
    if (p.baseHash) headers['x-delta-base'] = p.baseHash;
    if (store) headers['x-delta-result'] = hash;
    const r = await fetch(`${cfg.workerUrl}/relay`, { method: 'POST', headers, body: payload, signal, redirect: 'manual' });
    return { r, wire: payload.length };
  }

  // ---------- request paths ----------
  async function passthrough(req, res, target, body, signal, opts = {}) {
    stats.passthrough++;
    const headers = forwardHeaders(req);
    let out = body;
    // Only the Worker-unavailable fallback opts in; normal passthrough stays untouched.
    if (opts.gzip && body.length >= 512 && req.method !== 'GET' && req.method !== 'HEAD') {
      out = await gzip(body, { level: 6 });
      headers['content-encoding'] = 'gzip';
    }
    trace('direct send', req.method, target.href, { body: body.length, wire: out.length, gzip: !!headers['content-encoding'] });
    const t0 = Date.now();
    const r = await fetch(target, {
      method: req.method, headers,
      body: req.method === 'GET' || req.method === 'HEAD' ? undefined : out,
      signal, redirect: 'manual',
    });
    trace('direct response', r.status, { ms: Date.now() - t0, ct: r.headers.get('content-type'), te: r.headers.get('transfer-encoding') });
    await pipeResponse(r, res);
    return out.length;
  }

  async function relayDelta(req, res, target, body, signal) {
    const hash = sha256(body);
    let p = plan(body);
    // The shim owns the write decision: reuse the committed base until the recipe grows
    // past STORE_DELTA_MIN or the base ages past STORE_AGE_MAX. If the write fails, keep
    // using the old base (pinned stays unchanged) instead of re-uploading every turn.
    const age = pinned ? Date.now() - pinned.at : Infinity;
    const shouldStore = !cfg.pinBase || !pinned ||
      p.recipe.length >= cfg.storeDeltaMin || age >= cfg.storeAgeMs;
    let store = body.length >= cfg.storeMin && shouldStore;
    trace('worker send', req.method, req.url, { body: body.length, base: !!p.baseHash, recipe: p.recipe.length, store });
    const tw0 = Date.now();
    let { r, wire, err } = await sendViaWorker(req, target, body, p, hash, store, signal).catch((e) => ({ err: e }));
    stats.workerMs += Date.now() - tw0;
    trace('worker response', r ? r.status : 'network error', {
      ms: Date.now() - tw0, worker: r?.headers.get('x-delta-worker'), stored: r?.headers.get('x-delta-stored'),
      ct: r?.headers.get('content-type'), te: r?.headers.get('transfer-encoding'), err: err?.message, code: err?.cause?.code,
    });
    let totalWire = wire || 0;

    if (r && r.status === 409 && r.headers.get('x-delta-miss')) {
      stats.misses++;
      if (p.baseHash) cache.delete(p.baseHash);
      if (cfg.pinBase) pinned = null;
      await r.arrayBuffer().catch(() => {});
      p = { baseHash: null, recipe: encodeFull(body), shared: 0 };
      store = body.length >= cfg.storeMin; // a lost base must always be re-stored
      ({ r, wire } = await sendViaWorker(req, target, body, p, hash, store, signal).catch((e) => ({ err: e })));
      totalWire += wire || 0;
    }

    const workerBroke = !r || !r.headers.get('x-delta-worker');
    if (workerBroke && cfg.fallbackDirect) {
      stats.fallbacks++;
      const useGzip = cfg.fallbackGzip === true ||
        (cfg.fallbackGzip === 'auto' && !!req._decodedEncoding && req._decodedEncoding !== 'identity');
      log(`!! Worker unavailable (${r ? `HTTP ${r.status}` : 'network error'}); sending DIRECT` +
        (useGzip ? ' gzip' : ' full body') + ` ${kb(body.length)}`);
      if (r) await r.arrayBuffer().catch(() => {});
      stats.origBytes += body.length;
      const wire = await passthrough(req, res, target, body, signal, { gzip: !!useGzip });
      stats.wireBytes += wire; stats.fallbackWireBytes += wire;
      return;
    }
    if (!r) throw new Error('Worker unreachable');

    if (r.status === 401) log('!! Worker rejected SHIM_TOKEN (401)');
    const stored = r.headers.get('x-delta-stored') === '1';
    const currentBase = r.headers.get('x-delta-base-current');
    if (stored) {
      stats.stores++;
      if (store) cache.put(hash, body);
      if (cfg.pinBase) pinned = { hash, body, at: Date.now() };
    } else if (cfg.pinBase && currentBase && pinned && currentBase !== pinned.hash) {
      // Worker kept or switched to a different base; follow it.
      pinned = null;
    }

    stats.delta++; if (!p.baseHash) stats.fullUploads++;
    stats.origBytes += body.length; stats.wireBytes += totalWire;
    const compressed = req._decodedEncoding && req._decodedEncoding !== 'identity';
    const inNote = compressed
      ? `in ${req._decodedEncoding} ${kb(req._wireIn)} (decoded ${kb(body.length)})`
      : `in plain ${kb(req._wireIn)}`;
    log(`${req.method} ${new URL(req.url, 'http://x').pathname}  ${inNote} -> wire ${kb(totalWire)}` +
      (p.baseHash ? `  (${((p.shared / body.length) * 100).toFixed(0)}% shared prefix)` : '  (full)'));
    await pipeResponse(r, res);
  }

  async function handle(req, res) {
    // Drive-by protection: a web page in your browser must not be able to use this relay.
    if (!localHostOk(req) || req.headers.origin || req.headers['sec-fetch-site']) {
      res.writeHead(403); return res.end('forbidden');
    }
    if (req.url === '/__shim/health') {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, stats, cachedBodies: cache.m.size, cachedBytes: cache.bytes }));
    }
    const target = resolveTarget(req.url);
    if (!target) { res.writeHead(404); return res.end('unknown or disallowed target'); }

    stats.requests++;
    const t0 = Date.now();
    const body = await readBody(req);
    stats.incomingBytes += req._wireIn;
    if (req._decodedEncoding && req._decodedEncoding !== 'identity') stats.compressedRequests++;
    trace('request', req.method, req.url, { wireIn: req._wireIn, decoded: body.length, ct: req.headers['content-type'], ce: req._decodedEncoding });
    const ac = new AbortController();
    res.on('close', () => {
      const ms = Date.now() - t0;
      stats.totalMs += ms; stats.lastMs = ms;
      trace('client close', req.url, { finished: res.writableFinished, aborted: ac.signal.aborted, ms });
      if (!res.writableFinished) ac.abort();
    });

    // Compressed bodies are decoded in readBody, so content-encoding no longer excludes the delta path.
    const eligible = req.method === 'POST' && cfg.deltaPaths.test(target.pathname) &&
      /json/i.test(req.headers['content-type'] || '') && body.length > 0 && cfg.workerUrl && cfg.token;
    return eligible ? relayDelta(req, res, target, body, ac.signal) : passthrough(req, res, target, body, ac.signal);
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((e) => {
      trace('handler error', e.message, e.cause?.code, e.cause?.message, e.stack?.split('\n')[1]?.trim());
      log('error:', e.message);
      if (!res.headersSent) { res.writeHead(e.status || 502, { 'content-type': 'text/plain' }); res.end(`shim error: ${e.message}`); }
      else res.destroy();
    });
  });
  server.on('connection', (s) => s.setNoDelay(true));
  server.requestTimeout = 0; // long streaming responses
  server.headersTimeout = 60_000;
  return { server, stats, cache };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const cfg = configFromEnv();
  if (!cfg.workerUrl || !cfg.token) {
    console.error('Set WORKER_URL and SHIM_TOKEN (see README). Running as a plain allowlisted proxy, no delta.');
  }
  const { server } = createShim(cfg);
  server.listen(cfg.port, cfg.host, () => {
    console.error(`[shim] listening on http://${cfg.host}:${cfg.port}  allow=${cfg.allowHosts.join(',')}  worker=${cfg.workerUrl ? 'yes' : 'NO'}`);
  });
}
