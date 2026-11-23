// worker.mjs — deliberately dumb relay. No JSON, no session logic.
//   1. check secret path + token
//   2. read the recipe (optionally gzip), fetch the base body from KV by hash
//   3. splice bytes into the full request body, store it in KV under its hash
//   4. forward it to the (allowlisted) upstream and stream the response straight back
//
// Bindings / config:
//   KV            KV namespace            (binding)
//   PATH_SECRET   unguessable path part   (secret)  -> POST /<PATH_SECRET>/relay
//   SHIM_TOKEN    shared token            (secret)  -> x-shim-token header
//   ALLOW_HOSTS   comma list, default "opencode.ai", "*.example.com" allowed
//   UPSTREAM_API_KEY  optional secret: injected as the upstream key (untested against Zen)
//   KV_TTL        seconds a stored body lives, default 14400
//   STORE_WAIT_MS ms to wait for a KV write before falling back to waitUntil, default 500
//   MAX_BODY      bytes, default 64 MiB
//   ALLOW_HTTP    "1" to allow http:// targets (tests only)

const MiB = 1048576;
const KV_VALUE_MAX = 25 * MiB;

const STRIP_REQ = new Set([
  'host', 'connection', 'content-length', 'content-encoding', 'content-type', 'transfer-encoding',
  'accept-encoding', 'keep-alive', 'te', 'trailer', 'upgrade', 'proxy-authorization',
  'proxy-authenticate', 'x-shim-token', 'x-real-ip', 'true-client-ip',
]);
const STRIP_RES = new Set(['connection', 'keep-alive', 'transfer-encoding', 'content-length']);
const HEX64 = /^[0-9a-f]{64}$/;

function reply(status, text, extra = {}) {
  return new Response(text, { status, headers: { 'x-delta-worker': '1', ...extra } });
}

function safeEq(a, b) {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

function hostOk(host, list) {
  for (const p of list) {
    if (p.startsWith('*.') ? host.endsWith(p.slice(1)) : host === p) return true;
  }
  return false;
}

/** Rebuild the full body from base + recipe. Exported so the tests exercise this exact code. */
export function applyRecipe(base, recipe, outLen) {
  const out = new Uint8Array(outLen);
  const dv = new DataView(recipe.buffer, recipe.byteOffset, recipe.byteLength);
  let o = 0, p = 0;
  while (p < recipe.length) {
    const t = recipe[p];
    if (t === 1) {
      if (!base || p + 9 > recipe.length) throw new Error('bad copy');
      const off = dv.getUint32(p + 1), len = dv.getUint32(p + 5);
      p += 9;
      if (off + len > base.length || o + len > outLen) throw new Error('copy out of range');
      out.set(base.subarray(off, off + len), o);
      o += len;
    } else if (t === 2) {
      if (p + 5 > recipe.length) throw new Error('bad insert');
      const len = dv.getUint32(p + 1);
      p += 5;
      if (p + len > recipe.length || o + len > outLen) throw new Error('insert out of range');
      out.set(recipe.subarray(p, p + len), o);
      o += len; p += len;
    } else {
      throw new Error('unknown op');
    }
  }
  if (o !== outLen) throw new Error('length mismatch');
  return out;
}

export default {
  async fetch(req, env, ctx) {
    if (!env.PATH_SECRET || !env.SHIM_TOKEN || !env.KV) return new Response('misconfigured', { status: 500 });

    const url = new URL(req.url);
    if (req.method !== 'POST' || url.pathname !== `/${env.PATH_SECRET}/relay`) {
      return new Response('not found', { status: 404 });
    }
    if (!safeEq(req.headers.get('x-shim-token') || '', env.SHIM_TOKEN)) return reply(401, 'unauthorized');

    // --- target (allowlist) ---
    let target;
    try { target = new URL(req.headers.get('x-delta-target') || ''); } catch { return reply(400, 'bad target'); }
    const okScheme = target.protocol === 'https:' || (env.ALLOW_HTTP === '1' && target.protocol === 'http:');
    const hosts = (env.ALLOW_HOSTS || 'opencode.ai').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
    if (!okScheme || target.username || target.password || !hostOk(target.hostname, hosts)) {
      return reply(403, 'target not allowed');
    }

    // --- metadata ---
    const baseHash = req.headers.get('x-delta-base');
    const resultHash = req.headers.get('x-delta-result');
    const outLen = Number(req.headers.get('x-delta-len'));
    const maxBody = Number(env.MAX_BODY) || 64 * MiB;
    if ((baseHash && !HEX64.test(baseHash)) || (resultHash && !HEX64.test(resultHash))) return reply(400, 'bad hash');
    if (!Number.isInteger(outLen) || outLen <= 0) return reply(400, 'bad length');
    if (outLen > maxBody) return reply(413, 'too large');

    // --- recipe ---
    let recipeBuf;
    try {
      const stream = req.headers.get('x-delta-enc') === 'gzip'
        ? req.body.pipeThrough(new DecompressionStream('gzip'))
        : req.body;
      recipeBuf = new Uint8Array(await new Response(stream).arrayBuffer());
    } catch { return reply(400, 'bad recipe encoding'); }

    // --- base ---
    let base = null;
    if (baseHash) {
      const b = await env.KV.get(`b:${baseHash}`, 'arrayBuffer');
      if (!b) return reply(409, 'base missing', { 'x-delta-miss': '1' });
      base = new Uint8Array(b);
    }

    let out;
    try { out = applyRecipe(base, recipeBuf, outLen); } catch (e) {
      return reply(baseHash ? 409 : 400, `bad recipe: ${e.message}`, baseHash ? { 'x-delta-miss': '1' } : {});
    }

    // --- store when the shim asks ---.
    // The shim decides when to commit a new base; on a failed write we keep serving the old base
    // so deltas continue even when the free-plan write quota is exhausted.
    let stored = '0', currentBase = baseHash || '';
    if (resultHash && out.length <= KV_VALUE_MAX) {
      const waitRaw = Number(env.STORE_WAIT_MS);
      const waitMs = Number.isFinite(waitRaw) ? waitRaw : 500;
      // Only wait briefly on the critical path. Slow writes continue via waitUntil;
      // in that case the shim keeps the old base, which is still valid in KV.
      const settled = env.KV.put(`b:${resultHash}`, out, {
        expirationTtl: Math.max(60, Number(env.KV_TTL) || 14400),
        metadata: { t: Date.now() },
      }).then(() => 'ok', () => 'fail');
      let timer;
      const timedOut = new Promise((resolve) => { timer = setTimeout(() => resolve('timeout'), waitMs); });
      const outcome = await Promise.race([settled, timedOut]);
      clearTimeout(timer);
      if (outcome === 'ok') {
        stored = '1';
        currentBase = resultHash;
      } else if (outcome === 'timeout') {
        // Keep the 500 ms latency bound, but let the write finish and advance the pin.
        // Otherwise the base never refreshes and recipes grow until they approach a full body.
        ctx.waitUntil(settled);
        stored = '1';
        currentBase = resultHash;
      }
      // A hard 'fail' leaves the previous base in place.
    }

    // --- forward ---
    const h = new Headers();
    for (const [k, v] of req.headers) {
      if (STRIP_REQ.has(k) || k.startsWith('x-delta-') || k.startsWith('cf-') || k.startsWith('x-forwarded-')) continue;
      h.set(k, v);
    }
    const ct = req.headers.get('x-delta-ct');
    if (ct) h.set('content-type', ct);
    if (env.UPSTREAM_API_KEY) {
      h.set('authorization', `Bearer ${env.UPSTREAM_API_KEY}`);
      if (h.has('x-api-key')) h.set('x-api-key', env.UPSTREAM_API_KEY);
    }

    const up = await fetch(target, { method: 'POST', headers: h, body: out, redirect: 'manual' });

    const rh = new Headers();
    for (const [k, v] of up.headers) if (!STRIP_RES.has(k)) rh.set(k, v);
    rh.set('x-delta-worker', '1');
    rh.set('x-delta-stored', stored);
    rh.set('x-delta-base-current', currentBase);
    return new Response(up.body, { status: up.status, statusText: up.statusText, headers: rh });
  },
};
