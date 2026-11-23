import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import worker, { applyRecipe } from './worker.mjs';
import { makeRecipe, encodeRecipe } from './diff.mjs';
import { createShim, configFromEnv } from './shim.mjs';

// ---------------------------------------------------------------- helpers
const listen = (server) => new Promise((r) => server.listen(0, '127.0.0.1', () => r(server.address().port)));
const close = (server) => new Promise((r) => { server.closeAllConnections?.(); server.close(r); });

let seed = 12345;
const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 2 ** 32);
const words = ['alpha', 'beta', 'gamma', 'delta', 'tool_use', 'function', 'return', 'const', '{"a":1}', 'path/to/file.ts'];
function text(bytes) {
  let s = '';
  while (s.length < bytes) s += words[(rnd() * words.length) | 0] + (rnd() < 0.2 ? crypto.randomBytes(6).toString('hex') : '') + ' ';
  return s.slice(0, bytes);
}

function fakeKV() {
  const m = new Map();
  let writes = 0, failWrites = false;
  return {
    store: m,
    set failWrites(v) { failWrites = v; },
    get writes() { return writes; },
    async get(k) { const b = m.get(k)?.value; return b ? b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) : null; },
    async getWithMetadata(k) {
      const rec = m.get(k);
      if (!rec) return { value: null, metadata: null };
      return {
        value: rec.value.buffer.slice(rec.value.byteOffset, rec.value.byteOffset + rec.value.byteLength),
        metadata: rec.metadata,
      };
    },
    async put(k, v, opts) {
      if (failWrites) throw new Error('KV write quota exceeded');
      m.set(k, { value: Buffer.from(v), metadata: opts?.metadata || null });
      writes++;
    },
  };
}

async function startUpstream() {
  const seen = [];
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const c of req) chunks.push(c);
    let body = Buffer.concat(chunks);
    if ((req.headers['content-encoding'] || '').toLowerCase() === 'gzip') body = zlib.gunzipSync(body);
    seen.push({ method: req.method, url: req.url, headers: req.headers, body });
    if (req.method === 'GET') { res.writeHead(200, { 'content-type': 'application/json' }); return res.end('{"data":[]}'); }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: 1\n\n');
    await new Promise((r) => setTimeout(r, 200));
    res.end('data: 2\n\n');
  });
  return { server, seen, port: await listen(server) };
}

async function startWorker(overrides = {}) {
  const kv = fakeKV();
  const state = { requests: 0, fail: false };
  const env = { PATH_SECRET: 's3cret', SHIM_TOKEN: 'tok', ALLOW_HOSTS: '127.0.0.1', ALLOW_HTTP: '1', KV: kv, ...overrides };
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const c of req) chunks.push(c);
    state.requests++;
    if (state.fail) { res.writeHead(502); return res.end('bad gateway'); }
    const request = new Request('http://worker.test' + req.url, {
      method: req.method, headers: req.headers, body: req.method === 'GET' ? undefined : Buffer.concat(chunks),
    });
    const pending = [];
    let r;
    try { r = await worker.fetch(request, env, { waitUntil: (p) => pending.push(p) }); }
    catch (e) { res.writeHead(500); return res.end(`worker threw: ${e.message}`); }
    res.writeHead(r.status, Object.fromEntries(r.headers));
    if (r.body) for await (const c of r.body) res.write(c);
    res.end();
    await Promise.all(pending);
  });
  return { server, kv, state, env, port: await listen(server) };
}

async function startAll(shimOverrides = {}, workerOverrides = {}) {
  const up = await startUpstream();
  const w = await startWorker(workerOverrides);
  const cfg = {
    ...configFromEnv({}),
    workerUrl: `http://127.0.0.1:${w.port}/s3cret`, token: 'tok',
    allowHosts: ['127.0.0.1'], allowInsecure: true, quiet: true,
    aliases: { zen: `http://127.0.0.1:${up.port}/zen` },
    ...shimOverrides,
  };
  const shim = createShim(cfg);
  const shimPort = await listen(shim.server);
  return {
    up, w, shim, shimPort, base: `http://127.0.0.1:${shimPort}`,
    async stop() { await Promise.all([close(up.server), close(w.server), close(shim.server)]); },
  };
}

// Simulates OpenCode: grows by an assistant+tool-result pair each turn, moves cache_control to the last 2 messages,
// and puts a big tools array AFTER the messages (so the diff must handle a changing middle and a stable tail).
function session() {
  const history = [{ role: 'system', text: text(6000) }, { role: 'user', text: 'fix the bug' }];
  const tools = Array.from({ length: 30 }, (_, i) => ({ name: `tool_${i}`, description: text(500) }));
  return {
    add(kbs) { history.push({ role: 'assistant', text: text(300) }, { role: 'user', text: text(kbs * 1024) }); },
    body() {
      const n = history.length;
      const messages = history.map((m, i) => ({
        role: m.role,
        content: [{ type: 'text', text: m.text, ...(i >= n - 2 ? { cache_control: { type: 'ephemeral' } } : {}) }],
      }));
      return Buffer.from(JSON.stringify({ model: 'x', messages, tools, stream: true }));
    },
  };
}

const post = (url, body, headers = {}) =>
  fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body });

const postGzip = (url, body, headers = {}) =>
  post(url, zlib.gzipSync(body), { 'content-encoding': 'gzip', ...headers });

// ---------------------------------------------------------------- diff
test('diff: random edits always rebuild exactly, and local edits stay small', () => {
  for (let c = 0; c < 150; c++) {
    const base = Buffer.from(text(30000 + ((rnd() * 40000) | 0)));
    let next = Buffer.from(base);
    const edits = 1 + ((rnd() * 4) | 0);
    for (let e = 0; e < edits; e++) {
      const pos = (rnd() * next.length) | 0;
      const kind = rnd();
      if (kind < 0.4) next = Buffer.concat([next.subarray(0, pos), Buffer.from(text(1 + ((rnd() * 3000) | 0))), next.subarray(pos)]);
      else if (kind < 0.7) next = Buffer.concat([next.subarray(0, pos), next.subarray(Math.min(next.length, pos + 1 + ((rnd() * 5000) | 0)))]);
      else next = Buffer.concat([next.subarray(0, pos), Buffer.from('X'.repeat(1 + ((rnd() * 40) | 0))), next.subarray(pos + 20)]);
    }
    const recipe = encodeRecipe(makeRecipe(base, next), next);
    const out = applyRecipe(base, recipe, next.length);
    assert.ok(Buffer.from(out).equals(next), `case ${c} rebuilt wrongly`);
    if (edits === 1 && next.length > 30000) assert.ok(recipe.length < next.length * 0.35, `case ${c}: recipe ${recipe.length} of ${next.length}`);
  }
});

test('diff: unrelated bodies fall back to ~full insert but still rebuild', () => {
  const a = crypto.randomBytes(50000), b = crypto.randomBytes(50000);
  const out = applyRecipe(a, encodeRecipe(makeRecipe(a, b), b), b.length);
  assert.ok(Buffer.from(out).equals(b));
});

test('diff: big deletion (pruned tool output) is re-aligned', () => {
  const head = Buffer.from(text(20000)), gone = Buffer.from(text(200000)), tail = Buffer.from(text(60000));
  const base = Buffer.concat([head, gone, tail]);
  const next = Buffer.concat([head, Buffer.from('[output pruned]'), tail]);
  const recipe = encodeRecipe(makeRecipe(base, next), next);
  assert.ok(Buffer.from(applyRecipe(base, recipe, next.length)).equals(next));
  assert.ok(recipe.length < 1000, `recipe was ${recipe.length}`);
});

// ---------------------------------------------------------------- end to end
test('e2e: 12-turn session is byte-identical upstream and the wire size stays flat', async () => {
  const t = await startAll();
  try {
    const s = session();
    const wires = [], origs = [];
    for (let turn = 0; turn < 12; turn++) {
      s.add(16);
      const body = s.body();
      const before = t.shim.stats.wireBytes;
      const r = await post(`${t.base}/zen/v1/messages`, body, { authorization: 'Bearer real-key', 'x-test': 'yes' });
      assert.equal(r.status, 200);
      assert.equal(await r.text(), 'data: 1\n\ndata: 2\n\n');
      const got = t.up.seen.at(-1);
      assert.ok(got.body.equals(body), `turn ${turn}: upstream body differs`);
      assert.equal(got.url, '/zen/v1/messages');
      assert.equal(got.headers.authorization, 'Bearer real-key');
      assert.equal(got.headers['x-test'], 'yes');
      assert.equal(got.headers['content-type'], 'application/json');
      assert.equal(got.headers['x-shim-token'], undefined);
      origs.push(body.length); wires.push(t.shim.stats.wireBytes - before);
    }
    console.log('  orig KB:', origs.map((n) => (n / 1024) | 0).join(' '));
    console.log('  wire KB:', wires.map((n) => (n / 1024) | 0).join(' '));
    assert.ok(origs.at(-1) > 200 * 1024);
    for (const w of wires.slice(3)) assert.ok(w < 26 * 1024, `wire ${w}`); // ~16 KB new content per turn
    assert.ok(wires.at(-1) < origs.at(-1) * 0.12);
    assert.equal(t.shim.stats.misses, 0);
  } finally { await t.stop(); }
});

test('e2e: response streams through (first chunk arrives before the end)', async () => {
  const t = await startAll();
  try {
    const s = session(); s.add(40);
    const r = await post(`${t.base}/zen/v1/chat/completions`, s.body());
    const reader = r.body.getReader();
    const t0 = Date.now();
    await reader.read(); const first = Date.now() - t0;
    while (!(await reader.read()).done);
    const total = Date.now() - t0;
    assert.ok(total - first >= 120, `first=${first}ms total=${total}ms (not streamed?)`);
  } finally { await t.stop(); }
});

test('e2e: Worker lost its KV -> 409 -> one full resend, session continues', async () => {
  const t = await startAll();
  try {
    const s = session();
    for (let i = 0; i < 5; i++) { s.add(16); await (await post(`${t.base}/zen/v1/messages`, s.body())).text(); }
    t.w.kv.store.clear();
    s.add(16);
    const body = s.body();
    const r = await post(`${t.base}/zen/v1/messages`, body);
    assert.equal(r.status, 200); await r.text();
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.equal(t.shim.stats.misses, 1);
    // and the next turn is a delta again
    s.add(16);
    const before = t.shim.stats.wireBytes;
    await (await post(`${t.base}/zen/v1/messages`, s.body())).text();
    assert.ok(t.shim.stats.wireBytes - before < 26 * 1024);
  } finally { await t.stop(); }
});

test('e2e: parallel sessions each pick their own base', async () => {
  const t = await startAll();
  try {
    const a = session(), b = session();
    for (let i = 0; i < 4; i++) { a.add(16); b.add(16); await (await post(`${t.base}/zen/v1/messages`, a.body())).text(); await (await post(`${t.base}/zen/v1/messages`, b.body())).text(); }
    a.add(16);
    const before = t.shim.stats.wireBytes;
    await (await post(`${t.base}/zen/v1/messages`, a.body())).text();
    assert.ok(t.shim.stats.wireBytes - before < 26 * 1024);
  } finally { await t.stop(); }
});

test('e2e: broken Worker -> falls back to a direct upstream call', async () => {
  const t = await startAll();
  try {
    t.w.state.fail = true;
    const s = session(); s.add(40);
    const body = s.body();
    const r = await post(`${t.base}/zen/v1/chat/completions`, body);
    assert.equal(r.status, 200); await r.text();
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.equal(t.shim.stats.fallbacks, 1);
  } finally { await t.stop(); }
});

test('e2e: compressed request is decoded and still takes the delta path', async () => {
  const t = await startAll();
  try {
    const s = session(); s.add(16);
    const body = s.body();
    let r = await postGzip(`${t.base}/zen/v1/messages`, body);
    assert.equal(r.status, 200); await r.text();
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.equal(t.shim.stats.delta, 1);
    assert.equal(t.shim.stats.fallbacks, 0);
    s.add(16);
    const body2 = s.body();
    r = await postGzip(`${t.base}/zen/v1/messages`, body2);
    assert.equal(r.status, 200); await r.text();
    assert.ok(t.up.seen.at(-1).body.equals(body2));
    assert.equal(t.shim.stats.compressedRequests, 2);
    assert.ok(t.shim.stats.incomingBytes > t.shim.stats.wireBytes);
    assert.ok(t.shim.stats.wireBytes < body2.length);
  } finally { await t.stop(); }
});

test('e2e: fallback gzips when explicitly enabled', async () => {
  const t = await startAll({ fallbackGzip: true });
  try {
    t.w.state.fail = true;
    const s = session(); s.add(40);
    const body = s.body();
    const r = await post(`${t.base}/zen/v1/chat/completions`, body);
    assert.equal(r.status, 200); await r.text();
    assert.equal(t.up.seen.at(-1).headers['content-encoding'], 'gzip');
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.ok(t.shim.stats.fallbackWireBytes < body.length);
  } finally { await t.stop(); }
});

test('e2e: auto fallback gzips when the incoming request was compressed', async () => {
  const t = await startAll();
  try {
    t.w.state.fail = true;
    const s = session(); s.add(40);
    const body = s.body();
    const r = await postGzip(`${t.base}/zen/v1/chat/completions`, body);
    assert.equal(r.status, 200); await r.text();
    assert.equal(t.up.seen.at(-1).headers['content-encoding'], 'gzip');
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.ok(t.shim.stats.fallbackWireBytes < body.length);
  } finally { await t.stop(); }
});

test('e2e: fallback stays plain by default', async () => {
  const t = await startAll();
  try {
    t.w.state.fail = true;
    const s = session(); s.add(40);
    const body = s.body();
    const r = await post(`${t.base}/zen/v1/chat/completions`, body);
    assert.equal(r.status, 200); await r.text();
    assert.equal(t.up.seen.at(-1).headers['content-encoding'], undefined);
    assert.ok(t.up.seen.at(-1).body.equals(body));
    assert.equal(t.shim.stats.fallbackWireBytes, body.length);
  } finally { await t.stop(); }
});

test('e2e: GET /models bypasses the Worker', async () => {
  const t = await startAll();
  try {
    const r = await fetch(`${t.base}/zen/v1/models`, { headers: { authorization: 'Bearer k' } });
    assert.equal(r.status, 200); await r.text();
    assert.equal(t.w.state.requests, 0);
    assert.equal(t.up.seen.at(-1).headers.authorization, 'Bearer k');
  } finally { await t.stop(); }
});

// ---------------------------------------------------------------- security
test('shim: allowlist, browser-origin and Host checks', async () => {
  const t = await startAll();
  try {
    let r = await post(`${t.base}/r/https://evil.example/v1/messages`, '{}');
    assert.equal(r.status, 404);
    r = await post(`${t.base}/r/https://127.0.0.1@evil.example/v1/messages`, '{}');
    assert.equal(r.status, 404);
    r = await post(`${t.base}/nope/v1/messages`, '{}');
    assert.equal(r.status, 404);
    r = await post(`${t.base}/zen/v1/messages`, '{}', { origin: 'http://evil.example' });
    assert.equal(r.status, 403);
    const status = await new Promise((res) => {
      http.request({ host: '127.0.0.1', port: t.shimPort, path: '/zen/v1/models', headers: { host: 'evil.example' } }, (x) => { x.resume(); res(x.statusCode); }).end();
    });
    assert.equal(status, 403);
    assert.equal(t.up.seen.length, 0);
    // embedded form to an allowed host works
    r = await fetch(`${t.base}/r/http://127.0.0.1:${t.up.port}/zen/v1/models`);
    assert.equal(r.status, 200); await r.text();
  } finally { await t.stop(); }
});

test('worker: reuses one base until the diff threshold is reached', async () => {
  const t = await startAll({ storeDeltaMin: 1000000, storeAgeMs: 3600000 });
  try {
    const s = session();
    for (let i = 0; i < 12; i++) {
      s.add(16);
      const body = s.body();
      const r = await post(`${t.base}/zen/v1/messages`, body);
      assert.equal(r.status, 200); await r.text();
      assert.ok(t.up.seen.at(-1).body.equals(body), `turn ${i}: upstream body differs`);
    }
    assert.equal(t.w.kv.writes, 1);
    assert.equal(t.shim.stats.misses, 0);
  } finally { await t.stop(); }
});

test('worker: refreshes the base when it ages out', async () => {
  const t = await startAll({ storeDeltaMin: 1000000, storeAgeMs: 0 });
  try {
    const s = session();
    for (let i = 0; i < 6; i++) {
      s.add(16);
      const body = s.body();
      const r = await post(`${t.base}/zen/v1/messages`, body);
      assert.equal(r.status, 200); await r.text();
      assert.ok(t.up.seen.at(-1).body.equals(body));
    }
    assert.equal(t.w.kv.writes, 6);
  } finally { await t.stop(); }
});

test('worker: keeps using the old base when KV writes run out', async () => {
  const t = await startAll({ storeDeltaMin: 1000000, storeAgeMs: 3600000 });
  try {
    const s = session();
    const turn = async () => {
      s.add(16);
      const body = s.body();
      const r = await post(`${t.base}/zen/v1/messages`, body);
      assert.equal(r.status, 200); await r.text();
      assert.ok(t.up.seen.at(-1).body.equals(body));
    };
    for (let i = 0; i < 3; i++) await turn();
    assert.equal(t.w.kv.writes, 1);
    t.w.kv.failWrites = true;
    const before = t.shim.stats.wireBytes;
    for (let i = 0; i < 3; i++) await turn();
    assert.equal(t.w.kv.writes, 1);
    assert.equal(t.shim.stats.misses, 0);
    assert.ok(t.shim.stats.wireBytes - before < 60 * 1024);
  } finally { await t.stop(); }
});

test('worker: default thresholds reduce KV writes', async () => {
  const t = await startAll();
  try {
    const s = session();
    for (let i = 0; i < 12; i++) {
      s.add(16);
      const body = s.body();
      const r = await post(`${t.base}/zen/v1/messages`, body);
      assert.equal(r.status, 200); await r.text();
      assert.ok(t.up.seen.at(-1).body.equals(body));
    }
    assert.ok(t.w.kv.writes > 1 && t.w.kv.writes < 12, `writes = ${t.w.kv.writes}`);
  } finally { await t.stop(); }
});

test('worker: secret path, token and target allowlist', async () => {
  const t = await startAll();
  try {
    const hdr = (extra = {}) => ({ 'x-shim-token': 'tok', 'x-delta-target': `http://127.0.0.1:${t.up.port}/x`, 'x-delta-len': '1', ...extra });
    const call = (path, headers) => fetch(`http://127.0.0.1:${t.w.port}${path}`, { method: 'POST', headers, body: Buffer.from([2, 0, 0, 0, 1, 65]) });
    assert.equal((await call('/wrong/relay', hdr())).status, 404);
    assert.equal((await call('/s3cret/relay', hdr({ 'x-shim-token': 'bad' }))).status, 401);
    assert.equal((await call('/s3cret/relay', hdr({ 'x-delta-target': 'https://evil.example/x' }))).status, 403);
    assert.equal((await call('/s3cret/relay', hdr({ 'x-delta-target': `http://127.0.0.1@evil.example/x` }))).status, 403);
    assert.equal((await call('/s3cret/relay', hdr({ 'x-delta-base': 'a'.repeat(64) }))).status, 409);
    assert.equal(t.up.seen.length, 0);
    const ok = await call('/s3cret/relay', hdr());
    assert.equal(ok.status, 200);
    await ok.text();
    assert.equal(t.up.seen.length, 1);
  } finally { await t.stop(); }
});
