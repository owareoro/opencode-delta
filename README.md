# opencode-delta

Cut the bytes OpenCode uploads each turn. OpenCode re-sends the whole conversation on every request; this shim remembers what it already sent and uploads only what changed.

```
OpenCode --(localhost, full body)--> shim --(recipe: ~0.2 KB)--> Worker --(full body)--> model API
                                      |                            |
                             byte diff, allowlist,         auth, KV lookup, byte splice,
                             fallback                      forward, stream back
```

Real-world result on a Go session: a **702 KB** request body became **~0.2 KB** on the wire (98% shared prefix). The upstream receives a byte-identical body, so prompt caching and streamed responses are unaffected.

## How it works

- **Worker** (`worker.mjs`): deliberately dumb relay. Checks an unguessable path and a shared token, loads the previous body from KV by SHA-256 hash, applies the shim's copy/insert recipe, stores the new body, forwards to the allowlisted upstream, streams the response back.
- **Shim** (`shim.mjs`, `diff.mjs`): all the clever work. Byte-level diff against the most similar remembered body (no JSON parsing), host allowlist, pass-through for non-chat requests, and automatic fallback to a direct call if the Worker is unavailable.
- Only `POST` requests to `chat/completions`, `messages`, or `responses` use the delta path. Everything else (`GET /models`, auth, etc.) goes straight to the upstream.

## Requirements

- Node.js 20+ (tested on Node 24)
- A Cloudflare account (the free Workers plan is enough)
- OpenCode v2

## Deploy the Worker

```sh
npm ci                            # installs wrangler from package-lock.json
cp wrangler.toml.example wrangler.toml
npx wrangler login                # or use CLOUDFLARE_API_TOKEN
npx wrangler kv namespace create KV
```

Paste the returned namespace ID into your local `wrangler.toml` where it says `REPLACE_WITH_KV_NAMESPACE_ID` (`wrangler.toml` is gitignored; `wrangler.toml.example` is the tracked template).

Generate two secrets and upload them:

```sh
openssl rand -hex 24              # PATH_SECRET
openssl rand -hex 24              # SHIM_TOKEN

npx wrangler secret put PATH_SECRET
npx wrangler secret put SHIM_TOKEN
npx wrangler deploy
```

Your Worker URL is:

```
https://opencode-delta.<account>.workers.dev/<PATH_SECRET>
```

## Run the shim

```sh
cp .env.example .shim.env
# edit .shim.env: set WORKER_URL and SHIM_TOKEN to the same values as above
./opencode-proxy                  # or: npm run proxy
```

`opencode-proxy` sources `.shim.env` and runs the shim with `node --watch`, so edits to `shim.mjs` / `diff.mjs` reload automatically. It binds to `127.0.0.1:8787` by default.

Check it:

```sh
curl -s http://127.0.0.1:8787/__shim/health
```

The health response shows `origBytes` vs `wireBytes`, plus delta/fallback counters. `incomingBytes` is the actual number of bytes OpenCode sent to the shim — i.e. what a direct, no-proxy upload would have put on the wire — and `compressedRequests` counts how many of those arrived with `Content-Encoding`. `fallbackWireBytes` records actual bytes sent on fallback, and `stores` counts committed bases; `pinnedBases` and `pinnedKeys` show how many sessions currently hold a base and which session IDs they belong to. Each request log line includes `session=<id>` (falls back to `default` when no session header is present). Timing counters (`totalMs`, `lastMs`, `workerMs`, `pipeMs`) are cumulative milliseconds. Each request log line shows the incoming size and encoding alongside the wire size. Set `SHIM_TRACE=1` for verbose per-request tracing.

Incoming gzipped/br/zstd request bodies are decoded before diffing, so the delta path works with compressed clients. `Content-Encoding` is stripped before forwarding.

## OpenCode v2 config

The most reliable setup is to override the **built-in** provider's `baseURL` so OpenCode keeps its native authentication (including Console OAuth forwarded to OpenCode Go):

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "providers": {
    "opencode-go": {
      "settings": {
        "baseURL": "http://127.0.0.1:8787/go/v1"
      }
    }
  }
}
```

Add an `opencode` entry with `baseURL: "http://127.0.0.1:8787/zen/v1"` for Zen traffic too. Keep your API key / account connection in OpenCode as usual (`/connect`); the shim passes the Authorization header through and the Worker forwards it.

Path-style URLs also work for any allowlisted host:
`http://127.0.0.1:8787/r/https://opencode.ai/zen/v1`.

Config changes in OpenCode reload automatically; `opencode reload` or `/reload` forces a reload.

## Configuration

Shim (`.shim.env`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `WORKER_URL` | — | Worker base URL including `PATH_SECRET` (required) |
| `SHIM_TOKEN` | — | Must match the Worker secret (required) |
| `PORT` / `HOST` | `8787` / `127.0.0.1` | Local listener |
| `ALLOW_HOSTS` | `opencode.ai` | Comma-separated allowlist; `*.example.com` supported |
| `ALIASES` | zen/go defaults | JSON map of path aliases |
| `DELTA_PATHS` | `/(chat/completions|messages|responses)$` | Regex of POST paths using deltas |
| `STORE_MIN` | `32768` | Only store bodies at least this big (lower to get deltas on short turns) |
| `PIN_BASE` | on | Reuse one Worker base across turns; `0` disables pinning |
| `STORE_DELTA_MIN` | `65536` | Commit a new base when the recipe grows past this many bytes |
| `STORE_AGE_MAX` | `3600` | Or when the pinned base is this many seconds old |
| `BASE_MIN` | `4096` | Minimum shared prefix before using a base |
| `CACHE_MB` | `256` | In-memory body cache (bytes cap) |
| `CACHE_COUNT` | `16` | Max remembered bodies, roughly concurrent sessions |
| `FALLBACK_DIRECT` | on | Set `0` to disable direct fallback when the Worker is down |
| `FALLBACK_GZIP` | `auto` | `auto`: gzip the fallback only if the incoming request was compressed; `1` force on, `0` force off |
| `SHIM_TRACE` | off | Set `1` for verbose logging |

Worker (`wrangler.toml` / secrets):

| Variable | Default | Meaning |
| --- | --- | --- |
| `KV` | — | KV namespace binding |
| `PATH_SECRET` | — | Secret path component |
| `SHIM_TOKEN` | — | Shared token |
| `ALLOW_HOSTS` | `opencode.ai` | Upstream allowlist |
| `KV_TTL` | `14400` | Seconds a stored body lives |
| `STORE_WAIT_MS` | `500` | Max time to wait for a KV write before abandoning it |
| `MAX_BODY` | `64 MiB` | Maximum rebuilt body |
| `UPSTREAM_API_KEY` | — | Optional injected key (untested against Zen) |

## Safety

- Shim binds to loopback, rejects foreign `Host` headers and any `Origin` / `Sec-Fetch-Site` header (browser pages cannot use it), and only talks to `ALLOW_HOSTS`.
- Worker uses an unguessable path plus `x-shim-token`, has its own allowlist, and only accepts HTTPS.

## Limits and caveats

- **Free KV:** 100,000 reads/day and 1,000 writes/day (per account, shared across Workers), 1 GB storage, 25 MiB max value. Paid Workers removes the cap.
- **Base pinning saves writes:** the shim decides when to commit a new base — only when the recipe grows past `STORE_DELTA_MIN` or the base ages past `STORE_AGE_MAX`. Several turns reuse the same KV value. Pinning is per `x-opencode-session`, so concurrent sessions don't churn each other's base (requests without a session header share a `default` pin). If a write fails (e.g. the daily quota runs out) or takes longer than `STORE_WAIT_MS`, the shim keeps using the old base, so deltas continue with that prefix rather than re-uploading every turn. A slow write is abandoned rather than finished in the background: its new hash is never reported to the shim, so completing it would only consume quota. A 409 (base evicted or expired) clears that session's pin and forces one full resend.
- **Worker CPU:** copying the stored body takes ~1 ms at 0.4 MB, ~8 ms at 10 MB. Free plan allows 10 ms per invocation; large histories may be tight.
- Bodies over 25 MiB are forwarded but not stored.
- After compaction/pruning rewrites old history, the next request re-uploads most of the body once, then deltas resume.
- Incoming compressed request bodies are decoded before diffing, so `Content-Encoding` no longer excludes a request from the delta path.
- If the Worker is unreachable, the shim logs a warning and sends the request directly with the full body. Set `FALLBACK_DIRECT=0` to disable.
- **Important:** Zen free-tier models reject traffic relayed through a Cloudflare Worker egress IP (`429 Free usage limit reached`). Deltas work with OpenCode Go / paid traffic. This is a reason to prefer native support inside the client.
- `!! Worker rejected SHIM_TOKEN (401)` is logged for *any* upstream 401, not just token failures.

## Tests

```sh
npm test
```

22 tests: diff round-trips, a 12-turn session, streaming, KV loss, fallback, compressed requests, gzip fallback negotiation, base reuse, age refresh, per-session pinning, write-quota failure, slow-write timeout, allowlists. Tested against a mock upstream and `wrangler dev --local`; not against every deployed provider.

## License

MIT. See [LICENSE](LICENSE).
