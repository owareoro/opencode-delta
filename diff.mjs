// diff.mjs — shim-side byte diff. Turns `base` into `next` as a list of COPY/INSERT ops.
// No JSON knowledge: it works on raw bytes, so the rebuilt body is byte-identical by construction.
//
// Wire format of an encoded recipe (big-endian):
//   0x01 | u32 baseOffset | u32 length            COPY  length bytes from base
//   0x02 | u32 length     | <length bytes>        INSERT literal bytes

export const MIN_COPY = 32;       // a match shorter than this isn't worth a 9-byte COPY op
export const ANCHOR = 48;         // bytes used to re-find alignment after a difference
export const SMALL_WIN = 4096;    // phase 1: how far past the divergence point in base we look
export const FINE_STRIDE = 16;    // anchor spacing while scanning new content
export const COARSE_STRIDE = 256; // anchor spacing once we're deep inside a long insertion
export const FINE_LIMIT = 8192;
export const LARGE_K = 1024;      // phase 2: only try whole-base search for the first LARGE_K bytes
export const LARGE_STEP = 64;

/** Length of the common run of a[ai..] and b[bj..]. Native memcmp in 16 KB chunks. */
export function commonLen(a, ai, b, bj) {
  const max = Math.min(a.length - ai, b.length - bj);
  const CH = 16384;
  let n = 0;
  while (n + CH <= max && a.compare(b, bj + n, bj + n + CH, ai + n, ai + n + CH) === 0) n += CH;
  const end = Math.min(max, n + CH);
  while (n < end && a[ai + n] === b[bj + n]) n++;
  return n;
}

function extendBack(base, bi, next, nj) {
  let n = 0;
  while (bi - n > 0 && nj - n > 0 && base[bi - n - 1] === next[nj - n - 1]) n++;
  return n;
}

/**
 * After a difference at (i, j), find where `next` lines up with `base` again.
 * Phase 1 looks only just past i in base (handles insertions, and small shifts such as a
 * cache_control marker moving). Phase 2 searches all of base, but only for anchors near j,
 * to catch a large deletion (pruned tool output). Returns {k, pos} meaning next[j+k..]
 * continues base[i+pos..], or null.
 */
function resync(base, i, next, j) {
  const small = base.subarray(i, Math.min(base.length, i + SMALL_WIN));
  let stride = FINE_STRIDE;
  for (let k = 0; j + k + ANCHOR <= next.length; k += stride) {
    if (k >= FINE_LIMIT) stride = COARSE_STRIDE;
    const pos = small.indexOf(next.subarray(j + k, j + k + ANCHOR));
    if (pos >= 0) return back(base, i, next, j, k, pos);
  }
  const rest = base.subarray(i);
  for (let k = 0; k < LARGE_K && j + k + ANCHOR <= next.length; k += LARGE_STEP) {
    const pos = rest.indexOf(next.subarray(j + k, j + k + ANCHOR));
    if (pos >= 0) return back(base, i, next, j, k, pos);
  }
  return null;
}

function back(base, i, next, j, k, pos) {
  const b = Math.min(extendBack(base, i + pos, next, j + k), k);
  return { k: k - b, pos: pos - b };
}

/** ops: ['c', baseOffset, len] | ['i', nextStart, nextEnd] */
export function makeRecipe(base, next) {
  const ops = [];
  let i = 0, j = 0, lit = 0;
  while (j < next.length) {
    const n = i < base.length ? commonLen(base, i, next, j) : 0;
    if (n >= MIN_COPY) {
      if (j > lit) ops.push(['i', lit, j]);
      ops.push(['c', i, n]);
      i += n; j += n; lit = j;
      continue;
    }
    const r = i < base.length ? resync(base, i, next, j) : null;
    if (!r) { j = next.length; break; }
    j += r.k; i += r.pos;
  }
  if (j > lit) ops.push(['i', lit, next.length]);
  return ops;
}

export function encodeRecipe(ops, next) {
  const parts = [];
  for (const [t, a, b] of ops) {
    if (t === 'c') {
      const h = Buffer.allocUnsafe(9);
      h[0] = 1; h.writeUInt32BE(a, 1); h.writeUInt32BE(b, 5);
      parts.push(h);
    } else {
      const h = Buffer.allocUnsafe(5);
      h[0] = 2; h.writeUInt32BE(b - a, 1);
      parts.push(h, next.subarray(a, b));
    }
  }
  return Buffer.concat(parts);
}

export function encodeFull(next) {
  const h = Buffer.allocUnsafe(5);
  h[0] = 2; h.writeUInt32BE(next.length, 1);
  return Buffer.concat([h, next]);
}
