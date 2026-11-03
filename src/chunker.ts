/**
 * chunker.ts — content-defined chunking (FastCDC-style) for filesync.
 *
 * Content-defined boundaries mean an insert/edit early in a file does NOT
 * shift every subsequent piece boundary — only the pieces overlapping the
 * change are new, so re-syncing an edited file transmits just the delta.
 * The piece store (store.ts) then dedupes by sha256: unchanged pieces cost
 * zero bytes even when boundaries shift slightly.
 *
 * Deterministic: the same bytes always yield the same pieces (gear hash,
 * fixed masks) — required so two peers independently chunking the same file
 * agree on `pieceListHash` without exchanging piece lists up front.
 *
 * Knob (filesync.md): average piece ~256 KiB (MIN 64 KiB, MAX 512 KiB).
 */

import { createHash } from 'crypto';

export const PIECE_MIN = 64 * 1024;
export const PIECE_MAX = 512 * 1024;
export const PIECE_AVG = 256 * 1024;

/**
 * Gear table — 256 random-looking 32-bit values derived deterministically
 * from a fixed seed (splitmix64 over the byte value). Fixed at build time:
 * chunking MUST be identical across peers and versions within a protocol
 * version, so this is generated, never `Math.random`.
 */
const GEAR: Uint32Array = (() => {
  const t = new Uint32Array(256);
  let s = 0x9e3779b97f4a7c15n;
  for (let i = 0; i < 256; i++) {
    // splitmix64 step (mod 2^64), take the high 32 bits.
    s = (s + 0x9e3779b97f4a7c15n) & 0xffffffffffffffffn;
    let z = s;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & 0xffffffffffffffffn;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & 0xffffffffffffffffn;
    z = z ^ (z >> 31n);
    t[i] = Number(z >> 32n) >>> 0;
  }
  return t;
})();

/** Masks select the average piece size: ~1/N of positions pass the mask
 *  test, so maskBits ≈ log2(PIECE_AVG - PIECE_MIN) ≈ 16 → ~64 KiB + 2^16
 *  bytes ≈ 128 KiB expected; normalization table below tunes to 256 KiB. */
const MASK_BITS = 15;
const NORMALIZATION = 2; // expected piece ≈ MIN + 2^MASK_BITS * 2 ≈ 192–256 KiB

/** One chunked piece: raw bytes + its sha256 (hex). */
export interface Piece {
  /** sha256 hex of the raw bytes — the piece store key and wire identity. */
  hash: string;
  /** Raw piece bytes. */
  data: Buffer;
}

/**
 * Chunk a buffer into content-defined pieces. Deterministic pure function.
 * An empty buffer yields zero pieces (an empty file has `pieceListHash`
 * of the empty list — see index.ts).
 */
export function chunkBuffer(buf: Buffer): Piece[] {
  const pieces: Piece[] = [];
  let start = 0;
  let fp = 0;
  let i = 0;
  const n = buf.length;
  while (i < n) {
    fp = (((fp << 1) >>> 0) + GEAR[buf[i]!]!) >>> 0;
    i++;
    const size = i - start;
    if (size < PIECE_MIN) continue;
    if (size >= PIECE_MAX || (fp & ((1 << MASK_BITS) - 1)) === 0) {
      // Boundary found (or forced at MAX). Apply normalization: only cut
      // when past MIN + a normalized offset so the expected size lands at
      // PIECE_AVG. A forced MAX cut ignores normalization.
      if (size >= PIECE_MAX || size >= PIECE_MIN + (1 << MASK_BITS) / NORMALIZATION) {
        pieces.push(makePiece(buf, start, i));
        start = i;
        fp = 0;
      }
    }
  }
  if (start < n) pieces.push(makePiece(buf, start, n));
  return pieces;
}

function makePiece(buf: Buffer, start: number, end: number): Piece {
  const data = buf.subarray(start, end);
  const hash = createHash('sha256').update(data).digest('hex');
  return { hash, data: Buffer.from(data) };
}

/** sha256 hex of a file's ordered piece-hash list (concatenated raw hex
 *  digests) — the `pieceListHash` of filesync.md §2. Piece lists are
 *  exchanged lazily; this hash is the cheap equality test in the index. */
export function pieceListHash(pieces: readonly { hash: string }[]): string {
  const h = createHash('sha256');
  for (const p of pieces) h.update(p.hash, 'hex');
  return h.digest('hex');
}

/** sha256 hex of raw bytes — file-level content identity. */
export function sha256Hex(data: Buffer): string {
  return createHash('sha256').update(data).digest('hex');
}
