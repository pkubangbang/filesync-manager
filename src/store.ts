/**
 * store.ts — the piece store: content-addressed chunk persistence.
 *
 * `<stateDir>/pieces/<sha256>` — one file per piece, where `stateDir` is
 * supplied by the caller (no host-layout assumptions). The store is simultaneously:
 *   - the RESUME CURSOR (only pieces absent are ever requested),
 *   - the DEDUPE cache (unchanged pieces of an edited file cost zero bytes),
 *   - persistent retry state (survives process death — "retry" is just
 *     re-requesting still-missing hashes).
 *
 * Idempotent by construction: writing a piece that exists is a no-op
 * (content-addressing = same hash ⇒ same bytes). Partial writes are made
 * atomic via tmp + rename so a crash never leaves a corrupt piece that
 * would poison the hash key.
 */

import fs from 'fs';
import path from 'path';
import { sha256Hex } from './chunker.js';

export class PieceStore {
  readonly dir: string;

  /**
   * @param stateDir caller-owned directory for this share's state (pieces,
   *   index, audit). The package makes no assumptions about its location —
   *   the host decides (mycc: `<workDir>/.mycc/filesync/.sync/<share>`).
   */
  constructor(stateDir: string) {
    this.dir = path.join(stateDir, 'pieces');
    fs.mkdirSync(this.dir, { recursive: true });
  }

  private keyPath(hash: string): string {
    // R1 (security review): `hash` may come from an attacker-controlled
    // `pieceHashes` array (engine.onReq) — an absolute or `../` path here
    // would make store.get() stream an arbitrary local file back over the
    // wire (read/exfil). Enforce the content-address shape AND a resolved
    // prefix check so a traversal can never escape the pieces dir.
    if (!/^[0-9a-f]{64}$/.test(hash)) {
      throw new Error('filesync: invalid piece hash');
    }
    const p = path.resolve(this.dir, hash);
    const base = path.resolve(this.dir);
    if (!p.startsWith(base + path.sep)) {
      throw new Error('filesync: piece key escapes store');
    }
    return p;
  }

  has(hash: string): boolean {
    try { return fs.existsSync(this.keyPath(hash)); } catch { return false; }
  }

  /**
   * Store a piece. Verifies the hash matches the content BEFORE writing —
   * a corrupt/dishonest frame must never land under a wrong key (the whole
   * resume/dedupe machinery trusts the key). Returns false on mismatch.
   * Idempotent: existing key → true without touching disk.
   */
  put(hash: string, data: Buffer): boolean {
    if (sha256Hex(data) !== hash) return false;
    let p: string;
    try { p = this.keyPath(hash); } catch { return false; }
    if (fs.existsSync(p)) return true;
    const tmp = `${p}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, data);
    fs.renameSync(tmp, p); // atomic within the same directory
    return true;
  }

  get(hash: string): Buffer | null {
    try {
      return fs.readFileSync(this.keyPath(hash));
    } catch {
      return null;
    }
  }

  /** Which of `hashes` are missing from the store — the fetch engine's
   *  request list. Order preserved (sequential fetch order, filesync.md).
   *  Invalid (non-sha256) hashes are dropped (R1) — never requested. */
  missing(hashes: readonly string[]): string[] {
    return hashes.filter((h) => /^[0-9a-f]{64}$/.test(h) && !this.has(h));
  }

  /** Drop a piece (used when an apply fails verification — the piece
   *  itself is trusted but its assembly is not; cleanup keeps the store
   *  from retaining garbage after an abort). */
  delete(hash: string): void {
    try { fs.unlinkSync(this.keyPath(hash)); } catch { /* absent */ }
  }
}
