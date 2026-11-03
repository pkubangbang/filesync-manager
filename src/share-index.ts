/**
 * index.ts — the per-share index: file entries, tombstones, conflict log.
 *
 * The index is O(files) (filesync.md §2): entries carry `pieceListHash`
 * (hash of the piece-hash list), never the list itself — piece lists are
 * fetched lazily over the wire only when two peers' `pieceListHash` values
 * differ for a file.
 *
 * Version rule: `gen = (mtimeMs, sid)` — last-writer-wins by mtime, sid as
 * tiebreak. A losing local edit with DIFFERENT content is never destroyed
 * (see engine.ts apply): it is preserved as `<name>.conflict-<sid>-<ts>`
 * and logged here; the conflict-log entry travels in `sync-index` so every
 * member learns of it (filesync.md §4).
 *
 * Durability: persisted as JSON at `<stateDir>/index.json` (engine-written,
 * engine-read) — `stateDir` is supplied by the caller. Crash-resume needs no other state — the piece store holds
 * bytes and gossip diff rebuilds the view.
 */

import fs from 'fs';
import path from 'path';

export interface FileGen {
  mtimeMs: number;
  sid: string;
}

export interface FileEntry {
  relPath: string;
  gen: FileGen;
  size: number;
  pieceListHash: string;
  /** The file's ordered piece hashes (local view; peers fetch lazily). */
  pieceHashes: string[];
}

export interface Tombstone {
  relPath: string;
  gen: FileGen;
  by: string;
}

export interface ConflictEntry {
  relPath: string;
  keptGen: FileGen;
  loserSid: string;
  loserTs: number;
}

/** Compare two generations: >0 when a wins (LWW), <0 when b wins, 0 tie
 *  (identical) — sid tiebreaks equal mtimes deterministically on all
 *  members, so ties converge without coordination. */
export function compareGen(a: FileGen, b: FileGen): number {
  if (a.mtimeMs !== b.mtimeMs) return a.mtimeMs - b.mtimeMs;
  return a.sid < b.sid ? -1 : a.sid > b.sid ? 1 : 0;
}

/** Stable dedupe key for a conflict entry (seen-set + entry expiry). */
export function conflictKey(c: ConflictEntry): string {
  return `${c.relPath}|${c.loserSid}|${c.loserTs}`;
}

export class ShareIndex {
  readonly entries = new Map<string, FileEntry>();
  readonly tombstones = new Map<string, Tombstone>();
  readonly conflicts = new Map<string, ConflictEntry>();
  private readonly file: string;

  /** @param stateDir caller-owned state directory (see PieceStore). */
  constructor(stateDir: string) {
    this.file = path.join(stateDir, 'index.json');
    this.load();
  }

  private load(): void {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8')) as {
        entries?: FileEntry[]; tombstones?: Tombstone[]; conflicts?: ConflictEntry[];
      };
      for (const e of raw.entries ?? []) this.entries.set(e.relPath, e);
      for (const t of raw.tombstones ?? []) this.tombstones.set(t.relPath, t);
      for (const c of raw.conflicts ?? []) this.conflicts.set(conflictKey(c), c);
    } catch { /* first run */ }
  }

  persist(): void {
    const data = {
      entries: [...this.entries.values()],
      tombstones: [...this.tombstones.values()],
      conflicts: [...this.conflicts.values()],
    };
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(data));
    fs.renameSync(tmp, this.file);
  }

  get(relPath: string): FileEntry | undefined { return this.entries.get(relPath); }

  /** Upsert an entry (our own local change or an applied remote one). */
  set(entry: FileEntry): void {
    this.entries.set(entry.relPath, entry);
    this.tombstones.delete(entry.relPath);
    this.persist();
  }

  /** Record a delete. The gen must beat the current entry (else the entry
   *  is newer — a "newer edit resurrects" case handled by the caller). */
  tombstone(t: Tombstone): void {
    this.entries.delete(t.relPath);
    this.tombstones.set(t.relPath, t);
    this.persist();
  }

  /** Record a conflict (loser preserved on disk by the caller). */
  addConflict(c: ConflictEntry): void {
    this.conflicts.set(conflictKey(c), c);
    this.persist();
  }

  /**
   * Expire conflict entries whose conflict-copy no longer exists — the
   * "resolution is content" rule (filesync.md §4): deleting the
   * `.conflict-*` copy closes the conflict. `exists` resolves the copy
   * path (caller owns filesystem access).
   */
  expireResolved(exists: (c: ConflictEntry) => boolean): ConflictEntry[] {
    const expired: ConflictEntry[] = [];
    for (const [key, c] of [...this.conflicts]) {
      if (!exists(c)) { this.conflicts.delete(key); expired.push(c); }
    }
    if (expired.length) this.persist();
    return expired;
  }
}
