/**
 * engine.ts — the sync engine for ONE shared folder.
 *
 * Transport-agnostic by construction: the host application delivers peer
 * messages via `onMessage`/`onData` and the engine emits new ones through the
 * injected `PeerPort`. The engine never names a socket, a frame verb, or a
 * wire tag — see `protocol.ts` for the message vocabulary and the host-side
 * mapping (mycc: `src/peer/filesync/wire-codec.ts`).
 *
 * Responsibilities
 *   - scan: walk the share root, chunk changed files, update the index
 *   - gossip: emit our manifest (join, change, every GOSSIP_MS)
 *   - fetch: request missing pieces (window K), apply complete files
 *   - watch: debounce local fs events into a scan (latency, not correctness)
 *   - conflict: LWW with loser preservation + conflict log + one mail notice
 *
 * Resource lifecycle (review round 2)
 *   - `pending` is pruned on every gossip tick: entries for a peer that is no
 *     longer in `port.partners()` (wire dropped) and entries older than
 *     `PENDING_TTL_MS` (connected peer that never answered) are dropped, and
 *     the map is capped at `MAX_PENDING_FILES`. A dropped wire therefore
 *     aborts in-flight transfers; the next manifest exchange re-derives them
 *     from the piece store (the resume cursor).
 *   - There is no per-peer index cache and no remote conflict-notice
 *     surface: conflicts are local knowledge, mailed by the side that
 *     preserved the loser copy.
 *
 * Security hardening: R1 hash-shape validation (store.ts), G1 per-(peer,file)
 * pending, G2 gen/sid forgery guards, R2 safe date/filename handling, G3 input
 * caps, R3 one-gossip-per-manifest, G5 realpath+O_EXCL at apply.
 */

import fs from 'fs';
import path from 'path';
import { watch, type FSWatcher } from 'chokidar';
import { chunkBuffer, pieceListHash, type Piece } from './chunker.js';
import { PieceStore } from './store.js';
import {
  ShareIndex, compareGen,
  type ConflictEntry, type FileEntry, type FileGen,
} from './share-index.js';
import {
  type ManifestMessage, type SyncMessage, type SyncDataMessage,
} from './protocol.js';
import { FrameAuditLog } from './audit-log.js';

export const GOSSIP_MS = 30_000;
export const WATCH_DEBOUNCE_MS = 300;
/** Chokidar's awaitWriteFinish stability window. A save is only reported once
 *  the file has stopped growing for this long, which (a) collapses an editor's
 *  stream of partial writes into ONE `change` and (b) keeps us from chunking a
 *  half-written file. Chosen just above the write-debounce so an atomic
 *  tmp+rename save settles once. */
export const WRITE_STABILITY_MS = 200;
/** Ceiling on waiting for the watcher's baseline (`whenReady`). A watcher
 *  that never reports `ready` (platform quirk, huge tree) must not hang. */
export const WATCH_READY_TIMEOUT_MS = 10_000;
export const SKEW_DEADLINE_MS = 10_000;
export const REQUEST_WINDOW = 8;
/** In-flight fetch entries older than this are dropped on the gossip tick.
 *  Strictly ABOVE the wire's liveness window (mycc: 30 s ping × 3 misses
 *  ≈ 90 s) so a slow-but-alive peer is never pruned at the same instant its
 *  socket is still considered alive; 4 × 30 s = 120 s leaves one tick of
 *  margin. */
export const PENDING_TTL_MS = 4 * GOSSIP_MS;
/** Hard bound on in-flight fetches (each may hold a piece list). */
export const MAX_PENDING_FILES = 256;

const MAX_INDEX_ENTRIES = 10_000;   // G3
const MAX_PIECE_LIST = 20_000;      // G3
const MAX_REQ_BATCH = 32;           // G3 (2× REQUEST_WINDOW)
const GEN_MAX_SKEW_MS = 24 * 60 * 60 * 1000; // G2

/**
 * Outbound sink — implemented by the host (mycc: the wire registry);
 * fakeable in tests. `peer` is the partner's opaque id.
 *
 * ASYNCHRONOUS BY CONTRACT. The engine does not read a send result: delivery
 * is best-effort and correctness comes from the periodic manifest
 * re-exchange, so a transport that drops, defers or throws only costs a
 * retry cycle. Promises therefore express backpressure/latency (an HTTP or
 * queue-backed transport can await its write) without the engine having to
 * branch on transport type. Hosts that send synchronously (mycc's WebSocket)
 * return `Promise.resolve()`; a rejected promise is logged by the engine and
 * otherwise ignored.
 */
export interface PeerPort {
  sendMessage(peer: string, message: SyncMessage): Promise<void>;
  sendData(peer: string, data: SyncDataMessage): Promise<void>;
  /** Ids of peers currently reachable (live transport) for this share. */
  partners(share: string): Promise<string[]>;
}

/** Engine → host surface (conflict mail + diagnostics). */
export interface EngineHooks {
  appendLocalMail(from: string, title: string, content: string): void;
  verbose(tool: string, message: string): void;
  getSelfSid(): string;
  /**
   * A file changed in the share, observed by the engine. Populated so the HOST
   * can tell its agent to look — the engine syncs a folder, but only the host
   * knows how to rouse the LLM. Best-effort: a throwing hook never affects
   * sync. `origin` says whose edit it was: 'local' (this process scanned it)
   * or 'remote' (applied from a partner) — only 'remote' news is worth a note.
   */
  noteFileChange?(share: string, relPath: string, origin: 'local' | 'remote', change: 'added' | 'updated' | 'deleted'): void;
}

/** Share ids are used in paths and in the wire share field, so the PACKAGE
 *  validates them (a host must not be the only guard). */
const SHARE_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

export interface EngineOptions {
  /** Share identifier (validated: `[A-Za-z0-9_-]{1,64}`). Carried on the wire. */
  share: string;
  /** The directory whose CONTENTS are synchronized. Created if missing. */
  rootDir: string;
  /**
   * Directory for this share's state — piece store, index, audit log.
   * MUST be outside `rootDir` (otherwise state would sync itself). Kept as an
   * explicit input so the package carries no host layout convention; mycc
   * passes `<workDir>/.mycc/filesync/.sync/<share>`.
   */
  stateDir: string;
  hooks: EngineHooks;
  port: PeerPort;
  debugFilesync?: boolean;
  maxFileBytes?: number;
}

interface PendingFile {
  entry: FileEntry;
  missing: string[];
  requested: Map<string, number>;
  awaitingPieceList: boolean;
  /** Creation time (TTL anchor). */
  at: number;
}

/** G1 fix: pending is keyed per (peer, relPath) — one PendingFile PER FILE,
 *  so a multi-file manifest never overwrites/drops another file's fetch
 *  (which made the file vanish → scan() tombstone → swarm-wide del). */
function pendingKey(peer: string, relPath: string): string {
  return `${peer}\u0000${relPath}`;
}

/** Safe ISO timestamp for filenames/logs — never throws (R2). */
function safeIso(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0 || ms > 8.64e15) return 'invalid';
  return new Date(ms).toISOString();
}

/** Sanitize a sid for use in a filename (R2). */
function safeSidTag(sid: string): string {
  return String(sid).replace(/[^A-Za-z0-9]/g, '').slice(0, 8) || 'x';
}

/**
 * Containment check for a path that is about to be written (or read).
 *
 * `path.resolve` alone is lexical: a symlinked (or, on Windows, junctioned)
 * directory inside the share would be followed. We therefore compare the
 * REAL path of the target's parent directory against the REAL share root —
 * one native call that also normalizes drive-letter case and 8.3 names on
 * Windows, which a `startsWith` prefix test does not. POSIX additionally
 * refuses the write through any non-directory parent (ENOTDIR).
 *
 * Returns the resolved path, or throws when it escapes the root.
 */
function assertConfined(root: string, relPath: string, opts: { parentExists: boolean }): string {
  const abs = path.resolve(root, relPath);
  const realRoot = fs.realpathSync(root);
  if (abs === root || abs === path.resolve(root)) return abs;
  const dir = path.dirname(abs);
  let realDir: string;
  try {
    realDir = fs.realpathSync(dir);
  } catch {
    if (opts.parentExists) throw new Error(`filesync: cannot resolve parent of ${relPath}`);
    // Parent does not exist yet: fall back to the nearest existing ancestor,
    // which is where a symlink would have to live to redirect the write.
    let probe = dir;
    let realProbe: string | null = null;
    while (realProbe === null) {
      try { realProbe = fs.realpathSync(probe); } catch {
        const next = path.dirname(probe);
        if (next === probe) throw new Error(`filesync: cannot resolve ${relPath}`);
        probe = next;
      }
    }
    realDir = realProbe;
  }
  const rel = path.relative(realRoot, realDir);
  if (rel === '') return abs;
  if (rel === '..' || rel.startsWith(`..${path.sep}`) || path.isAbsolute(rel)) {
    throw new Error(`filesync: relPath escapes share root: ${relPath}`);
  }
  return abs;
}

/** True when `child` is `parent` or nested under it (lexical, exact). */
function isInside(parent: string, child: string): boolean {
  const rel = path.relative(parent, child);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${path.sep}`) && !path.isAbsolute(rel));
}

/**
 * Real-path containment test for two paths that may not exist yet.
 *
 * `fs.realpathSync(p)` resolves every path ELEMENT, but only those that exist;
 * the first missing element (and everything after it) is passed through
 * verbatim. That is exactly right here: `realpathSync(/tmp/alias/state)` on a
 * not-yet-created `state` still resolves the `alias` symlink to the root, so
 * an alias into the root is detected. Both sides go through the same
 * resolution, so two ordinary not-yet-created siblings stay distinct.
 *
 * Returns true when `b` resolves inside (or equal to) `a`.
 */
function intersects(a: string, b: string): boolean {
  const real = (p: string): string => {
    try { return fs.realpathSync(p); } catch {
      // Deepest existing ancestor + the literal remainder.
      let probe = p;
      for (;;) {
        try { return path.join(fs.realpathSync(probe), path.relative(probe, p)); } catch {
          const next = path.dirname(probe);
          if (next === probe) return p;
          probe = next;
        }
      }
    }
  };
  return isInside(real(a), real(b));
}
export class FilesEngine {
  readonly share: string;
  readonly root: string;
  readonly store: PieceStore;
  readonly index: ShareIndex;
  readonly audit: FrameAuditLog;
  private readonly hooks: EngineHooks;
  private readonly port: PeerPort;
  private readonly maxFileBytes: number;
  /** Partner snapshot, refreshed asynchronously by `tick()`/`gossip()`. */
  private partnersCache: string[] = [];
  private readonly pending = new Map<string, PendingFile>();
  private watchTimer: NodeJS.Timeout | null = null;
  private gossipTimer: NodeJS.Timeout | null = null;
  private watcher: FSWatcher | null = null;
  private watchReady = false;
  private closed = false;

  constructor(opts: EngineOptions) {
    if (!SHARE_ID_RE.test(opts.share)) {
      throw new Error(`filesync: invalid share id (expected [A-Za-z0-9_-]{1,64})`);
    }
    const rootDir = path.resolve(opts.rootDir);
    const stateDir = path.resolve(opts.stateDir);
    // Lexical check first (a child named `..weird` must still count as a
    // child), then resolve the deepest EXISTING ancestor COMMON to both paths
    // and re-compare the tails — so a symlinked or case-aliased alias into the
    // root cannot smuggle state inside it. (Resolving each side independently
    // is wrong: two not-yet-created siblings collapse to the same ancestor.)
    if (isInside(rootDir, stateDir) || intersects(rootDir, stateDir)) {
      throw new Error('filesync: stateDir must be outside rootDir');
    }
    this.share = opts.share;
    this.hooks = opts.hooks;
    this.port = opts.port;
    this.maxFileBytes = opts.maxFileBytes ?? 500 * 1024 * 1024;
    this.root = rootDir;
    fs.mkdirSync(this.root, { recursive: true });
    fs.mkdirSync(stateDir, { recursive: true });
    this.store = new PieceStore(stateDir);
    this.index = new ShareIndex(stateDir);
    this.audit = new FrameAuditLog(stateDir, opts.debugFilesync ?? false);
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  start(): void {
    this.scan();
    this.tick();
    this.gossipTimer = setInterval(() => this.tick(), GOSSIP_MS);
    this.gossipTimer.unref?.();
    try {
      this.watcher = this.createWatcher();
    } catch (err) {
      this.hooks.verbose('filesync', `watch unavailable for ${this.share}; relying on gossip (${GOSSIP_MS}ms): ${String(err)}`);
    }
  }

  /**
   * The fs watcher: LATENCY ONLY, never correctness (filesync.md §2).
   *
   * Chokidar rather than raw `fs.watch`, because the raw API loses exactly the
   * events an editor produces: a truncate-and-rewrite of identical size emits
   * NOTHING, and an atomic save (`.tmp` + rename) emits three or four
   * indistinguishable `change`s with no write boundary. Chokidar normalizes
   * both backends to semantic add/change/unlink events and — with
   * `awaitWriteFinish` — reports a file only after it has stopped growing, so
   * one save is one scan and we never chunk a half-written file.
   *
   * `ignoreInitial: false` is deliberate and NOT an optimisation choice.
   * With `ignoreInitial: true`, chokidar seeds its baseline with a directory
   * read and then reports only later deltas — but a file created in the gap
   * between `start()`'s synchronous `scan()` and the watcher's baseline read
   * is in NEITHER: not in our index (created after the scan) and not a delta
   * (present at baseline). It stays invisible until the 30 s gossip tick
   * re-scans. With `ignoreInitial: false` the baseline arrives as ordinary
   * `add` events, so that gap closes and the file is ingested immediately;
   * the cost is one `ingest` per existing file, which the mtime+size
   * pre-filter turns into a stat-and-skip.
   *
   * Events are coalesced into ONE dirty flag → one `scan()` per
   * WATCH_DEBOUNCE_MS burst. The scan stays whole-root on purpose: it is the
   * reconciliation pass that also tombstones paths the watcher missed, so an
   * incremental per-filename ingest would need a second delete-detection path.
   */
  private createWatcher(): FSWatcher {
    const w = watch(this.root, {
      ignoreInitial: false,
      // Editor scratch files are not content: a `vim` save would otherwise
      // ship a swap file, and a failed atomic save would ship its `.tmp`.
      ignored: (p: string) => {
        const base = path.basename(p);
        return base.includes('.conflict-')      // our own preserved losers
          || base.startsWith('.goutputstream-') // GVFS atomic save
          || base.endsWith('.filesync-tmp')     // our apply temp
          || base.endsWith('.swp') || base.endsWith('.swx')
          || base.endsWith('~');
      },
      awaitWriteFinish: { stabilityThreshold: WRITE_STABILITY_MS, pollInterval: 50 },
      // A deleted directory subtree arrives as unlinkDir + unlink per file;
      // one debounced scan covers them all, so no special handling is needed.
    });
    // `addDir` is structural noise: `scan()` walks directories implicitly and
    // an empty new directory syncs nothing (directories are implicit in file
    // entries, filesync.md §2). Ignoring it avoids a scan per mkdir -p level.
    w.on('add', (_p: string) => this.onWatchEvent('add'));
    w.on('change', (_p: string) => this.onWatchEvent('change'));
    w.on('unlink', (_p: string) => this.onWatchEvent('unlink'));
    w.on('unlinkDir', (_p: string) => this.onWatchEvent('unlinkDir'));
    w.on('error', (err: unknown) => {
      this.hooks.verbose('filesync', `watch error for ${this.share}: ${String(err)}`);
    });
    return w;
  }

  private onWatchEvent(event: string): void {
    if (this.closed) return;
    this.hooks.verbose('filesync', `watch ${event}`);
    this.scheduleScan();
  }

  /** Resolves once the watcher has reported its baseline (the initial tree
   *  has been ingested). Tests await this instead of sleeping blindly; a
   *  missing watcher (unsupported platform) resolves immediately. */
  whenReady(): Promise<void> {
    if (!this.watcher) return Promise.resolve();
    if (this.watchReady) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const done = (): void => { this.watchReady = true; resolve(); };
      this.watcher!.on('ready', done);
      // Belt and braces: a watcher that never emits `ready` must not hang a
      // caller forever — WATCH_READY_TIMEOUT_MS is the ceiling.
      setTimeout(done, WATCH_READY_TIMEOUT_MS).unref?.();
    });
  }

  close(): void {
    this.closed = true;
    if (this.gossipTimer) clearInterval(this.gossipTimer);
    if (this.watchTimer) clearTimeout(this.watchTimer);
    // chokidar's close() is async; fire-and-forget is safe here because the
    // `closed` flag already makes every queued event a no-op.
    try { void this.watcher?.close(); } catch { /* closing */ }
    this.audit.close();
    this.index.persist();
  }

  private scheduleScan(): void {
    if (this.closed) return;
    if (this.watchTimer) clearTimeout(this.watchTimer);
    this.watchTimer = setTimeout(() => { this.scan(); this.gossipNow(); }, WATCH_DEBOUNCE_MS);
  }

  /** One periodic pass: reap in-flight state, then announce our manifest. */
  private tick(): void {
    if (this.closed) return;
    // Partner discovery is a transport call — refresh it, then reap, then
    // gossip, so pruning sees a current view. `gossip` re-uses the snapshot
    // this tick just refreshed instead of asking again.
    void this.partners()
      .then((peers) => { this.prunePending(); this.gossipTo(peers); })
      .catch(() => { try { this.prunePending(); } catch { /* never break the tick */ } });
  }

  /** Refresh the partner snapshot from the transport, logging the RESULT.
   *
   *  This is the single fact a live convergence failure turns on: "which
   *  peers did the transport consider partners at the moment we tried to
   *  send?" Without it a frames log holding only `rx` lines cannot tell a
   *  transport that never confirmed a partner apart from an engine that had
   *  partners but an empty manifest. Logged on every refresh (cheap: one
   *  array), so the audit log carries the same answer as `verbose`. */
  private async partners(): Promise<string[]> {
    try {
      this.partnersCache = (await this.port.partners(this.share)) ?? [];
    } catch (err) {
      this.hooks.verbose('filesync', `partner lookup failed for ${this.share}: ${String(err)}`);
    }
    this.audit.log('tx', '*', 'partners', this.share, {
      partners: [...this.partnersCache], count: this.partnersCache.length,
      entries: this.index.entries.size,
    });
    return this.partnersCache;
  }

  /**
   * Bound the in-flight fetch map. Three rules, in order:
   *   1. a peer no longer in `partners()` lost its wire → drop its entries
   *      (in-flight transfers are aborted; the piece store keeps the bytes
   *      already received and the next manifest re-derives the remainder);
   *   2. an entry older than PENDING_TTL_MS → a peer that never answered;
   *   3. over MAX_PENDING_FILES → drop the oldest entries.
   */
  prunePending(limitOverride: number = MAX_PENDING_FILES): void {
    if (!this.pending.size) return;
    const now = Date.now();
    const live = new Set(this.partnersCache);
    // A SUCCESSFUL but empty snapshot (e.g. mid socket replacement) must not
    // read as "everyone left" — the TTL and the cap remain the backstop.
    const authoritative = live.size > 0;
    for (const [key, p] of [...this.pending]) {
      const peer = key.slice(0, key.indexOf('\u0000'));
      const drop = (authoritative && !live.has(peer)) || now - p.at > PENDING_TTL_MS;
      if (drop) this.pending.delete(key);
    }
    // `limit` lets the admission gate ask for room for the entry it is about
    // to insert (MAX - 1), so `pending.size <= MAX` holds at every moment
    // rather than only between two gossip ticks.
    const limit = Math.min(limitOverride, MAX_PENDING_FILES);
    if (this.pending.size <= limit) return;
    // Map iteration order is INSERTION order — the cheapest, and the only
    // order that is stable at a given millisecond (many entries can share
    // `at` when they arrive in one manifest, which makes a timestamp sort
    // leave the tiebreak to the sort implementation).
    const excess = this.pending.size - limit;
    let dropped = 0;
    for (const key of this.pending.keys()) {
      if (dropped >= excess) break;
      this.pending.delete(key);
      dropped++;
    }
  }

  /** In-flight fetch count (diagnostics / tests). */
  get pendingCount(): number { return this.pending.size; }

  // ── Scan: local truth → index ────────────────────────────────────────────

  scan(): void {
    const seen = new Set<string>();
    for (const relPath of this.walk(this.root)) {
      seen.add(relPath);
      this.ingest(relPath);
    }
    for (const relPath of [...this.index.entries.keys()]) {
      if (seen.has(relPath)) continue;
      const entry = this.index.entries.get(relPath)!;
      // ONE generation for both the local tombstone and the peer notice, so
      // every member compares the same delete against later edits.
      // Derive the delete gen from the moment of deletion as well as the
      // entry gen: a timestamp-preserving restore (tar, `cp -p`, git
      // checkout) carries an OLD mtime, so `entry.gen.mtimeMs + 1` alone
      // would leave the restored file permanently behind its own tombstone.
      const gen: FileGen = {
        mtimeMs: Math.max(entry.gen.mtimeMs + 1, Date.now()),
        sid: this.hooks.getSelfSid(),
      };
      this.index.tombstone({ relPath, gen, by: this.hooks.getSelfSid() });
      this.broadcast({ kind: 'deletion', share: this.share, relPath, gen });
    }
    this.index.expireResolved((c) => fs.existsSync(this.conflictPath(c)));
    // A tombstone that was never unlinked locally (peer delete) is applied
    // here — but only when the local file has not been edited since the
    // delete arrived. A newer local edit wins (it will be gossiped as a
    // resurrection attempt and re-checked by the receiver).
    for (const t of [...this.index.tombstones.values()]) {
      if (t.by === this.hooks.getSelfSid()) continue;
      if (!this.index.entries.has(t.relPath)) continue;
      try {
        const abs = assertConfined(this.root, t.relPath, { parentExists: true });
        if (fs.statSync(abs).mtimeMs > t.gen.mtimeMs) continue; // newer local edit — keep it
        fs.unlinkSync(abs);
      } catch { /* absent */ }
      this.index.entries.delete(t.relPath);
      this.index.persist();
    }
  }

  private walk(dir: string, prefix = ''): string[] {
    const out: string[] = [];
    let names: string[];
    try { names = fs.readdirSync(dir); } catch { return out; }
    for (const name of names) {
      // No excluded names: state lives OUTSIDE rootDir by construction, so a
      // user directory named `.sync` is ordinary content, not host layout.
      const abs = path.join(dir, name);
      const relPath = prefix ? `${prefix}/${name}` : name;
      let st: fs.Stats;
      try { st = fs.lstatSync(abs); } catch { continue; }
      if (st.isSymbolicLink()) { this.hooks.verbose('filesync', `skipping symlink ${relPath}`); continue; }
      if (st.isDirectory()) { out.push(...this.walk(abs, relPath)); continue; }
      if (!st.isFile()) continue;
      if (name.includes('.conflict-')) continue;
      out.push(relPath);
    }
    return out;
  }

  private ingest(relPath: string): void {
    let abs: string;
    try { abs = assertConfined(this.root, relPath, { parentExists: true }); } catch { return; }
    let st: fs.Stats;
    try { st = fs.statSync(abs); } catch { return; }
    if (st.size > this.maxFileBytes) {
      this.hooks.verbose('filesync', `skipping oversize ${relPath} (${st.size} bytes)`);
      return;
    }
    const prev = this.index.get(relPath);
    // G8 fix: an mtime pre-filter (not size) so same-size edits re-sync.
    if (prev && prev.gen.mtimeMs === st.mtimeMs && prev.size === st.size) return;
    let data: Buffer;
    try { data = fs.readFileSync(abs); } catch { return; }
    const pieces = chunkBuffer(data);
    for (const p of pieces) this.store.put(p.hash, p.data);
    const plh = pieceListHash(pieces);
    if (prev && prev.pieceListHash === plh) return;
    // A tombstoned path may only come back with a gen that BEATS the delete
    // (a genuinely newer edit). A restore that carries an older mtime is
    // shadowed by the tombstone: we must not advertise it, or every peer
    // holding the delete unlinks it again and the update is lost silently.
    const tomb = this.index.tombstones.get(relPath);
    const newGen: FileGen = { mtimeMs: st.mtimeMs, sid: this.hooks.getSelfSid() };
    if (tomb && compareGen(newGen, tomb.gen) <= 0) {
      this.hooks.verbose('filesync', `restore of ${relPath} is older than its delete; not advertised`);
      return;
    }
    this.index.set({
      relPath,
      gen: newGen,
      size: st.size,
      pieceListHash: plh,
      pieceHashes: pieces.map((p) => p.hash),
    });
  }

  // ── Gossip ───────────────────────────────────────────────────────────────

  /** Our whole-share manifest (host sends it to every partner). */
  manifest(): ManifestMessage {
    return {
      kind: 'manifest',
      share: this.share,
      files: [...this.index.entries.values()].map((e) => ({
        relPath: e.relPath, gen: e.gen, size: e.size, pieceListHash: e.pieceListHash,
      })),
      tombstones: [...this.index.tombstones.values()],
    };
  }

  gossip(): void {
    // Public name kept for hosts/hooks (the watch path and `onPartnerShares`
    // call it): always refresh through the transport.
    this.gossipNow();
  }

  /** Announce our manifest to an ALREADY-resolved partner list. The tick and
   *  the post-handshake path both use this, so neither waits on a second
   *  `port.partners()` round-trip (a proof-confirmed partner must get its
   *  index exchange NOW — join latency, filesync.md §3). */
  private gossipTo(peers: string[]): void {
    const frame = this.manifest();
    // Per-PARTNER tx lines, written at the real send point (`send`). A single
    // `peer:"*"` line here could not answer the audit questions filesync.md
    // §8 exists for ("did the tombstone go out before the index?"), because
    // it does not say WHICH peer received it or in what order relative to
    // that peer's rx lines.
    for (const peer of peers) this.send(peer, frame);
  }

  /** Announce our manifest to every peer, asking the transport who they are
   *  FIRST. A watcher event is the one moment we know a file just changed, so
   *  it must not wait on the periodic snapshot: that is 30 s of dead time (the
   *  original "my file never shows up" bug). This is the only path in which a
   *  stale `partnersCache` could gate a send; the tick and the post-handshake
   *  path pass an already-resolved list instead. */
  gossipNow(): void {
    // `.catch` mirrors tick(): `send` calls `hooks.verbose`, and a throwing
    // host hook inside this untethered chain would surface as an unhandled
    // rejection (fatal on modern Node) from a path the caller cannot await.
    void this.partners().then((peers) => this.gossipTo(peers)).catch(() => { /* next tick */ });
  }

  /** Send to every current partner. Uses the cache when it is warm; a cold
   *  cache (e.g. a local delete before the first tick) refreshes first, so a
   *  deletion notice is never silently dropped. */
  private broadcast(frame: SyncMessage): void {
    if (this.partnersCache.length) {
      for (const peer of this.partnersCache) this.send(peer, frame);
      return;
    }
    void this.partners().then((peers) => {
      for (const peer of peers) this.send(peer, frame);
    }).catch(() => { /* next tick */ });
  }

  /** Fire-and-forget: delivery is best-effort, correctness comes from the
   *  next manifest exchange. A rejected send is logged, never thrown. */
  private send(peer: string, frame: SyncMessage): void {
    // ONE tap for every outbound frame, manifests included: the audit log is
    // per (peer, frame) so its sequence can be read alongside that peer's rx
    // lines. Manifests are summarised (counts) to keep the line small.
    if (frame.kind === 'manifest') {
      this.audit.log('tx', peer, 'manifest', this.share, {
        entries: frame.files.length, tombstones: frame.tombstones.length,
      });
    } else {
      this.audit.log('tx', peer, frame.kind, this.share, frame as unknown as Record<string, unknown>);
    }
    try {
      void this.port.sendMessage(peer, frame).catch((err) => {
        this.hooks.verbose('filesync', `send to ${peer} failed: ${String(err)}`);
      });
    } catch (err) {
      // A transport that throws synchronously never reaches .catch().
      this.hooks.verbose('filesync', `send to ${peer} threw: ${String(err)}`);
    }
  }

  private sendData(peer: string, pieceHash: string, data: Buffer): void {
    this.audit.log('tx', peer, 'piece', this.share, { piece: pieceHash, bytes: data.length });
    try {
      void this.port.sendData(peer, { kind: 'piece', share: this.share, pieceHash, data }).catch((err) => {
        this.hooks.verbose('filesync', `send piece to ${peer} failed: ${String(err)}`);
      });
    } catch (err) {
      this.hooks.verbose('filesync', `send piece to ${peer} threw: ${String(err)}`);
    }
  }

  // ── Inbound (host dispatch) ──────────────────────────────────────────────

  onMessage(peer: string, frame: SyncMessage): void {
    this.audit.log('rx', peer, frame.kind, this.share, frame as unknown as Record<string, unknown>);
    if ('share' in frame && frame.share !== this.share) return;
    switch (frame.kind) {
      case 'manifest': return this.onManifest(peer, frame);
      case 'want-list': return this.onWantList(peer, frame.relPath);
      case 'piece-list': return this.onPieceList(peer, frame.relPath, frame.pieceHashes);
      case 'want-pieces': return this.onWantPieces(peer, frame.pieceHashes);
      case 'deletion': return this.onDeletion(peer, frame.relPath, frame.gen);
      case 'error': return this.hooks.verbose('filesync', `peer ${peer} error: ${frame.reason}`);
    }
  }

  onData(peer: string, frame: SyncDataMessage): void {
    this.audit.log('rx', peer, 'piece', this.share, { piece: frame.pieceHash, bytes: frame.data.length });
    if (frame.share !== this.share) return;
    if (!/^[0-9a-f]{64}$/.test(frame.pieceHash)) return; // R1
    // Unsolicited pieces are dropped (filesync.md §3) — request-set only.
    for (const [key, p] of this.pending) {
      if (!key.startsWith(`${peer}\u0000`)) continue;
      if (!p.requested.has(frame.pieceHash)) continue;
      if (!this.store.put(frame.pieceHash, frame.data)) {
        this.hooks.verbose('filesync', `piece hash mismatch from ${peer}: ${frame.pieceHash}`);
        return;
      }
      p.requested.delete(frame.pieceHash);
      p.missing = p.missing.filter((h) => h !== frame.pieceHash);
      this.pump(peer, p.entry.relPath);
      this.maybeApply(peer, p.entry.relPath, p);
      return;
    }
  }

  /** G2 fix: reject forged future gens and normalize sid to the transport's
   *  peer (never trust the frame's own sid claim). */
  private sanitizeGen(gen: FileGen | undefined, peer: string): FileGen | null {
    if (!gen || !Number.isFinite(gen.mtimeMs) || gen.mtimeMs < 0) return null;
    if (gen.mtimeMs > Date.now() + GEN_MAX_SKEW_MS) return null;
    return { mtimeMs: gen.mtimeMs, sid: peer };
  }

  private onManifest(peer: string, frame: ManifestMessage): void {
    if (frame.share !== this.share) return;
    const remote = new Map<string, FileEntry>();
    let count = 0;
    for (const e of frame.files) {
      if (count++ >= MAX_INDEX_ENTRIES) break; // G3
      const gen = this.sanitizeGen(e.gen, peer);
      if (!gen) continue;
      if (e.size > this.maxFileBytes) continue; // incoming size cap
      remote.set(e.relPath, { relPath: e.relPath, gen, size: e.size, pieceListHash: e.pieceListHash, pieceHashes: [] });
    }

    let needGossip = false; // R3: at most ONE gossip per manifest
    for (const [relPath, re] of remote) {
      const mine = this.index.get(relPath);
      if (mine && mine.pieceListHash === re.pieceListHash) continue;
      // A delete outranks the live entry: compare against whichever version
      // we hold — entry or tombstone — so a re-advertised file cannot
      // resurrect a path we (or a peer) already deleted.
      const tomb = this.index.tombstones.get(relPath);
      const current = mine?.gen ?? tomb?.gen;
      if (current && compareGen(re.gen, current) <= 0) continue;
      // Storm guard (R3): skip files already being fetched from this peer.
      if (this.pending.has(pendingKey(peer, relPath))) continue;
      if (mine) this.preserveLoser(relPath, mine, re.gen);
      this.requestFile(peer, relPath, re);
    }
    for (const t of frame.tombstones) {
      const gen = this.sanitizeGen(t.gen, peer);
      if (!gen) continue;
      // A deletion is knowledge, not an edit of a local file: merge it even
      // when we never held the path (an offline replica that missed both the
      // file and the direct `deletion` frame). Compare against the entry OR an
      // existing tombstone so only the winning generation is recorded.
      const mine = this.index.get(t.relPath);
      const tomb = this.index.tombstones.get(t.relPath);
      const current = mine?.gen ?? tomb?.gen;
      if (current && compareGen(gen, current) <= 0) { needGossip = true; continue; }
      this.index.tombstone({ relPath: t.relPath, gen, by: mine ? peer : t.by || peer });
      if (mine) {
        // We DO hold it: apply the delete to disk.
        try { fs.unlinkSync(assertConfined(this.root, t.relPath, { parentExists: true })); } catch { /* absent */ }
        try { this.hooks.noteFileChange?.(this.share, t.relPath, 'remote', 'deleted'); } catch { /* best-effort */ }
      }
    }
    if (needGossip) this.gossipNow();
  }

  private onDeletion(peer: string, relPath: string, genIn: FileGen): void {
    const gen = this.sanitizeGen(genIn, peer);
    if (!gen) return;
    const mine = this.index.get(relPath);
    const tomb = this.index.tombstones.get(relPath);
    const current = mine?.gen ?? tomb?.gen;
    // Record the deletion even when we never had the file — it is knowledge
    // that must survive to block a later stale advertisement.
    if (current && compareGen(gen, current) <= 0) { this.gossipNow(); return; }
    this.index.tombstone({ relPath, gen, by: peer });
    if (mine) {
      try { fs.unlinkSync(assertConfined(this.root, relPath, { parentExists: true })); } catch { /* absent */ }
    }
  }

  private onWantList(peer: string, relPath: string): void {
    const mine = this.index.get(relPath);
    if (!mine) return;
    this.send(peer, {
      kind: 'piece-list', share: this.share, relPath: mine.relPath, pieceHashes: mine.pieceHashes,
    });
  }

  private onWantPieces(peer: string, pieceHashes: string[]): void {
    const hashes = (pieceHashes ?? []).slice(0, MAX_REQ_BATCH); // G3
    for (const h of hashes) {
      if (!/^[0-9a-f]{64}$/.test(h)) continue; // R1
      const data = this.store.get(h);
      if (!data) continue;
      this.sendData(peer, h, data);
    }
  }

  // ── Fetch & apply ────────────────────────────────────────────────────────

  private requestFile(peer: string, relPath: string, re: FileEntry): void {
    const key = pendingKey(peer, relPath);
    let p = this.pending.get(key);
    if (!p) {
      // Admission gate: prune until there is room for THIS entry. One prune only
      // guarantees `<= MAX` when the map starts under the cap, so a manifest
      // burst (or a hostile one) could push past it by insertion rate alone.
      // Looping makes `pending.size <= MAX_PENDING_FILES` a hard bound, not a
      // best-effort one — and the old code had a 1ms window where it was 257.
      if (this.pending.size >= MAX_PENDING_FILES) this.prunePending(MAX_PENDING_FILES - 1);
      p = { entry: re, missing: [], requested: new Map(), awaitingPieceList: true, at: Date.now() };
      this.pending.set(key, p);
    }
    p.entry = re;
    p.awaitingPieceList = true;
    p.at = Date.now();
    this.send(peer, { kind: 'want-list', share: this.share, relPath });
  }

  onPieceList(peer: string, relPath: string, hashes: string[]): void {
    const key = pendingKey(peer, relPath);
    const p = this.pending.get(key);
    if (!p || p.entry.relPath !== relPath) return;
    if (hashes.length > MAX_PIECE_LIST) return; // G3
    // G9 fix: verify the supplied piece list hashes to the expected
    // pieceListHash before pumping — a junk list can no longer pin us.
    const clean = hashes.filter((h) => /^[0-9a-f]{64}$/.test(h)); // R1
    if (pieceListHash(clean.map((h) => ({ hash: h }))) !== p.entry.pieceListHash) {
      this.hooks.verbose('filesync', `piece list hash mismatch for ${relPath} from ${peer}`);
      this.pending.delete(key);
      return;
    }
    p.entry = { ...p.entry, pieceHashes: clean };
    p.awaitingPieceList = false;
    p.missing = this.store.missing(clean);
    this.pump(peer, relPath);
    this.maybeApply(peer, relPath, p);
  }

  private pump(peer: string, relPath: string): void {
    const p = this.pending.get(pendingKey(peer, relPath));
    if (!p) return;
    const now = Date.now();
    for (const [h, deadline] of p.requested) {
      if (deadline <= now) p.requested.delete(h);
    }
    const want: string[] = [];
    for (const h of p.missing) {
      if (p.requested.has(h)) continue;
      if (p.requested.size + want.length >= REQUEST_WINDOW) break;
      want.push(h);
    }
    if (!want.length) return;
    for (const h of want) p.requested.set(h, now + SKEW_DEADLINE_MS);
    this.send(peer, { kind: 'want-pieces', share: this.share, pieceHashes: want });
  }

  private maybeApply(peer: string, relPath: string, p: PendingFile): void {
    if (p.awaitingPieceList || p.missing.length) return;
    const { entry } = p;
    const key = pendingKey(peer, relPath);
    const parts: Piece[] = [];
    for (const h of entry.pieceHashes) {
      const data = this.store.get(h);
      if (!data) return;
      parts.push({ hash: h, data });
    }
    const out = Buffer.concat(parts.map((x) => x.data));
    if (pieceListHash(parts) !== entry.pieceListHash || out.length !== entry.size) {
      this.hooks.verbose('filesync', `assemble verify failed for ${entry.relPath} from ${peer}`);
      this.pending.delete(key);
      return;
    }
    let abs: string;
    try {
      // Containment FIRST, on the paths that already exist: the deepest
      // existing ancestor must really live inside the root. Only then create
      // the missing directories — otherwise `mkdir -p` would happily create
      // empty directories outside the share through a symlinked parent
      // before the check ever ran.
      abs = assertConfined(this.root, entry.relPath, { parentExists: false });
      fs.mkdirSync(path.dirname(abs), { recursive: true });
      // Re-check on the now-complete REAL path (the mkdir may have raced a
      // symlink into place between the two calls).
      abs = assertConfined(this.root, entry.relPath, { parentExists: true });
    } catch (err) {
      this.hooks.verbose('filesync', `apply refused for ${entry.relPath}: ${String(err)}`);
      this.pending.delete(key);
      return;
    }
    // G5: O_EXCL (+ O_NOFOLLOW where the platform defines it) tmp write — a
    // pre-planted symlink at the tmp name cannot clobber a file outside root.
    const tmp = `${abs}.filesync-tmp`;
    try {
      const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
      fs.writeSync(fd, out);
      fs.closeSync(fd);
      fs.renameSync(tmp, abs);
    } catch (err) {
      try { fs.unlinkSync(tmp); } catch { /* ignore */ }
      this.hooks.verbose('filesync', `apply failed for ${entry.relPath}: ${String(err)}`);
      this.pending.delete(key);
      return;
    }
    try { fs.utimesSync(abs, new Date(entry.gen.mtimeMs), new Date(entry.gen.mtimeMs)); } catch { /* best effort */ }
    this.index.set({ ...entry });
    this.pending.delete(key);
    // Tell the host AFTER the file is on disk and indexed: a note that says
    // "look in the folder" is useless until the folder holds the file.
    try { this.hooks.noteFileChange?.(this.share, entry.relPath, 'remote', 'added'); } catch { /* best-effort */ }
    this.gossipNow();
  }

  // ── Conflict handling (filesync.md §4) ───────────────────────────────────

  private preserveLoser(relPath: string, mine: FileEntry, theirs: FileGen): void {
    const c: ConflictEntry = {
      relPath,
      keptGen: theirs,
      loserSid: mine.gen.sid,
      loserTs: mine.gen.mtimeMs,
    };
    const copy = this.conflictPath(c);
    try {
      const abs = assertConfined(this.root, relPath, { parentExists: true });
      fs.renameSync(abs, copy);
    } catch { /* best effort — the remote copy still lands */ }
    this.index.addConflict(c);
    this.hooks.appendLocalMail(
      'filesync',
      `[sync-conflict] ${relPath} (share ${this.share})`,
      `local version (${safeIso(c.loserTs)}) lost to a newer remote edit (${safeIso(c.keptGen.mtimeMs)}); ` +
      `yours is preserved as ${c.relPath}.conflict-${safeSidTag(c.loserSid)}-${safeIso(c.loserTs).replace(/[:.]/g, '-')}.\n` +
      `Resolve in chat: compare the two files, then rm the conflict copy (accept theirs) or edit/overwrite the kept file (merge/keep yours) — the resolution is a normal file change and syncs like any other.`,
    );
  }

  private conflictPath(c: ConflictEntry): string {
    const d = path.dirname(c.relPath);
    const base = path.basename(c.relPath);
    const ts = safeIso(c.loserTs).replace(/[:.]/g, '-'); // R2
    const name = `${base}.conflict-${safeSidTag(c.loserSid)}-${ts}`; // R2
    return assertConfined(this.root, d === '.' ? name : `${d}/${name}`, { parentExists: true });
  }
}
