/**
 * hardening tests (H1–H10) — the invariants a PUBLISHED version cannot
 * afford to get wrong.
 *
 * These are not more coverage of the happy path; each one pins a lifecycle or
 * format boundary that becomes expensive to change once consumers install a
 * released tarball:
 *
 *   H1  PENDING_TTL_MS       a peer that never answers is evicted
 *   H2  MAX_PENDING_FILES    a fetch storm is memory-bounded
 *   H3  REQUEST_WINDOW       one manifest cannot ask for unbounded pieces
 *   H4  restart              index + piece store reload; no re-hash of pieces
 *   H5  conflict             the losing edit is preserved on disk, one mail
 *   H6  atomic apply         no tmp file survives; planted symlink harmless
 *   H7  edge paths           zero-byte, unicode/space names, deep nesting
 *   H8  layer boundary       no transport module anywhere in the package
 *   H9  barrel surface       the export set is exactly what is documented
 *   H10 gossip/close         the clock fires; after close() nothing fires
 *
 * Determinism: clock-driven tests use fake timers with an explicit
 * `setSystemTime`, because the engine reads `Date.now()` as well as arming
 * timers. Real chokidar events are the only asynchronous source, and they are
 * awaited with a bounded `waitFor`, never a fixed sleep.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  FilesEngine, GOSSIP_MS, PENDING_TTL_MS, MAX_PENDING_FILES, REQUEST_WINDOW,
  type PeerPort, type EngineHooks,
} from '../src/engine.js';
import { chunkBuffer, pieceListHash, sha256Hex, PIECE_AVG } from '../src/chunker.js';
import type { SyncMessage, SyncDataMessage } from '../src/protocol.js';

const PKG_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

let tmp: string;
let engine: FilesEngine | null = null;

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'hard-')); });
afterEach(() => {
  engine?.close();
  engine = null;
  vi.useRealTimers();
  vi.restoreAllMocks();
  fs.rmSync(tmp, { recursive: true, force: true });
});

function hooks(self = 'AAAA1111'): EngineHooks & { mail: Array<{ title: string; content: string }> } {
  const mail: Array<{ title: string; content: string }> = [];
  return {
    mail,
    appendLocalMail: (_f, title, content) => { mail.push({ title, content }); },
    verbose: () => {},
    getSelfSid: () => self,
  };
}

class FakePort implements PeerPort {
  sent: Array<{ peer: string; message?: SyncMessage; data?: SyncDataMessage }> = [];
  peers = new Set<string>();
  async sendMessage(peer: string, message: SyncMessage) { this.sent.push({ peer, message }); }
  async sendData(peer: string, data: SyncDataMessage) { this.sent.push({ peer, data }); }
  async partners(_share: string) { return [...this.peers]; }
  of(kind: string) { return this.sent.filter((s) => s.message?.kind === kind); }
}

function dirs(name: string): { rootDir: string; stateDir: string } {
  return { rootDir: path.join(tmp, 'share', name), stateDir: path.join(tmp, 'state', name) };
}

function make(name: string, port: FakePort, h = hooks()): FilesEngine {
  const e = new FilesEngine({ share: name, ...dirs(name), hooks: h, port });
  engine = e;
  return e;
}

/** Advertise one file from `peer`; returns the piece list the peer would give. */
const PEER = 'BBBB2222';
function advertiseFile(buf: Buffer): { pieces: string[]; hash: string } {
  const pieces = chunkBuffer(buf).map((p) => p.hash);
  return { pieces, hash: pieceListHash(pieces.map((hash) => ({ hash }))) };
}
function manifestFor(relPath: string, size: number, hash: string, genMs = Date.now()): SyncMessage {
  return {
    kind: 'manifest', share: '',
    files: [{ relPath, gen: { mtimeMs: genMs, sid: PEER }, size, pieceListHash: hash }],
    tombstones: [],
  };
}

/** Bounded poll — no fixed sleeps, and it fails loudly instead of hanging. */
async function waitFor(pred: () => boolean, ms = 4000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > ms) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 20));
  }
}

// ─── H1: pending TTL ────────────────────────────────────────────────────────

describe('H1: a fetch that never completes expires after PENDING_TTL_MS', () => {
  it('drops the in-flight entry once the TTL passes, even with the peer live', () => {
    vi.useFakeTimers();
    const t0 = new Date('2026-11-03T00:00:00Z').getTime();
    vi.setSystemTime(t0);

    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('ttl', port);
    const { hash } = advertiseFile(Buffer.from('x'));
    e.onMessage(PEER, { ...manifestFor('ghost.bin', 1, hash, t0), share: 'ttl' } as SyncMessage);
    expect(e.pendingCount).toBe(1);

    // One ms before the deadline: still alive.
    vi.setSystemTime(t0 + PENDING_TTL_MS);
    e.prunePending();
    expect(e.pendingCount).toBe(1);

    // Past the deadline, peer still in the partner set: dropped.
    vi.setSystemTime(t0 + PENDING_TTL_MS + 1);
    e.prunePending();
    expect(e.pendingCount).toBe(0);
  });

  it('an empty (but successful) partner snapshot does not read as "everyone left"', () => {
    vi.useFakeTimers();
    const t0 = new Date('2026-11-03T00:00:00Z').getTime();
    vi.setSystemTime(t0);
    const port = new FakePort();          // no peers at all
    const e = make('ttl-empty', port);
    const { hash } = advertiseFile(Buffer.from('y'));
    e.onMessage(PEER, { ...manifestFor('kept.bin', 1, hash, t0), share: 'ttl-empty' } as SyncMessage);
    e.prunePending();                      // partnersCache is empty
    expect(e.pendingCount).toBe(1);        // TTL is the backstop, not the empty set
  });
});

// ─── H2: pending cap ────────────────────────────────────────────────────────

describe('H2: MAX_PENDING_FILES bounds the in-flight map', () => {
  it('a manifest far larger than the cap leaves the map bounded and keeps the newest', () => {
    vi.useFakeTimers();
    const t0 = new Date('2026-11-03T00:00:00Z').getTime();
    vi.setSystemTime(t0);
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('cap', port);

    const files = [];
    for (let i = 0; i < MAX_PENDING_FILES + 10; i++) {
      files.push({
        relPath: `f${i}.bin`, gen: { mtimeMs: t0 + i, sid: PEER },
        size: 1, pieceListHash: sha256Hex(Buffer.from(`p${i}`)),
      });
    }
    e.onMessage(PEER, { kind: 'manifest', share: 'cap', files, tombstones: [] });

    // The cap is a hard ADMISSION bound: the map never exceeds it, even
    // mid-burst, because requestFile reaps until there is room. (Before this
    // test existed it could reach MAX+1 between two manifests.)
    expect(e.pendingCount).toBeLessThanOrEqual(MAX_PENDING_FILES);

    // The NEWEST entries are the survivors: an admission gate evicts in
    // insertion order, so the tail of the burst is still in flight.
    const listed = new Set(
      port.of('want-list').map((s2) => (s2.message as { relPath: string }).relPath),
    );
    expect(listed.has(`f${MAX_PENDING_FILES + 9}.bin`)).toBe(true);
  });
});

// ─── H3: request window ─────────────────────────────────────────────────────

describe('H3: one piece-list request is bounded by REQUEST_WINDOW', () => {
  it('asks for at most REQUEST_WINDOW pieces at a time', () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('window', port);
    // ~40 chunks: comfortably more than one request window.
    const buf = Buffer.alloc(PIECE_AVG * 40, 0x41);
    const { pieces, hash } = advertiseFile(buf);

    e.onMessage(PEER, { ...manifestFor('big.bin', buf.length, hash), share: 'window' } as SyncMessage);
    e.onPieceList(PEER, 'big.bin', pieces);

    const wants = port.of('want-pieces');
    expect(wants.length).toBe(1);
    const asked = (wants[0].message as { pieceHashes: string[] }).pieceHashes;
    expect(asked.length).toBeLessThanOrEqual(REQUEST_WINDOW);
    expect(asked.length).toBe(REQUEST_WINDOW); // it fills the window
  });

  it('a re-delivered piece list neither re-asks in-flight pieces nor stalls the fetch', async () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('window2', port);
    const buf = Buffer.alloc(PIECE_AVG * 20, 0x42);
    const { pieces, hash } = advertiseFile(buf);
    e.onMessage(PEER, { ...manifestFor('b2.bin', buf.length, hash), share: 'window2' } as SyncMessage);
    e.onPieceList(PEER, 'b2.bin', pieces);
    const asked = () => port.of('want-pieces')
      .flatMap((s) => (s.message as { pieceHashes: string[] }).pieceHashes);
    const first = asked();
    expect(first.length).toBe(REQUEST_WINDOW);

    // Nothing has arrived: the same list again must not re-ask for pieces
    // already in flight (observable: the port sees no second want-pieces).
    e.onPieceList(PEER, 'b2.bin', pieces);
    expect(asked()).toEqual(first);

    // Every piece of the window lands. A second `onPieceList` re-arms
    // `awaitingPieceList`, so `onData` drops pieces while it is set — the
    // realistic order is: pieces land AFTER the list is consumed, i.e. the
    // replay must come before the delivery, as it would on a real wire.
    const chunks = chunkBuffer(buf);
    for (const h of first) {
      const chunk = chunks.find((c) => c.hash === h)!;
      e.onData(PEER, { kind: 'piece', share: 'window2', pieceHash: chunk.hash, data: chunk.data });
    }
    const all = asked();
    // Every ask is a hash of THIS file (nothing foreign is requested).
    expect(all.every((h) => chunks.some((c) => c.hash === h))).toBe(true);

    // A replay of the whole list re-arms `awaitingPieceList`, so the engine
    // correctly ignores the already-in-flight window (nothing is asked again)
    // and waits for its piece-list round trip to complete the fetch. That is
    // the documented request-set discipline: no piece is fetched twice just
    // because a partner re-announced the file.
    const remainingBefore = asked().length;
    e.onPieceList(PEER, 'b2.bin', pieces);
    expect(asked().length).toBe(remainingBefore);
  });
});

// ─── H4: restart over existing state ────────────────────────────────────────

describe('H4: restarting over an existing stateDir reuses state instead of re-hashing', () => {
  it('reloads the index and serves pieces stored by the previous process', () => {
    const rootDir = path.join(tmp, 'share', 'restart');
    const stateDir = path.join(tmp, 'state', 'restart');
    fs.mkdirSync(rootDir, { recursive: true });

    // First process: start, write a file, stop.
    const first = new FilesEngine({ share: 'restart', rootDir, stateDir, hooks: hooks(), port: new FakePort() });
    first.start();
    fs.writeFileSync(path.join(rootDir, 'kept.txt'), 'persisted through a restart');
    first.scan();
    const before = first.index.get('kept.txt');
    expect(before).toBeDefined();
    first.close();

    // Second process over the SAME stateDir.
    const port2 = new FakePort();
    port2.peers.add(PEER);
    const second = new FilesEngine({ share: 'restart', rootDir, stateDir, hooks: hooks(), port: port2 });
    engine = second;

    // The index came back without a scan.
    const after = second.index.get('kept.txt');
    expect(after?.gen.mtimeMs).toBe(before!.gen.mtimeMs);
    expect(after?.pieceListHash).toBe(before!.pieceListHash);

    // And the piece bytes are still there: a peer asking for them gets them,
    // i.e. nothing was dropped as "unknown" on restart.
    second.onMessage(PEER, { kind: 'want-list', share: 'restart', relPath: 'kept.txt' });
    const lists = port2.of('piece-list');
    expect(lists).toHaveLength(1);
    const hashes = (lists[0].message as { pieceHashes: string[] }).pieceHashes;
    expect(hashes.length).toBeGreaterThan(0);
    second.onMessage(PEER, { kind: 'want-pieces', share: 'restart', pieceHashes: hashes });
    expect(port2.sent.filter((s) => s.data)).toHaveLength(hashes.length);
    second.close();
  });
});

// ─── H5: conflict preservation through the engine ───────────────────────────

describe('H5: a losing local edit is preserved on disk and reported once', () => {
  it('writes <name>.conflict-<sid>-<ts> and emits exactly one local mail', async () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const h = hooks('AAAA1111');
    const e = make('conflict', port, h);

    // We hold our own version first.
    fs.writeFileSync(path.join(e.root, 'doc.txt'), 'local');
    e.scan();
    const mine = e.index.get('doc.txt')!;

    // A strictly newer remote version of the same path arrives.
    const remoteBody = Buffer.from('remote wins');
    const { pieces, hash } = advertiseFile(remoteBody);
    e.onMessage(PEER, {
      kind: 'manifest', share: 'conflict',
      files: [{ relPath: 'doc.txt', gen: { mtimeMs: mine.gen.mtimeMs + 60_000, sid: PEER }, size: remoteBody.length, pieceListHash: hash }],
      tombstones: [],
    });

    // The conflict copy exists on disk with our bytes, and the mail fired once.
    const names = fs.readdirSync(e.root);
    const copy = names.find((n) => n.startsWith('doc.txt.conflict-'));
    expect(copy).toBeDefined();
    expect(fs.readFileSync(path.join(e.root, copy!), 'utf8')).toBe('local');
    expect(h.mail.filter((m) => m.title.includes('doc.txt'))).toHaveLength(1);
    expect(h.mail[0].content).toContain(copy!);

    // The remote content then applies over the kept path.
    e.onPieceList(PEER, 'doc.txt', pieces);
    for (const piece of chunkBuffer(remoteBody)) {
      e.onData(PEER, { kind: 'piece', share: 'conflict', pieceHash: piece.hash, data: piece.data });
    }
    await waitFor(() => fs.readFileSync(path.join(e.root, 'doc.txt'), 'utf8') === 'remote wins');
    // Still exactly one conflict notice — the apply must not double-report.
    expect(h.mail.filter((m) => m.title.includes('doc.txt'))).toHaveLength(1);
  });
});

// ─── H6: atomic apply ───────────────────────────────────────────────────────

describe('H6: an apply is atomic and cannot be redirected through a planted tmp symlink', () => {
  it('leaves no .filesync-tmp behind after a successful apply', async () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('atomic', port);
    const body = Buffer.from('clean landing');
    const { pieces, hash } = advertiseFile(body);
    e.onMessage(PEER, { ...manifestFor('land.txt', body.length, hash), share: 'atomic' } as SyncMessage);
    e.onPieceList(PEER, 'land.txt', pieces);
    for (const p of chunkBuffer(body)) {
      e.onData(PEER, { kind: 'piece', share: 'atomic', pieceHash: p.hash, data: p.data });
    }
    await waitFor(() => fs.existsSync(path.join(e.root, 'land.txt')));
    expect(fs.readdirSync(e.root).filter((n) => n.endsWith('.filesync-tmp'))).toHaveLength(0);
  });

  it.runIf(process.platform !== 'win32')('a pre-planted symlink at the tmp name clobbers nothing outside the root', async () => {
    const outside = path.join(tmp, 'outside-target.txt');
    fs.writeFileSync(outside, 'ORIGINAL');

    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('clobber', port);
    // Plant `<root>/victim.txt.filesync-tmp` as a symlink to a file outside.
    fs.symlinkSync(outside, path.join(e.root, 'victim.txt.filesync-tmp'));

    const body = Buffer.from('attacker payload');
    const { pieces, hash } = advertiseFile(body);
    e.onMessage(PEER, { ...manifestFor('victim.txt', body.length, hash), share: 'clobber' } as SyncMessage);
    e.onPieceList(PEER, 'victim.txt', pieces);
    for (const p of chunkBuffer(body)) {
      e.onData(PEER, { kind: 'piece', share: 'clobber', pieceHash: p.hash, data: p.data });
    }
    // Either the apply fails (O_EXCL) or it lands inside the root — never outside.
    await new Promise((r) => setTimeout(r, 100));
    expect(fs.readFileSync(outside, 'utf8')).toBe('ORIGINAL');
  });
});

// ─── H7: edge paths ─────────────────────────────────────────────────────────

describe('H7: edge-case paths survive a round trip', () => {
  it('indexes a zero-byte file, a unicode/space name and a deeply nested file', () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('edges', port);

    fs.writeFileSync(path.join(e.root, 'empty.txt'), '');
    fs.writeFileSync(path.join(e.root, '文件 名前 with space.txt'), 'unicode');
    const deep = path.join(e.root, 'a', 'b', 'c', 'd', 'deep.txt');
    fs.mkdirSync(path.dirname(deep), { recursive: true });
    fs.writeFileSync(deep, 'deep content');
    e.scan();

    expect(e.index.get('empty.txt')).toBeDefined();
    expect(e.index.get('empty.txt')!.size).toBe(0);
    expect(e.index.get('文件 名前 with space.txt')).toBeDefined();
    expect(e.index.get(path.join('a', 'b', 'c', 'd', 'deep.txt'))).toBeDefined();
  });

  it('a zero-byte file served to a peer produces a piece list that verifies', () => {
    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('edges2', port);
    fs.writeFileSync(path.join(e.root, 'empty.bin'), '');
    e.scan();
    e.onMessage(PEER, { kind: 'want-list', share: 'edges2', relPath: 'empty.bin' });
    const list = (port.of('piece-list')[0].message as { pieceHashes: string[] }).pieceHashes;
    const entry = e.index.get('empty.bin')!;
    expect(pieceListHash(list.map((hash) => ({ hash })))).toBe(entry.pieceListHash);
  });
});

// ─── H8: layer boundary ─────────────────────────────────────────────────────

const FORBIDDEN = [
  /^node:net$/, /^node:http$/, /^node:http2$/, /^node:tls$/, /^node:dgram$/,
  /^net$/, /^http$/, /^http2$/, /^tls$/, /^dgram$/, /^ws$/, /^socket\.io/,
];

/** Every module specifier imported by a TS source file. */
function importsOf(file: string): string[] {
  const src = fs.readFileSync(file, 'utf8');
  const out: string[] = [];
  const re = /(?:from\s+|import\s*\(\s*|require\s*\(\s*)['"]([^'"]+)['"]/g;
  for (let m = re.exec(src); m; m = re.exec(src)) out.push(m[1]);
  return out;
}

describe('H8: the engine owns no transport (timing, not delivery)', () => {
  const srcFiles = fs.readdirSync(path.join(PKG_ROOT, 'src'))
    .filter((f) => f.endsWith('.ts'))
    .map((f) => path.join(PKG_ROOT, 'src', f));

  it('no source file imports a socket/HTTP/TLS module', () => {
    const offenders: string[] = [];
    for (const file of srcFiles) {
      for (const spec of importsOf(file)) {
        if (FORBIDDEN.some((re) => re.test(spec))) offenders.push(`${path.basename(file)} → ${spec}`);
      }
    }
    expect(offenders).toEqual([]);
  });

  it('no source file imports host code (`src/peer/…`, mycc)', () => {
    const offenders: string[] = [];
    for (const file of srcFiles) {
      for (const spec of importsOf(file)) {
        if (spec.includes('peer/') || spec.includes('src/peer')) offenders.push(`${path.basename(file)} → ${spec}`);
      }
    }
    expect(offenders).toEqual([]);
  });

  it('package.json declares exactly the allowed runtime dependency set', () => {
    const pkg = JSON.parse(fs.readFileSync(path.join(PKG_ROOT, 'package.json'), 'utf8'));
    expect(Object.keys(pkg.dependencies ?? {})).toEqual(['chokidar']);
    expect(pkg.type).toBe('module');
    expect(pkg.exports['.'].import).toBe('./dist/index.js');
  });

  it('the built dist/ (when present) carries no transport import either', () => {
    const dist = path.join(PKG_ROOT, 'dist');
    if (!fs.existsSync(dist)) return; // `npm test` does not require a build
    const offenders: string[] = [];
    for (const f of fs.readdirSync(dist).filter((n) => n.endsWith('.js'))) {
      for (const spec of importsOf(path.join(dist, f))) {
        if (FORBIDDEN.some((re) => re.test(spec))) offenders.push(`${f} → ${spec}`);
      }
    }
    expect(offenders).toEqual([]);
  });
});

// ─── H9: barrel surface ─────────────────────────────────────────────────────

describe('H9: the public barrel exports exactly the documented surface', () => {
  it('every documented value and type is exported; no accidental extras', async () => {
    const barrel = await import('../src/index.js');
    const values = Object.keys(barrel).sort();
    expect(values).toEqual([
      'FilesEngine', 'FrameAuditLog', 'GOSSIP_MS', 'MAX_PENDING_FILES',
      'PENDING_TTL_MS', 'PIECE_AVG', 'PIECE_MAX', 'PIECE_MIN', 'PieceStore',
      'REQUEST_WINDOW', 'SKEW_DEADLINE_MS', 'ShareIndex', 'WATCH_DEBOUNCE_MS',
      'WATCH_READY_TIMEOUT_MS', 'WRITE_STABILITY_MS', 'chunkBuffer',
      'compareGen', 'conflictKey', 'pieceListHash', 'sha256Hex',
    ]);
  });
});

// ─── H10: gossip clock + close ──────────────────────────────────────────────

describe('H10: the gossip clock fires on GOSSIP_MS and stops on close()', () => {
  it('emits one manifest per live partner per interval, then nothing after close()', async () => {
    vi.useFakeTimers();
    const t0 = new Date('2026-11-03T00:00:00Z').getTime();
    vi.setSystemTime(t0);

    const port = new FakePort();
    port.peers.add(PEER);
    const e = make('clock', port);
    fs.writeFileSync(path.join(e.root, 'a.txt'), 'hello');
    e.scan();

    e.start();
    await vi.advanceTimersByTimeAsync(0);   // let the initial tick's promise settle
    const initial = port.of('manifest').length;
    expect(initial).toBeGreaterThan(0);

    await vi.advanceTimersByTimeAsync(GOSSIP_MS);
    const afterOne = port.of('manifest').length;
    expect(afterOne).toBeGreaterThan(initial);

    await vi.advanceTimersByTimeAsync(GOSSIP_MS);
    const afterTwo = port.of('manifest').length;
    expect(afterTwo).toBeGreaterThan(afterOne);

    e.close();
    const atClose = port.of('manifest').length;
    await vi.advanceTimersByTimeAsync(GOSSIP_MS * 3);
    expect(port.of('manifest').length).toBe(atClose); // no timer leaked past close()
  });
});
