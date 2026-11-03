/**
 * filesync unit tests — chunking, piece store integrity, index LWW/conflict,
 * frame codec, and the security fixes (R1 path traversal, G1 per-file
 * pending, G2 gen forgery, G9 piece-list pin). Filesystem-backed via a temp
 * dir; no network.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { chunkBuffer, pieceListHash, sha256Hex } from '../src/chunker.js';
import { PieceStore } from '../src/store.js';
import { ShareIndex, compareGen, conflictKey } from '../src/share-index.js';
import { FilesEngine, type PeerPort, type EngineHooks } from '../src/engine.js';
import type { SyncMessage, SyncDataMessage } from '../src/protocol.js';

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'filesync-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

function hooks(self = 'AAAA1111'): EngineHooks & { mail: Array<{ title: string }> } {
  const mail: Array<{ title: string }> = [];
  return {
    mail,
    appendLocalMail: (_f, title) => { mail.push({ title }); },
    verbose: () => {},
    getSelfSid: () => self,
  };
}

class FakePort implements PeerPort {
  sent: Array<{ peer: string; message?: SyncMessage; data?: SyncDataMessage }> = [];
  async sendMessage(peer: string, message: SyncMessage) { this.sent.push({ peer, message }); }
  async sendData(peer: string, data: SyncDataMessage) { this.sent.push({ peer, data }); }
  peers = new Set<string>();
  async partners(_share: string) { return [...this.peers]; }
}

/** Explicit directories for a test share (the package has no layout). */
function dirs(name: string): { rootDir: string; stateDir: string } {
  return { rootDir: path.join(tmp, 'share', name), stateDir: path.join(tmp, 'state', name) };
}

describe('chunker', () => {
  it('is deterministic and hashes verify', () => {
    const buf = Buffer.from('hello world '.repeat(5000));
    const a = chunkBuffer(buf);
    const b = chunkBuffer(buf);
    expect(a.map((p) => p.hash)).toEqual(b.map((p) => p.hash));
    for (const p of a) expect(sha256Hex(p.data)).toBe(p.hash);
    expect(pieceListHash(a)).toBe(pieceListHash(b));
    expect(Buffer.concat(a.map((p) => p.data)).equals(buf)).toBe(true);
  });
  it('reuses unchanged pieces after a small edit', () => {
    const base = Buffer.alloc(600_000, 7);
    const edited = Buffer.from(base); edited[300_000] = 9;
    const a = new Set(chunkBuffer(base).map((p) => p.hash));
    const b = chunkBuffer(edited);
    const overlap = b.filter((p) => a.has(p.hash)).length;
    expect(overlap).toBeGreaterThan(0);
  });
});

describe('PieceStore (R1 path traversal)', () => {
  it('rejects non-sha256 keys and traversal', () => {
    const store = new PieceStore(path.join(tmp, 'state', 'sh1'));
    expect(() => store.get('/etc/passwd')).not.toThrow(); // returns null, not read
    expect(store.get('/etc/passwd')).toBeNull();
    expect(store.get('../../../etc/passwd')).toBeNull();
    expect(store.has('../escape')).toBe(false);
    expect(store.missing(['/etc/passwd', '../x'])).toEqual([]); // invalid dropped
  });
  it('stores and verifies pieces by content hash', () => {
    const store = new PieceStore(path.join(tmp, 'state', 'sh1'));
    const data = Buffer.from('piece-data');
    const hash = sha256Hex(data);
    expect(store.put(hash, data)).toBe(true);
    expect(store.has(hash)).toBe(true);
    expect(store.get(hash)?.equals(data)).toBe(true);
    expect(store.put(hash, Buffer.from('tampered'))).toBe(false); // mismatch rejected
  });
});

describe('ShareIndex LWW + conflict', () => {
  it('compareGen: mtime then sid tiebreak', () => {
    expect(compareGen({ mtimeMs: 2, sid: 'a' }, { mtimeMs: 1, sid: 'b' })).toBeGreaterThan(0);
    expect(compareGen({ mtimeMs: 1, sid: 'b' }, { mtimeMs: 1, sid: 'a' })).toBeGreaterThan(0);
    expect(compareGen({ mtimeMs: 1, sid: 'a' }, { mtimeMs: 1, sid: 'a' })).toBe(0);
  });
  it('persists entries/tombstones/conflicts and expires resolved conflicts', () => {
    const idx = new ShareIndex(path.join(tmp, 'state', 'sh1'));
    idx.set({ relPath: 'a.txt', gen: { mtimeMs: 1, sid: 'A' }, size: 1, pieceListHash: 'x', pieceHashes: ['h'] });
    idx.tombstone({ relPath: 'b.txt', gen: { mtimeMs: 2, sid: 'A' }, by: 'A' });
    const c = { relPath: 'a.txt', keptGen: { mtimeMs: 3, sid: 'B' }, loserSid: 'A', loserTs: 1 };
    idx.addConflict(c);
    expect(idx.conflicts.has(conflictKey(c))).toBe(true);
    // resolution = conflict copy gone → entry expires
    idx.expireResolved(() => false);
    expect(idx.conflicts.has(conflictKey(c))).toBe(false);
  });
});

describe('engine sync + security', () => {
  it('G1: multi-file index converges both files (no dropped fetch)', async () => {
    const h = hooks();
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: h, port });
    // Two files from partner, both newer than ours (we have none).
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [
        { relPath: 'one.txt', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: 3, pieceListHash: sha256Hex(Buffer.from('abc')) },
        { relPath: 'two.txt', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: 3, pieceListHash: sha256Hex(Buffer.from('def')) },
      ],
      tombstones: [],
    });
    // Both files must be requested (pending per (peer,relPath) — G1).
    const reqs = port.sent.filter((s) => s.message?.kind === 'want-list');
    const files = reqs.map((s) => (s.message as { relPath?: string }).relPath);
    expect(files).toContain('one.txt');
    expect(files).toContain('two.txt');
  });

  it('R1: a req for /etc/passwd serves nothing (no exfil)', () => {
    const h = hooks();
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: h, port });
    e.onMessage('BBBB2222', { kind: 'want-pieces', share: 'sh1', pieceHashes: ['/etc/passwd', '../../secret'] });
    expect(port.sent.filter((s) => s.data)).toHaveLength(0);
  });

  it('G2: forged future-mtime tombstone is ignored', () => {
    const h = hooks();
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: h, port });
    const st = fs.statSync(e.root);
    void st;
    e.index.set({ relPath: 'keep.txt', gen: { mtimeMs: Date.now(), sid: 'AAAA1111' }, size: 1, pieceListHash: 'x', pieceHashes: ['h'] });
    e.onMessage('BBBB2222', { kind: 'deletion', share: 'sh1', relPath: 'keep.txt', gen: { mtimeMs: 8.6e15, sid: 'BBBB2222' } });
    expect(e.index.get('keep.txt')).toBeDefined(); // not deleted
  });

  it('G9: a junk piece list cannot pin a fetch (hash mismatch drops it)', () => {
    const h = hooks();
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: h, port });
    const good = sha256Hex(Buffer.from('payload'));
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath: 'f.bin', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: 7, pieceListHash: sha256Hex(Buffer.from('payload')) }],
      tombstones: [],
    });
    // Junk piece list (hashes don't match expected pieceListHash) → dropped.
    e.onPieceList('BBBB2222', 'f.bin', ['0'.repeat(64)]);
    const reqs = port.sent.filter((s) => s.message?.kind === 'want-pieces' && (s.message as { pieceHashes?: string[] }).pieceHashes);
    // Should not request the junk hash.
    expect(reqs.every((s) => !(s.message as { pieceHashes: string[] }).pieceHashes.includes('0'.repeat(64)))).toBe(true);
    void good;
  });

  it('pending is pruned for a peer that left the partner set', async () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath: 'x.bin', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: 1, pieceListHash: 'p' }],
      tombstones: [],
    });
    expect(e.pendingCount).toBe(1);
    // The partner snapshot is refreshed asynchronously (tick()/gossip()); warm
    // it the way the engine does before pruning.
    expect(() => e.gossip()).not.toThrow();
    await new Promise((r) => setTimeout(r, 0));
    // Wire dropped: the peer leaves an otherwise NON-EMPTY snapshot (an empty
    // snapshot is deliberately not authoritative — see the test below).
    port.peers.delete('BBBB2222');
    port.peers.add('CCCC3333');
    e.gossip();
    await new Promise((r) => setTimeout(r, 0));
    e.prunePending();
    expect(e.pendingCount).toBe(0);
  });

  it('a manifest cannot resurrect a path we tombstoned', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    const t = Date.now();
    // We deleted the file: tombstone at t+1 (mirrors scan()).
    e.index.tombstone({ relPath: 'gone.txt', gen: { mtimeMs: t, sid: 'AAAA1111' }, by: 'AAAA1111' });
    // Peer advertises an older live version → must be ignored, no fetch.
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath: 'gone.txt', gen: { mtimeMs: t - 1000, sid: 'BBBB2222' }, size: 1, pieceListHash: 'p' }],
      tombstones: [],
    });
    expect(port.sent.filter((s) => s.message?.kind === 'want-list')).toHaveLength(0);
  });

  it('a winning peer tombstone removes the local file', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    fs.writeFileSync(path.join(e.root, 'victim.txt'), 'bye');
    e.scan();
    expect(e.index.get('victim.txt')).toBeDefined();
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1', files: [],
      tombstones: [{ relPath: 'victim.txt', gen: { mtimeMs: Date.now() + 5, sid: 'BBBB2222' }, by: 'BBBB2222' }],
    });
    expect(fs.existsSync(path.join(e.root, 'victim.txt'))).toBe(false);
    expect(e.index.get('victim.txt')).toBeUndefined();
  });

  it.runIf(process.platform !== 'win32')('a symlinked parent cannot redirect an apply outside the root', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    const outside = path.join(tmp, 'outside');
    fs.mkdirSync(outside, { recursive: true });
    fs.symlinkSync(outside, path.join(e.root, 'escape'));
    const data = Buffer.from('payload');
    const hash = sha256Hex(data);
    e.store.put(hash, data);
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath: 'escape/pwn.txt', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: data.length, pieceListHash: 'x' }],
      tombstones: [],
    });
    e.onPieceList('BBBB2222', 'escape/pwn.txt', [hash]);
    expect(fs.existsSync(path.join(outside, 'pwn.txt'))).toBe(false);
  });

  // Review finding P2-1 (gate 1): a timestamp-preserving restore must not be
  // advertised behind its own tombstone — a peer holding the delete would
  // unlink it again and the update would be lost silently.
  it('a timestamp-preserving restore does not resurrect behind a newer delete', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    const abs = path.join(e.root, 'restored.txt');
    const old = new Date('2020-01-01T00:00:00Z');
    fs.writeFileSync(abs, 'v1');
    fs.utimesSync(abs, old, old);
    e.scan();
    expect(e.index.get('restored.txt')).toBeDefined();
    // Local delete → tombstone gen is derived from NOW, not from the stale mtime.
    fs.unlinkSync(abs);
    e.scan();
    expect(e.index.tombstones.get('restored.txt')).toBeDefined();
    // A restore that preserves the ORIGINAL (old) mtime.
    fs.writeFileSync(abs, 'v2');
    fs.utimesSync(abs, old, old);
    e.scan();
    expect(e.index.get('restored.txt')).toBeUndefined(); // not re-advertised
    expect(e.index.tombstones.get('restored.txt')).toBeDefined();
  });

  it('a genuinely newer edit still beats the tombstone', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    const abs = path.join(e.root, 'edit.txt');
    fs.writeFileSync(abs, 'v1');
    e.scan();
    fs.unlinkSync(abs);
    e.scan();
    // A GENUINELY newer edit: its mtime must beat the delete generation, which
    // is anchored at the deletion instant (Date.now()) — a same-millisecond
    // re-create legitimately ties and is shadowed, so step the clock forward.
    fs.writeFileSync(abs, 'v2-newer');
    const newer = new Date(Date.now() + 5);
    fs.utimesSync(abs, newer, newer);
    e.scan();
    expect(e.index.get('edit.txt')).toBeDefined(); // resurrection allowed
    expect(e.index.tombstones.get('edit.txt')).toBeUndefined();
  });

  // Review finding (gate 1): containment must be decided BEFORE `mkdir -p`.
  // With a symlinked parent and a not-yet-existing sub-directory, an apply
  // used to create `sub/` and `sub/deep/` OUTSIDE the share before the
  // realpath check refused the file.
  it.runIf(process.platform !== 'win32')('a nested symlinked parent creates no directories outside the root', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    const outside = path.join(tmp, 'outside2');
    fs.mkdirSync(outside, { recursive: true });
    fs.symlinkSync(outside, path.join(e.root, 'link'));
    const data = Buffer.from('payload');
    const hash = sha256Hex(data);
    e.store.put(hash, data);
    const relPath = 'link/sub/deep/pwn.txt';
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath, gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: data.length, pieceListHash: 'x' }],
      tombstones: [],
    });
    e.onPieceList('BBBB2222', relPath, [hash]);
    // Nothing may appear outside — not even an empty directory.
    expect(fs.readdirSync(outside)).toEqual([]);
    expect(fs.existsSync(path.join(outside, 'pwn.txt'))).toBe(false);
  });

  // ── package-level API contracts (architecture review) ──────────────────

  it('rejects an invalid share id instead of building a path from it', () => {
    const port = new FakePort();
    for (const bad of ['../escape', 'a/b', '', 'x'.repeat(65), 'has space']) {
      expect(() => new FilesEngine({ share: bad, rootDir: path.join(tmp, 'r'), stateDir: path.join(tmp, 's'), hooks: hooks(), port })).toThrow(/invalid share id/);
    }
    // and accepts the documented alphabet
    expect(() => new FilesEngine({ share: 'ok-Share_1', rootDir: path.join(tmp, 'r'), stateDir: path.join(tmp, 's'), hooks: hooks(), port })).not.toThrow();
  });

  it('refuses a stateDir inside rootDir (state would sync itself)', () => {
    const port = new FakePort();
    const rootDir = path.join(tmp, 'r2');
    expect(() => new FilesEngine({ share: 'sh', rootDir, stateDir: path.join(rootDir, '.state'), hooks: hooks(), port })).toThrow(/stateDir must be outside rootDir/);
  });

  it('uses exactly the directories the caller supplied (no host layout)', () => {
    const port = new FakePort();
    const rootDir = path.join(tmp, 'custom-root');
    const stateDir = path.join(tmp, 'custom-state');
    const e = new FilesEngine({ share: 'sh', rootDir, stateDir, hooks: hooks(), port });
    expect(e.root).toBe(path.resolve(rootDir));
    expect(fs.existsSync(path.join(stateDir, 'pieces'))).toBe(true);
    expect(fs.existsSync(path.join(stateDir, 'index.json'))).toBe(false); // written on first persist
    expect(fs.readdirSync(e.root)).toEqual([]);
  });

  it('a rejecting transport does not throw out of the engine', async () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    port.sendMessage = async () => { throw new Error('transport down'); };
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    expect(() => e.gossip()).not.toThrow();          // fire-and-forget
    await new Promise((r) => setTimeout(r, 0));       // the rejection is swallowed
    expect(port.peers.size).toBe(1);
  });

  // ── reviewer round 2 ────────────────────────────────────────────────────

  it('a user directory named .sync is ordinary content (no hidden skip)', () => {
    const port = new FakePort();
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    fs.mkdirSync(path.join(e.root, '.sync'), { recursive: true });
    fs.writeFileSync(path.join(e.root, '.sync', 'note.txt'), 'not host state');
    e.scan();
    expect(e.index.get('.sync/note.txt')).toBeDefined();
  });

  it('a child directory whose NAME starts with ".." still counts as inside', () => {
    const port = new FakePort();
    const rootDir = path.join(tmp, 'r3');
    // stateDir is <rootDir>/..weird — inside rootDir, despite the leading dots.
    expect(() => new FilesEngine({ share: 'sh', rootDir, stateDir: path.join(rootDir, '..weird'), hooks: hooks(), port }))
      .toThrow(/stateDir must be outside rootDir/);
  });

  it.runIf(process.platform !== 'win32')('a symlinked stateDir into the root is refused', () => {
    const port = new FakePort();
    const rootDir = path.join(tmp, 'r4');
    fs.mkdirSync(rootDir, { recursive: true });
    const alias = path.join(tmp, 'alias');
    fs.symlinkSync(rootDir, alias);              // alias → the root
    // stateDir spells as <tmp>/alias/state but RESOLVES to <root>/state.
    expect(() => new FilesEngine({ share: 'sh', rootDir, stateDir: path.join(alias, 'state'), hooks: hooks(), port }))
      .toThrow(/stateDir must be outside rootDir/);
    // and the honest case still works
    expect(() => new FilesEngine({ share: 'sh', rootDir, stateDir: path.join(tmp, 'real-state'), hooks: hooks(), port }))
      .not.toThrow();
  });

  it('an empty partner snapshot does not prune every in-flight fetch', () => {
    const port = new FakePort();
    port.peers.add('BBBB2222');
    const e = new FilesEngine({ share: 'sh1', ...dirs('sh1'), hooks: hooks(), port });
    e.onMessage('BBBB2222', {
      kind: 'manifest', share: 'sh1',
      files: [{ relPath: 'p.bin', gen: { mtimeMs: Date.now(), sid: 'BBBB2222' }, size: 1, pieceListHash: 'x' }],
      tombstones: [],
    });
    expect(e.pendingCount).toBe(1);
    port.peers.clear();          // successful but EMPTY snapshot
    e.prunePending();
    expect(e.pendingCount).toBe(1); // kept (TTL/cap are the backstop)
  });
});
