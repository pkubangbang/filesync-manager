/**
 * watcher tests — the chokidar-based latency path (engine.ts createWatcher).
 *
 * These pin the behaviours raw `fs.watch` could not give us, all measured on
 * the ENGINE's own index (not on chokidar internals):
 *
 *   - a write that lands in the gap between `start()`'s scan and the
 *     watcher's baseline is still ingested (the `ignoreInitial: false`
 *     regression: with `true` that file was invisible until the gossip tick);
 *   - an atomic save (tmp + rename) is detected and the swap file never
 *     enters the index;
 *   - a truncate-and-rewrite to the SAME size is detected even with the
 *     mtime pinned back (the content-hash comparison, not the pre-filter);
 *   - an in-place append via an open fd is detected;
 *   - a new file in a NESTED directory is detected (recursion);
 *   - editor scratch files are ignored;
 *   - `close()` stops the watcher (no scan after close).
 *
 * Synchronisation: `whenReady()` replaces blind sleeps for the BASELINE;
 * each mutation is then followed by `settle()` (awaitWriteFinish + debounce).
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import {
  FilesEngine, WATCH_DEBOUNCE_MS, WRITE_STABILITY_MS,
  type PeerPort, type EngineHooks,
} from '../src/engine.js';
import type { SyncMessage, SyncDataMessage } from '../src/protocol.js';

let tmp: string;
let engine: FilesEngine | null = null;

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'watch-')); });
afterEach(() => {
  engine?.close();
  engine = null;
  fs.rmSync(tmp, { recursive: true, force: true });
});

class SilentPort implements PeerPort {
  async sendMessage(_p: string, _m: SyncMessage) { /* no peers in these tests */ }
  async sendData(_p: string, _d: SyncDataMessage) { /* no peers */ }
  async partners(_share: string): Promise<string[]> { return []; }
}

const hooks: EngineHooks = {
  appendLocalMail: () => {}, verbose: () => {}, getSelfSid: () => 'WATCH111',
};

/** awaitWriteFinish stability + one debounce, with margin for CI scheduling. */
const SETTLE_MS = WRITE_STABILITY_MS + WATCH_DEBOUNCE_MS * 2 + 400;
const settle = () => new Promise((r) => setTimeout(r, SETTLE_MS));

async function start(): Promise<{ engine: FilesEngine; root: string }> {
  const e = new FilesEngine({
    share: 'w',
    rootDir: path.join(tmp, 'share'),
    stateDir: path.join(tmp, 'state'),
    hooks,
    port: new SilentPort(),
  });
  engine = e;
  e.start();
  await e.whenReady();
  return { engine: e, root: e.root };
}

describe('filesync watcher (chokidar)', () => {
  it('ingests a file created in the scan→baseline gap (no 30 s wait)', async () => {
    // Write BEFORE the watcher has a baseline — the exact race the
    // ignoreInitial:true setting lost. `whenReady()` then replays it as an
    // `add`, so the file must be in the index without a gossip tick.
    const e = new FilesEngine({
      share: 'w',
      rootDir: path.join(tmp, 'share'),
      stateDir: path.join(tmp, 'state'),
      hooks,
      port: new SilentPort(),
    });
    engine = e;
    e.start();
    fs.writeFileSync(path.join(e.root, 'gap.txt'), 'written before ready');
    await e.whenReady();
    await settle();
    expect(e.index.get('gap.txt')).toBeDefined();
  });

  it('detects an atomic save (tmp + rename) and never indexes the swap file', async () => {
    const { engine: e, root } = await start();
    fs.writeFileSync(path.join(root, 'doc.txt'), 'v1');
    await settle();
    expect(e.index.get('doc.txt')).toBeDefined();

    const swap = path.join(root, '.doc.txt.swp');
    fs.writeFileSync(swap, 'scratch');
    fs.renameSync(swap, path.join(root, 'doc.txt'));
    await settle();

    const entry = e.index.get('doc.txt');
    expect(entry).toBeDefined();
    expect(entry!.pieceHashes.length).toBeGreaterThan(0);
    expect(e.index.get('.doc.txt.swp')).toBeUndefined();
  });

  it('detects a truncate-and-rewrite to the same size (mtime blind spot)', async () => {
    const { engine: e, root } = await start();
    const file = path.join(root, 'same.txt');
    fs.writeFileSync(file, 'AAAA');
    await settle();
    const before = e.index.get('same.txt')!.pieceListHash;

    // Same byte count, different bytes, mtime pinned back: the pre-filter
    // cannot rescue this — only the content hash can.
    fs.writeFileSync(file, 'BBBB');
    const t = new Date(Date.now() - 5_000);
    fs.utimesSync(file, t, t);
    await settle();

    expect(e.index.get('same.txt')!.pieceListHash).not.toBe(before);
  });

  it('detects an in-place append through an open fd (no rename involved)', async () => {
    const { engine: e, root } = await start();
    const file = path.join(root, 'log.txt');
    fs.writeFileSync(file, 'line1\n');
    await settle();
    const before = e.index.get('log.txt')!.size;

    const fd = fs.openSync(file, 'a');
    fs.writeSync(fd, 'line2\n');
    fs.closeSync(fd);
    await settle();

    expect(e.index.get('log.txt')!.size).toBeGreaterThan(before);
  });

  it('detects a new file in a nested directory (recursive watch)', async () => {
    const { engine: e, root } = await start();
    fs.mkdirSync(path.join(root, 'a', 'b'), { recursive: true });
    await settle();
    fs.writeFileSync(path.join(root, 'a', 'b', 'deep.txt'), 'nested');
    await settle();
    expect(e.index.get('a/b/deep.txt')).toBeDefined();
  });

  it('ignores editor scratch files and our own conflict copies', async () => {
    const { engine: e, root } = await start();
    const names = ['x.swp', 'x.swx', 'x~', '.goutputstream-abc123', 'y.filesync-tmp', 'z.conflict-abc-2026'];
    for (const name of names) fs.writeFileSync(path.join(root, name), 'junk');
    await settle();
    for (const name of names) expect(e.index.get(name)).toBeUndefined();
  });

  it('close() stops the watcher — no scan lands after teardown', async () => {
    const { root } = await start();
    await settle();
    engine!.close();
    const e = engine!;
    engine = null; // afterEach must not close twice
    fs.writeFileSync(path.join(root, 'late.txt'), 'after close');
    await settle();
    expect(e.index.get('late.txt')).toBeUndefined();
  });
});
