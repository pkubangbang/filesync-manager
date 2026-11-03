/**
 * Share-change notification (filesync.md §4): the engine reports a partner's
 * file to the host so the agent can be told to look in the share folder.
 *
 * Only REMOTE changes raise a note — our own local edits are made by the
 * agent itself, which does not need to be told what it just wrote — and the
 * note is delivered AFTER the bytes are on disk and indexed, otherwise it
 * would point at a folder that does not yet contain the file.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { FilesEngine, type PeerPort, type EngineHooks } from '../src/engine.js';
import type { SyncMessage, SyncDataMessage } from '../src/protocol.js';
import { ShareIndex } from '../src/share-index.js';
import { chunkBuffer, pieceListHash } from '../src/chunker.js';

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'note-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

class SilentPort implements PeerPort {
  async sendMessage(_p: string, _m: SyncMessage) { /* none */ }
  async sendData(_p: string, _d: SyncDataMessage) { /* none */ }
  async partners() { return ['PEER1']; }
}

function makeEngine(notes: Array<{ relPath: string; origin: string; change: string; onDisk: boolean }>): FilesEngine {
  const root = path.join(tmp, 'share');
  const hooks: EngineHooks = {
    appendLocalMail: () => {},
    verbose: () => {},
    getSelfSid: () => 'SELF',
    noteFileChange: (_share, relPath, origin, change) => {
      notes.push({ relPath, origin, change, onDisk: fs.existsSync(path.join(root, relPath)) });
    },
  };
  return new FilesEngine({ share: 'n', rootDir: root, stateDir: path.join(tmp, 'state'), hooks, port: new SilentPort() });
}

describe('share change notification', () => {
  it('reports a partner file only once it is on disk, tagged remote', () => {
    const notes: Array<{ relPath: string; origin: string; change: string; onDisk: boolean }> = [];
    const e = makeEngine(notes);
    e.scan();

    const data = Buffer.from('hello from a peer');
    const pieces = chunkBuffer(data);
    for (const p of pieces) e.store.put(p.hash, p.data);

    // Deliver the manifest for a file the peer holds, then its piece list and
    // pieces via the engine's public message entry points.
    e.onMessage('PEER1', {
      kind: 'manifest', share: 'n',
      files: [{ relPath: 'note.txt', gen: { mtimeMs: Date.now(), sid: 'PEER1' }, size: data.length, pieceListHash: pieceListHash(pieces) }],
      tombstones: [],
    });
    e.onMessage('PEER1', { kind: 'piece-list', share: 'n', relPath: 'note.txt', pieceHashes: pieces.map((p) => p.hash) });
    // maybeApply runs on receipt of the pieces; the store already has them, so
    // re-delivering the list completed the file. If it did not, feed them.
    if (!fs.existsSync(path.join(e.root, 'note.txt'))) {
      for (const p of pieces) e.onData('PEER1', { kind: 'piece', share: 'n', pieceHash: p.hash, data: p.data });
    }

    expect(fs.readFileSync(path.join(e.root, 'note.txt'), 'utf8')).toBe('hello from a peer');
    expect(notes).toEqual([{ relPath: 'note.txt', origin: 'remote', change: 'added', onDisk: true }]);
    e.close();
  });

  it('does not report our own local edits', () => {
    const notes: Array<{ relPath: string; origin: string; change: string; onDisk: boolean }> = [];
    const e = makeEngine(notes);
    e.scan();
    fs.writeFileSync(path.join(e.root, 'mine.txt'), 'written locally');
    e.scan();
    expect(new ShareIndex(path.join(tmp, 'state')).get('mine.txt')).toBeDefined();
    expect(notes).toEqual([]);
    e.close();
  });
});
