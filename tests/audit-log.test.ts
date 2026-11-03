/**
 * --debug-filesync transcript contract (docs/peers/filesync.md §8).
 *
 * The flag exists to AUDIT protocol correctness after the fact, so the log
 * must be a per-(peer, direction) total order:
 *   - one line per frame, tx BEFORE rx for the same exchange is not asserted
 *     (sockets reorder), but every tx line names its REAL partner — an
 *     aggregate `peer:"*"` line cannot be paired with that partner's rx lines
 *     and so cannot answer "did the manifest arrive before the want-list?";
 *   - binary data frames log metadata only (piece sha256 + bytes), never the
 *     payload;
 *   - the log is off by default and empty when disabled.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { FilesEngine, type PeerPort, type EngineHooks } from '../src/engine.js';
import type { SyncMessage, SyncDataMessage } from '../src/protocol.js';

const PEER = 'PEER0001';

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'audit-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

const hooks: EngineHooks = {
  appendLocalMail: () => {}, verbose: () => {}, getSelfSid: () => 'SELF0001',
};

function makeEngine(debug: boolean): { engine: FilesEngine; sent: Array<{ peer: string; msg: SyncMessage | SyncDataMessage }> } {
  const sent: Array<{ peer: string; msg: SyncMessage | SyncDataMessage }> = [];
  const port: PeerPort = {
    async sendMessage(peer, msg) { sent.push({ peer, msg }); },
    async sendData(peer, data) { sent.push({ peer, msg: data }); },
    async partners() { return [PEER]; },
  };
  const engine = new FilesEngine({
    share: 'aud',
    rootDir: path.join(tmp, 'share'),
    stateDir: path.join(tmp, 'state'),
    hooks,
    port,
    debugFilesync: debug,
  });
  return { engine, sent };
}

/** Read the JSONL transcript (empty when the flag is off). */
function lines(): Array<Record<string, unknown>> {
  const dir = path.join(tmp, 'state');
  const file = fs.readdirSync(dir).find((f) => f.startsWith('frames-'));
  if (!file) return [];
  return fs.readFileSync(path.join(dir, file), 'utf8').trim().split('\n').filter(Boolean)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
}

const drain = () => new Promise((r) => setTimeout(r, 20));

describe('--debug-filesync transcript', () => {
  it('writes nothing when the flag is off', async () => {
    const { engine } = makeEngine(false);
    engine.scan();
    engine.gossip();
    await drain();
    expect(lines()).toEqual([]);
    engine.close();
  });

  it('logs each outbound manifest per PARTNER, not as a `*` aggregate', async () => {
    const { engine } = makeEngine(true);
    fs.writeFileSync(path.join(engine.root, 'a.txt'), 'hello');
    engine.scan();
    engine.gossip();
    await drain();
    const tx = lines().filter((l) => l.dir === 'tx' && l.frame === 'manifest');
    expect(tx.length).toBeGreaterThan(0);
    expect(tx.every((l) => l.peer === PEER)).toBe(true);
    expect(tx.some((l) => l.peer === '*')).toBe(false);
    expect(tx[0]!.entries).toBe(1);
    engine.close();
  });

  it('logs rx control frames and binary pieces as metadata only', async () => {
    const { engine } = makeEngine(true);
    engine.onMessage(PEER, { kind: 'want-list', share: 'aud', relPath: 'a.txt' });
    engine.onData(PEER, { kind: 'piece', share: 'aud', pieceHash: 'f'.repeat(64), data: Buffer.from('SECRET-BYTES') });
    await drain();
    const out = lines();
    expect(out.some((l) => l.dir === 'rx' && l.frame === 'want-list' && l.peer === PEER)).toBe(true);
    const piece = out.find((l) => l.frame === 'piece');
    expect(piece).toBeDefined();
    expect(piece!.bytes).toBe('SECRET-BYTES'.length);
    expect(piece!.piece).toBe('f'.repeat(64));
    // The payload itself must never appear in the transcript.
    expect(JSON.stringify(out)).not.toContain('SECRET-BYTES');
    engine.close();
  });

  it('logs each partner lookup with the peers it returned', async () => {
    // The fact a live convergence failure turns on: a transcript whose only
    // tx lines are inbound-replies cannot distinguish "the transport never
    // named a partner" from "we had partners and sent them nothing". The
    // `partners` line answers it, and it is written for both an empty and a
    // populated answer.
    let peers: string[] = [];
    const port: PeerPort = {
      async sendMessage() {}, async sendData() {}, async partners() { return peers; },
    };
    const engine = new FilesEngine({
      share: 'aud', rootDir: path.join(tmp, 'share'), stateDir: path.join(tmp, 'state'),
      hooks, port, debugFilesync: true,
    });
    engine.gossipNow();
    await drain();
    peers = [PEER];
    engine.gossipNow();
    await drain();
    const lookups = lines().filter((l) => l.frame === 'partners');
    expect(lookups.map((l) => l.partners)).toEqual([[], [PEER]]);
    expect(lookups.map((l) => l.count)).toEqual([0, 1]);
    engine.close();
  });
});
