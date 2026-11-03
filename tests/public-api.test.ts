/**
 * The README usage example, as a test — the package's PUBLIC CONTRACT.
 *
 * Every other test in this folder imports the internals (`../engine.js`,
 * `../chunker.js`, …), which proves the engine works but not that an outside
 * consumer can drive it. This one imports ONLY the barrel (the file
 * `package.json` `exports` resolves to) and uses exactly the surface the
 * README documents: construct a `FilesEngine` with a hand-written `PeerPort`,
 * `start()`, then feed inbound messages back through `onMessage` / `onData`.
 * Convergence is the assertion.
 *
 * The import path is `../src/index.js` on purpose: importing by package name
 * would resolve through `node_modules` to `dist/`, and a test that needs
 * `npm run build` first is not a contract test a contributor can run.
 *
 * If a barrel export is dropped, renamed, or stops being sufficient on its
 * own, this test fails while every internal test keeps passing — which is
 * exactly the signal a published package needs.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import {
  FilesEngine,
  type PeerPort,
  type SyncMessage,
  type SyncDataMessage,
} from '../src/index.js';

/** What the fake transport carries: a control message or a piece frame. */
type Delivered =
  | { kind: 'control'; peer: string; msg: SyncMessage }
  | { kind: 'data'; peer: string; piece: SyncDataMessage };

/** A synchronous in-memory transport: whatever one engine sends, the other
 *  receives — no sockets, no timers, no host code. */
class Loop implements PeerPort {
  peer!: Loop;
  readonly outbox: Delivered[] = [];
  async sendMessage(peer: string, msg: SyncMessage): Promise<void> {
    this.outbox.push({ kind: 'control', peer, msg });
  }
  async sendData(peer: string, piece: SyncDataMessage): Promise<void> {
    this.outbox.push({ kind: 'data', peer, piece });
  }
  async partners(): Promise<string[]> { return ['peer']; }
}

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'contract-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

function makeEngines(): { a: FilesEngine; b: FilesEngine; portA: Loop; portB: Loop } {
  const portA = new Loop();
  const portB = new Loop();
  portA.peer = portB;
  portB.peer = portA;
  const hooks = (sid: string) => ({
    appendLocalMail: () => {}, verbose: () => {}, getSelfSid: () => sid,
  });
  const a = new FilesEngine({
    share: 'contract', rootDir: path.join(tmp, 'a'), stateDir: path.join(tmp, 'a-state'),
    hooks: hooks('A'), port: portA,
  });
  const b = new FilesEngine({
    share: 'contract', rootDir: path.join(tmp, 'b'), stateDir: path.join(tmp, 'b-state'),
    hooks: hooks('B'), port: portB,
  });
  return { a, b, portA, portB };
}

describe('public contract (README usage example)', () => {
  it('converges one file between two consumers that only use the barrel', async () => {
    const { a, b, portA, portB } = makeEngines();
    a.start();
    b.start();

    const body = 'hello down there — this file crossed a fake transport';
    fs.writeFileSync(path.join(tmp, 'a', 'note.txt'), body);
    // `scan()` is itself part of the documented surface: it is how a caller
    // who owns a different notification source (the README example uses the
    // built-in watcher via `start()`) forces a pass.
    a.scan();

    // The transport loop the README leaves to the caller: drain both
    // outboxes into the other engine's receive entry points, until quiet.
    const drain = (from: Loop, into: FilesEngine): void => {
      for (const item of from.outbox.splice(0)) {
        if (item.kind === 'control') into.onMessage(item.peer, item.msg);
        else into.onData(item.peer, item.piece);
      }
    };
    // Stop only after the file is APPLIED, not when an outbox runs dry: a
    // gap between the request and the fetch is normal (`partners()` is async),
    // so an empty round is a pause, not the end of the exchange.
    for (let i = 0; i < 40; i++) {
      drain(portA, b);
      drain(portB, a);
      await new Promise((r) => setTimeout(r, 5));
      if (fs.existsSync(path.join(tmp, 'b', 'note.txt'))) break;
    }
    expect(fs.readFileSync(path.join(tmp, 'b', 'note.txt'), 'utf8')).toBe(body);

    a.close();
    b.close();
  });
});
