# @pkubangbang/filesync-manager

**Transport-agnostic local file sync engine.** One shared folder, many peers,
ordinary file operations converge — and the package knows nothing about how
messages travel.

```
scan → chunk (FastCDC) → content-addressed piece store → manifest
     ⇄ your transport ⇄ → fetch missing pieces → atomic apply → LWW conflict
```

## Why a separate package

The engine does **disk work only**:

| Layer | Owns | Lives in |
|---|---|---|
| **Engine** (this package) | walk the folder, FastCDC chunking, content-addressed piece store, manifests + tombstones + LWW ordering, conflict preservation, atomic apply, the fs watcher, the gossip *clock* | `@pkubangbang/filesync-manager` — zero transport dependencies |
| **Host** (you) | the channel: sockets, framing, discovery, liveness, membership, authentication, compression, encryption | your application |

The engine decides **what** to send and **when**; it learns **which** peers
exist only as a returned set (`port.partners(share)`). It never stores channel
state. That is the whole contract: implement three async methods, hand the
engine a folder, and the folder syncs.

[mycc](https://github.com/pkubangbang/mycc) is one such host — it wires the
engine to a WebSocket peer channel (`src/peer/filesync/`: frame verbs, binary
piece frames, pairwise discovery).

## Install

```sh
npm i @pkubangbang/filesync-manager
```

Requires **Node.js >= 22.22**. ESM only.

## Usage

```ts
import { FilesEngine, type PeerPort } from '@pkubangbang/filesync-manager';

const port: PeerPort = {
  // Deliver one domain message to ONE peer over your own transport.
  async sendMessage(peer, message) { await myTransport.send(peer, message); },
  // Deliver piece bytes (binary channel recommended — a piece is 64 KiB … 512 KiB).
  async sendData(peer, piece) { await myTransport.sendBinary(peer, piece); },
  // Which peers are currently reachable for this share (live transport).
  async partners(share) { return myTransport.livePeers(share); },
};

const engine = new FilesEngine({
  share: 'photos',                          // validated: /^[A-Za-z0-9_-]{1,64}$/
  rootDir: '/srv/photos',                   // the folder whose CONTENTS sync; created if missing
  stateDir: '/var/lib/myapp/photos-state',  // pieces/index/audit; MUST be outside rootDir
  port,
  hooks: {
    getSelfSid: () => myPeerId,                                  // writer identity for LWW
    appendLocalMail: (from, title, content) => notify(title, content),
    verbose: (tool, msg) => console.debug(`[${tool}] ${msg}`),
    noteFileChange: (share, relPath, origin, change) => {         // optional
      if (origin === 'remote') wakeMyAgent(share, relPath, change);
    },
  },
});

engine.start();                   // scan + watch + periodic manifest gossip
engine.onMessage(peer, message);  // call for every inbound SyncMessage
engine.onData(peer, piece);       // call for every inbound SyncDataMessage
engine.close();                   // stop the watcher and the timers
```

The package imposes **no** filesystem layout: `rootDir` and `stateDir` are
yours. `stateDir` must resolve outside `rootDir` — the constructor refuses it
(including through a symlink), because state inside the shared folder would
sync itself.

### The port is asynchronous on purpose

`sendMessage` / `sendData` / `partners` return promises, but the engine **never
reads a send result**. Delivery is best-effort; correctness comes from the
periodic manifest re-exchange, which is the retry. A transport that drops,
defers or rejects only costs one gossip cycle. A synchronous transport simply
returns `Promise.resolve()`.

## Data flow — what actually travels

Everything on the "wire" between you and the engine is a plain object from
`src/protocol.ts`. There are no verbs, no frame tags, no socket types — mapping
them onto your frames is the host's job.

```ts
// Whole-share state — sent on start, on every local change, and every 30 s.
{ kind: 'manifest', share: 'photos',
  files: [{ relPath: 'a.jpg', gen: { mtimeMs: 1793000000000, sid: 'peerA' },
            size: 1048576, pieceListHash: '9f2c…' }],
  tombstones: [{ relPath: 'old.jpg', gen: { mtimeMs: 1792990000000, sid: 'peerB' }, by: 'peerB' }] }

// Receiver wants the piece list of one file.
{ kind: 'want-list', share: 'photos', relPath: 'a.jpg' }

// Reply: the file's ORDERED piece hashes (sha256 hex).
{ kind: 'piece-list', share: 'photos', relPath: 'a.jpg',
  pieceHashes: ['3ab1…', '77e0…', 'c9d4…'] }

// Receiver wants specific pieces (bounded batches).
{ kind: 'want-pieces', share: 'photos', pieceHashes: ['3ab1…', '77e0…'] }

// A chunk's bytes — delivered through onData / sendData, verified by hash on receipt.
{ kind: 'piece', share: 'photos', pieceHash: '3ab1…', data: <Buffer> }

// A path was deleted at generation gen (outranks any older live entry).
{ kind: 'deletion', share: 'photos', relPath: 'old.jpg',
  gen: { mtimeMs: 1792990000000, sid: 'peerB' } }

// A protocol-level refusal (bad share, oversized file, …).
{ kind: 'error', share: 'photos', reason: 'file too large' }
```

A full exchange for one new file:

```
A: manifest(files=[a.jpg])            ──▶ B
B: want-list(a.jpg)                   ──▶ A
A: piece-list([h1, h2, h3])           ──▶ B
B: want-pieces([h1, h2, h3])          ──▶ A
A: piece(h1), piece(h2), piece(h3)    ──▶ B      (sendData)
B: writes a.jpg atomically, then sends its own manifest (converged)
```

## API

### `class FilesEngine`

| Member | Meaning |
|---|---|
| `new FilesEngine(options)` | Validates `share`, resolves and creates `rootDir`/`stateDir`, refuses a state dir inside the root |
| `start()` | Initial scan, first gossip, 30 s gossip timer, chokidar watcher |
| `scan()` | Force a full pass (when you own a different change signal) |
| `onMessage(peer, message)` | Inbound `SyncMessage` |
| `onData(peer, piece)` | Inbound `SyncDataMessage` |
| `onPieceList(peer, relPath, hashes)` | Inbound piece list, if your transport splits it out |
| `gossip()` / `gossipNow()` | Send a manifest now |
| `close()` | Stop watcher + timers; nothing fires afterwards |
| `share`, `root`, `store`, `index`, `audit` | Read-only handles (inspection/instrumentation) |

### `EngineOptions`

| Field | Meaning |
|---|---|
| `share` | Share id, `[A-Za-z0-9_-]{1,64}`; travels with every message |
| `rootDir` | Synced folder (contents only). Created if missing |
| `stateDir` | Pieces, index, audit log. Must be outside `rootDir` |
| `port` | `PeerPort` — your transport |
| `hooks` | `EngineHooks` — `getSelfSid` (required), `appendLocalMail`, `verbose`, `noteFileChange` (optional) |
| `debugFilesync?` | Write a `frames-*.jsonl` transcript under `stateDir` |
| `maxFileBytes?` | Refuse files above this size (default 500 MiB) |

### `EngineHooks`

| Hook | Meaning |
|---|---|
| `getSelfSid()` | This process's writer id — the LWW tiebreak key |
| `appendLocalMail(from, title, content)` | A conflict needs the host's attention (the losing copy is preserved on disk) |
| `verbose(tool, message)` | Diagnostics; never affects sync |
| `noteFileChange?(share, relPath, origin, change)` | A file landed in the share. `origin` is `'local'` (we scanned it) or `'remote'` (applied from a partner) — only `'remote'` news is worth waking anyone |

### Lower-level exports

`chunkBuffer`, `pieceListHash`, `sha256Hex`, `PIECE_MIN/MAX/AVG` (64/512/256 KiB),
`PieceStore`, `ShareIndex`, `compareGen`, `conflictKey`, `FrameAuditLog`, the
protocol types, and the tuning constants `GOSSIP_MS` (30 s), `WATCH_DEBOUNCE_MS`,
`WRITE_STABILITY_MS`, `WATCH_READY_TIMEOUT_MS`, `SKEW_DEADLINE_MS`,
`REQUEST_WINDOW`, `PENDING_TTL_MS`, `MAX_PENDING_FILES`. `prunePending()`
takes an optional `limit` so a caller can ask for room before inserting.

## Guarantees

- **Integrity.** Every piece is verified against its sha256 before storage; a
  file is verified against its `pieceListHash` and `size` before apply.
- **Confinement.** Writes are checked against the *real* share root, so a
  symlinked (or Windows-junctioned) parent cannot redirect an apply outside it;
  temp files are created `O_EXCL | O_NOFOLLOW` and renamed into place.
- **Deletes are first-class.** A delete is a tombstone with a generation that
  outranks an older live entry — a re-advertised file cannot resurrect a path
  that was deleted.
- **Transport drops abort in-flight transfers.** Pending fetches are pruned
  when their peer leaves `port.partners(share)`, expire after
  `PENDING_TTL_MS`, and are bounded by `MAX_PENDING_FILES` as a **hard**
  admission limit: the map never exceeds the cap, evicting in insertion order
  (oldest first). Resume is emergent:
  the piece store keeps what arrived and the next manifest asks for the rest.
- **The watcher is latency only, never correctness.** It is chokidar (raw
  `fs.watch` misses same-size rewrites and atomic saves); if it cannot start,
  the engine degrades to gossip and sync still converges.
- **Conflicts are local knowledge.** A losing local edit is preserved as
  `<name>.conflict-<sid>-<ts>` and reported through `appendLocalMail`; the
  entry expires once that copy is removed.

## Development

```sh
npm install
npm run build       # tsc → dist/ (also runs on prepack)
npm run typecheck   # src/ (the published program)
npm run typecheck:test
npm test            # vitest — tests/ at the repo root: unit (chunker/store/index), engine security, two-peer, watcher, audit, public contract
```

`tests/public-api.test.ts` is the **contract test**: it imports only the
barrel (`../src/index.js`) and drives the engine exactly as this README documents.
If a published-surface export is dropped or stops being sufficient, that test
fails while every internal test still passes.

## Not included (by design)

Wire format, sockets, peer discovery, membership gossip, authentication,
compression, encryption, piece garbage collection, and storage quotas. The host
decides all of it.

## License

MIT
