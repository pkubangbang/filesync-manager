/**
 * @pkubangbang/filesync-manager — public surface.
 *
 * A transport-agnostic local file sync engine. The host application supplies
 * a `PeerPort` (how messages leave the process) and delivers inbound peer
 * messages via `FilesEngine.onMessage` / `onData`; everything else — scan,
 * chunk, store, manifest, fetch, apply, conflict preservation — lives here.
 */

export { chunkBuffer, pieceListHash, sha256Hex, PIECE_MIN, PIECE_MAX, PIECE_AVG, type Piece } from './chunker.js';
export { PieceStore } from './store.js';
export {
  ShareIndex, compareGen, conflictKey,
  type FileEntry, type FileGen, type Tombstone, type ConflictEntry,
} from './share-index.js';
export {
  type SyncMessage, type SyncDataMessage,
  type ManifestMessage, type WantListMessage, type PieceListMessage,
  type WantPiecesMessage, type DeletionMessage, type ErrorMessage,
  type PieceMessage, type FileMeta, type TombstoneMeta,
} from './protocol.js';
export { FrameAuditLog, type FrameLogLine } from './audit-log.js';
export {
  FilesEngine, GOSSIP_MS, WATCH_DEBOUNCE_MS, WRITE_STABILITY_MS, WATCH_READY_TIMEOUT_MS,
  SKEW_DEADLINE_MS, REQUEST_WINDOW, PENDING_TTL_MS, MAX_PENDING_FILES,
  type PeerPort, type EngineHooks, type EngineOptions,
} from './engine.js';
