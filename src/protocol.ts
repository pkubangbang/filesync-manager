/**
 * protocol.ts — the transport-agnostic message vocabulary of the sync engine.
 *
 * These are PLAIN DATA. They carry no wire verbs (`t:'index'`), no frame
 * tags (`sync-index`), and no socket types — the host application owns the
 * wire format and folds these messages onto whatever frames its transport
 * uses (mycc: `src/peer/filesync/wire-codec.ts` → `sync-index` / `0x04`…).
 *
 * Mapping to the filesync design doc (docs/peers/filesync.md §3):
 *   ManifestMessage   ⇄ wire `sync-index`
 *   WantListMessage   ⇄ wire `sync-req` with `file` + `want:'pieces'`
 *   PieceListMessage  ⇄ wire `sync-pieces`
 *   WantPiecesMessage ⇄ wire `sync-req` with `pieceHashes`
 *   DeletionMessage   ⇄ wire `sync-del`
 *   ErrorMessage      ⇄ wire `sync-err`
 *   PieceMessage      ⇄ wire `sync-data` (binary frame)
 */

/** A file's generation: LWW ordering key (mtime, sid as tiebreak). */
export interface FileGen {
  mtimeMs: number;
  /** Writer identity — the HOST's peer id, normalized by the host on receipt. */
  sid: string;
}

export interface FileMeta {
  relPath: string;
  gen: FileGen;
  size: number;
  /** sha256 of the file's ordered piece-hash list (cheap equality test). */
  pieceListHash: string;
}

export interface TombstoneMeta {
  relPath: string;
  gen: FileGen;
  by: string;
}

/** Whole-share state, exchanged on join / change / periodic reconcile. */
export interface ManifestMessage {
  kind: 'manifest';
  share: string;
  files: FileMeta[];
  tombstones: TombstoneMeta[];
}

/** "Send me the piece list of this file." */
export interface WantListMessage {
  kind: 'want-list';
  share: string;
  relPath: string;
}

/** Reply to WantListMessage — the file's ordered piece hashes. */
export interface PieceListMessage {
  kind: 'piece-list';
  share: string;
  relPath: string;
  pieceHashes: string[];
}

/** "Send me these pieces." */
export interface WantPiecesMessage {
  kind: 'want-pieces';
  share: string;
  pieceHashes: string[];
}

export interface DeletionMessage {
  kind: 'deletion';
  share: string;
  relPath: string;
  gen: FileGen;
}

export interface ErrorMessage {
  kind: 'error';
  share?: string;
  reason: string;
}

/** One chunk's bytes (already hash-verified by the receiver). */
export interface PieceMessage {
  kind: 'piece';
  share: string;
  /** sha256 hex of `data`. */
  pieceHash: string;
  data: Buffer;
}

export type SyncMessage =
  | ManifestMessage
  | WantListMessage
  | PieceListMessage
  | WantPiecesMessage
  | DeletionMessage
  | ErrorMessage;

export type SyncDataMessage = PieceMessage;
