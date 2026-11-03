/**
 * audit-log.ts — `--debug-filesync` frame transcript (filesync.md §8).
 *
 * One JSONL line per sync frame in BOTH directions. Binary data frames are
 * logged as metadata + piece sha256 + byte count (never raw payload); the
 * log is a total-order protocol transcript per socket (TCP-ordered), so
 * correctness questions become greps and tests can assert frame sequences.
 *
 * Default OFF — zero cost when disabled. Lives at
 * `<stateDir>/frames-<session-ts>.jsonl` (caller-owned; the host decides
 * where state lives and keeps it outside the synced folder).
 */

import fs from 'fs';
import path from 'path';

export interface FrameLogLine {
  ts: string;
  dir: 'tx' | 'rx';
  peer: string;
  frame: string;
  share: string;
  [key: string]: unknown;
}

export class FrameAuditLog {
  private stream: fs.WriteStream | null = null;
  private seq = 0;

  /** @param stateDir caller-owned state directory (see PieceStore). */
  constructor(stateDir: string, enabled: boolean) {
    if (!enabled) return;
    const dir = stateDir;
    fs.mkdirSync(dir, { recursive: true });
    const name = `frames-${new Date().toISOString().replace(/[:.]/g, '-')}.jsonl`;
    this.stream = fs.createWriteStream(path.join(dir, name), { flags: 'a' });
  }

  get enabled(): boolean { return this.stream !== null; }

  /** Append one frame record. `fields` carries frame-specific metadata
   *  (piece hash + bytes for data frames; payload fields for control). */
  log(dir: 'tx' | 'rx', peer: string, frame: string, share: string, fields: Record<string, unknown> = {}): void {
    if (!this.stream) return;
    const line: FrameLogLine = {
      ts: new Date().toISOString(),
      dir,
      peer,
      frame,
      share,
      seq: this.seq++,
      ...fields,
    };
    this.stream.write(`${JSON.stringify(line)}\n`); // flushed per line (append-only)
  }

  close(): void {
    this.stream?.end();
    this.stream = null;
  }
}
