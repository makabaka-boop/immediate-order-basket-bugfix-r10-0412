// SQLite event ledger.
//
// This is the single durability point. Each processed request is stored as
// one commit row plus its ordered event rows; event ids come from
// AUTOINCREMENT and are *never reused*, even after pruning, so subscriber
// positions stay valid for life.
//
// Writes for a commit run in a single deferred transaction. Nothing is
// published to subscribers until `commit()` returns successfully (the
// service enforces this). A fault hook is provided so tests can force a
// commit failure and assert the no-publish / rebuild-on-retry behavior.

import {
  type Database,
  type Options,
  default as DatabaseImpl,
} from "better-sqlite3";
import type { BookSnapshotData, MatchEvent, RequestReceipt } from "./types.js";

export interface LedgerOptions {
  /** Number of upcoming commits to fail (fault injection for tests). */
  failCommits?: number;
}

export interface EventRow {
  seq: number;
  recvSeq: number;
  payload: MatchEvent;
}

export interface SnapshotRow {
  id: number;
  eventSeq: number;
  recvSeq: number;
  data: BookSnapshotData;
}

export interface CommittedRequest {
  key: string;
  requestHash: string;
  receipt: RequestReceipt;
  recvSeq: number;
}

export class Ledger {
  readonly db: Database;
  /** Number of upcoming commits that will be forced to fail. */
  private failCommitsRemaining: number;

  constructor(path: string | ":memory:", options: LedgerOptions = {}) {
    const opts: Options = { fileMustExist: false };
    this.db = new DatabaseImpl(path, opts);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = FULL");
    this.db.pragma("foreign_keys = ON");
    this.failCommitsRemaining = options.failCommits ?? 0;
    this.migrate();
  }

  /** Arm N future commit failures (fault injection). */
  armFailures(n = 1): void {
    this.failCommitsRemaining += n;
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      -- One row per committed request (recvSeq is the commit sequence).
      CREATE TABLE IF NOT EXISTS commits (
        recv_seq   INTEGER PRIMARY KEY,
        event_count INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      );

      -- Append-only event log; AUTOINCREMENT => event seqs are never reused.
      CREATE TABLE IF NOT EXISTS events (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        recv_seq INTEGER NOT NULL,
        type     TEXT NOT NULL,
        payload  TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_events_recv ON events(recv_seq, id);

      -- Idempotency table: request key -> canonical payload hash + receipt.
      CREATE TABLE IF NOT EXISTS requests (
        req_key      TEXT PRIMARY KEY,
        request_hash TEXT NOT NULL,
        recv_seq     INTEGER NOT NULL,
        receipt      TEXT NOT NULL,
        created_at   INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS snapshots (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        event_seq INTEGER NOT NULL UNIQUE,
        recv_seq  INTEGER NOT NULL,
        payload   TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
    `);
    this.db
      .prepare(
        `INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1')`,
      )
      .run();
  }

  // ---- Commit path -----------------------------------------------------

  /**
   * Atomically persist one request commit: its commit row, its ordered
   * event rows and (optionally) its idempotency-key record, all in one
   * deferred transaction. Returns the assigned event seq range.
   *
   * Throws CommitFailureError (and rolls back) when the fault hook is armed
   * or SQLite fails; in that case no rows are visible and callers must
   * discard the tentative in-memory engine state.
   */
  commitRequest(args: {
    recvSeq: number;
    events: MatchEvent[];
    key?: string;
    requestHash?: string;
    receipt?: RequestReceipt;
  }): { firstSeq: number; lastSeq: number } {
    const { recvSeq, events } = args;

    const body = this.db.transaction(
      (): { firstSeq: number; lastSeq: number } => {
        const inserted = this.db
          .prepare(
            `INSERT INTO commits(recv_seq, event_count, created_at) VALUES (?, ?, ?)`,
          )
          .run(recvSeq, events.length, Date.now());
        if (inserted.changes !== 1) {
          throw new Error(`commit row insert conflict for recvSeq ${recvSeq}`);
        }

        const ins = this.db.prepare(
          `INSERT INTO events(recv_seq, type, payload) VALUES (?, ?, ?)`,
        );
        let firstSeq = 0;
        let lastSeq = 0;
        for (const ev of events) {
          const id = Number(
            ins.run(ev.recvSeq, ev.type, JSON.stringify(ev)).lastInsertRowid,
          );
          if (firstSeq === 0) firstSeq = id;
          lastSeq = id;
        }

        if (
          args.key !== undefined &&
          args.receipt !== undefined &&
          args.requestHash !== undefined
        ) {
          this.db
            .prepare(
              `INSERT OR IGNORE INTO requests(req_key, request_hash, recv_seq, receipt, created_at)
             VALUES (?, ?, ?, ?, ?)`,
            )
            .run(
              args.key,
              args.requestHash,
              recvSeq,
              JSON.stringify(args.receipt),
              Date.now(),
            );
        }
        return { firstSeq, lastSeq };
      },
    );

    // Fault is raised at the durable boundary, before any SQL runs.
    if (this.failCommitsRemaining > 0) {
      this.failCommitsRemaining -= 1;
      throw new CommitFailureError(
        `injected commit failure for recvSeq ${recvSeq} (${this.failCommitsRemaining} left armed)`,
      );
    }

    try {
      return body();
    } catch (err) {
      if (err instanceof CommitFailureError) throw err;
      throw new CommitFailureError(
        `sqlite commit failed for recvSeq ${recvSeq}: ${(err as Error).message}`,
        { cause: err as Error },
      );
    }
  }

  /** Persist idempotency key record on its own (used outside commits). */
  saveRequestKey(rec: CommittedRequest): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO requests(req_key, request_hash, recv_seq, receipt, created_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(
        rec.key,
        rec.requestHash,
        rec.recvSeq,
        JSON.stringify(rec.receipt),
        Date.now(),
      );
  }

  getRequest(
    key: string,
  ): { requestHash: string; receipt: RequestReceipt; recvSeq: number } | null {
    const row = this.db
      .prepare(
        `SELECT request_hash AS requestHash, receipt, recv_seq AS recvSeq FROM requests WHERE req_key = ?`,
      )
      .get(key) as
      | { requestHash: string; receipt: string; recvSeq: number }
      | undefined;
    if (!row) return null;
    return {
      requestHash: row.requestHash,
      receipt: JSON.parse(row.receipt) as RequestReceipt,
      recvSeq: row.recvSeq,
    };
  }

  // ---- Read path -------------------------------------------------------

  maxCommitSeq(): number {
    const row = this.db
      .prepare(`SELECT COALESCE(MAX(recv_seq), 0) AS m FROM commits`)
      .get() as { m: number };
    return row.m;
  }

  loadEventsAfter(seq: number, limit?: number): EventRow[] {
    const sql = limit
      ? `SELECT id, recv_seq, payload FROM events WHERE id > ? ORDER BY id ASC LIMIT ?`
      : `SELECT id, recv_seq, payload FROM events WHERE id > ? ORDER BY id ASC`;
    const stmt = this.db.prepare(sql);
    const rows = (limit ? stmt.all(seq, limit) : stmt.all(seq)) as Array<{
      id: number;
      recv_seq: number;
      payload: string;
    }>;
    return rows.map((r) => ({
      seq: r.id,
      recvSeq: r.recv_seq,
      payload: JSON.parse(r.payload) as MatchEvent,
    }));
  }

  /** Smallest event seq still retained (1-based), or null if the log is empty. */
  minEventSeq(): number | null {
    const row = this.db.prepare(`SELECT MIN(id) AS m FROM events`).get() as {
      m: number | null;
    };
    return row.m ?? null;
  }

  maxEventSeq(): number {
    const row = this.db
      .prepare(`SELECT COALESCE(MAX(id), 0) AS m FROM events`)
      .get() as { m: number };
    return row.m;
  }

  // ---- Snapshots -------------------------------------------------------

  saveSnapshot(
    eventSeq: number,
    recvSeq: number,
    data: BookSnapshotData,
  ): SnapshotRow {
    const id = Number(
      this.db
        .prepare(
          `INSERT INTO snapshots(event_seq, recv_seq, payload, created_at)
           VALUES (?, ?, ?, ?)`,
        )
        .run(eventSeq, recvSeq, JSON.stringify(data), Date.now())
        .lastInsertRowid,
    );
    return { id, eventSeq, recvSeq, data };
  }

  latestSnapshot(): SnapshotRow | null {
    const row = this.db
      .prepare(
        `SELECT id, event_seq AS eventSeq, recv_seq AS recvSeq, payload
         FROM snapshots ORDER BY event_seq DESC LIMIT 1`,
      )
      .get() as
      | { id: number; eventSeq: number; recvSeq: number; payload: string }
      | undefined;
    return row ? this.toSnapshotRow(row) : null;
  }

  /**
   * Newest snapshot at or before seq (used to bridge a subscriber that
   * starts inside retained history but before the in-memory buffer's edge).
   */
  snapshotAtOrBefore(seq: number): SnapshotRow | null {
    const row = this.db
      .prepare(
        `SELECT id, event_seq AS eventSeq, recv_seq AS recvSeq, payload
         FROM snapshots WHERE event_seq <= ? ORDER BY event_seq DESC LIMIT 1`,
      )
      .get(seq) as
      | { id: number; eventSeq: number; recvSeq: number; payload: string }
      | undefined;
    return row ? this.toSnapshotRow(row) : null;
  }

  /**
   * Prune: keep the newest `keep` snapshots and drop events fully covered by
   * the oldest retained snapshot. Returns deleted row counts.
   */
  prune(keep: number): { snapshotsRemoved: number; eventsRemoved: number } {
    const tx = this.db.transaction(() => {
      let snapshotsRemoved = 0;
      let eventsRemoved = 0;

      // Event seq of the oldest snapshot among the newest `keep`.
      const boundary = this.db
        .prepare(
          `SELECT event_seq AS s FROM snapshots ORDER BY event_seq DESC LIMIT 1 OFFSET ?`,
        )
        .get(keep - 1) as { s: number } | undefined;

      if (boundary !== undefined) {
        const removedSnaps = this.db
          .prepare(`DELETE FROM snapshots WHERE event_seq < ?`)
          .run(boundary.s);
        snapshotsRemoved = removedSnaps.changes;

        // State up to and including the oldest kept snapshot is fully
        // derivable from that snapshot, so those event rows are redundant.
        const removedEvents = this.db
          .prepare(`DELETE FROM events WHERE id <= ?`)
          .run(boundary.s);
        eventsRemoved = removedEvents.changes;
      }
      return { snapshotsRemoved, eventsRemoved };
    });
    return tx();
  }

  private toSnapshotRow(row: {
    id: number;
    eventSeq: number;
    recvSeq: number;
    payload: string;
  }): SnapshotRow {
    return {
      id: row.id,
      eventSeq: row.eventSeq,
      recvSeq: row.recvSeq,
      data: JSON.parse(row.payload) as BookSnapshotData,
    };
  }

  // ---- Lifecycle -------------------------------------------------------

  checkpoint(): void {
    this.db.pragma("wal_checkpoint(TRUNCATE)");
  }

  close(): void {
    this.db.close();
  }
}

export class CommitFailureError extends Error {
  constructor(message: string, opts?: { cause?: Error }) {
    super(message);
    this.name = "CommitFailureError";
    if (opts?.cause) this.cause = opts.cause;
  }
}

/** Canonical hash of a request payload, used for key-reuse detection. */
export function canonicalRequestHash(request: unknown): string {
  return stableStringify(request);
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object")
    return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
}
