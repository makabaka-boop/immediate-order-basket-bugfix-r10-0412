// Ledger replay / state recovery.
//
// Recovery NEVER re-runs the matching algorithm: trade events are consumed
// as already-decided facts (they only adjust filled quantities). This makes
// a restart idempotent — replaying committed events cannot regenerate or
// duplicate trades that already exist in the ledger.
//
// Two entry points:
//   replayAll(events, maxCommitSeq)        -> rebuild from an empty state
//   replayFromSnapshot(snap, events, maxCommitSeq) -> continue after a point
//
// Events are grouped by recvSeq (one commit = one recvSeq batch) because an
// accepted event and its trades share a batch: whether a GTC taker rests
// depends on its remaining quantity *after* the batch.

import { MatchingEngine } from "./matching-engine.js";
import type { BookSnapshotData, MatchEvent } from "./types.js";

export interface ReplayResult {
  engine: MatchingEngine;
  /** Highest eventSeq consumed (0 if none). */
  lastEventSeq: number;
  /** Highest commitSeq present in the ledger. */
  lastCommitSeq: number;
}

export interface LedgerEventRow {
  seq: number;
  recvSeq: number;
  payload: MatchEvent;
}

export function replayFromEvents(
  rows: LedgerEventRow[],
  maxCommitSeq: number,
  base?: { engine: MatchingEngine; throughSeq: number },
): ReplayResult {
  const engine = base?.engine ?? new MatchingEngine();
  let lastEventSeq = base?.throughSeq ?? 0;

  // Track tif of orders accepted in the current commit batch.
  let batchSeq = -1;
  const acceptedInBatch = new Map<string, "GTC" | "IOC">();

  const closeBatch = () => {
    if (batchSeq >= 0) {
      engine.hydrateCommitBoundary(batchSeq, acceptedInBatch);
    }
    batchSeq = -1;
  };

  for (const row of rows) {
    if (row.seq <= lastEventSeq) continue; // skip anything already covered
    const ev = row.payload;

    if (batchSeq === -1) {
      batchSeq = ev.recvSeq;
    } else if (ev.recvSeq !== batchSeq) {
      closeBatch();
      batchSeq = ev.recvSeq;
    }

    switch (ev.type) {
      case "accepted":
        engine.hydrateAccepted(ev);
        acceptedInBatch.set(ev.orderId, ev.tif);
        break;
      case "trade":
        engine.hydrateTrade(ev);
        break;
      case "cancelled":
        engine.hydrateCancelled(ev);
        break;
      case "amended": {
        // tif of an amended order is always GTC (IOC never rests).
        engine.hydrateAmended(ev, "GTC");
        break;
      }
    }
    lastEventSeq = row.seq;
  }
  closeBatch();

  engine.setCommittedSeq(maxCommitSeq);
  return { engine, lastEventSeq, lastCommitSeq: maxCommitSeq };
}

export function replayAll(
  rows: LedgerEventRow[],
  maxCommitSeq: number,
): ReplayResult {
  return replayFromEvents(rows, maxCommitSeq);
}

export function replayFromSnapshot(
  snap: BookSnapshotData,
  rows: LedgerEventRow[],
  maxCommitSeq: number,
): ReplayResult {
  const engine = MatchingEngine.fromSnapshot(snap);
  return replayFromEvents(rows, maxCommitSeq, {
    engine,
    throughSeq: snap.snapshotSeq,
  });
}
