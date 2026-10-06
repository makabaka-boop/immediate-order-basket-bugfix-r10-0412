// MatchingService: orchestrates engine, ledger and publisher around one
// monotonic commit sequence.
//
// Invariants enforced here:
//   * A request is matched tentatively in the engine, then durably
//     committed. If the commit fails, the tentative engine is DISCARDED and
//     rebuilt by replaying the ledger, so un-durable trades can never be
//     published and receive-order priority cannot be corrupted.
//   * Events are published only after the ledger transaction for the
//     commit returned and the engine noted the commit.
//   * Idempotency keys live in the SAME transaction as their events:
//     a duplicate key returns the stored receipt; a reused key with a
//     different payload is rejected before any processing.
//   * Restart recovery replays committed facts (never re-matches) and
//     primes the publisher tail from the durable log.

import { Ledger, canonicalRequestHash, CommitFailureError } from "./ledger.js";
import { MatchingEngine } from "./matching-engine.js";
import { Publisher, type PublishedEvent } from "./publisher.js";
import { replayAll, replayFromSnapshot } from "./replay.js";
import type {
  BookSnapshotData,
  BookView,
  RequestReceipt,
  SubmitEnvelope,
  TradeRequest,
} from "./types.js";

export interface ServiceOptions {
  /** Write a snapshot at least every N committed events. Default 256. */
  snapshotEveryEvents?: number;
  /** Number of snapshots to retain. Default 3. */
  retainSnapshots?: number;
  publisherBufferSize?: number;
  /** Fault injection: force the next N commits to fail durability. */
  failNextCommits?: number;
}

export interface SubmitResult {
  ok: boolean;
  receipt?: RequestReceipt;
  errorCode?: "INVALID_REQUEST" | "REUSED_KEY" | "PERSISTENCE";
  errorMessage?: string;
  /** Set when the request was rebuilt away after a durability failure. */
  rebuilt?: boolean;
}

export class MatchingService {
  readonly ledger: Ledger;
  readonly publisher: Publisher;
  private engine: MatchingEngine;
  private readonly snapshotEveryEvents: number;
  private readonly retainSnapshots: number;
  private eventsSinceSnapshot: number;
  private serializeChain: Promise<unknown> = Promise.resolve();

  private constructor(
    ledger: Ledger,
    engine: MatchingEngine,
    publisher: Publisher,
    options: Required<
      Pick<ServiceOptions, "snapshotEveryEvents" | "retainSnapshots">
    >,
  ) {
    this.ledger = ledger;
    this.engine = engine;
    this.publisher = publisher;
    this.snapshotEveryEvents = options.snapshotEveryEvents;
    this.retainSnapshots = options.retainSnapshots;
    this.eventsSinceSnapshot = 0;

    publisher.setSnapshotProvider((atOrBefore) =>
      this.provideSnapshot(atOrBefore),
    );
  }

  /** Open (or create) a service backed by a SQLite file / memory DB. */
  static open(
    path: string | ":memory:",
    options: ServiceOptions = {},
  ): MatchingService {
    const ledger = new Ledger(path, {
      failCommits: options.failNextCommits ?? 0,
    });
    return MatchingService.recover(ledger, options);
  }

  /** Build a service around an already-open ledger (tests). */
  static recover(
    ledger: Ledger,
    options: ServiceOptions = {},
  ): MatchingService {
    const publisher = new Publisher({
      bufferSize: options.publisherBufferSize ?? 128,
    });
    const maxCommitSeq = ledger.maxCommitSeq();
    const maxEventSeq = ledger.maxEventSeq();

    let engine: MatchingEngine;
    const snap = ledger.latestSnapshot();
    if (snap) {
      const rows = ledger.loadEventsAfter(snap.eventSeq);
      ({ engine } = replayFromSnapshot(snap.data, rows, maxCommitSeq));
    } else {
      const rows = ledger.loadEventsAfter(0);
      ({ engine } = replayAll(rows, maxCommitSeq));
    }

    // Prime the in-process tail so fresh subscribers can resume without a
    // snapshot when the gap is small.
    const tail = ledger.loadEventsAfter(
      Math.max(0, maxEventSeq - (options.publisherBufferSize ?? 128)),
    );
    publisher.prime(
      tail.map((r) => ({ seq: r.seq, commitSeq: r.recvSeq, event: r.payload })),
    );

    const service = new MatchingService(ledger, engine, publisher, {
      snapshotEveryEvents: options.snapshotEveryEvents ?? 256,
      retainSnapshots: options.retainSnapshots ?? 3,
    });
    service.eventsSinceSnapshot = snap
      ? maxEventSeq - snap.eventSeq
      : maxEventSeq;
    return service;
  }

  /**
   * Submit one request. Serialized internally; callers may invoke
   * concurrently — receive order is the invocation resolution order of
   * this chain.
   */
  submit(envelope: SubmitEnvelope): Promise<SubmitResult> {
    const run = async (): Promise<SubmitResult> => this.submitOnce(envelope);
    const result = this.serializeChain.then(run, run);
    // Keep the chain alive regardless of outcome.
    this.serializeChain = result.catch(() => undefined);
    return result;
  }

  private submitOnce(envelope: SubmitEnvelope): SubmitResult {
    const { key, request } = normalizeEnvelope(envelope);

    // ---- Idempotency lookup (durable, before any processing) ----
    if (key !== undefined) {
      const prior = this.ledger.getRequest(key);
      if (prior) {
        if (prior.requestHash !== canonicalRequestHash(request)) {
          return {
            ok: false,
            errorCode: "REUSED_KEY",
            errorMessage: `key '${key}' was already used with a different request payload`,
          };
        }
        // Exact duplicate: replay the ORIGINAL receipt. No new seq, no event.
        return { ok: true, receipt: prior.receipt };
      }
    }

    // ---- Static validation ----
    const invalid = this.engine.validate(request);
    if (invalid !== null) {
      // Structural invalidity is not an accepted request: it consumes no
      // receive sequence and produces no ledger events. Keyed callers do
      // not cache it either, so a corrected retry with the same key works.
      return { ok: false, errorCode: "INVALID_REQUEST", errorMessage: invalid };
    }

    const recvSeq = this.engine.lastSeq + 1;

    // ---- Tentative match ----
    const outcome = this.engine.process(request, recvSeq);
    const events = outcome.events;
    const receipt = outcome.receipt;

    // ---- Durable commit (events + idempotency key in ONE transaction) ----
    let range: { firstSeq: number; lastSeq: number };
    try {
      range = this.ledger.commitRequest({
        recvSeq,
        events,
        ...(key !== undefined
          ? {
              key,
              requestHash: canonicalRequestHash(request),
              receipt,
            }
          : {}),
      });
    } catch (err) {
      if (err instanceof CommitFailureError) {
        // Discard tentative state and rebuild from durable truth. Nothing
        // was published. The failed recvSeq is simply not part of history;
        // the next submit retries with the same number.
        this.rebuildAfterFailure();
        return {
          ok: false,
          errorCode: "PERSISTENCE",
          errorMessage: err.message,
          rebuilt: true,
        };
      }
      throw err;
    }

    // ---- Commit line: only now does the world advance together ----
    this.engine.noteCommit(recvSeq);

    if (events.length > 0) {
      const batch: PublishedEvent[] = events.map((ev, i) => ({
        seq: range.firstSeq + i,
        commitSeq: recvSeq,
        event: ev,
      }));
      // Durability already holds -> safe to publish.
      this.publisher.publish(batch);
    }

    // ---- Snapshot bookkeeping (best-effort; must not block publishes) ----
    this.eventsSinceSnapshot += events.length;
    if (
      this.eventsSinceSnapshot > 0 &&
      this.eventsSinceSnapshot >= this.snapshotEveryEvents
    ) {
      try {
        this.writeSnapshot(range.lastSeq || this.ledger.maxEventSeq());
      } catch (err) {
        // Snapshot failure degrades fast-subscribe only; trades are safe.
        // Surface it on stderr-like hook in real deployments.
        // eslint-disable-next-line no-console
        console.error("snapshot write failed:", (err as Error).message);
      }
    }

    return { ok: true, receipt };
  }

  private rebuildAfterFailure(): void {
    // Reconstruct engine state from the durable ledger via a throwaway
    // service, but keep THIS service's publisher/ledger identities so
    // subscriber sockets stay attached to the same object.
    const recovered = MatchingService.recover(this.ledger, {
      snapshotEveryEvents: this.snapshotEveryEvents,
      retainSnapshots: this.retainSnapshots,
      publisherBufferSize: this.publisher.bufferCapacity,
    });
    this.engine = recovered.engine;
    this.eventsSinceSnapshot = recovered.eventsSinceSnapshot;
  }

  // ---- Snapshots -------------------------------------------------------

  private writeSnapshot(atEventSeq: number): void {
    const snap: BookSnapshotData = this.engine.snapshot(atEventSeq);
    this.ledger.saveSnapshot(atEventSeq, this.engine.lastSeq, snap);
    this.eventsSinceSnapshot = 0;
    this.ledger.prune(this.retainSnapshots);
  }

  private provideSnapshot(atOrBefore: number | null) {
    const row =
      atOrBefore === null
        ? this.ledger.latestSnapshot()
        : (this.ledger.snapshotAtOrBefore(atOrBefore) ??
          this.ledger.latestSnapshot());
    if (!row) return null;
    return {
      seq: row.data.snapshotSeq,
      recvSeq: row.data.atRecvSeq,
      book: snapshotToView(row.data, this.engine.lastCommitSeq),
    };
  }

  // ---- Views / lifecycle ----------------------------------------------

  book(): BookView {
    return this.engine.view();
  }

  lastCommitSeq(): number {
    return this.engine.lastCommitSeq;
  }

  headEventSeq(): number {
    return this.publisher.headSeq;
  }

  close(): void {
    this.ledger.close();
  }
}

function normalizeEnvelope(envelope: SubmitEnvelope): {
  key: string | undefined;
  request: TradeRequest;
} {
  const raw = envelope as Partial<SubmitEnvelope> | null;
  const key =
    typeof raw?.key === "string" && raw.key.length > 0 ? raw.key : undefined;
  return { key, request: raw!.request as TradeRequest };
}

/** Build a public BookView directly from a stored snapshot payload. */
export function snapshotToView(
  snap: BookSnapshotData,
  currentCommitSeq: number,
): BookView {
  const bidMap = new Map<number, BookView["bids"][number]["orders"]>();
  const askMap = new Map<number, BookView["asks"][number]["orders"]>();
  for (const r of snap.resting) {
    const entry = {
      orderId: r.id,
      ...(r.clientOrderId !== undefined
        ? { clientOrderId: r.clientOrderId }
        : {}),
      side: r.side,
      price: r.price,
      remainingQty: r.totalQty - r.filledQty,
      prioritySeq: r.recvSeq,
    };
    const map = r.side === "buy" ? bidMap : askMap;
    const list = map.get(r.price);
    if (list) list.push(entry);
    else map.set(r.price, [entry]);
  }
  const build = (
    map: Map<number, BookView["bids"][number]["orders"]>,
    side: "buy" | "sell",
  ): BookView["bids"] =>
    [...map.entries()]
      .sort(([a], [b]) => (side === "buy" ? b - a : a - b))
      .map(([price, orders]) => ({
        price,
        remainingQty: orders.reduce((s, o) => s + o.remainingQty, 0),
        orders: orders.sort((a, b) => a.prioritySeq - b.prioritySeq),
      }));
  return {
    // The snapshot reflects state up to its own commit; currentCommitSeq is
    // used only for fresh hello frames, so report the snapshot point here.
    lastCommitSeq: snap.atRecvSeq,
    bids: build(bidMap, "buy"),
    asks: build(askMap, "sell"),
  };
}
