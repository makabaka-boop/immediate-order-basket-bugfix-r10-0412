// WebSocket event publisher.
//
// The publisher advances on the *same* commit sequence as the engine and
// ledger: the service enqueues only after the ledger commit for a recvSeq
// has succeeded and the engine's noteCommit ran, so subscribers can never
// observe an un-durable trade.
//
// Resumption contract (subscribe with lastSeq):
//   1. lastSeq >= current head                 -> live tail only.
//   2. the gap is inside the in-memory buffer  -> replay buffered events.
//   3. the gap starts before the buffer / after a restart
//      (i.e. exceeds what is retained live)    -> first deliver the
//      appropriate snapshot, then replay every retained event after it.
//
// Envelope event seqs are the ledger AUTOINCREMENT ids and never repeat.

import type { Ledger } from "./ledger.js";
import type {
  BookView,
  MatchEvent,
  ServerMessage,
  WireError,
  WireEvent,
} from "./types.js";

export interface PublishedEvent {
  seq: number;
  commitSeq: number; // recvSeq of the commit
  event: MatchEvent;
}

export interface SnapshotResponse {
  seq: number;
  recvSeq: number;
  book: BookView;
}

type SnapshotProvider = (
  atOrBefore: number | null,
) => Promise<SnapshotResponse | null> | SnapshotResponse | null;

interface Subscriber {
  send: (msg: ServerMessage) => void;
  lastSeq: number;
  live: boolean;
}

export interface PublisherOptions {
  /** Number of recent events kept in memory. */
  bufferSize?: number;
}

export class Publisher {
  private buffer: PublishedEvent[] = [];
  private readonly bufferSize: number;
  private head = 0; // highest seq ever enqueued in this process
  private subscribers = new Set<Subscriber>();
  private snapshotProvider: SnapshotProvider | null = null;

  constructor(options: PublisherOptions = {}) {
    this.bufferSize = options.bufferSize ?? 1024;
  }

  setSnapshotProvider(provider: SnapshotProvider): void {
    this.snapshotProvider = provider;
  }

  /** Current head (highest committed event seq known in this process). */
  get headSeq(): number {
    return this.head;
  }

  get bufferCapacity(): number {
    return this.bufferSize;
  }

  /** Lowest seq currently retained in the in-memory buffer. */
  get oldestBufferedSeq(): number | null {
    return this.buffer.length > 0 ? this.buffer[0]!.seq : null;
  }

  /**
   * Enqueue events of one successful commit. Must be called in commit order
   * with contiguous seqs. Returns the number of events published.
   */
  publish(batch: PublishedEvent[]): void {
    if (batch.length === 0) return;
    // Strict ordering check against the head (first publish may start > 0
    // after a restart if the service primes the buffer — see prime()).
    const expectedStart = this.head === 0 ? batch[0]!.seq : this.head + 1;
    if (batch[0]!.seq !== expectedStart) {
      throw new Error(
        `publisher seq gap: expected ${expectedStart}, got ${batch[0]!.seq}`,
      );
    }
    for (let i = 1; i < batch.length; i++) {
      if (batch[i]!.seq !== batch[i - 1]!.seq + 1) {
        throw new Error(`publisher batch non-contiguous at ${batch[i]!.seq}`);
      }
    }

    this.buffer.push(...batch);
    if (this.buffer.length > this.bufferSize) {
      this.buffer.splice(0, this.buffer.length - this.bufferSize);
    }
    this.head = batch[batch.length - 1]!.seq;

    for (const sub of this.subscribers) {
      if (!sub.live) continue;
      for (const ev of batch) {
        if (ev.seq > sub.lastSeq) this.deliver(sub, ev);
      }
    }
  }

  /**
   * Prime the tail buffer after a restart from the tail of the ledger so
   * fresh in-process subscribers can resume without a snapshot.
   */
  prime(batch: PublishedEvent[]): void {
    // Only used once at startup; set buffer directly, respecting size.
    this.buffer = batch.slice(-this.bufferSize);
    this.head = batch.length > 0 ? batch[batch.length - 1]!.seq : this.head;
  }

  /**
   * Register a subscriber and perform initial catch-up. `send` receives
   * already-shaped wire messages. Resolves once catch-up delivery is done.
   */
  async subscribe(
    send: (msg: ServerMessage) => void,
    lastSeq: number | undefined,
    currentCommitSeq: number,
    ledger: Pick<Ledger, "loadEventsAfter">,
  ): Promise<void> {
    const sub: Subscriber = { send, lastSeq: lastSeq ?? 0, live: false };
    this.subscribers.add(sub);

    try {
      const from = sub.lastSeq;

      if (from >= this.head) {
        // Caught up: nothing historical to send.
        sub.live = true;
        send({
          type: "hello",
          lastSeq: this.head,
          commitSeq: currentCommitSeq,
        });
        return;
      }

      const buffered = this.buffer.filter((e) => e.seq > from);
      const oldestBuffered =
        this.buffer.length > 0 ? this.buffer[0]!.seq : null;

      if (
        buffered.length > 0 &&
        (oldestBuffered === null || oldestBuffered <= from + 1)
      ) {
        // Case 2: the full gap is inside the retained buffer.
        for (const ev of buffered) this.deliver(sub, ev);
        sub.live = true;
        send({
          type: "caught_up",
          lastSeq: this.head,
          commitSeq: currentCommitSeq,
        });
        return;
      }

      // Case 3: gap exceeds the live-retained range -> snapshot first.
      //
      // Pick the newest snapshot at or before the subscriber position when
      // possible; if none exists there, use the newest available snapshot
      // (covers "gap beyond retention" after restart/pruning).
      let snap: SnapshotResponse | null = null;
      if (this.snapshotProvider) {
        snap = await this.snapshotProvider(from > 0 ? from : null);
        if (snap === null && from > 0) {
          snap = await this.snapshotProvider(null);
        }
      }

      if (snap === null) {
        // No snapshot exists at all (fresh system): attempt a raw replay of
        // every retained event, else start from live head.
        const rows = ledger.loadEventsAfter(from);
        if (rows.length > 0 && rows[0]!.seq === from + 1) {
          for (const row of rows) {
            this.deliver(sub, {
              seq: row.seq,
              commitSeq: row.recvSeq,
              event: row.payload,
            });
          }
        } else {
          const err: WireError = {
            type: "error",
            code: "PROTOCOL",
            message:
              "requested position is before retained history and no snapshot is available; subscribing from live head",
          };
          send(err);
          sub.lastSeq = this.head;
          sub.live = true;
          send({
            type: "hello",
            lastSeq: this.head,
            commitSeq: currentCommitSeq,
          });
          return;
        }
        sub.live = true;
        send({
          type: "caught_up",
          lastSeq: this.head,
          commitSeq: currentCommitSeq,
        });
        return;
      }

      // Send the snapshot BEFORE any older event (it carries the book state).
      send({
        type: "snapshot",
        seq: snap.seq,
        recvSeq: snap.recvSeq,
        resync: snap.seq > from,
        book: snap.book,
      });
      sub.lastSeq = snap.seq;

      // Replay every retained event after the snapshot point from the
      // durable ledger; after that the live buffer / tail takes over.
      const rows = ledger.loadEventsAfter(snap.seq);
      for (const row of rows) {
        if (row.seq <= this.head) {
          this.deliver(sub, {
            seq: row.seq,
            commitSeq: row.recvSeq,
            event: row.payload,
          });
        }
      }
      // Also replay anything buffered in-process that wasn't in the DB read
      // (e.g. very recent tail), without duplicates.
      for (const ev of this.buffer) {
        if (ev.seq > sub.lastSeq) this.deliver(sub, ev);
      }

      sub.live = true;
      send({
        type: "caught_up",
        lastSeq: this.head,
        commitSeq: currentCommitSeq,
      });
    } finally {
      // Registration remains so live events flow; unsubscribe removes it.
    }
  }

  unsubscribe(send: (msg: ServerMessage) => void): void {
    for (const sub of this.subscribers) {
      if (sub.send === send) {
        this.subscribers.delete(sub);
        return;
      }
    }
  }

  /** Force a subscriber into live mode at head (used by tests). */
  liveAtHead(send: (msg: ServerMessage) => void): void {
    for (const sub of this.subscribers) {
      if (sub.send === send) {
        sub.lastSeq = this.head;
        sub.live = true;
      }
    }
  }

  subscriberCount(): number {
    return this.subscribers.size;
  }

  private deliver(sub: Subscriber, ev: PublishedEvent): void {
    const wire: WireEvent = {
      type: "event",
      seq: ev.seq,
      commitSeq: ev.commitSeq,
      event: ev.event,
    };
    sub.send(wire);
    sub.lastSeq = ev.seq;
  }
}
