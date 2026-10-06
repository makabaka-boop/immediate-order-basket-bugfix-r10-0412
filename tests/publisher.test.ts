// Tests for subscriber resumption:
//   * gap fully inside the in-memory buffer -> events replay, no snapshot;
//   * gap exceeding the retained range -> snapshot FIRST, then events;
//   * a restarted publisher (empty buffer) uses the durable snapshot path;
//   * live tail: events after subscribe are delivered in order.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Publisher, type PublishedEvent } from "../src/publisher.js";
import type {
  AcceptedEvent,
  BookView,
  MatchEvent,
  ServerMessage,
} from "../src/types.js";

function accepted(seq: number): AcceptedEvent {
  return {
    type: "accepted",
    orderId: `o-${seq}`,
    side: "buy",
    price: 10,
    totalQty: 1,
    tif: "GTC",
    recvSeq: seq,
  };
}

function ev(seq: number, commitSeq = seq): PublishedEvent {
  return { seq, commitSeq, event: accepted(seq) };
}

function emptyBook(lastCommitSeq = 0): BookView {
  return { lastCommitSeq, bids: [], asks: [] };
}

/** Minimal fake ledger for catch-up reads. */
function fakeLedger(
  rows: Array<{ seq: number; recvSeq: number; payload: MatchEvent }>,
) {
  return {
    loadEventsAfter: (seq: number) => rows.filter((r) => r.seq > seq),
    minEventSeq: () => (rows.length ? rows[0]!.seq : null),
  };
}

function collect(): {
  messages: ServerMessage[];
  send: (m: ServerMessage) => void;
} {
  const messages: ServerMessage[] = [];
  return { messages, send: (m) => messages.push(m) };
}

test("resume within buffer: replays missing events, no snapshot frame", async () => {
  const p = new Publisher({ bufferSize: 4 });
  p.publish([ev(1), ev(2), ev(3), ev(4)]);
  let snapCalls = 0;
  p.setSnapshotProvider(() => {
    snapCalls += 1;
    return null;
  });

  const { messages, send } = collect();
  await p.subscribe(send, 2, 4, fakeLedger([]));

  assert.equal(snapCalls, 0);
  const types = messages.map((m) => m.type);
  assert.deepEqual(types, ["event", "event", "caught_up"]);
  const seqs = messages
    .filter((m) => m.type === "event")
    .map((m) => (m as { seq: number }).seq);
  assert.deepEqual(seqs, [3, 4]);
});

test("gap beyond retained range: snapshot delivered FIRST then events", async () => {
  const p = new Publisher({ bufferSize: 2 });
  // Publisher only knows 9,10 (e.g. after a restart); ledger has 3..10.
  const durable = [3, 4, 5, 6, 7, 8, 9, 10].map((n) => ({
    seq: n,
    recvSeq: n,
    payload: accepted(n),
  }));
  p.prime([ev(9, 9), ev(10, 10)]);

  p.setSnapshotProvider(() => ({
    seq: 6,
    recvSeq: 6,
    book: emptyBook(6),
  }));

  const { messages, send } = collect();
  // Subscriber last saw event 1 (older than retention).
  await p.subscribe(send, 1, 10, fakeLedger(durable));

  assert.equal(messages[0]!.type, "snapshot", "snapshot frame must be first");
  const snap = messages[0] as Extract<ServerMessage, { type: "snapshot" }>;
  assert.equal(snap.seq, 6);
  assert.equal(snap.resync, true, "snapshot ahead of subscriber => resync");

  const eventSeqs = messages
    .filter((m) => m.type === "event")
    .map((m) => (m as { seq: number }).seq);
  assert.deepEqual(
    eventSeqs,
    [7, 8, 9, 10],
    "events strictly after snapshot point",
  );
  assert.equal(messages[messages.length - 1]!.type, "caught_up");
});

test("fresh subscriber defaults to lastSeq=0 and replays retained history", async () => {
  const p = new Publisher({ bufferSize: 8 });
  p.publish([ev(1), ev(2)]);
  const { messages, send } = collect();
  await p.subscribe(send, undefined, 2, fakeLedger([]));
  const eventSeqs = messages
    .filter((m) => m.type === "event")
    .map((m) => (m as { seq: number }).seq);
  assert.deepEqual(eventSeqs, [1, 2]);
  assert.equal(messages[messages.length - 1]!.type, "caught_up");

  p.publish([ev(3)]);
  const live = messages.filter((m) => m.type === "event") as Array<
    Extract<ServerMessage, { type: "event" }>
  >;
  assert.deepEqual(
    live.map((m) => m.seq),
    [1, 2, 3],
  );
});

test("subscriber already at head gets hello and only subsequent events", async () => {
  const p = new Publisher({ bufferSize: 8 });
  p.publish([ev(1), ev(2)]);
  const { messages, send } = collect();
  await p.subscribe(send, 2, 2, fakeLedger([]));
  assert.equal(messages[0]!.type, "hello");
  p.publish([ev(3)]);
  const live = messages.filter((m) => m.type === "event") as Array<
    Extract<ServerMessage, { type: "event" }>
  >;
  assert.deepEqual(
    live.map((m) => m.seq),
    [3],
  );
});

test("publisher rejects non-contiguous publishes (seq invariant)", () => {
  const p = new Publisher();
  p.publish([ev(1)]);
  assert.throws(() => p.publish([ev(3)]), /seq gap/);
});

test("buffer evicts oldest and snapshot path covers the evicted range", async () => {
  const p = new Publisher({ bufferSize: 3 });
  p.publish([ev(1), ev(2), ev(3), ev(4), ev(5)]); // keeps 3,4,5
  assert.equal(p.oldestBufferedSeq, 3);

  p.setSnapshotProvider(() => ({ seq: 2, recvSeq: 2, book: emptyBook(2) }));
  const durable = [3, 4, 5].map((n) => ({
    seq: n,
    recvSeq: n,
    payload: accepted(n),
  }));

  const { messages, send } = collect();
  await p.subscribe(send, 1, 5, fakeLedger(durable));
  assert.equal(messages[0]!.type, "snapshot");
  const seqs = messages
    .filter((m) => m.type === "event")
    .map((m) => (m as { seq: number }).seq);
  assert.deepEqual(seqs, [3, 4, 5]);
});
