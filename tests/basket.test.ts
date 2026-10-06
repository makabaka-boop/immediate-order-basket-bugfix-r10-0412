// Basket (immediate all-or-nothing combination order) tests:
//   * legs execute in declared order against the residual book left by the
//     preceding legs, at maker prices, under ordinary price/time priority;
//   * a successful basket is ONE durable commit at one recvSeq; trade ids
//     are unique within and across baskets, and receipt fills correspond
//     1:1 to published/persisted trade facts;
//   * a rejected basket (any leg cannot fully fill within its limit)
//     produces no trades and no book changes, but still durably records
//     its receipt and receive sequence;
//   * persistence failure, same-key retry, snapshot recovery, restart and
//     subscriber replay all observe the same committed basket facts.

import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";
import { MatchingEngine } from "../src/matching-engine.js";
import { MatchingService } from "../src/service.js";
import type {
  BasketReceipt,
  MatchEvent,
  PlaceOrderRequest,
  PlaceReceipt,
  TradeEvent,
} from "../src/types.js";

function tempDb(): { path: string; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), "match-basket-"));
  return { path: join(dir, "market.db"), dir };
}

function leg(
  side: "buy" | "sell",
  price: number,
  qty: number,
  clientOrderId?: string,
): PlaceOrderRequest {
  return {
    kind: "place",
    side,
    price,
    qty,
    tif: "IOC",
    ...(clientOrderId !== undefined ? { clientOrderId } : {}),
  };
}

function gtc(side: "buy" | "sell", price: number, qty: number) {
  return { request: { kind: "place" as const, side, price, qty, tif: "GTC" as const } };
}

function tradesOf(events: MatchEvent[]): TradeEvent[] {
  return events.filter((e): e is TradeEvent => e.type === "trade");
}

// ---- Engine-level semantics ---------------------------------------------

test("basket: legs execute in order against residual liquidity at maker prices", () => {
  const e = new MatchingEngine();
  // Asks: 2@100 (seq1), 3@100 (seq2), 4@101 (seq3).
  e.process({ kind: "place", side: "sell", price: 100, qty: 2, tif: "GTC" }, 1);
  e.process({ kind: "place", side: "sell", price: 100, qty: 3, tif: "GTC" }, 2);
  e.process({ kind: "place", side: "sell", price: 101, qty: 4, tif: "GTC" }, 3);
  e.noteCommit(1);
  e.noteCommit(2);
  e.noteCommit(3);

  const out = e.process(
    {
      kind: "basket",
      legs: [leg("buy", 100, 3), leg("buy", 101, 4)],
    },
    4,
  );
  e.noteCommit(4);

  const receipt = out.receipt as BasketReceipt;
  assert.equal(receipt.status, "committed");
  assert.equal(receipt.legs.length, 2);
  assert.ok(receipt.legs.every((l) => l.status === "fully_filled"));

  // Leg 1 (limit 100): consumes 2@100(seq1) + 1@100(seq2) — price/time order.
  const leg1Fills = receipt.legs[0]!.fills;
  assert.deepEqual(
    leg1Fills.map((f) => [f.price, f.qty]),
    [
      [100, 2],
      [100, 1],
    ],
  );
  // Leg 2 (limit 101) sees the RESIDUAL book: 2@100 left, then 2@101.
  const leg2Fills = receipt.legs[1]!.fills;
  assert.deepEqual(
    leg2Fills.map((f) => [f.price, f.qty]),
    [
      [100, 2],
      [101, 2],
    ],
  );

  // All events carry the single basket recvSeq; trade ids are unique.
  assert.ok(out.events.every((ev) => ev.recvSeq === 4));
  const ids = tradesOf(out.events).map((t) => t.tradeId);
  assert.equal(new Set(ids).size, ids.length, "trade ids unique within basket");

  // Receipt fills correspond 1:1 to the emitted trade events.
  const receiptIds = receipt.legs.flatMap((l) => l.fills.map((f) => f.tradeId));
  assert.deepEqual(
    [...receiptIds].sort(),
    [...ids].sort(),
    "receipt fills match emitted trades",
  );

  // Residual book: 2@101 left.
  const view = e.view();
  assert.equal(view.asks.length, 1);
  assert.equal(view.asks[0]!.price, 101);
  assert.equal(view.asks[0]!.remainingQty, 2);
});

test("basket: rejection is all-or-nothing — no events, no book change", () => {
  const e = new MatchingEngine();
  e.process({ kind: "place", side: "sell", price: 100, qty: 5, tif: "GTC" }, 1);
  e.noteCommit(1);
  const before = e.view();

  const out = e.process(
    {
      kind: "basket",
      legs: [
        leg("buy", 100, 3), // would fill
        leg("buy", 100, 10), // only 2 would remain -> fails
        leg("sell", 90, 1), // never attempted
      ],
    },
    2,
  );
  e.noteCommit(2);

  assert.equal(out.events.length, 0, "rejected basket emits no events");
  const receipt = out.receipt as BasketReceipt;
  assert.equal(receipt.status, "rejected");
  assert.equal(receipt.failedLeg, 1);
  assert.equal(receipt.rejectReason, "insufficient immediate liquidity");
  assert.equal(receipt.legs.length, 3, "receipt covers the complete request");
  for (const l of receipt.legs) {
    assert.equal(l.status, "rejected");
    assert.equal(l.filledQty, 0);
    assert.equal(l.fills.length, 0, "no per-leg fills on rejection");
    assert.equal(l.remainingQty, l.submitQty);
  }

  // Book liquidity is byte-identical to before the basket (the commit
  // cursor itself advances: the rejection consumes its recvSeq).
  assert.deepEqual(
    { bids: e.view().bids, asks: e.view().asks },
    { bids: before.bids, asks: before.asks },
  );
});

test("basket: leg failing on liquidity its predecessor consumed rolls everything back", () => {
  const e = new MatchingEngine();
  e.process({ kind: "place", side: "sell", price: 50, qty: 4, tif: "GTC" }, 1);
  e.noteCommit(1);

  // Leg 1 wants 3 of the 4; leg 2 wants 2 but only 1 remains -> reject.
  const out = e.process(
    { kind: "basket", legs: [leg("buy", 50, 3), leg("buy", 50, 2)] },
    2,
  );
  e.noteCommit(2);
  assert.equal((out.receipt as BasketReceipt).status, "rejected");
  assert.equal((out.receipt as BasketReceipt).failedLeg, 1);
  assert.equal(out.events.length, 0);
  assert.equal(
    e.view().asks[0]!.remainingQty,
    4,
    "first leg's tentative consumption is rolled back",
  );
});

test("basket: leg limit price is enforced per leg", () => {
  const e = new MatchingEngine();
  e.process({ kind: "place", side: "sell", price: 100, qty: 5, tif: "GTC" }, 1);
  e.process({ kind: "place", side: "sell", price: 101, qty: 5, tif: "GTC" }, 2);
  e.noteCommit(1);
  e.noteCommit(2);

  // Leg limit 100 cannot take the 101 ask -> rejection, nothing happens.
  const out = e.process({ kind: "basket", legs: [leg("buy", 100, 8)] }, 3);
  e.noteCommit(3);
  assert.equal((out.receipt as BasketReceipt).status, "rejected");
  assert.equal(out.events.length, 0);
  assert.equal(e.view().asks[0]!.remainingQty, 5);
  assert.equal(e.view().asks[1]!.remainingQty, 5);
});

test("basket: IOC legs never rest and never trade against each other", () => {
  const e = new MatchingEngine();
  e.process({ kind: "place", side: "sell", price: 100, qty: 3, tif: "GTC" }, 1);
  e.noteCommit(1);

  // Buy leg fills against the book; the sell leg must NOT see the buy leg
  // (IOC never rests), so it finds no bid and the basket rejects.
  const out = e.process(
    { kind: "basket", legs: [leg("buy", 100, 3), leg("sell", 50, 1)] },
    2,
  );
  e.noteCommit(2);
  const receipt = out.receipt as BasketReceipt;
  assert.equal(receipt.status, "rejected");
  assert.equal(receipt.failedLeg, 1);
  assert.equal(out.events.length, 0);
  assert.equal(e.view().asks[0]!.remainingQty, 3);
  assert.equal(e.view().bids.length, 0);
});

// ---- Service-level durability & consistency ------------------------------

test("basket: success is one durable commit; ids unique across baskets", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  await svc.submit(gtc("sell", 100, 5)); // seq1
  await svc.submit(gtc("sell", 101, 5)); // seq2

  const published: Array<{ seq: number; commitSeq: number; event: MatchEvent }> = [];
  await svc.publisher.subscribe(
    (m) => {
      if (m.type === "event") {
        published.push({ seq: m.seq, commitSeq: m.commitSeq, event: m.event });
      }
    },
    0,
    svc.lastCommitSeq(),
    svc.ledger,
  );
  published.length = 0;

  const r1 = await svc.submit({
    key: "basket-1",
    request: { kind: "basket", legs: [leg("buy", 100, 2), leg("buy", 101, 3)] },
  });
  assert.ok(r1.ok);
  const rcpt1 = r1.receipt as BasketReceipt;
  assert.equal(rcpt1.status, "committed");

  // One commit: every published event of the basket shares commitSeq 3.
  const basketEvents = published.filter((p) => p.commitSeq === 3);
  assert.ok(basketEvents.length > 0);
  assert.equal(svc.lastCommitSeq(), 3);

  // Receipt fills correspond 1:1 to published trade facts (unique ids).
  const publishedTradeIds = basketEvents
    .filter((p) => p.event.type === "trade")
    .map((p) => (p.event as TradeEvent).tradeId);
  const receiptTradeIds = rcpt1.legs.flatMap((l) => l.fills.map((f) => f.tradeId));
  assert.deepEqual(
    [...receiptTradeIds].sort(),
    [...publishedTradeIds].sort(),
  );
  assert.equal(new Set(publishedTradeIds).size, publishedTradeIds.length);

  // Persisted events are exactly the published ones, in order.
  const stored = svc.ledger.loadEventsAfter(0).filter((r) => r.recvSeq === 3);
  assert.deepEqual(
    stored.map((s) => s.payload),
    basketEvents.map((b) => b.event),
  );

  // Second basket: trade ids unique also ACROSS baskets.
  const r2 = await svc.submit({
    key: "basket-2",
    request: { kind: "basket", legs: [leg("buy", 101, 2)] },
  });
  const rcpt2 = r2.receipt as BasketReceipt;
  assert.equal(rcpt2.status, "committed");
  const ids2 = rcpt2.legs.flatMap((l) => l.fills.map((f) => f.tradeId));
  for (const id of ids2) assert.ok(!receiptTradeIds.includes(id));
});

test("basket: rejection consumes a recvSeq, records receipt, changes nothing", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  await svc.submit(gtc("sell", 100, 5)); // seq1
  const published: MatchEvent[] = [];
  await svc.publisher.subscribe(
    (m) => {
      if (m.type === "event") published.push(m.event);
    },
    0,
    svc.lastCommitSeq(),
    svc.ledger,
  );
  published.length = 0;
  const eventsBefore = svc.ledger.maxEventSeq();
  const bookBefore = svc.book();

  const rejected = await svc.submit({
    key: "rb",
    request: { kind: "basket", legs: [leg("buy", 100, 3), leg("buy", 100, 9)] },
  });
  assert.ok(rejected.ok, "a valid rejected basket is still a processed request");
  const rcpt = rejected.receipt as BasketReceipt;
  assert.equal(rcpt.status, "rejected");
  assert.equal(rcpt.failedLeg, 1);

  // No trades, no book/orders change, nothing published...
  assert.equal(published.length, 0);
  assert.equal(svc.ledger.maxEventSeq(), eventsBefore);
  assert.deepEqual(
    { bids: svc.book().bids, asks: svc.book().asks },
    { bids: bookBefore.bids, asks: bookBefore.asks },
  );

  // ...but the receive sequence was durably consumed.
  assert.equal(svc.lastCommitSeq(), 2);
  const commits = svc.ledger.db
    .prepare(`SELECT recv_seq AS r, event_count AS c FROM commits ORDER BY recv_seq`)
    .all() as Array<{ r: number; c: number }>;
  assert.deepEqual(commits.map((c) => [c.r, c.c]), [
    [1, 1],
    [2, 0],
  ]);

  // Same-key retry returns the ORIGINAL rejection receipt, consuming nothing.
  const dup = await svc.submit({
    key: "rb",
    request: { kind: "basket", legs: [leg("buy", 100, 3), leg("buy", 100, 9)] },
  });
  assert.ok(dup.ok);
  assert.deepEqual(dup.receipt, rejected.receipt);
  assert.equal(svc.lastCommitSeq(), 2, "duplicate key consumes no sequence");

  // Same key, altered basket -> REUSED_KEY, never executed.
  const reused = await svc.submit({
    key: "rb",
    request: { kind: "basket", legs: [leg("buy", 100, 3)] },
  });
  assert.equal(reused.ok, false);
  assert.equal(reused.errorCode, "REUSED_KEY");
  assert.equal(svc.lastCommitSeq(), 2);
  assert.deepEqual(
    { bids: svc.book().bids, asks: svc.book().asks },
    { bids: bookBefore.bids, asks: bookBefore.asks },
  );
});

test("basket: restart after a rejected basket leaves no recvSeq gap", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path);

  await svc.submit(gtc("sell", 100, 5)); // seq1
  const rej = await svc.submit({
    key: "rb",
    request: { kind: "basket", legs: [leg("buy", 100, 10)] }, // seq2, rejected
  });
  assert.equal((rej.receipt as BasketReceipt).status, "rejected");
  assert.equal(svc.lastCommitSeq(), 2);
  svc.close();

  // Reopen: the zero-event commit must not wedge or skip the sequence.
  svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const next = await svc.submit(gtc("buy", 100, 2)); // must be seq3
  assert.ok(next.ok, "submit after restart succeeds");
  assert.equal((next.receipt as PlaceReceipt).status, "fully_filled");
  assert.equal(svc.lastCommitSeq(), 3);
  assert.equal(svc.book().asks[0]!.remainingQty, 3);

  // The persisted rejection receipt is still returned for the same key.
  const again = await svc.submit({
    key: "rb",
    request: { kind: "basket", legs: [leg("buy", 100, 10)] },
  });
  assert.ok(again.ok);
  assert.deepEqual(again.receipt, rej.receipt);
  assert.equal(svc.lastCommitSeq(), 3, "replayed receipt consumes no sequence");

  // Commits are contiguous: 1 (place), 2 (rejected basket), 3 (buy).
  const rows = svc.ledger.db
    .prepare(`SELECT recv_seq AS r FROM commits ORDER BY recv_seq`)
    .all() as Array<{ r: number }>;
  assert.deepEqual(
    rows.map((r) => r.r),
    [1, 2, 3],
  );
});

test("basket: persistence failure publishes nothing and retry is consistent", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  await svc.submit(gtc("sell", 100, 6)); // seq1
  const published: MatchEvent[] = [];
  await svc.publisher.subscribe(
    (m) => {
      if (m.type === "event") published.push(m.event);
    },
    0,
    svc.lastCommitSeq(),
    svc.ledger,
  );
  published.length = 0;

  const basketReq = {
    kind: "basket" as const,
    legs: [leg("buy", 100, 2), leg("buy", 100, 3)],
  };
  svc.ledger.armFailures(1);
  const failed = await svc.submit({ key: "bk", request: basketReq });
  assert.equal(failed.ok, false);
  assert.equal(failed.errorCode, "PERSISTENCE");

  // No book effect, no published event, no durable commit.
  assert.equal(published.length, 0);
  assert.equal(svc.book().asks[0]!.remainingQty, 6);
  assert.equal(svc.lastCommitSeq(), 1);

  // Retry with the same key: executes once, at the SAME recvSeq (2).
  const retry = await svc.submit({ key: "bk", request: basketReq });
  assert.ok(retry.ok);
  const rcpt = retry.receipt as BasketReceipt;
  assert.equal(rcpt.status, "committed");
  assert.equal(svc.lastCommitSeq(), 2);
  assert.equal(svc.book().asks[0]!.remainingQty, 1);
  const tradeRecvSeqs = tradesOf(published).map((t) => t.recvSeq);
  assert.ok(tradeRecvSeqs.every((s) => s === 2));

  // And the same key once more replays the stored committed receipt.
  const dup = await svc.submit({ key: "bk", request: basketReq });
  assert.deepEqual(dup.receipt, retry.receipt);
  assert.equal(svc.lastCommitSeq(), 2);
});

test("basket: restart and snapshot recovery preserve committed basket facts", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path, {
    snapshotEveryEvents: 4,
    retainSnapshots: 2,
  });

  await svc.submit(gtc("sell", 100, 5)); // seq1
  await svc.submit(gtc("sell", 101, 5)); // seq2
  const b1 = await svc.submit({
    key: "b1",
    request: { kind: "basket", legs: [leg("buy", 100, 2), leg("buy", 101, 2)] },
  }); // seq3
  assert.equal((b1.receipt as BasketReceipt).status, "committed");
  await svc.submit({
    key: "b2",
    request: { kind: "basket", legs: [leg("buy", 101, 10)] }, // rejected, seq4
  });
  await svc.submit(gtc("sell", 102, 4)); // seq5
  const bookBefore = svc.book();
  const tradeIdsBefore = svc.ledger
    .loadEventsAfter(0)
    .filter((r) => r.payload.type === "trade")
    .map((r) => (r.payload as TradeEvent).tradeId);
  const maxEventSeq = svc.ledger.maxEventSeq();
  svc.close();

  // Reopen (snapshot + tail replay): identical book, no regenerated trades.
  svc = MatchingService.open(path, {
    snapshotEveryEvents: 4,
    retainSnapshots: 2,
  });
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });
  assert.deepEqual(svc.book(), bookBefore);
  assert.equal(svc.ledger.maxEventSeq(), maxEventSeq, "replay adds no events");
  const tradeIdsAfter = svc.ledger
    .loadEventsAfter(0)
    .filter((r) => r.payload.type === "trade")
    .map((r) => (r.payload as TradeEvent).tradeId);
  assert.deepEqual(tradeIdsAfter, tradeIdsBefore);
  assert.equal(svc.lastCommitSeq(), 5);

  // Keyed basket receipts survive the restart exactly.
  const dup = await svc.submit({
    key: "b1",
    request: { kind: "basket", legs: [leg("buy", 100, 2), leg("buy", 101, 2)] },
  });
  assert.deepEqual(dup.receipt, b1.receipt);
});

test("basket: subscriber resync after restart replays basket events in order", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path, { publisherBufferSize: 2 });
  await svc.submit(gtc("sell", 100, 5)); // seq1
  await svc.submit({
    key: "b1",
    request: { kind: "basket", legs: [leg("buy", 100, 2), leg("buy", 100, 2)] },
  }); // seq2
  const head = svc.headEventSeq();
  svc.close();

  svc = MatchingService.open(path, { publisherBufferSize: 2 });
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const frames: Array<{ seq: number; commitSeq: number; event: MatchEvent }> = [];
  let sawSnapshot = false;
  await svc.publisher.subscribe(
    (m) => {
      if (m.type === "snapshot") sawSnapshot = true;
      if (m.type === "event") {
        frames.push({ seq: m.seq, commitSeq: m.commitSeq, event: m.event });
      }
    },
    0,
    svc.lastCommitSeq(),
    svc.ledger,
  );

  // Buffer too small for full history -> snapshot path or ledger replay;
  // either way the delivered event stream is contiguous and complete.
  const seqs = frames.map((f) => f.seq);
  for (let i = 1; i < seqs.length; i++) {
    assert.equal(seqs[i], seqs[i - 1]! + 1, "resent stream is contiguous");
  }
  assert.equal(seqs[seqs.length - 1], head);
  const basketTrades = frames.filter(
    (f) => f.commitSeq === 2 && f.event.type === "trade",
  );
  assert.equal(basketTrades.length, 2, "both basket trades resent");
  const ids = basketTrades.map((f) => (f.event as TradeEvent).tradeId);
  assert.equal(new Set(ids).size, 2, "resent trade ids are unique");
  assert.ok(sawSnapshot || frames.length === head);
});
