import { strict as assert } from "node:assert";
import { test } from "node:test";
import { MatchingEngine } from "../src/matching-engine.js";
import type {
  AmendOrderRequest,
  PlaceOrderRequest,
  TradeRequest,
} from "../src/types.js";

function place(
  over: Partial<PlaceOrderRequest> &
    Pick<PlaceOrderRequest, "side" | "price" | "qty">,
): PlaceOrderRequest {
  return { kind: "place", tif: "GTC", ...over };
}

test("price-time priority: better price first, then receive seq", () => {
  const e = new MatchingEngine();

  // Two sells at 100 (seq1, seq2), one at 99 (seq3).
  const r1 = e.process(place({ side: "sell", price: 100, qty: 5 }), 1)
    .receipt as { orderId: string };
  e.process(place({ side: "sell", price: 100, qty: 7 }), 2);
  e.process(place({ side: "sell", price: 99, qty: 4 }), 3);

  // Aggressive buy at 100 for 10: hits 99 first (4), then seq1 (5), then 1 of seq2.
  const out = e.process(place({ side: "buy", price: 100, qty: 10 }), 4);
  e.noteCommit(1);
  e.noteCommit(2);
  e.noteCommit(3);
  e.noteCommit(4);
  assert.equal(out.events.filter((x) => x.type === "trade").length, 3);
  const trades = out.events.filter((x) => x.type === "trade") as Array<{
    price: number;
    qty: number;
    makerOrderId: string;
  }>;
  assert.deepEqual(
    trades.map((t) => [t.price, t.qty]),
    [
      [99, 4],
      [100, 5],
      [100, 1],
    ],
  );
  assert.equal(
    trades[1]!.makerOrderId,
    r1.orderId,
    "same price -> earlier recvSeq wins",
  );

  const view = e.view();
  assert.equal(view.asks[0]!.price, 100);
  assert.equal(view.asks[0]!.remainingQty, 6); // 7 - 1
});

test("partial fill: resting maker keeps queue position and remainder", () => {
  const e = new MatchingEngine();
  const maker = e.process(place({ side: "sell", price: 50, qty: 10 }), 1);
  e.noteCommit(1);
  const makerId = (maker.receipt as { orderId: string }).orderId;

  const buy = e.process(place({ side: "buy", price: 50, qty: 4 }), 2);
  e.noteCommit(2);
  assert.equal((buy.receipt as { filledQty: number }).filledQty, 4);
  assert.equal((buy.receipt as { remainingQty: number }).remainingQty, 0);

  // Another seller at same price queues behind the partially filled maker.
  e.process(place({ side: "sell", price: 50, qty: 10 }), 3);
  e.noteCommit(3);

  const buy2 = e.process(place({ side: "buy", price: 50, qty: 10 }), 4);
  e.noteCommit(4);
  const ts = buy2.events.filter((x) => x.type === "trade") as Array<{
    qty: number;
    makerOrderId: string;
  }>;
  assert.deepEqual(
    ts.map((t) => [t.makerOrderId, t.qty]),
    [
      [makerId, 6],
      [ts[1]!.makerOrderId, 4],
    ],
  );
});

test("IOC: immediate fill, remainder canceled, nothing rests", () => {
  const e = new MatchingEngine();
  e.process(place({ side: "sell", price: 10, qty: 3 }), 1);
  e.noteCommit(1);

  const partial = e.process(
    place({ side: "buy", price: 10, qty: 8, tif: "IOC" }),
    2,
  );
  e.noteCommit(2);
  const pr = partial.receipt as {
    status: string;
    filledQty: number;
    remainingQty: number;
  };
  assert.equal(pr.status, "partially_filled");
  assert.equal(pr.filledQty, 3);
  assert.equal(pr.remainingQty, 5);
  assert.equal(e.view().bids.length, 0, "IOC remainder must not rest");

  // No liquidity at all: expired.
  const expired = e.process(
    place({ side: "buy", price: 5, tif: "IOC", qty: 2 }),
    3,
  );
  e.noteCommit(3);
  assert.equal((expired.receipt as { status: string }).status, "expired_ioc");
});

test("amend keeps queue position when only shrinking quantity at same price", () => {
  const e = new MatchingEngine();
  const a = e.process(place({ side: "sell", price: 20, qty: 5 }), 1)
    .receipt as { orderId: string };
  const b = e.process(place({ side: "sell", price: 20, qty: 5 }), 2)
    .receipt as { orderId: string };
  e.noteCommit(1);
  e.noteCommit(2);

  const am: AmendOrderRequest = {
    kind: "amend",
    orderId: a.orderId,
    newQty: 4,
  };
  const out = e.process(am, 3);
  e.noteCommit(3);
  const ev = out.events.find((x) => x.type === "amended") as {
    requeued: boolean;
  };
  assert.equal(ev.requeued, false, "shrink at same price keeps position");

  const level = e.view().asks[0]!;
  assert.deepEqual(
    level.orders.map((o) => [o.orderId, o.prioritySeq]),
    [
      [a.orderId, 1],
      [b.orderId, 2],
    ],
  );
});

test("amend loses queue position on price change and on quantity increase", () => {
  const e = new MatchingEngine();
  const a = e.process(place({ side: "sell", price: 20, qty: 5 }), 1)
    .receipt as { orderId: string };
  e.process(place({ side: "sell", price: 20, qty: 5 }), 2);
  e.noteCommit(1);
  e.noteCommit(2);

  const inc = e.process({ kind: "amend", orderId: a.orderId, newQty: 9 }, 3);
  e.noteCommit(3);
  assert.equal(
    (inc.events.find((x) => x.type === "amended") as { requeued: boolean })
      .requeued,
    true,
  );
  const level = e.view().asks[0]!;
  assert.equal(
    level.orders[1]!.orderId,
    a.orderId,
    "a moved to the back of the level",
  );
  assert.equal(level.orders[1]!.prioritySeq, 3);
  assert.equal(level.orders[1]!.remainingQty, 9);
});

test("amend to a crossing price aggresses immediately", () => {
  const e = new MatchingEngine();
  const seller = e.process(place({ side: "sell", price: 30, qty: 4 }), 1)
    .receipt as { orderId: string };
  const buyer = e.process(place({ side: "buy", price: 20, qty: 6 }), 2)
    .receipt as { orderId: string };
  e.noteCommit(1);
  e.noteCommit(2);

  const out = e.process(
    { kind: "amend", orderId: buyer.orderId, newPrice: 30 },
    3,
  );
  e.noteCommit(3);
  const trades = out.events.filter((x) => x.type === "trade") as Array<{
    qty: number;
    price: number;
    makerOrderId: string;
    takerOrderId: string;
  }>;
  assert.equal(trades.length, 1);
  assert.equal(trades[0]!.qty, 4);
  assert.equal(
    trades[0]!.price,
    30,
    "trades print at the resting (maker) price",
  );
  assert.equal(trades[0]!.makerOrderId, seller.orderId);
  assert.equal(trades[0]!.takerOrderId, buyer.orderId);

  const receipt = out.receipt as { remainingQty: number; filledQty: number };
  assert.equal(receipt.filledQty, 4);
  assert.equal(receipt.remainingQty, 2);
});

test("cancel and cancel-again (not_found) semantics", () => {
  const e = new MatchingEngine();
  const a = e.process(place({ side: "buy", price: 1, qty: 7 }), 1).receipt as {
    orderId: string;
  };
  e.noteCommit(1);

  const ok = e.process({ kind: "cancel", orderId: a.orderId }, 2);
  e.noteCommit(2);
  assert.equal((ok.receipt as { status: string }).status, "cancelled");
  assert.equal((ok.receipt as { cancelledQty: number }).cancelledQty, 7);
  assert.equal(e.view().bids.length, 0);

  const again = e.process({ kind: "cancel", orderId: a.orderId }, 3);
  e.noteCommit(3);
  assert.equal((again.receipt as { status: string }).status, "not_found");
  assert.equal(again.events.length, 0, "not_found produces no ledger event");
});

test("amend below filled quantity is rejected with state unchanged", () => {
  const e = new MatchingEngine();
  const s = e.process(place({ side: "sell", price: 5, qty: 5 }), 1);
  e.noteCommit(1);
  const id = (s.receipt as { orderId: string }).orderId;
  e.process(place({ side: "buy", price: 5, qty: 3 }), 2);
  e.noteCommit(2);

  const out = e.process(
    { kind: "amend", orderId: id, newQty: 2 } as TradeRequest,
    3,
  );
  assert.equal((out.receipt as { status: string }).status, "rejected");
  assert.equal(out.events.length, 0);
  // order remains with original remaining 2
  assert.equal(e.getOrder(id)!.totalQty, 5);
});

test("engine rejects out-of-order recvSeq", () => {
  const e = new MatchingEngine();
  e.process(place({ side: "buy", price: 1, qty: 1 }), 1);
  assert.throws(() => e.process(place({ side: "buy", price: 1, qty: 1 }), 3));
});

test("view: levels aggregated and ordered correctly", () => {
  const e = new MatchingEngine();
  e.process(place({ side: "buy", price: 9, qty: 2 }), 1);
  e.process(place({ side: "buy", price: 10, qty: 3 }), 2);
  e.process(place({ side: "buy", price: 10, qty: 4 }), 3);
  e.process(place({ side: "sell", price: 12, qty: 1 }), 4);
  e.process(place({ side: "sell", price: 11, qty: 1 }), 5);
  for (let i = 1; i <= 5; i++) e.noteCommit(i);

  const v = e.view();
  assert.deepEqual(
    v.bids.map((l) => l.price),
    [10, 9],
  );
  assert.equal(v.bids[0]!.remainingQty, 7);
  assert.deepEqual(
    v.asks.map((l) => l.price),
    [11, 12],
  );
});
