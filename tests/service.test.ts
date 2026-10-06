// Tests for the orchestrating service + SQLite ledger:
//   * idempotency keys: duplicate returns original receipt; altered payload
//     with the same key is rejected;
//   * durability failure: no events published, engine rebuilt from ledger,
//     retry with the same recvSeq succeeds and ordering is intact;
//   * restart replay: committed trades are NOT regenerated (event ids do
//     not repeat; replaying the log reproduces book state without matching);
//   * snapshots + pruning: old events dropped, subscribers still resync.

import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";
import { Ledger } from "../src/ledger.js";
import { replayAll, replayFromSnapshot } from "../src/replay.js";
import { MatchingService } from "../src/service.js";
import type { MatchEvent, PlaceReceipt } from "../src/types.js";

function tempDb(): { path: string; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), "match-"));
  return { path: join(dir, "market.db"), dir };
}

function place(
  side: "buy" | "sell",
  price: number,
  qty: number,
  extra: { tif?: "GTC" | "IOC"; clientOrderId?: string; key?: string } = {},
) {
  return {
    ...(extra.key !== undefined ? { key: extra.key } : {}),
    request: {
      kind: "place" as const,
      side,
      price,
      qty,
      tif: extra.tif ?? "GTC",
      ...(extra.clientOrderId !== undefined
        ? { clientOrderId: extra.clientOrderId }
        : {}),
    },
  };
}

test("idempotency: duplicate key returns the original receipt", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const req = place("buy", 100, 5, { key: "k1" });
  const first = await svc.submit(req);
  assert.ok(first.ok);
  const firstReceipt = first.receipt as PlaceReceipt;
  assert.equal(firstReceipt.orderId.length > 0, true);

  // Exact duplicate envelope: same receipt object shape, same order id,
  // and NO extra commit/seq consumed.
  const dup = await svc.submit({ key: "k1", request: { ...req.request } });
  assert.ok(dup.ok);
  assert.deepEqual(dup.receipt, first.receipt);
  assert.equal(
    svc.lastCommitSeq(),
    1,
    "duplicate must not advance the commit seq",
  );
});

test("idempotency: same key, changed payload is rejected", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  await svc.submit(place("buy", 100, 5, { key: "k1" }));
  const changed = await svc.submit(place("buy", 101, 5, { key: "k1" }));
  assert.equal(changed.ok, false);
  assert.equal(changed.errorCode, "REUSED_KEY");
  assert.equal(svc.lastCommitSeq(), 1, "rejected reuse must not advance seq");

  // The book must reflect the original price (100), never 101.
  assert.equal(svc.book().bids[0]!.price, 100);
});

test("durability failure: no trade published; engine rebuilt; retry succeeds", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  // Seed a resting seller (committed seq 1).
  const seller = await svc.submit(place("sell", 50, 10));
  assert.ok(seller.ok);
  const published: Array<{
    seq: number;
    commitSeq: number;
    event: MatchEvent;
  }> = [];
  // In-process subscription with direct sink: catches publishes.
  await svc.publisher.subscribe(
    (msg) => {
      if (msg.type === "event") {
        published.push({
          seq: msg.seq,
          commitSeq: msg.commitSeq,
          event: msg.event,
        });
      }
    },
    0,
    svc.lastCommitSeq(),
    svc.ledger,
  );

  // Arm exactly one failure. The next request (aggressive buyer) matches
  // against the seller, but its commit must fail.
  svc.ledger.armFailures(1);
  const headBefore = svc.headEventSeq();
  const failed = await svc.submit(place("buy", 50, 4));
  assert.equal(failed.ok, false);
  assert.equal(failed.errorCode, "PERSISTENCE");
  assert.equal(failed.rebuilt, true);

  // Nothing from the failed request may have been published...
  assert.equal(
    svc.headEventSeq(),
    headBefore,
    "failed commit must not publish events",
  );
  assert.ok(
    !published.some((p) => p.commitSeq === 2),
    "no seq-2 events leaked",
  );

  // ...and the seller must still be fully intact in the rebuilt engine.
  assert.equal(
    svc.book().asks[0]!.remainingQty,
    10,
    "seller qty restored after rebuild",
  );

  // Retry: same recvSeq value (2), now durable, and its trade publishes.
  const retry = await svc.submit(place("buy", 50, 4));
  assert.ok(retry.ok, "retry after rebuild succeeds");
  assert.equal(svc.lastCommitSeq(), 2);
  assert.equal(svc.book().asks[0]!.remainingQty, 6);
  const trades = published.filter((p) => p.event.type === "trade");
  assert.equal(trades.length, 1, "exactly one trade exists post-retry");
  assert.equal(
    (trades[0]!.event as Extract<MatchEvent, { type: "trade" }>).qty,
    4,
  );
});

test("interleaved: fill -> fail -> cancel -> fill all agree with ledger truth", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  await svc.submit(place("sell", 10, 5)); // seq1
  await svc.submit(place("sell", 10, 5)); // seq2
  await svc.submit(place("buy", 10, 3)); // seq3 partial fill on seq1

  let asks = svc.book().asks[0]!;
  assert.equal(asks.remainingQty, 7); // maker1 has 2 left, maker2 5
  const maker1Id = asks.orders.find((o) => o.prioritySeq === 1)!.orderId;

  // Fail the next commit: a cancel of maker1 must NOT take effect durably.
  svc.ledger.armFailures(1);
  const failedCancel = await svc.submit({
    request: { kind: "cancel", orderId: maker1Id },
  });
  assert.equal(failedCancel.ok, false);
  assert.equal(failedCancel.errorCode, "PERSISTENCE");
  // maker1 still present after rebuild
  assert.ok(svc.book().asks[0]!.orders.some((o) => o.orderId === maker1Id));

  // Now cancel for real (reuses seq 4).
  const cancel = await svc.submit({
    request: { kind: "cancel", orderId: maker1Id },
  });
  assert.ok(cancel.ok);
  assert.equal((cancel.receipt as { cancelledQty: number }).cancelledQty, 2);

  // Next buy consumes maker2 fully.
  const final = await svc.submit(place("buy", 10, 5));
  assert.ok(final.ok);
  assert.equal((final.receipt as PlaceReceipt).status, "fully_filled");
  assert.equal(svc.book().asks.length, 0);

  // Ledger: commits 1..5, and trade events correspond exactly to
  // 3 + 5 = 8 units.
  const events = svc.ledger.loadEventsAfter(0);
  const tradeEvents = events.filter(
    (e) => e.payload.type === "trade",
  ) as Array<{
    payload: Extract<MatchEvent, { type: "trade" }>;
  }>;
  const totalQty = tradeEvents.reduce((s, e) => s + e.payload.qty, 0);
  assert.equal(totalQty, 8);
});

test("restart replay: trades are facts, never regenerated; book identical", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path);

  await svc.submit(place("sell", 20, 6));
  await svc.submit(place("sell", 21, 4));
  await svc.submit(place("buy", 21, 8)); // crosses both: 6@20 + 2@21
  await svc.submit(place("buy", 19, 3));
  const before = svc.book();
  const maxEventSeq = svc.ledger.maxEventSeq();
  const tradeIdsBefore = new Set(
    svc.ledger
      .loadEventsAfter(0)
      .filter((r) => r.payload.type === "trade")
      .map(
        (r) => (r.payload as Extract<MatchEvent, { type: "trade" }>).tradeId,
      ),
  );
  svc.close();

  // Reopen: replay reconstructs state without creating any new event rows.
  svc = MatchingService.open(path);
  after(() => {
    svc.close();
    rmSync(dir, { recursive: true, force: true });
  });

  assert.deepEqual(
    svc.book(),
    before,
    "book after replay equals book before restart",
  );
  assert.equal(
    svc.ledger.maxEventSeq(),
    maxEventSeq,
    "replay inserts no new events",
  );
  const tradeIdsAfter = new Set(
    svc.ledger
      .loadEventsAfter(0)
      .filter((r) => r.payload.type === "trade")
      .map(
        (r) => (r.payload as Extract<MatchEvent, { type: "trade" }>).tradeId,
      ),
  );
  assert.deepEqual([...tradeIdsAfter].sort(), [...tradeIdsBefore].sort());
  assert.equal(svc.lastCommitSeq(), 4);
});

test("snapshot: written at boundary; restart resumes from it", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path, {
    snapshotEveryEvents: 6,
    retainSnapshots: 2,
  });

  await svc.submit(place("sell", 100, 10)); // accepted
  await svc.submit(place("sell", 101, 10)); // accepted
  await svc.submit(place("buy", 101, 5)); // accepted+trade => 3 events -> snapshot soon
  await svc.submit(place("buy", 99, 4)); // accepted
  await svc.submit(place("sell", 100, 7)); // accepted => crosses buy99? no, sell100 vs buy99 no trade
  await svc.submit(place("sell", 99, 4)); // accepted + trade with buy99 (4) -> snapshot boundary

  const snap = svc.ledger.latestSnapshot();
  assert.ok(snap, "snapshot should have been written");
  const afterSnapBook = svc.book();

  // Continue beyond the snapshot.
  await svc.submit(place("buy", 100, 3));
  await svc.submit(place("buy", 101, 20)); // sweeps multiple makers
  const finalBook = svc.book();
  svc.close();

  svc = MatchingService.open(path, {
    snapshotEveryEvents: 6,
    retainSnapshots: 2,
  });
  assert.deepEqual(svc.book(), finalBook);
  assert.notDeepEqual(
    svc.book(),
    afterSnapBook,
    "more commits happened after snapshot",
  );
  svc.close();
  rmSync(dir, { recursive: true, force: true });
});

test("pruning: old events removed but snapshot-based replay still works", async () => {
  const { path, dir } = tempDb();
  let svc = MatchingService.open(path, {
    snapshotEveryEvents: 4,
    retainSnapshots: 2,
  });
  await svc.submit(place("sell", 10, 2));
  await svc.submit(place("buy", 10, 2)); // events -> may snapshot+prune
  await svc.submit(place("sell", 20, 2));
  await svc.submit(place("sell", 20, 2));
  await svc.submit(place("buy", 20, 2));
  await svc.submit(place("buy", 15, 2));
  await svc.submit(place("sell", 20, 2));
  await svc.submit(place("buy", 20, 2));
  const book = svc.book();
  const snap = svc.ledger.latestSnapshot();
  assert.ok(snap);
  svc.close();

  svc = MatchingService.open(path, {
    snapshotEveryEvents: 4,
    retainSnapshots: 2,
  });
  assert.deepEqual(svc.book(), book);
  svc.close();
  rmSync(dir, { recursive: true, force: true });
});

test("event seqs are AUTOINCREMENT monotonic across pruning", async () => {
  const { path, dir } = tempDb();
  const svc = MatchingService.open(path, {
    snapshotEveryEvents: 3,
    retainSnapshots: 1,
  });
  await svc.submit(place("sell", 1, 1));
  await svc.submit(place("buy", 1, 1));
  await svc.submit(place("sell", 2, 1));
  await svc.submit(place("buy", 2, 1));
  await svc.submit(place("sell", 3, 1));
  const ledger = svc.ledger;
  // sqlite_sequence holds the high-water mark regardless of deletes.
  const row = ledger.db
    .prepare(`SELECT seq FROM sqlite_sequence WHERE name='events'`)
    .get() as { seq: number } | undefined;
  assert.ok(row && row.seq >= 5, "event ids are never reused after pruning");
  svc.close();
  rmSync(dir, { recursive: true, force: true });
});

test("snapshot + replayFromSnapshot matches live engine state", async () => {
  const ledger = new Ledger(":memory:");
  // Build a history WITH a snapshot.
  const svc = MatchingService.recover(ledger, {
    snapshotEveryEvents: 2,
    retainSnapshots: 5,
  });
  // submit is serialized promises; await them.
  const mk = (side: "buy" | "sell", price: number, qty: number) =>
    svc.submit({ request: { kind: "place", side, price, qty, tif: "GTC" } });
  await mk("sell", 10, 3);
  await mk("buy", 10, 1);
  await mk("sell", 11, 2);
  await mk("buy", 11, 2);
  await mk("buy", 9, 5);

  const snap = ledger.latestSnapshot();
  assert.ok(snap, "snapshot exists");

  // Full replay (on a fresh ledger copy is hard in :memory:; instead,
  // compare replayFromSnapshot result with the live engine view).
  const rowsAfter = ledger.loadEventsAfter(snap!.eventSeq);
  const fromSnap = replayFromSnapshot(
    snap!.data,
    rowsAfter,
    ledger.maxCommitSeq(),
  );
  assert.deepEqual(fromSnap.engine.view(), svc.book());
  assert.equal(fromSnap.lastCommitSeq, ledger.maxCommitSeq());
});
