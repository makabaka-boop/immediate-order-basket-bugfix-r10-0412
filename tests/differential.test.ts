// Differential tests: run the same randomized request stream through the
// production MatchingEngine and the independent ReferenceModel, then
// compare (a) every fill and (b) the final resting book.
//
// The stream deliberately interleaves placements (incl. IOC), partial
// fills, cancels and amends (shrink keeps priority; increase / price
// change loses it; crossing amends aggress).

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { MatchingEngine } from "../src/matching-engine.js";
import { ReferenceModel, type RefFill } from "./reference-model.js";
import type { MatchEvent, TradeRequest } from "../src/types.js";

/** Mulberry32 tiny deterministic PRNG. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function parseId(orderId: string): number {
  return Number(orderId.split("-")[1]);
}

function clamp(n: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, n));
}

function engineFillsFor(events: MatchEvent[]): RefFill[] {
  return events
    .filter(
      (e): e is Extract<MatchEvent, { type: "trade" }> => e.type === "trade",
    )
    .map((e) => ({
      taker: parseId(e.takerOrderId),
      maker: parseId(e.makerOrderId),
      price: e.price,
      qty: e.qty,
    }));
}

function assertFillsEqual(
  label: string,
  got: RefFill[],
  want: RefFill[],
): void {
  assert.equal(got.length, want.length, `${label}: fill count`);
  for (let i = 0; i < want.length; i++) {
    assert.deepEqual(
      [got[i]!.maker, got[i]!.price, got[i]!.qty],
      [want[i]!.maker, want[i]!.price, want[i]!.qty],
      `${label}: fill ${i}`,
    );
  }
}

type ProdBook = ReturnType<MatchingEngine["view"]>;
type RefBook = ReturnType<ReferenceModel["book"]>;

function compareBooks(
  engine: MatchingEngine,
  ref: ReferenceModel,
  seed: number,
): void {
  const v: ProdBook = engine.view();
  const rb: RefBook = ref.book();

  const convert = (levels: ProdBook["bids"]) =>
    levels.map((l) => ({
      price: l.price,
      orders: l.orders.map((o) => ({
        id: parseId(o.orderId),
        size: o.remainingQty,
        priority: o.prioritySeq,
      })),
    }));

  assert.deepEqual(convert(v.bids), rb.bids, `seed ${seed}: bids differ`);
  assert.deepEqual(convert(v.asks), rb.asks, `seed ${seed}: asks differ`);
}

/** Run one deterministic stream; returns the final engine. */
function runStream(seed: number, steps: number): MatchingEngine {
  const rand = rng(seed);
  const engine = new MatchingEngine();
  const ref = new ReferenceModel();
  // placement-seq -> production order id string
  const ids = new Map<number, string>();
  // placement seqs currently resting according to the reference truth
  const live = new Set<number>();

  const chooseLive = (): number => {
    const list = [...live];
    return list[Math.floor(rand() * list.length)]!;
  };

  let seq = 0;
  for (let step = 0; step < steps; step++) {
    seq += 1;
    const roll = rand();
    let request: TradeRequest;

    if (roll < 0.62 || live.size === 0) {
      // ---- Place ----
      const side = rand() < 0.5 ? "buy" : "sell";
      const price = 90 + Math.floor(rand() * 21); // 90..110
      const qty = 1 + Math.floor(rand() * 8);
      const tif = rand() < 0.25 ? "IOC" : "GTC";
      request = { kind: "place", side, price, qty, tif };

      const refRes = ref.place({ side, price, qty, tif }, seq);
      const out = engine.process(request, seq);
      engine.noteCommit(seq);
      const receipt = out.receipt as { orderId: string; status: string };
      ids.set(refRes.id, receipt.orderId);
      assert.equal(
        parseId(receipt.orderId),
        refRes.id,
        `seed ${seed} step ${step}: id`,
      );
      assert.equal(
        receipt.status,
        refRes.status,
        `seed ${seed} step ${step}: place status`,
      );
      assertFillsEqual(
        `seed ${seed} step ${step}`,
        engineFillsFor(out.events),
        refRes.fills,
      );

      if (refRes.status === "resting" || refRes.status === "partially_filled") {
        live.add(refRes.id);
      }
      for (const f of refRes.fills) {
        if (!refStillHas(ref, f.maker)) live.delete(f.maker);
      }
    } else if (roll < 0.8) {
      // ---- Cancel ----
      const target = chooseLive();
      request = { kind: "cancel", orderId: ids.get(target)! };

      const refRes = ref.cancel(target, seq);
      const out = engine.process(request, seq);
      engine.noteCommit(seq);
      const receipt = out.receipt as { status: string; cancelledQty?: number };
      assert.equal(
        receipt.status,
        refRes.status,
        `seed ${seed} step ${step}: cancel status`,
      );
      if (refRes.status === "cancelled") {
        assert.equal(receipt.cancelledQty, refRes.qty);
        live.delete(target);
      }
    } else {
      // ---- Amend ----
      const target = chooseLive();
      const changePrice = rand() < 0.6;
      const drift = (rand() < 0.5 ? -1 : 1) * (1 + Math.floor(rand() * 4));
      const amend: TradeRequest = changePrice
        ? {
            kind: "amend",
            orderId: ids.get(target)!,
            newPrice: clamp(95 + drift + Math.floor(rand() * 11), 85, 115),
          }
        : {
            kind: "amend",
            orderId: ids.get(target)!,
            newQty: 1 + Math.floor(rand() * 14),
          };

      const refChange =
        "newPrice" in amend && amend.newPrice !== undefined
          ? { price: amend.newPrice }
          : { qty: (amend as { newQty: number }).newQty };
      const refRes = ref.amend(target, refChange, seq);

      const out = engine.process(amend, seq);
      engine.noteCommit(seq);
      assertFillsEqual(
        `seed ${seed} amend step ${step}`,
        engineFillsFor(out.events),
        refRes.fills,
      );
      const receipt = out.receipt as {
        status: string;
        requeued?: boolean;
      };

      if (refRes.status === "rejected") {
        assert.equal(receipt.status, "rejected");
      } else if (refRes.status === "fully_filled") {
        assert.equal(receipt.status, "fully_filled");
        live.delete(target);
      } else if (refRes.status === "not_found") {
        assert.equal(receipt.status, "not_found");
      } else {
        assert.equal(receipt.status, "amended_resting");
        assert.equal(receipt.requeued, refRes.requeued, "requeue flag");
        live.add(target);
        for (const f of refRes.fills) {
          if (!refStillHas(ref, f.maker)) live.delete(f.maker);
        }
      }
    }
  }

  compareBooks(engine, ref, seed);
  return engine;
}

function refStillHas(ref: ReferenceModel, id: number): boolean {
  const b = ref.book();
  return [...b.bids, ...b.asks].some((lvl) =>
    lvl.orders.some((o) => o.id === id),
  );
}

test("differential: 60 randomized streams match the independent model", () => {
  for (let seed = 1; seed <= 60; seed++) {
    runStream(seed, 150);
  }
});

test("differential: stress with amend-heavy seed", () => {
  // Seed 7 chosen by inspection-free confidence: loops run regardless.
  const engine = runStream(42, 400);
  assert.ok(engine.view().lastCommitSeq === 400);
});
