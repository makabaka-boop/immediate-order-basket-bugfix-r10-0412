// In-memory price-time priority matching engine for a single market.
//
// Priority rule: better price first (higher bid / lower ask); at the same
// price the order with the lower recvSeq wins. recvSeq is assigned by the
// service in server-receive order *before* processing.
//
// Requeue rule: an amend that changes the price, or increases the total
// quantity, drops the old queue position: the surviving order gets the
// current recvSeq as its priority. A price-preserving, non-size-increasing
// amend keeps the original recvSeq.
//
// The engine performs matching only on the live path (`process`). Recovery
// never re-runs matching: the replayer (`replay.ts`) reconstructs state by
// consuming already-persisted trade facts, so trades cannot be regenerated.

import { committedBasketReceipt, rejectedBasketReceipt } from "./basket.js";
import type {
  TradeRequest,
  BasketOrderRequest,
  AcceptedEvent,
  AmendOrderRequest,
  AmendReceipt,
  BookOrder,
  BookSnapshotData,
  BookView,
  CancelOrderRequest,
  CancelReceipt,
  CancelledEvent,
  MatchEvent,
  PlaceOrderRequest,
  PlaceReceipt,
  ProcessOutcome,
  RequestReceipt,
  RestingOrder,
  Side,
} from "./types.js";

interface PriceLevel {
  // FIFO queue ordered by ascending priority recvSeq.
  orders: RestingOrder[];
}

/**
 * Consistent copy of the mutable book structures. Every resting order is
 * cloned exactly once, so the identity sharing between the price-level
 * queues and the id index is preserved inside the copy.
 */
interface BookState {
  bids: Map<number, PriceLevel>;
  asks: Map<number, PriceLevel>;
  orders: Map<string, RestingOrder>;
}

let monotonicOrderId = 0;

function newOrderId(recvSeq: number): string {
  monotonicOrderId += 1;
  return `o-${recvSeq}-${monotonicOrderId.toString(36)}`;
}

export function isPositiveInt(v: unknown): v is number {
  return typeof v === "number" && Number.isSafeInteger(v) && v > 0;
}

/** Deterministic trade ids so each aggressor produces stable ids. */
function tradeId(aggressorSeq: number, counter: number): string {
  return `t-${aggressorSeq}-${counter}`;
}

interface HydrateState {
  order: RestingOrder;
  live: boolean; // true while the order rests in the book
}

export class MatchingEngine {
  private bids = new Map<number, PriceLevel>();
  private asks = new Map<number, PriceLevel>();
  private orders = new Map<string, RestingOrder>();
  private lastRecvSeq = 0;
  private committedSeq = 0;
  /**
   * Trade ordinal within the request currently being processed. Reset once
   * per recvSeq in `process`, so every trade id `t-<recvSeq>-<n>` is unique
   * within a commit (including all legs of a basket) and across commits.
   */
  private tradesThisSeq = 0;

  get lastCommitSeq(): number {
    return this.committedSeq;
  }

  get lastSeq(): number {
    return this.lastRecvSeq;
  }

  // ---- Validation ------------------------------------------------------

  /** Static (state-independent) validation. */
  validate(req: TradeRequest): string | null {
    if (req === null || typeof req !== "object")
      return "request must be an object";
    switch (req.kind) {
      case "basket": {
        if (
          !Array.isArray(req.legs) ||
          req.legs.length < 1 ||
          req.legs.length > 8
        )
          return "basket requires 1..8 legs";
        const ids = new Set<string>();
        for (const leg of req.legs) {
          if (!leg || leg.kind !== "place" || leg.tif !== "IOC")
            return "basket legs must be IOC orders";
          const error = this.validate(leg);
          if (error) return error;
          if (leg.clientOrderId !== undefined) {
            if (
              typeof leg.clientOrderId !== "string" ||
              !leg.clientOrderId ||
              ids.has(leg.clientOrderId)
            )
              return "invalid basket client identity";
            ids.add(leg.clientOrderId);
          }
        }
        return null;
      }
      case "place": {
        if (req.side !== "buy" && req.side !== "sell") return "invalid side";
        if (!isPositiveInt(req.price))
          return "price must be a positive integer";
        if (!isPositiveInt(req.qty)) return "qty must be a positive integer";
        if (req.tif !== "GTC" && req.tif !== "IOC") return "invalid tif";
        return null;
      }
      case "cancel":
        return typeof req.orderId === "string" && req.orderId.length > 0
          ? null
          : "orderId required";
      case "amend": {
        if (typeof req.orderId !== "string" || req.orderId.length === 0)
          return "orderId required";
        if (req.newPrice !== undefined && !isPositiveInt(req.newPrice))
          return "newPrice must be a positive integer";
        if (req.newQty !== undefined && !isPositiveInt(req.newQty))
          return "newQty must be a positive integer";
        if (req.newPrice === undefined && req.newQty === undefined)
          return "amend must change price or qty";
        return null;
      }
      default:
        return "unknown request kind";
    }
  }

  // ---- Live processing -------------------------------------------------

  /**
   * Apply a valid request at the given receive sequence. Must be called in
   * strictly ascending recvSeq. State-dependent failures (order not found,
   * shrinking below filled qty) produce a receipt but never throw.
   *
   * The returned events are *tentative* until the caller durably commits
   * them. On commit failure the service discards this engine and rebuilds
   * from the ledger (see replay.ts), so no un-durable effect ever leaks.
   */
  process(req: TradeRequest, recvSeq: number): ProcessOutcome {
    if (recvSeq !== this.lastRecvSeq + 1) {
      throw new Error(
        `recvSeq gap: expected ${this.lastRecvSeq + 1}, got ${recvSeq}`,
      );
    }
    this.lastRecvSeq = recvSeq;
    this.tradesThisSeq = 0;

    switch (req.kind) {
      case "basket":
        return this.basket(req, recvSeq);
      case "place":
        return this.place(req, recvSeq);
      case "cancel":
        return this.cancel(req, recvSeq);
      case "amend":
        return this.amend(req, recvSeq);
    }
  }

  /**
   * All-or-nothing immediate basket. Legs are evaluated in declared order,
   * each against the residual book left by the preceding legs. If every leg
   * fully fills within its own limit, the whole basket is one outcome at
   * this recvSeq. Otherwise the book is restored to its pre-basket state
   * and the outcome carries NO events: the rejection still consumes the
   * recvSeq and is durably committed by the caller (commit row + receipt),
   * but no trade/order event exists and nothing is published.
   */
  private basket(req: BasketOrderRequest, recvSeq: number): ProcessOutcome {
    const saved = this.saveBookState();
    const events: MatchEvent[] = [];
    const legs: PlaceReceipt[] = [];
    let failedLeg = -1;

    for (let i = 0; i < req.legs.length; i++) {
      const legReq = req.legs[i]!;
      if (failedLeg >= 0) {
        // Basket already failed: this leg is never attempted.
        legs.push(this.unattemptedLegReceipt(legReq, recvSeq));
        continue;
      }
      const outcome = this.place(legReq, recvSeq);
      const receipt = outcome.receipt as PlaceReceipt;
      legs.push(receipt);
      if (receipt.status === "fully_filled") {
        events.push(...outcome.events);
      } else {
        failedLeg = i;
      }
    }

    if (failedLeg >= 0) {
      // Roll back every tentative effect of the earlier legs: the makers
      // they consumed are restored exactly, and no event escapes.
      this.restoreBookState(saved);
      return { events: [], receipt: rejectedBasketReceipt(legs, failedLeg) };
    }
    return { events, receipt: committedBasketReceipt(legs) };
  }

  /** Receipt for a leg that was never matched because an earlier leg failed. */
  private unattemptedLegReceipt(
    req: PlaceOrderRequest,
    recvSeq: number,
  ): PlaceReceipt {
    return {
      kind: "place",
      orderId: newOrderId(recvSeq),
      ...(req.clientOrderId !== undefined
        ? { clientOrderId: req.clientOrderId }
        : {}),
      status: "rejected",
      side: req.side,
      price: req.price,
      submitQty: req.qty,
      filledQty: 0,
      remainingQty: req.qty,
      fills: [],
      rejectReason: "basket rejected before this leg was attempted",
    };
  }

  private place(req: PlaceOrderRequest, recvSeq: number): ProcessOutcome {
    const events: MatchEvent[] = [];
    const orderId = newOrderId(recvSeq);
    const order: RestingOrder = {
      id: orderId,
      ...(req.clientOrderId !== undefined
        ? { clientOrderId: req.clientOrderId }
        : {}),
      side: req.side,
      price: req.price,
      totalQty: req.qty,
      filledQty: 0,
      recvSeq,
    };

    const accepted: AcceptedEvent = {
      type: "accepted",
      orderId,
      ...(req.clientOrderId !== undefined
        ? { clientOrderId: req.clientOrderId }
        : {}),
      side: req.side,
      price: req.price,
      totalQty: req.qty,
      tif: req.tif,
      recvSeq,
    };
    events.push(accepted);

    const { fills } = this.matchAggressor(order, recvSeq, events);

    let status: PlaceReceipt["status"];
    if (order.filledQty >= order.totalQty) {
      status = "fully_filled";
    } else if (req.tif === "IOC") {
      status = order.filledQty > 0 ? "partially_filled" : "expired_ioc";
    } else {
      this.insertOrder(order);
      status = order.filledQty > 0 ? "partially_filled" : "resting";
    }

    const receipt: PlaceReceipt = {
      kind: "place",
      orderId,
      ...(req.clientOrderId !== undefined
        ? { clientOrderId: req.clientOrderId }
        : {}),
      status,
      side: req.side,
      price: req.price,
      submitQty: req.qty,
      filledQty: order.filledQty,
      remainingQty: order.totalQty - order.filledQty,
      fills,
    };
    return { events, receipt };
  }

  /**
   * Match an aggressor against the opposite book. Mutates resting orders
   * and appends trade events. Limit prices are enforced; trades execute at
   * the resting order's price (maker price). Trade ids draw from the
   * per-recvSeq counter, so consecutive aggressors of one request (basket
   * legs) never repeat an id.
   */
  private matchAggressor(
    aggressor: RestingOrder,
    recvSeq: number,
    events: MatchEvent[],
  ): { fills: PlaceReceipt["fills"] } {
    const fills: PlaceReceipt["fills"] = [];
    const opposite = aggressor.side === "buy" ? this.asks : this.bids;

    while (aggressor.filledQty < aggressor.totalQty) {
      const best = this.bestPrice(opposite, aggressor.side);
      if (best === null) break;
      // Buy aggressor hits asks: lowest ask; sell hits bids: highest bid.
      if (aggressor.side === "buy" && best > aggressor.price) break;
      if (aggressor.side === "sell" && best < aggressor.price) break;

      const level = opposite.get(best)!;
      while (
        level.orders.length > 0 &&
        aggressor.filledQty < aggressor.totalQty
      ) {
        const resting = level.orders[0]!;
        const avail = resting.totalQty - resting.filledQty;
        const want = aggressor.totalQty - aggressor.filledQty;
        const qty = Math.min(avail, want);

        resting.filledQty += qty;
        aggressor.filledQty += qty;
        this.tradesThisSeq += 1;
        const tid = tradeId(recvSeq, this.tradesThisSeq);

        events.push({
          type: "trade",
          tradeId: tid,
          recvSeq,
          makerOrderId: resting.id,
          takerOrderId: aggressor.id,
          makerSide: aggressor.side === "buy" ? "sell" : "buy",
          price: best,
          qty,
        });
        fills.push({
          tradeId: tid,
          counterOrderId: resting.id,
          price: best,
          qty,
        });

        if (resting.filledQty >= resting.totalQty) {
          level.orders.shift();
          this.orders.delete(resting.id);
        }
      }
      if (level.orders.length === 0) opposite.delete(best);
    }
    return { fills };
  }

  private cancel(req: CancelOrderRequest, recvSeq: number): ProcessOutcome {
    const order = this.orders.get(req.orderId);
    if (order === undefined) {
      const receipt: CancelReceipt = {
        kind: "cancel",
        status: "not_found",
        orderId: req.orderId,
      };
      return { events: [], receipt };
    }

    const remainingQty = order.totalQty - order.filledQty;
    this.removeOrder(order);

    const event: CancelledEvent = {
      type: "cancelled",
      orderId: order.id,
      recvSeq,
      remainingQty,
    };
    const receipt: CancelReceipt = {
      kind: "cancel",
      status: "cancelled",
      orderId: order.id,
      cancelledQty: remainingQty,
    };
    return { events: [event], receipt };
  }

  private amend(req: AmendOrderRequest, recvSeq: number): ProcessOutcome {
    const existing = this.orders.get(req.orderId);
    if (existing === undefined) {
      return {
        events: [],
        receipt: {
          kind: "amend",
          status: "not_found",
          orderId: req.orderId,
          filledQty: 0,
          remainingQty: 0,
          requeued: false,
          fills: [],
        },
      };
    }

    const oldPrice = existing.price;
    const newPrice = req.newPrice ?? existing.price;
    const newTotalQty = req.newQty ?? existing.totalQty;

    if (newTotalQty < existing.filledQty) {
      const receipt: AmendReceipt = {
        kind: "amend",
        status: "rejected",
        orderId: existing.id,
        newPrice,
        newTotalQty,
        filledQty: existing.filledQty,
        remainingQty: existing.totalQty - existing.filledQty,
        requeued: false,
        fills: [],
        rejectReason: `newQty ${newTotalQty} below filledQty ${existing.filledQty}`,
      };
      return { events: [], receipt };
    }

    const requeued = newPrice !== oldPrice || newTotalQty > existing.totalQty;

    // Detach and apply new terms; if the new price crosses, the amended
    // order becomes the aggressor.
    this.removeOrder(existing);
    existing.price = newPrice;
    existing.totalQty = newTotalQty;
    if (requeued) existing.recvSeq = recvSeq;

    const events: MatchEvent[] = [];
    const { fills } = this.matchAggressor(existing, recvSeq, events);

    let status: AmendReceipt["status"];
    if (existing.filledQty >= existing.totalQty) {
      this.orders.delete(existing.id);
      status = "fully_filled";
    } else {
      this.insertOrder(existing);
      status = "amended_resting";
    }

    // 'amended' describes the terminal state, hence after its trades.
    events.push({
      type: "amended",
      orderId: existing.id,
      recvSeq,
      oldPrice,
      newPrice,
      newTotalQty,
      filledQty: existing.filledQty,
      requeued,
    });

    const receipt: AmendReceipt = {
      kind: "amend",
      status,
      orderId: existing.id,
      newPrice,
      newTotalQty,
      filledQty: existing.filledQty,
      remainingQty: existing.totalQty - existing.filledQty,
      requeued,
      fills,
    };
    return { events, receipt };
  }

  // ---- Book structure --------------------------------------------------

  /**
   * Deep-copy the mutable book state so a tentative multi-leg execution can
   * be rolled back. Each resting order is cloned exactly once: the copy
   * keeps the same identity sharing between level queues and the id index
   * as the live structures.
   */
  private saveBookState(): BookState {
    const clones = new Map<string, RestingOrder>();
    const clone = (o: RestingOrder): RestingOrder => {
      let c = clones.get(o.id);
      if (c === undefined) {
        c = { ...o };
        clones.set(o.id, c);
      }
      return c;
    };
    const copySide = (
      src: Map<number, PriceLevel>,
    ): Map<number, PriceLevel> => {
      const dst = new Map<number, PriceLevel>();
      for (const [price, level] of src) {
        dst.set(price, { orders: level.orders.map(clone) });
      }
      return dst;
    };
    const orders = new Map<string, RestingOrder>();
    for (const [id, o] of this.orders) orders.set(id, clone(o));
    return { bids: copySide(this.bids), asks: copySide(this.asks), orders };
  }

  /** Restore a state produced by saveBookState, discarding tentative edits. */
  private restoreBookState(state: BookState): void {
    this.bids = state.bids;
    this.asks = state.asks;
    this.orders = state.orders;
  }

  private bookFor(side: Side): Map<number, PriceLevel> {
    return side === "buy" ? this.bids : this.asks;
  }

  private insertOrder(order: RestingOrder): void {
    const book = this.bookFor(order.side);
    let level = book.get(order.price);
    if (level === undefined) {
      level = { orders: [] };
      book.set(order.price, level);
    }
    // Maintain ascending priority seq (FIFO). Sorted insertion keeps this
    // correct for snapshot restore regardless of input order.
    let i = level.orders.length;
    while (i > 0 && level.orders[i - 1]!.recvSeq > order.recvSeq) i -= 1;
    level.orders.splice(i, 0, order);
    this.orders.set(order.id, order);
  }

  private removeOrder(order: RestingOrder): void {
    const book = this.bookFor(order.side);
    const level = book.get(order.price);
    if (level === undefined) return;
    const idx = level.orders.findIndex((o) => o.id === order.id);
    if (idx >= 0) level.orders.splice(idx, 1);
    if (level.orders.length === 0) book.delete(order.price);
    this.orders.delete(order.id);
  }

  private bestPrice(
    book: Map<number, PriceLevel>,
    takerSide: Side,
  ): number | null {
    if (book.size === 0) return null;
    return takerSide === "buy"
      ? Math.min(...book.keys()) // best ask
      : Math.max(...book.keys()); // best bid
  }

  // ---- Commit / snapshot ----------------------------------------------

  noteCommit(recvSeq: number): void {
    if (recvSeq !== this.committedSeq + 1) {
      throw new Error(
        `commit gap: expected ${this.committedSeq + 1}, got ${recvSeq}`,
      );
    }
    this.committedSeq = recvSeq;
  }

  /** Restore-side commit bookkeeping; see replay.ts. */
  setCommittedSeq(seq: number): void {
    this.committedSeq = seq;
  }

  /**
   * Restore-side receive high-water; see replay.ts. Commits that produce no
   * events (a rejected all-or-nothing basket, a not_found cancel, ...) leave
   * no event rows, so replaying events alone cannot reveal their recvSeq.
   * The ledger's commit high-water is the truth: the next receive sequence
   * must continue after it, otherwise a reused recvSeq would collide with
   * the commits primary key and wedge every future submit.
   */
  setRecvSeqHighWater(seq: number): void {
    this.lastRecvSeq = Math.max(this.lastRecvSeq, seq);
  }

  snapshot(snapshotSeq: number): BookSnapshotData {
    const resting: BookSnapshotData["resting"] = [];
    const collect = (level: PriceLevel) => {
      for (const o of level.orders) {
        resting.push(
          o.clientOrderId !== undefined
            ? {
                id: o.id,
                clientOrderId: o.clientOrderId,
                side: o.side,
                price: o.price,
                totalQty: o.totalQty,
                filledQty: o.filledQty,
                recvSeq: o.recvSeq,
              }
            : {
                id: o.id,
                side: o.side,
                price: o.price,
                totalQty: o.totalQty,
                filledQty: o.filledQty,
                recvSeq: o.recvSeq,
              },
        );
      }
    };
    for (const [, level] of this.bids) collect(level);
    for (const [, level] of this.asks) collect(level);
    return {
      version: 1,
      atRecvSeq: this.lastRecvSeq,
      snapshotSeq,
      resting,
    };
  }

  /**
   * Hydration entry used ONLY by the replayer. Orders are seeded from a
   * snapshot or reconstructed from accepted events; trade facts then just
   * mutate quantities. No matching ever happens here.
   */
  hydrateOrder(o: RestingOrder, live: boolean): void {
    this.hydrate.set(o.id, { order: o, live });
    this.lastRecvSeq = Math.max(this.lastRecvSeq, o.recvSeq);
    if (live) this.insertOrder(o);
  }

  protected hydrate = new Map<string, HydrateState>();

  hydrateAccepted(ev: AcceptedEvent): void {
    if (this.hydrate.has(ev.orderId)) return; // idempotent
    const o: RestingOrder = {
      id: ev.orderId,
      ...(ev.clientOrderId !== undefined
        ? { clientOrderId: ev.clientOrderId }
        : {}),
      side: ev.side,
      price: ev.price,
      totalQty: ev.totalQty,
      filledQty: 0,
      recvSeq: ev.recvSeq,
    };
    // Accepted first, then same-batch trades decide final remaining qty;
    // insertion into the book is finalized in hydrateCommitBoundary.
    this.hydrate.set(o.id, { order: o, live: false });
    this.lastRecvSeq = Math.max(this.lastRecvSeq, ev.recvSeq);
  }

  hydrateTrade(ev: Extract<MatchEvent, { type: "trade" }>): void {
    const maker = this.hydrate.get(ev.makerOrderId);
    const taker = this.hydrate.get(ev.takerOrderId);
    if (!maker || !taker)
      throw new Error(`replay: unknown order in trade ${ev.tradeId}`);
    maker.order.filledQty += ev.qty;
    taker.order.filledQty += ev.qty;
    // Fully filled makers leave the book; taker book membership is decided
    // at the commit boundary (IOC never rests, GTC rests iff quantity left).
    if (maker.order.filledQty >= maker.order.totalQty) {
      if (maker.live) this.removeOrder(maker.order);
      maker.live = false;
    }
  }

  hydrateCancelled(ev: Extract<MatchEvent, { type: "cancelled" }>): void {
    const h = this.hydrate.get(ev.orderId);
    if (!h) throw new Error(`replay: unknown cancelled order ${ev.orderId}`);
    if (h.live) this.removeOrder(h.order);
    h.live = false;
  }

  hydrateAmended(
    ev: Extract<MatchEvent, { type: "amended" }>,
    tif: TimeInForceKnown,
  ): void {
    const h = this.hydrate.get(ev.orderId);
    if (!h) throw new Error(`replay: unknown amended order ${ev.orderId}`);
    if (h.live) this.removeOrder(h.order);
    h.order.price = ev.newPrice;
    h.order.totalQty = ev.newTotalQty;
    h.order.filledQty = ev.filledQty;
    if (ev.requeued) h.order.recvSeq = ev.recvSeq;
    h.live = ev.filledQty < ev.newTotalQty;
    if (h.live) {
      // Amendments only apply to resting GTC orders.
      void tif;
      this.insertOrder(h.order);
    }
    this.lastRecvSeq = Math.max(this.lastRecvSeq, ev.recvSeq);
  }

  /**
   * Finalize a replay commit boundary (recvSeq). A newly accepted order
   * rests iff it is GTC and still has quantity left after the batch.
   */
  hydrateCommitBoundary(
    recvSeq: number,
    acceptedThisCommit: Map<string, TimeInForceKnown>,
  ): void {
    for (const [id, tif] of acceptedThisCommit) {
      const h = this.hydrate.get(id)!;
      if (!h.live && tif === "GTC" && h.order.filledQty < h.order.totalQty) {
        h.live = true;
        this.insertOrder(h.order);
      }
    }
    acceptedThisCommit.clear();
    this.lastRecvSeq = Math.max(this.lastRecvSeq, recvSeq);
  }

  /** Seed state directly from a snapshot point. */
  static fromSnapshot(snap: BookSnapshotData): MatchingEngine {
    const e = new MatchingEngine();
    const orders = snap.resting.map((r) => {
      const o: RestingOrder = {
        id: r.id,
        ...(r.clientOrderId !== undefined
          ? { clientOrderId: r.clientOrderId }
          : {}),
        side: r.side,
        price: r.price,
        totalQty: r.totalQty,
        filledQty: r.filledQty,
        recvSeq: r.recvSeq,
      };
      return o;
    });
    orders.sort((a, b) => a.recvSeq - b.recvSeq);
    for (const o of orders) {
      e.hydrate.set(o.id, { order: o, live: false });
      e.insertOrder(o);
      e.hydrate.get(o.id)!.live = true;
      e.lastRecvSeq = Math.max(e.lastRecvSeq, o.recvSeq);
    }
    monotonicOrderId = Math.max(
      monotonicOrderId,
      ...orders.map((o) => Number(o.id.split("-")[2] ?? 0)),
    );
    e.lastRecvSeq = Math.max(e.lastRecvSeq, snap.atRecvSeq);
    return e;
  }

  // ---- Views -----------------------------------------------------------

  view(): BookView {
    return {
      lastCommitSeq: this.committedSeq,
      bids: this.sideView(this.bids, "buy"),
      asks: this.sideView(this.asks, "sell"),
    };
  }

  private sideView(
    book: Map<number, PriceLevel>,
    side: Side,
  ): BookView["bids"] {
    const prices = [...book.keys()].sort((a, b) =>
      side === "buy" ? b - a : a - b,
    );
    return prices.map((price) => {
      const level = book.get(price)!;
      const orders: BookOrder[] = level.orders.map((o) => ({
        orderId: o.id,
        ...(o.clientOrderId !== undefined
          ? { clientOrderId: o.clientOrderId }
          : {}),
        side: o.side,
        price: o.price,
        remainingQty: o.totalQty - o.filledQty,
        prioritySeq: o.recvSeq,
      }));
      return {
        price,
        remainingQty: orders.reduce((s, o) => s + o.remainingQty, 0),
        orders,
      };
    });
  }

  getOrder(id: string): RestingOrder | undefined {
    return this.orders.get(id);
  }
}

type TimeInForceKnown = "GTC" | "IOC";
