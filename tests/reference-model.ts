// Independent miniature matching model used ONLY by tests to cross-check
// the production engine. Deliberately written in a different style
// (flat sorted arrays, no events, hand-rolled queues) so agreement is
// meaningful rather than a copy.
//
// Identities are the recvSeq at placement (a number); production order ids
// are `o-<recvSeq>-<n>`, so tests parse the prefix to bridge the two.

export interface RefOrder {
  id: number; // placement recvSeq
  side: "buy" | "sell";
  price: number;
  size: number; // REMAINING size
  priority: number; // queue priority (recvSeq of last enqueue)
}

export interface RefFill {
  taker: number; // aggressor placement seq
  maker: number; // resting placement seq
  price: number;
  qty: number;
}

export interface RefAmendResult {
  status: "resting" | "fully_filled" | "not_found" | "rejected";
  requeued: boolean;
  fills: RefFill[];
}

/** Orders flattened into a list; re-sorted whenever needed. */
export class ReferenceModel {
  private resting: RefOrder[] = [];
  private seq = 0;
  readonly fills: RefFill[] = [];

  /** Place; returns { id, status, fills }. `seq` is this request's
   * server-receive sequence (cancels/amends also consume one), so queue
   * priorities are directly comparable to production recvSeq. */
  place(
    input: {
      side: "buy" | "sell";
      price: number;
      qty: number;
      tif: "GTC" | "IOC";
    },
    seq: number,
  ): {
    id: number;
    status: "resting" | "fully_filled" | "partially_filled" | "expired_ioc";
    fills: RefFill[];
  } {
    this.seq = seq;
    const id = seq;
    const order: RefOrder = {
      id,
      side: input.side,
      price: input.price,
      size: input.qty,
      priority: id,
    };
    // Record the original total BEFORE sweeping so originalTotal is exact.
    this.totals.set(id, input.qty);
    const produced = this.sweep(order);

    let status: "resting" | "fully_filled" | "partially_filled" | "expired_ioc";
    if (order.size === 0) status = "fully_filled";
    else if (input.tif === "IOC")
      status = produced > 0 ? "partially_filled" : "expired_ioc";
    else {
      this.resting.push(order);
      status = produced > 0 ? "partially_filled" : "resting";
    }
    return { id, status, fills: this.lastFills(id) };
  }

  cancel(
    id: number,
    seq: number,
  ): { status: "cancelled" | "not_found"; qty: number } {
    this.seq = seq;
    const idx = this.resting.findIndex((o) => o.id === id);
    if (idx === -1) return { status: "not_found", qty: 0 };
    const [removed] = this.resting.splice(idx, 1);
    return { status: "cancelled", qty: removed!.size };
  }

  amend(
    id: number,
    change: { price?: number; qty?: number },
    seq: number,
  ): RefAmendResult {
    this.seq = seq;
    const idx = this.resting.findIndex((o) => o.id === id);
    if (idx === -1) return { status: "not_found", requeued: false, fills: [] };

    const order = this.resting[idx]!;
    const oldPrice = order.price;
    const oldTotal = this.originalTotal(id);
    const newPrice = change.price ?? oldPrice;
    const newTotal = change.qty ?? oldTotal;
    const filledBefore = this.filledTotal(id);

    if (newTotal < filledBefore) {
      return { status: "rejected", requeued: false, fills: [] };
    }

    const requeued = newPrice !== oldPrice || newTotal > oldTotal;
    // Remove from book; it becomes aggressor under new terms.
    this.resting.splice(idx, 1);
    order.price = newPrice;
    order.size = newTotal - filledBefore;
    if (requeued) order.priority = seq;

    const before = this.fills.length;
    this.sweep(order);
    const producedFills = this.fills.slice(before);
    let status: RefAmendResult["status"];
    if (order.size === 0) status = "fully_filled";
    else {
      this.resting.push(order);
      status = "resting";
    }
    this.recordTotals(id, newTotal);
    return { status, requeued, fills: producedFills };
  }

  /** Final book as bids (desc price) / asks (asc price), grouped by price. */
  book(): {
    bids: Array<{
      price: number;
      orders: Array<{ id: number; size: number; priority: number }>;
    }>;
    asks: Array<{
      price: number;
      orders: Array<{ id: number; size: number; priority: number }>;
    }>;
  } {
    const group = (side: "buy" | "sell") => {
      const orders = this.resting
        .filter((o) => o.side === side)
        .slice()
        .sort((a, b) =>
          a.price === b.price
            ? a.priority - b.priority
            : side === "buy"
              ? b.price - a.price
              : a.price - b.price,
        );
      const levels = new Map<
        number,
        Array<{ id: number; size: number; priority: number }>
      >();
      for (const o of orders) {
        const list = levels.get(o.price);
        const entry = { id: o.id, size: o.size, priority: o.priority };
        if (list) list.push(entry);
        else levels.set(o.price, [entry]);
      }
      return [...levels.entries()]
        .sort(([pa], [pb]) => (side === "buy" ? pb - pa : pa - pb))
        .map(([price, os]) => ({ price, orders: os }));
    };
    return { bids: group("buy"), asks: group("sell") };
  }

  // ---- internals -------------------------------------------------------

  private totals = new Map<number, number>();

  private originalTotal(id: number): number {
    return this.totals.get(id) ?? 0;
  }

  private recordTotals(id: number, total: number): void {
    this.totals.set(id, total);
  }

  /** Cumulative filled quantity for an order, from the fill log. */
  private filledTotal(id: number): number {
    let s = 0;
    for (const f of this.fills) {
      if (f.taker === id || f.maker === id) s += f.qty;
    }
    return s;
  }

  /**
   * Sweep the aggressor through the opposite book, mutating `aggressor.size`
   * down and removing exhausted makers. Returns number of fills produced.
   */
  private sweep(aggressor: RefOrder): number {
    let produced = 0;
    for (;;) {
      if (aggressor.size <= 0) break;
      // Candidates sorted best-price then earliest priority.
      const candidates = this.resting
        .filter((o) => o.side !== aggressor.side)
        .filter((o) =>
          aggressor.side === "buy"
            ? o.price <= aggressor.price
            : o.price >= aggressor.price,
        )
        .sort((a, b) =>
          a.price === b.price
            ? a.priority - b.priority
            : aggressor.side === "buy"
              ? a.price - b.price
              : b.price - a.price,
        );
      const maker = candidates[0];
      if (!maker) break;
      const qty = Math.min(maker.size, aggressor.size);
      maker.size -= qty;
      aggressor.size -= qty;
      this.fills.push({
        taker: aggressor.id,
        maker: maker.id,
        price: maker.price,
        qty,
      });
      produced += 1;
      if (maker.size === 0) {
        this.resting = this.resting.filter((o) => o !== maker);
      }
    }
    return produced;
  }

  private lastFills(taker: number): RefFill[] {
    return this.fills.filter((f) => f.taker === taker);
  }
}
