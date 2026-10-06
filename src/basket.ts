import type { BasketReceipt, PlaceReceipt } from "./types.js";

/**
 * Committed basket: every leg fully filled within its limit. The per-leg
 * receipts (including their fills) are exactly what was committed to the
 * ledger and published, so receipt fills correspond 1:1 to trade facts.
 */
export function committedBasketReceipt(legs: PlaceReceipt[]): BasketReceipt {
  return { kind: "basket", status: "committed", legs };
}

/**
 * Rejected basket: all-or-nothing means NOTHING executed — no trades, no
 * book changes. The receipt still describes the complete request (one entry
 * per requested leg, in declared order) but every leg is reported with zero
 * fills, so the receipt never claims trades that were not committed.
 * `failedLeg` is the zero-based index of the first leg that could not fill
 * immediately within its limit.
 */
export function rejectedBasketReceipt(
  legs: PlaceReceipt[],
  failedLeg: number,
): BasketReceipt {
  return {
    kind: "basket",
    status: "rejected",
    legs: legs.map((leg, i) => ({
      ...leg,
      status: "rejected",
      filledQty: 0,
      remainingQty: leg.submitQty,
      fills: [],
      ...(i === failedLeg
        ? { rejectReason: "insufficient immediate liquidity" }
        : {}),
    })),
    failedLeg,
    rejectReason: "insufficient immediate liquidity",
  };
}
