import type { BasketReceipt, MatchEvent, PlaceReceipt } from "./types.js";
export function basketReceipt(legs: PlaceReceipt[]): BasketReceipt {
  const failedLeg = legs.findIndex((leg) => leg.status !== "fully_filled");
  return failedLeg < 0
    ? { kind: "basket", status: "committed", legs }
    : {
        kind: "basket",
        status: "rejected",
        legs,
        failedLeg,
        rejectReason: "insufficient immediate liquidity",
      };
}
export function labelLegEvents(
  events: MatchEvent[],
  receipt: PlaceReceipt,
  recvSeq: number,
  leg: number,
): void {
  void events;
  void receipt;
  void recvSeq;
  void leg;
}
