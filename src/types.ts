// Domain types for the single-market matching service.
//
// Sequencing model
// ----------------
// recvSeq  : server receive sequence. Assigned monotonically in receive
//            order to each processed request. Ties at the same price are
//            broken by the lower recvSeq ("server receive order wins").
// commitSeq: recvSeq of the latest request whose effects were durably
//            committed. The engine, SQLite ledger and WebSocket publisher
//            all advance on this number: nothing is published until the
//            commit for that seq is durable.
// eventSeq : 1-based global ordinal of a ledger event. One commit may
//            contain several events. Subscribers resume by this number;
//            it is consumed from SQLite AUTOINCREMENT and never reused.

export type Side = "buy" | "sell";
export type TimeInForce = "GTC" | "IOC";

export interface PlaceOrderRequest {
  kind: "place";
  clientOrderId?: string;
  side: Side;
  /** Positive integer price, quote units (integer tick). */
  price: number;
  /** Positive integer quantity in base units. */
  qty: number;
  tif: TimeInForce;
}

export interface CancelOrderRequest {
  kind: "cancel";
  orderId: string;
}

export interface AmendOrderRequest {
  kind: "amend";
  orderId: string;
  /** New price. Present and different => leaves the old queue position. */
  newPrice?: number;
  /**
   * New *total* quantity (including already filled). Present and greater
   * than the prior total quantity => loses the old queue position.
   */
  newQty?: number;
}

export interface BasketOrderRequest {
  kind: "basket";
  legs: PlaceOrderRequest[];
}
export interface BasketReceipt {
  kind: "basket";
  status: "committed" | "rejected";
  legs: PlaceReceipt[];
  failedLeg?: number;
  rejectReason?: string;
}
export type TradeRequest =
  | PlaceOrderRequest
  | CancelOrderRequest
  | AmendOrderRequest
  | BasketOrderRequest;

/** Client envelope: a request may carry an idempotency key. */
export interface SubmitEnvelope {
  key?: string;
  request: TradeRequest;
}

/** Resting order as stored inside the matching engine. */
export interface RestingOrder {
  id: string;
  clientOrderId?: string;
  side: Side;
  price: number;
  totalQty: number;
  filledQty: number;
  /** Priority sequence (recvSeq when enqueued / last requeued). */
  recvSeq: number;
}

export interface BookOrder {
  orderId: string;
  clientOrderId?: string;
  side: Side;
  price: number;
  remainingQty: number;
  prioritySeq: number;
}

export interface AcceptedEvent {
  type: "accepted";
  orderId: string;
  clientOrderId?: string;
  side: Side;
  price: number;
  totalQty: number;
  tif: TimeInForce;
  recvSeq: number;
}

export interface TradeEvent {
  type: "trade";
  tradeId: string;
  /** recvSeq of the aggressive request producing this trade. */
  recvSeq: number;
  makerOrderId: string;
  takerOrderId: string;
  makerSide: Side;
  price: number;
  qty: number;
}

export interface CancelledEvent {
  type: "cancelled";
  orderId: string;
  recvSeq: number;
  remainingQty: number;
}

export interface AmendedEvent {
  type: "amended";
  orderId: string;
  recvSeq: number;
  oldPrice: number;
  newPrice: number;
  newTotalQty: number;
  filledQty: number;
  /** True when the amendment made the order leave its prior queue slot. */
  requeued: boolean;
}

export type MatchEvent =
  | AcceptedEvent
  | TradeEvent
  | CancelledEvent
  | AmendedEvent;

export interface Fill {
  tradeId: string;
  counterOrderId: string;
  price: number;
  qty: number;
}

export interface PlaceReceipt {
  kind: "place";
  orderId: string;
  clientOrderId?: string;
  status:
    | "resting"
    | "fully_filled"
    | "partially_filled"
    | "expired_ioc"
    | "rejected";
  side: Side;
  price: number;
  submitQty: number;
  filledQty: number;
  remainingQty: number;
  fills: Fill[];
  rejectReason?: string;
}

export interface CancelReceipt {
  kind: "cancel";
  status: "cancelled" | "not_found" | "rejected";
  orderId: string;
  cancelledQty?: number;
  rejectReason?: string;
}

export interface AmendReceipt {
  kind: "amend";
  status: "amended_resting" | "fully_filled" | "not_found" | "rejected";
  orderId: string;
  newPrice?: number;
  newTotalQty?: number;
  filledQty: number;
  remainingQty: number;
  requeued: boolean;
  fills: Fill[];
  rejectReason?: string;
}

export type RequestReceipt =
  | PlaceReceipt
  | CancelReceipt
  | AmendReceipt
  | BasketReceipt;

/** Result of applying a request to an engine state. */
export interface ProcessOutcome {
  events: MatchEvent[];
  receipt: RequestReceipt;
}

/** Persistent snapshot of engine state, versioned for forward-compat. */
export interface BookSnapshotData {
  version: 1;
  /** Highest recvSeq reflected in this snapshot. */
  atRecvSeq: number;
  /** Event seq of the snapshot point (state is inclusive of that seq). */
  snapshotSeq: number;
  resting: Array<{
    id: string;
    clientOrderId?: string;
    side: Side;
    price: number;
    totalQty: number;
    filledQty: number;
    recvSeq: number;
  }>;
}

export interface BookLevelView {
  price: number;
  remainingQty: number;
  orders: BookOrder[];
}

export interface BookView {
  lastCommitSeq: number;
  bids: BookLevelView[];
  asks: BookLevelView[];
}

// ---- WebSocket wire protocol -------------------------------------------

export type ClientMessage =
  | { type: "subscribe"; lastSeq?: number }
  | { type: "submit"; key?: string; request: TradeRequest };

export interface WireSnapshot {
  type: "snapshot";
  seq: number;
  recvSeq: number;
  resync: boolean;
  book: BookView;
}

export interface WireEvent {
  type: "event";
  seq: number;
  commitSeq: number;
  event: MatchEvent;
}

export interface WireReceipt {
  type: "receipt";
  key?: string;
  receipt: RequestReceipt;
}

export interface WireError {
  type: "error";
  code:
    | "INVALID_REQUEST"
    | "REUSED_KEY"
    | "PERSISTENCE"
    | "BAD_MESSAGE"
    | "PROTOCOL";
  message: string;
  key?: string;
}

export interface WireNotice {
  type: "hello" | "caught_up";
  lastSeq: number;
  commitSeq: number;
}

export type ServerMessage =
  | WireSnapshot
  | WireEvent
  | WireReceipt
  | WireError
  | WireNotice;
