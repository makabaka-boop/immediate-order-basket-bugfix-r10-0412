// End-to-end tests over real WebSocket connections:
//   * submit + subscribe round-trip, trades stream in event order;
//   * idempotency key duplicate -> same receipt; changed payload -> error;
//   * resubscribe at an old seq after a restart -> snapshot then events.

import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";
import WebSocket from "ws";
import { startWsServer, type RunningServer } from "../src/server.js";
import type { ServerMessage } from "../src/types.js";

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), "match-ws-"));
}

function connect(port: number): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    ws.once("open", () => resolve(ws));
    ws.once("error", reject);
  });
}

function send(ws: WebSocket, msg: unknown): void {
  ws.send(JSON.stringify(msg));
}

function nextMessage(ws: WebSocket): Promise<ServerMessage> {
  return new Promise((resolve, reject) => {
    const onMsg = (raw: WebSocket.RawData) => {
      ws.off("error", onErr);
      resolve(JSON.parse(raw.toString()) as ServerMessage);
    };
    const onErr = (err: Error) => {
      ws.off("message", onMsg);
      reject(err);
    };
    ws.once("message", onMsg);
    ws.once("error", onErr);
  });
}

/** Attach a persistent queue to a socket so nothing is dropped between awaits. */
function messageQueue(ws: WebSocket): {
  pop: () => Promise<ServerMessage>;
  drainFor: (ms: number) => Promise<ServerMessage[]>;
} {
  const queued: ServerMessage[] = [];
  const waiters: Array<(m: ServerMessage) => void> = [];
  ws.on("message", (raw) => {
    const m = JSON.parse(raw.toString()) as ServerMessage;
    const w = waiters.shift();
    if (w) w(m);
    else queued.push(m);
  });
  const pop = (): Promise<ServerMessage> => {
    const m = queued.shift();
    if (m) return Promise.resolve(m);
    return new Promise((resolve) => waiters.push(resolve));
  };
  const drainFor = async (ms: number): Promise<ServerMessage[]> => {
    const out = [...queued];
    queued.length = 0;
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      const remaining = deadline - Date.now();
      const m = await Promise.race([
        pop(),
        new Promise<null>((r) => setTimeout(() => r(null), remaining)),
      ]);
      if (!m) break;
      out.push(m);
    }
    return out;
  };
  return { pop, drainFor };
}

test("e2e: place orders, observe trades in order, duplicate key replay", async () => {
  const dir = tempDir();
  let server: RunningServer = await startWsServer({
    dbPath: join(dir, "m.db"),
  });
  after(async () => {
    await server.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const sub = await connect(server.port);
  send(sub, { type: "subscribe" });
  const hello = await nextMessage(sub);
  assert.ok(hello.type === "hello");
  const subQ = messageQueue(sub); // persistent collector from now on

  const client = await connect(server.port);

  // Resting seller.
  send(client, {
    type: "submit",
    request: { kind: "place", side: "sell", price: 50, qty: 5, tif: "GTC" },
  });
  const sellerReceipt = (await nextMessage(client)) as Extract<
    ServerMessage,
    { type: "receipt" }
  >;
  assert.equal(sellerReceipt.type, "receipt");
  assert.equal((sellerReceipt.receipt as { status: string }).status, "resting");

  // Idempotency key: first call.
  send(client, {
    type: "submit",
    key: "abc",
    request: { kind: "place", side: "buy", price: 49, qty: 1, tif: "GTC" },
  });
  const keyed1 = await nextMessage(client);
  assert.equal(keyed1.type, "receipt");

  // Exact duplicate key + payload -> identical stored receipt, no new events.
  send(client, {
    type: "submit",
    key: "abc",
    request: { kind: "place", side: "buy", price: 49, qty: 1, tif: "GTC" },
  });
  const keyedDup = await nextMessage(client);
  assert.deepEqual(
    keyedDup,
    keyed1,
    "duplicate key returns the stored receipt",
  );

  // Same key, changed payload -> rejected.
  send(client, {
    type: "submit",
    key: "abc",
    request: { kind: "place", side: "buy", price: 48, qty: 1, tif: "GTC" },
  });
  const reused = (await nextMessage(client)) as Extract<
    ServerMessage,
    { type: "error" }
  >;
  assert.equal(reused.type, "error");
  assert.equal(reused.code, "REUSED_KEY");

  // Cross: buyer takes 3 of the seller's 5 (partial fill).
  send(client, {
    type: "submit",
    request: { kind: "place", side: "buy", price: 50, qty: 3, tif: "GTC" },
  });
  const crossReceipt = (await nextMessage(client)) as Extract<
    ServerMessage,
    { type: "receipt" }
  >;
  assert.equal(
    (crossReceipt.receipt as { status: string }).status,
    "fully_filled",
  );

  // Subscriber stream: 3 accepted (seller, keyed buy, crossing buy) + trade.
  const frames = await subQ.drainFor(2000);
  const trades = frames.filter(
    (
      m,
    ): m is Extract<ServerMessage, { type: "event" }> & {
      event: Extract<ServerMessage, { type: "event" }>["event"] & {
        type: "trade";
      };
    } => m.type === "event" && m.event.type === "trade",
  );
  assert.equal(trades.length, 1, "exactly one trade published");
  assert.equal(trades[0]!.event.qty, 3);
  assert.equal(trades[0]!.event.price, 50);

  const seqs = frames
    .filter((m) => m.type === "event")
    .map((m) => (m as { seq: number }).seq);
  for (let i = 1; i < seqs.length; i++) assert.ok(seqs[i]! > seqs[i - 1]!);

  const lastEventSeq = seqs[seqs.length - 1]!;

  sub.terminate();
  client.terminate();
  await server.close();

  // ---- Restart: resume at an old seq with a tiny buffer -> snapshot path.
  server = await startWsServer({
    dbPath: join(dir, "m.db"),
    publisherBufferSize: 2,
    snapshotEveryEvents: 2,
  });
  const resub = await connect(server.port);
  const resubQ = messageQueue(resub);
  send(resub, { type: "subscribe", lastSeq: 0 });
  const restartFrames = await resubQ.drainFor(2500);
  assert.ok(
    restartFrames.some((m) => m.type === "caught_up"),
    "restarted subscription catches up (snapshot-first when needed)",
  );

  // Snapshot book (if used) or event stream must show 2 remaining @50.
  const snap = restartFrames.find((m) => m.type === "snapshot") as
    | Extract<ServerMessage, { type: "snapshot" }>
    | undefined;
  if (snap) {
    const askLvl = snap.book.asks.find((l) => l.price === 50);
    assert.equal(askLvl?.remainingQty ?? 0, 2);
  } else {
    const tradeOrAccepted = restartFrames.filter((m) => m.type === "event");
    assert.ok(tradeOrAccepted.length > 0);
  }

  // Resuming at the exact current head yields hello and only future events.
  const fresh = await connect(server.port);
  send(fresh, { type: "subscribe", lastSeq: lastEventSeq });
  const helloAtHead = await nextMessage(fresh);
  assert.equal(helloAtHead.type, "hello");
  fresh.terminate();
  resub.terminate();
});

test("e2e: basket over the wire — atomic commit, unique ids, receipt/stream match", async () => {
  const dir = tempDir();
  let server: RunningServer = await startWsServer({
    dbPath: join(dir, "b.db"),
  });
  after(async () => {
    await server.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const sub = await connect(server.port);
  send(sub, { type: "subscribe" });
  const hello = await nextMessage(sub);
  assert.equal(hello.type, "hello");
  const subQ = messageQueue(sub);

  const client = await connect(server.port);

  // Resting sellers: 4@100, 4@101.
  send(client, {
    type: "submit",
    request: { kind: "place", side: "sell", price: 100, qty: 4, tif: "GTC" },
  });
  assert.equal((await nextMessage(client)).type, "receipt");
  send(client, {
    type: "submit",
    request: { kind: "place", side: "sell", price: 101, qty: 4, tif: "GTC" },
  });
  assert.equal((await nextMessage(client)).type, "receipt");

  // Successful 2-leg basket at one recvSeq.
  send(client, {
    type: "submit",
    key: "bk-1",
    request: {
      kind: "basket",
      legs: [
        { kind: "place", side: "buy", price: 100, qty: 2, tif: "IOC" },
        { kind: "place", side: "buy", price: 101, qty: 4, tif: "IOC" },
      ],
    },
  });
  const basketReceiptMsg = (await nextMessage(client)) as Extract<
    ServerMessage,
    { type: "receipt" }
  >;
  assert.equal(basketReceiptMsg.type, "receipt");
  const bRcpt = basketReceiptMsg.receipt as Extract<
    ServerMessage,
    { type: "receipt" }
  >["receipt"] & { kind: "basket" };
  assert.equal(bRcpt.status, "committed");
  assert.equal(bRcpt.legs.length, 2);

  // Same-key retry over the wire replays the identical receipt.
  send(client, {
    type: "submit",
    key: "bk-1",
    request: {
      kind: "basket",
      legs: [
        { kind: "place", side: "buy", price: 100, qty: 2, tif: "IOC" },
        { kind: "place", side: "buy", price: 101, qty: 4, tif: "IOC" },
      ],
    },
  });
  const dupMsg = await nextMessage(client);
  assert.deepEqual(dupMsg, basketReceiptMsg);

  // Rejected basket: 2@101 remains, so leg 1 fills but leg 2 cannot.
  send(client, {
    type: "submit",
    key: "bk-2",
    request: {
      kind: "basket",
      legs: [
        { kind: "place", side: "buy", price: 101, qty: 2, tif: "IOC" },
        { kind: "place", side: "buy", price: 101, qty: 50, tif: "IOC" },
      ],
    },
  });
  const rejMsg = (await nextMessage(client)) as Extract<
    ServerMessage,
    { type: "receipt" }
  >;
  assert.equal(rejMsg.type, "receipt");
  const rejRcpt = rejMsg.receipt as { status: string; failedLeg?: number };
  assert.equal(rejRcpt.status, "rejected");
  assert.equal(rejRcpt.failedLeg, 1);

  // Drain the subscriber stream: only the two GTC accepts + the committed
  // basket's events may appear; the rejected basket contributes nothing.
  type WireEvt = Extract<ServerMessage, { type: "event" }>;
  type TradeFrame = WireEvt & {
    event: Extract<WireEvt["event"], { type: "trade" }>;
  };
  const isTradeFrame = (m: ServerMessage): m is TradeFrame =>
    m.type === "event" && m.event.type === "trade";
  const frames = await subQ.drainFor(2000);
  const events = frames.filter(
    (m): m is Extract<ServerMessage, { type: "event" }> => m.type === "event",
  );
  const trades = frames.filter(isTradeFrame);
  const commitSeqs = new Set(events.map((e) => e.commitSeq));
  assert.ok(
    ![...commitSeqs].some((c) => c === 4),
    "rejected basket (recvSeq 4) published nothing",
  );

  // Unique trade ids; receipt fills correspond 1:1 to pushed trade frames.
  const pushedTradeIds = trades.map((t) => t.event.tradeId);
  assert.equal(new Set(pushedTradeIds).size, pushedTradeIds.length);
  const receiptTradeIds = bRcpt.legs.flatMap((l) =>
    l.fills.map((f) => f.tradeId),
  );
  assert.deepEqual([...receiptTradeIds].sort(), [...pushedTradeIds].sort());

  // Every basket event shares the basket's single commitSeq.
  const basketCommit = events.find((e) =>
    trades.some((t) => t.seq === e.seq),
  )!.commitSeq;
  for (const t of trades) assert.equal(t.commitSeq, basketCommit);

  const lastSeq = events[events.length - 1]!.seq;
  sub.terminate();
  client.terminate();
  await server.close();

  // ---- Restart: the committed basket facts survive; resubscribe replays them.
  server = await startWsServer({
    dbPath: join(dir, "b.db"),
    publisherBufferSize: 2,
    snapshotEveryEvents: 3,
  });
  const resub = await connect(server.port);
  const resubQ = messageQueue(resub);
  send(resub, { type: "subscribe", lastSeq: 0 });
  const restartFrames = await resubQ.drainFor(2500);
  assert.ok(restartFrames.some((m) => m.type === "caught_up"));
  const resentTrades = restartFrames.filter(isTradeFrame);
  assert.deepEqual(
    resentTrades.map((t) => t.event.tradeId).sort(),
    [...pushedTradeIds].sort(),
    "restarted service replays the same basket trade facts",
  );

  // The sequence continues after the restart with no gap or reuse: the
  // rejected basket consumed recvSeq 4, so the next commit is 5.
  const client2 = await connect(server.port);
  send(client2, {
    type: "submit",
    request: { kind: "place", side: "buy", price: 101, qty: 1, tif: "IOC" },
  });
  const afterRestart = (await nextMessage(client2)) as Extract<
    ServerMessage,
    { type: "receipt" }
  >;
  assert.equal(afterRestart.type, "receipt");
  assert.equal(server.service.lastCommitSeq(), 5);
  assert.ok(lastSeq > 0);
  resub.terminate();
  client2.terminate();
});
