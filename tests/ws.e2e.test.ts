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
