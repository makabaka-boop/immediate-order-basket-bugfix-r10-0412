import { startWsServer } from "./server.js";

const port = Number(process.env.PORT ?? 8080);
const dbPath = process.env.DB_PATH ?? "./market.db";

const server = await startWsServer({
  dbPath,
  port,
  snapshotEveryEvents: Number(process.env.SNAPSHOT_EVERY ?? 256),
  retainSnapshots: Number(process.env.RETAIN_SNAPSHOTS ?? 3),
  publisherBufferSize: Number(process.env.PUBLISHER_BUFFER ?? 1024),
});

// eslint-disable-next-line no-console
console.log(
  JSON.stringify({
    ready: true,
    port: server.port,
    dbPath,
    lastCommitSeq: server.service.lastCommitSeq(),
  }),
);

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void server.close().then(() => process.exit(0));
  });
}
