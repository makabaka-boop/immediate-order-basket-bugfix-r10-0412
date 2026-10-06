# matching-service

单一模拟市场的 TypeScript 撮合服务：内存价格-时间优先订单簿 + SQLite 事件账本 +
WebSocket 发布，三者围绕同一条**提交序号（commitSeq = 服务端接收序号 recvSeq）**
推进。

## 撮合规则

- **价格优先**：买高卖低优先成交；同价**先到先得**，以服务端接收序号 `recvSeq` 为准。
- **限价单（GTC）**挂单；**IOC** 立即成交、剩余撤销，从不进入订单簿。
- 成交价取**挂单方（maker）价格**；支持部分成交，未成交部分保留原排队位置。
- **取消**：移除挂单；重复取消返回 `not_found`，不产生事件。
- **改单**：
  - 改价或**增加**总量 → 失去原排队位置（以当前 recvSeq 重新排队）；
  - 同价且不增量（含减仓）→ 保留原位置；
  - 新价穿越对手盘 → 改单立即成为主动方撮合成交；
  - 新总量低于已成交量 → 拒绝，状态不变。

## 序号与一致性（核心设计）

- 每个请求按到达顺序分配单调 `recvSeq`（服务内部串行提交）。
- 撮合先在引擎中**试探性**进行；`ledger.commitRequest()` 在**一个 SQLite 事务**里
  原子写入 commit 行、事件行和幂等键行。事件序号是 `AUTOINCREMENT`，**永不复用**。
- **落盘失败绝不发布**：提交失败时丢弃试探引擎，从账本重放重建；失败的 recvSeq
  不属于历史，下次提交沿用该序号重试。发布器只在事务成功、`noteCommit` 之后推进。
- **重启重放不重新撮合**：重放器把 `trade` 事件当作既定事实，只调整成交量，
  因此不会再次生成已存在的成交；状态可由全量事件重建，或由快照 + 其后事件续建。
- **幂等键**：键与载荷哈希同事务持久化；重复键返回**原回执**（不占序号）；
  同键不同载荷直接拒绝（`REUSED_KEY`），绝不执行第二次。

## 订阅续读

客户端发送 `{"type":"subscribe","lastSeq":N}` 按事件序号续读：

1. 已在头部 → 只收后续实时事件；
2. 缺口在内存环形缓冲内 → 直接补发缓冲事件；
3. 缺口超出保留范围（或重启/裁剪之后）→ **先发对应快照**，再从账本补发快照点
   之后的全部事件，最后 `caught_up`。

快照按事件数周期性落盘，并保留最近若干份；更旧事件被裁剪，但订阅者始终能通过
快照恢复。

## 协议（JSON over WebSocket）

客户端：

```json
{ "type": "subscribe", "lastSeq": 12 }
{ "type": "submit", "key": "client-key",
  "request": { "kind": "place", "side": "buy", "price": 100, "qty": 5, "tif": "GTC" } }
{ "type": "submit",
  "request": { "kind": "cancel", "orderId": "o-3-3" } }
{ "type": "submit",
  "request": { "kind": "amend", "orderId": "o-3-3", "newPrice": 99, "newQty": 8 } }
```

服务端帧：`hello` / `caught_up` / `snapshot` / `event` / `receipt` / `error`。
`event` 形如 `{ "type":"event", "seq":N, "commitSeq":M, "event": <accepted|trade|cancelled|amended> }`。

错误码：`INVALID_REQUEST`、`REUSED_KEY`、`PERSISTENCE`、`BAD_MESSAGE`、`PROTOCOL`。

## 运行

```bash
npm install
npm run build          # 类型检查并输出 dist/
npm test               # 全部测试（28 个）
PORT=8080 DB_PATH=./market.db npm start
```

环境变量：`PORT`（默认 8080）、`DB_PATH`、`SNAPSHOT_EVERY`（默认 256 事件）、
`RETAIN_SNAPSHOTS`（默认 3）、`PUBLISHER_BUFFER`（默认 1024）。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `src/matching-engine.ts` | 价格-时间优先撮合、改单重排队、快照序列化、重放注水入口 |
| `src/ledger.ts` | SQLite 事件账本、原子提交、幂等键、快照与裁剪、故障注入 |
| `src/replay.ts` | 仅消费既定事件事实的恢复器（绝不重新撮合） |
| `src/publisher.ts` | 环形缓冲、按序号续读、超缺口先快照后事件 |
| `src/service.ts` | 编排：试探撮合 → 原子落盘 → 推进序号 → 发布；失败重建 |
| `src/server.ts` / `src/main.ts` | WebSocket 入口与进程启动 |
| `tests/reference-model.ts` | **独立小型撮合模型**（扁平数组、不同实现风格） |
| `tests/differential.test.ts` | 随机交错成交/IOC/取消/改单，与参考模型逐笔、逐盘口对账 |
| `tests/service.test.ts` | 幂等键、落盘失败不发布、交错成交/取消/失败、重启不重复成交、快照裁剪 |
| `tests/publisher.test.ts` | 缓冲内续读、超保留先快照、序号连续不变量 |
| `tests/ws.e2e.test.ts` | 真实 WebSocket 端到端：下单、成交流、键语义、重启续读 |

## 故障注入

`ledger.armFailures(n)` 可让接下来 n 次提交在 durable 边界抛出 `CommitFailureError`，
用于验证「不发布 + 重建 + 重试序号不丢」的行为（见 `tests/service.test.ts`）。


## Immediate order basket
WebSocket submit accepts `{kind:"basket",legs:[{kind:"place",side,price,qty,tif:"IOC",clientOrderId?},...]}` with 1..8 legs. Legs are evaluated in declared order against the residual book left by preceding legs. Every leg must immediately fill its whole quantity within its own limit price; otherwise the whole basket is rejected with a zero-based failedLeg, no per-leg fills and no order/trade/cancel events. Successful baskets follow ordinary price/time priority, execute at maker prices, and commit all legs at one recvSeq. Each trade ID is unique within and across baskets, with receipt fills matching published and persisted trade facts. A valid rejected basket still durably records its receipt and receive sequence. The idempotency key binds the complete ordered basket. Persistence failure leaves no book effects or published events. Restart, snapshots and subscriber replay preserve the same committed basket facts; ordinary place, cancel and amend remain available.
