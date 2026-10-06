# Chandy–Lamport 全局快照演示服务

三个逻辑节点持续交换演示筹码，运维人员可在**不暂停交换**的情况下取得包含在途筹码的全局快照（Chandy–Lamport 算法）。

## 架构

- **3 个节点**，每个节点一个独立消息循环 goroutine，余额私有——只有节点自己的循环能触碰，其他节点与协调器都无法直接读取。
- **6 条有向可靠 FIFO 通道**（0→1、1→0、0→2、2→0、1→2、2→1），进程内实现，无消息中间件。每条通道带**测试屏障**（hold/release）：暂缓投递但不丢不乱序。
- **发送侧**：扣款与入队在节点单次循环迭代内完成，是该节点的一个原子动作；**接收侧**：入账只发生在投递时。每笔转移有唯一编号 `tx-NNNNNN`。
- **标记规则**：节点首次记录本地状态后，先在所有出通道排入标记，才处理后续转移（同一循环迭代内完成，FIFO 保证标记不会被后续转移超车）；其余入通道记录"本地切面之后、该通道标记之前"收到的转移作为通道状态。
- **协调器**只负责：下发"开始"给发起节点、收集各节点上报的切面记录。它不冻结任何队列，也从不读取当前余额来补算结果。
- **一次只允许一个活动快照**（进行中再发起返回 409）；完成后可再次采集，标记携带快照编号，各次记录绝不混用。通道被屏障暂缓时查询返回 `running` 与已收报告数，绝不伪造完整结果。

## 运行

```bash
docker compose up --build        # 仅部署本实验服务
# 或本地：go run .
```

环境变量：`PORT`（默认 8080）、`INITIAL_BALANCE`（每节点初始筹码，默认 1000）、`TRAFFIC_INTERVAL_MS`（后台流量间隔，0 关闭）。

## API

| 方法/路径 | 说明 |
|---|---|
| `POST /transfers` | `{"from":0,"to":1,"amount":25}` 提交正整数转移 → `202` 返回 `txId`；非正数/自转/未知节点 `400`，余额不足 `409` |
| `POST /snapshots` | 发起快照 → `202` 返回 `snapshotId`；已有活动快照 `409` |
| `GET /snapshots/{id}` | 查询：`running`（含 `reportsReceived/Expected`）或 `complete`（含各节点余额、在途转移明细、一致性校验） |
| `GET /snapshots` | 列出所有快照 |
| `GET /debug/balances` | 实时余额（仅可观测性用途，快照绝不使用） |
| `GET /debug/channels` | 各通道队列深度与屏障状态 |
| `POST /debug/channels/{from}/{to}/hold` / `release` | 操作测试屏障 |

### 示例

```bash
curl -X POST localhost:8080/debug/channels/1/2/hold      # 暂缓 1→2
curl -X POST localhost:8080/snapshots                    # {"snapshotId":1,...}
curl localhost:8080/snapshots/1                          # {"status":"running","reportsReceived":2,...}
curl -X POST localhost:8080/debug/channels/1/2/release
curl localhost:8080/snapshots/1                          # complete：balances + inFlight，consistent:true
```

完整快照视图中：`sumBalances + sumInFlight == expectedTotal`（初始总额），`inFlight` 逐笔列出 `txId/from/to/amount`，可追溯每笔在途转移。

## 测试

```bash
go test -race ./...
```

- `TestSnapshotCapturesDebitedButNotReceived` — **已扣未收**：屏障拦住转移，快照将其捕获为在途；屏障未放时查询返回 `running`。
- `TestMarkerInterleavingWithControlledDelivery` — **标记穿插**：六通道全部屏障，按固定顺序逐条释放，精确复现预定切面（余额 90/80/95 + 在途 35 = 300）。
- `TestTwoConsecutiveSnapshotsDoNotMix` — **连续两次快照**：各自一致、编号独立、互不染指，首次记录事后不被篡改。
- `TestOnlyOneActiveSnapshot` / `TestTransferValidation` / `TestSnapshotUnderConcurrentTraffic` — 单活动快照、入参校验、并发流量下（`-race`）反复快照均一致。
