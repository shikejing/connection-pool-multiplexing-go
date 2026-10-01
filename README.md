# muxpool：客户端连接复用与请求多路复用

一个**纯本地、零外部依赖**的 Go 客户端连接池，在少量 TCP 连接上并发承载大量逻辑请求，
并解决连接复用、多路配对、背压、取消隔离、失效恢复与优雅关闭等问题。所有行为都可在
本机通过 `go test` 复现（内置一个可编排的 TCP 测试后端）。

## 解决的问题

同一后端每次请求都新建连接时，握手开销随并发升高成为瓶颈，连接数也没有上限。本库提供：

1. **连接生命周期**：建立、保活（Ping/Pong）、复用、按空闲时间与最大寿命回收。
2. **单连接多路复用**：一条连接上并发承载多个在途请求，按 `streamID` 精确配对请求/响应，
   支持**响应乱序**返回。
3. **容量上界与可区分策略**：单连接在途数有硬上限；超限时可配置「立即拒绝」或「有界排队」，
   两者以不同错误区分。
4. **失效一致性**：连接中途失效、被对端静默回收、半关闭、协议错位时，其上所有在途请求
   得到一致且可解释的结论；幂等请求可安全重试，非幂等请求绝不重放。
5. **背压与流控**：等待队列深度与等待时长均有上界；慢响应受读超时约束；慢写入受写超时约束。
6. **取消隔离**：取消单个请求只影响该流（尽力发 CANCEL 帧），不影响同连接上的其他请求。
7. **优雅关闭**：在途请求限期完成或被明确拒绝，不留下悬挂等待方，不泄漏连接与协程。
8. **可观测**：暴露连接数、空闲数、在途数、队列深度、复用命中、拒绝、超时、取消、重试、
   摘除、失效等指标，并支持按条件主动摘除异常连接。

## 快速开始

```go
import muxpool "github.com/shikejing/connection-pool-multiplexing-go"

client, err := muxpool.New(muxpool.Config{
    Dial: func() (muxpool.Conn, error) {
        return net.Dial("tcp", "127.0.0.1:9000")
    },
    MaxConns:    4,   // 物理连接数硬上限
    MaxInflight: 16,  // 单连接在途请求数硬上限
})
if err != nil { ... }
defer client.Close()

resp, err := client.Do(ctx, payload, &muxpool.RequestOptions{Idempotent: true})
```

`Dial` 返回的 `muxpool.Conn` 是标准的全双工流接口（`net.Conn` 天然满足），载荷 `[]byte`
对业务透明；本库在其之上承载私有二进制帧协议。

## 私有帧协议（本地实现，15 字节定长头）

```
magic(1=0x6D) | type(1) | streamID(8, BE) | flags(1) | length(4, BE) | payload(length)
```

- `type`：`request(1)` / `response(2)` / `cancel(3)` / `ping(4)` / `pong(5)`。
- `streamID`：由每条连接单调递增，请求与响应据此配对；乱序返回天然支持。
- `flags`（response）：`error(1<<0)`、`retryable(1<<1)`、`halfClose(1<<2)`。
- 载荷上限 16 MiB；magic/长度非法即判协议错位。

> 该协议仅用于在本地完整演示多路复用语义，不与任何外部系统互通。

## 关键语义

### 容量、排队与拒绝

- 连接选择顺序：空闲连接 → 任意仍有在途余量的活跃连接 → 新建（未超 `MaxConns`）。
- 连接数与在途数都到顶后：
  - `QueuePolicyReject`：`Do` **立即**返回 `ErrQueueFull`。
  - `QueuePolicyQueue`：进入 FIFO 等待队列，槽位释放时在同一临界区按 FIFO 移交，
    不存在「有空槽却有等待者」的窗口；队列满返回 `ErrQueueFull`，
    超过 `MaxWaitTimeout` 返回 `ErrWaitTimeout`，两者与 ctx 取消都可区分。

### 连接归还与复用安全性

- 连接归还前必须满足：不在半关闭、未带毒、在途数归零；否则不收为空闲。
- **带毒（dirty）连接永不复用**：出现非法 magic、未知帧类型、非取消流的未知响应等
  协议错位/残留数据情况时立即关闭，从台账摘除。
- 已取消流的迟到响应被安全丢弃（取消 ID 集合有界，超 `2*MaxInflight` 即清空，
  命中清空后的极端迟到响应会保守判毒，绝不带病复用）。

### 失效、半关闭与重试

- 对端 FIN/RST、读写错误、慢读取超时、保活探测超时 → 连接失效，其上所有**仍在途**的流
  收到同一个错误（`ErrConnClosed` 等）；已完成/已取消的流不被重复结论。
- 半关闭：响应帧带 `halfClose` 后该连接不再接新流，在途流照常完成，最后一个流结束即回收。
- 仅当 `RequestOptions.Idempotent=true` 且失败类型为「连接级失败」或「服务端显式标记
  retryable 的错误」时，才在 `MaxAttempts` 预算内重试；非幂等请求即使失败也不重放，
  避免副作用重复执行。

### 背压

- 在途总量上界：`MaxConns * MaxInflight`；等待总量上界：`MaxWaitQueue`。
- 慢响应：`ReadTimeout` 仅在存在在途流时生效（空闲连接靠保活探活，不会被读超时误杀）。
- 慢写入：`WriteTimeout` 约束单帧写出。
- 等待时长：`MaxWaitTimeout`（绝对截止点），与 ctx 截止共同取较早者。

### 取消隔离

`ctx` 取消时只终态化当前流并尽力发送 CANCEL；若响应恰好先到则以响应为准。
槽位通过「单一终态」原语（`finishStream` 恰好一次）释放，不会重复计数或影响其他流。

### 优雅关闭

- `Close()` / `Shutdown(0)`：立即停止接新，等待方收到 `ErrPoolClosed`，全部在途流立即失败，
  等待读循环/保活/回收协程退出，无连接与协程泄漏。
- `Shutdown(grace)`：先半关闭全部连接，在 `grace` 内等待在途自然完成；超时强制失败剩余流。
- 重复 `Close()` 幂等返回 `nil`；关闭后 `Do` 返回 `ErrPoolClosed`。

## 配置项（`muxpool.Config`）

| 配置 | 含义 | 默认/零值行为 |
| --- | --- | --- |
| `Dial` | 建立全新底层连接（必填） | — |
| `MaxConns` | 物理连接数硬上限 | `1` |
| `MaxInflight` | 单连接在途请求数硬上限 | `8` |
| `QueuePolicy` | `Reject` 立即拒绝 / `Queue` 有界排队 | `Reject` |
| `MaxWaitQueue` | 等待队列深度上界 | `MaxConns*MaxInflight` |
| `MaxWaitTimeout` | 等待槽位的时长上界 | `0` = 仅受 ctx 约束 |
| `IdleTimeout` | 空闲连接淘汰时间 | `0` = 不按空闲淘汰 |
| `MaxConnLife` | 连接最大寿命（到期半关闭） | `0` = 不限 |
| `DialTimeout` | 建连超时 | `0` = 不限 |
| `ReadTimeout` | 在途流单帧读取超时（慢响应背压） | `0` = 不限 |
| `WriteTimeout` | 单帧写出超时 | `0` = 不限 |
| `PingInterval` | 保活探测间隔 | `0` = 不主动保活 |
| `PingTimeout` | 保活响应等待超时 | 无 Ping 时默认 3s |
| `DefaultMaxAttempts` | 默认尝试次数（含首次） | `1` = 不重试 |

## 可观测指标（`client.Stats()`）

`Stats` 返回某一时刻的采样快照：

| 字段 | 含义 |
| --- | --- |
| `Conns` | 当前连接总数（含半关闭中） |
| `IdleConns` | 当前空闲可复用连接数 |
| `Inflight` | 当前在途逻辑请求总数 |
| `WaitQueueDepth` | 当前等待队列深度 |
| `TotalRequests` | 进入 `Do` 的累计请求数 |
| `ReusedHits` | 命中复用连接的次数 |
| `NewConns` / `DialsFailed` | 新建连接数 / 建连失败数 |
| `Rejected` | 因容量上界被拒绝次数（`ErrQueueFull`） |
| `Timeouts` | 等待超时/读超时次数 |
| `Canceled` | 调用方主动取消次数 |
| `Retries` | 重试次数 |
| `ConnEvictions` | 空闲/寿命/主动摘除的连接数 |
| `ConnFailures` | 连接失效次数（静默回收、IO 错误、保活失败、协议错位） |

主动摘除：`client.Evict(func(*ConnInfo) bool)`，可按连接 ID、年龄、空闲时长、在途数、
是否半关闭等条件摘除；无在途流的连接立即关闭，有在途流的转半关闭、待其完成后回收。

## 错误区分（`errors.Is` / `errors.As`）

| 错误 | 触发场景 |
| --- | --- |
| `ErrQueueFull` | 拒绝策略下无容量，或排队策略下队列已满 |
| `ErrWaitTimeout` | 在等待队列中超过 `MaxWaitTimeout` |
| `ErrConnClosed` | 连接已关闭或在请求过程中失效（对端回收/IO 错误等） |
| `ErrStreamCanceled` | 调用方取消/ctx 超时，仅作用于本流 |
| `ErrProtocol` | 协议错位，连接带毒且不可复用 |
| `ErrFrameTooLarge` | 帧载荷超过 16 MiB |
| `ErrServerRejected` + `*ServerError` | 服务端错误响应帧；`ServerError.Retryable` 标记可否重试 |
| `ErrPoolClosed` | 客户端已关闭 |
| `ErrShutdownTimeout` | 优雅关闭宽限期到期（`Shutdown` 返回值） |

## 测试

```bash
go test -race ./...        # 全部场景 + 竞态检测
go test ./... -count=10    # 稳定性
```

测试覆盖（内置可编排 TCP 后端，支持正常/延迟/乱序/挂起/执行后断连/静默回收/半关闭/
错误响应/不回 Pong 等模式）：

- 连接复用命中、单连接多路复用配对、响应乱序不串流；
- 在途上限拒绝、等待队列深度/超时上界、拒绝与超时可区分；
- 连接中途失效一致失败、幂等重试成功、非幂等不重放、服务端错误不重放；
- 取消隔离（取消一个流不影响同连接其他请求）；
- 半关闭、优雅关闭、宽限期强制关闭、关闭后拒绝新请求；
- 空闲淘汰、保活探活（FIN 与静默黑洞两条路径）、慢读取背压、空闲不误杀；
- 指标正确性、按条件主动摘除；
- 高并发下连接/在途/队列有界，以及反复建销后的协程不泄漏。
