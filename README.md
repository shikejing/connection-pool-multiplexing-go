# muxpool：客户端连接复用与请求多路复用

本地进程内的 TCP 连接池 + 多路复用库。同一后端不再每次请求新建连接，而是在单条连接上并发承载多个逻辑请求，请求与响应按请求标识配对。

## 快速开始

```go
srv := testserver.New(handler)       // 本地后端
srv.Listen()

cfg := muxpool.DefaultConfig()
cfg.MaxConns = 4                      // 最大连接数
cfg.MaxInFlightPerConn = 64           // 单连接最大在途请求
pool := muxpool.NewPool(cfg, muxpool.TCPDialer(srv.Addr()))
defer pool.Close()

client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())
resp, err := client.Do(context.Background(), []byte("hello"))
```

## 复用语义

- **连接建立**：`Acquire` 时按需拨号建立连接，受 `MaxConns` 上限约束。拨号中有专门的 `dialing` 计数，避免并发场景下重复建连。
- **复用**：优先复用已有连接。复用策略为"负载最高者优先"（在未达在途上限的连接中选在途数最多的），使部分连接保持空闲以便淘汰。
- **保活**：周期性发送 ping 帧，超时未响应则判定连接死亡并摘除。
- **回收**：连接归还后不关闭，留在池中供后续请求复用。归还时释放未消费的预留槽位。
- **空闲淘汰**：空闲数超过 `MaxIdleConns` 且空闲时长超过 `MaxIdleTime` 的连接被淘汰，但保留 `MinIdleConns` 条。
- **超龄淘汰**：存活时长超过 `MaxLifetime` 的连接被回收。
- **坏连接不复用**：出现协议错位（坏帧）、残留数据、保活失败、读错误的连接立即标记死亡，从池中摘除，不再复用。

### 在途上限与排队

- 同一连接上的在途请求数（含已预留未消费的槽位）不得超过 `MaxInFlightPerConn`。
- 连接数达到 `MaxConns` 且所有连接在途已满时，请求进入有界等待队列。
- 等待队列深度受 `WaitQueueDepth` 约束，超出时返回 `ErrQueueFull`（确定可区分）。
- 等待时长受 `WaitTimeout` 约束，超时返回 `ErrWaitTimeout`（确定可区分）。

### 预留机制

`Acquire` 成功后会在目标连接上预留一个槽位（`reserved` 计数），调用方随后调用 `Session.Do` 消费该槽位。这避免了 `Acquire` 与 `Do` 之间的竞态导致在途数超限。若 `Do` 未消费（如直接 `Release`），预留槽位自动释放。

## 多路复用

- 单条连接上并发承载多个逻辑请求，每个请求有唯一标识（`RequestID`）。
- 响应按标识正确配对，支持乱序返回（服务端可按任意顺序响应）。
- 帧格式：16 字节定长头（magic + version + flags + requestID + payloadLen）+ 变长载荷。
- 半关闭：`Session.CloseWrite()` 通知对端不再发送新请求，已有在途请求继续等待响应。
- 连接中途失效（读错误、EOF、对端悄悄回收）时，其上所有在途请求得到一致结论：未响应的请求返回 `ErrSessionDead`（可重试），已缓冲的响应正常投递。

## 重试语义

- 重试仅在请求幂等（`RetryPolicy.Idempotent=true`）时发生。
- 可重试的失败：会话失效（`ErrSessionDead`）、写入失败等未确认处理的场景。
- 不可重试的失败：服务端错误响应、请求取消、请求超时（服务端可能已处理，重放会导致副作用重复）。
- 退避采用指数退避 + 抖动，避免重试风暴。

## 并发配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MaxConns` | 64 | 最大连接（会话）数，0 表示不限制 |
| `MinIdleConns` | 1 | 最小空闲连接数，低于该值不主动淘汰 |
| `MaxIdleConns` | 16 | 最大空闲连接数，超出的空闲连接被淘汰 |
| `MaxIdleTime` | 5m | 空闲连接最长存活时间 |
| `MaxLifetime` | 30m | 连接最长存活时间 |
| `DialTimeout` | 5s | 建立连接的超时 |
| `MaxInFlightPerConn` | 64 | 单条连接最大在途请求数 |
| `WaitQueueDepth` | 256 | 等待队列深度，超出返回 `ErrQueueFull` |
| `WaitTimeout` | 30s | 等待可用连接的最长时间，超出返回 `ErrWaitTimeout` |
| `KeepAliveInterval` | 30s | 保活探测间隔，0 关闭保活 |
| `KeepAliveTimeout` | 5s | 保活探测超时 |
| `RequestTimeout` | 30s | 单个请求默认超时 |
| `CloseTimeout` | 10s | 优雅关闭时等待在途请求的最长时间 |
| `MaxFrameSize` | 1MiB | 单帧载荷最大字节数 |

## 可观测指标

`Pool.Stats()` 返回以下指标的原子快照：

| 指标 | 说明 |
|------|------|
| `Conns` | 当前连接总数 |
| `IdleConns` | 空闲连接数（无在途请求） |
| `ActiveConns` | 活跃连接数（有在途请求） |
| `InFlight` | 在途请求总数 |
| `WaitQueue` | 等待队列长度 |
| `ReuseHits` | 复用命中次数（复用已有连接发起请求） |
| `ReuseMisses` | 复用未命中次数（新建连接） |
| `RejectTotal` | 被拒绝的请求总数（队列满/等待超时） |
| `EvictedTotal` | 被淘汰的连接总数 |
| `DeadTotal` | 异常死亡的连接总数 |
| `RequestsTotal` | 请求总数 |
| `RetriesTotal` | 重试总数 |

所有指标通过原子操作累计，长时间高并发运行下不会无界增长。

## 主动摘除异常连接

`Pool.Remove(sess, reason)` 可按条件主动摘除异常会话：连接立即关闭且不再复用，其上在途请求得到一致失败结论。

## 优雅关闭

`Pool.Close()` / `Client.Close()` 执行优雅关闭：
1. 停止接受新请求，唤醒所有等待者（返回 `ErrPoolClosed`）。
2. 停止后台保活与淘汰任务。
3. 等待在途请求完成（受 `CloseTimeout` 约束），超时则强制关闭。
4. 不留下悬挂的等待方，不泄漏连接与协程。

## 测试

```bash
go test ./muxpool/ -v
```

覆盖场景：连接复用、多路复用配对（乱序响应）、连接失效（中途断连、非幂等不重放）、半关闭、背压（队列满、等待超时）、取消与超时、优雅关闭、主动摘除、协议错位不复用、空闲淘汰、保活探测、长时间高并发指标稳定。

## 示例

```bash
go run ./examples
```

启动本地后端，并发发起 32 个请求，展示复用、乱序配对与可观测指标。
