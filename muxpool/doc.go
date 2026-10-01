// Package muxpool 提供客户端连接复用与请求多路复用能力。
//
// 核心类型：
//   - Pool：管理一组可复用的多路复用会话，负责连接建立、保活、复用、回收、空闲淘汰与容量控制。
//   - Session：承载多路复用请求的单条连接，请求与响应按请求标识配对，支持乱序返回。
//   - Client：在 Pool 之上提供带幂等重试的请求入口。
//
// 并发模型：多个 goroutine 可同时调用同一 Session.Do；同一连接上的在途请求数受
// Config.MaxInFlightPerConn 约束；连接总数受 Config.MaxConns 约束；超出部分进入
// 有界等待队列（WaitQueueDepth），等待有时限（WaitTimeout），行为确定且可观测。
package muxpool
