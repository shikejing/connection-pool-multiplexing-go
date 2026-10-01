package muxpool

import "sync/atomic"

// Stats 连接池运行时可观测指标。
type Stats struct {
	Conns         int64 // 当前连接（会话）总数
	IdleConns     int64 // 空闲连接数
	ActiveConns   int64 // 有在途请求的连接数
	InFlight      int64 // 在途请求总数
	WaitQueue     int64 // 等待队列长度
	ReuseHits     int64 // 复用命中次数（复用已有连接发起请求）
	ReuseMisses   int64 // 复用未命中次数（新建连接）
	RejectTotal   int64 // 被拒绝的请求总数（队列满/等待超时/在途超限）
	EvictedTotal  int64 // 被淘汰的连接总数
	DeadTotal     int64 // 异常死亡的连接总数
	RequestsTotal int64 // 请求总数
	RetriesTotal  int64 // 重试总数
}

// statsAggregator 以原子方式累计指标，避免长时间运行下无界增长。
type statsAggregator struct {
	conns         atomic.Int64
	idleConns     atomic.Int64
	activeConns   atomic.Int64
	inFlight      atomic.Int64
	waitQueue     atomic.Int64
	reuseHits     atomic.Int64
	reuseMisses   atomic.Int64
	rejectTotal   atomic.Int64
	evictedTotal  atomic.Int64
	deadTotal     atomic.Int64
	requestsTotal atomic.Int64
	retriesTotal  atomic.Int64
}

func (s *statsAggregator) snapshot() Stats {
	return Stats{
		Conns:         s.conns.Load(),
		IdleConns:     s.idleConns.Load(),
		ActiveConns:   s.activeConns.Load(),
		InFlight:      s.inFlight.Load(),
		WaitQueue:     s.waitQueue.Load(),
		ReuseHits:     s.reuseHits.Load(),
		ReuseMisses:   s.reuseMisses.Load(),
		RejectTotal:   s.rejectTotal.Load(),
		EvictedTotal:  s.evictedTotal.Load(),
		DeadTotal:     s.deadTotal.Load(),
		RequestsTotal: s.requestsTotal.Load(),
		RetriesTotal:  s.retriesTotal.Load(),
	}
}
