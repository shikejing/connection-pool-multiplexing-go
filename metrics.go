package muxpool

import "sync/atomic"

// Stats 是某一时刻的可观测快照（全部为单调计数器或瞬时量的采样值）。
type Stats struct {
	// Conns 当前连接总数（含半关闭中）。
	Conns int64
	// IdleConns 当前空闲且可复用的连接数。
	IdleConns int64
	// Inflight 当前在途逻辑请求总数。
	Inflight int64
	// WaitQueueDepth 当前等待队列深度。
	WaitQueueDepth int64
	// TotalRequests 进入 Do 的请求总数。
	TotalRequests int64
	// ReusedHits 命中已有空闲/复用连接的次数。
	ReusedHits int64
	// NewConns 新建连接的次数。
	NewConns int64
	// DialsFailed 建连失败次数。
	DialsFailed int64
	// Rejected 因容量上界被拒绝的次数（ErrQueueFull）。
	Rejected int64
	// Timeouts 等待超时或请求读超时次数。
	Timeouts int64
	// Canceled 调用方主动取消的次数。
	Canceled int64
	// Retries 发生重试的次数。
	Retries int64
	// ConnEvictions 空闲/寿命/主动摘除的连接数。
	ConnEvictions int64
	// ConnFailures 连接失效次数（含对端静默回收、协议错位、保活失败）。
	ConnFailures int64
}

type metrics struct {
	conns         atomic.Int64
	idleConns     atomic.Int64
	inflight      atomic.Int64
	waitQueue     atomic.Int64
	totalRequests atomic.Int64
	reusedHits    atomic.Int64
	newConns      atomic.Int64
	dialsFailed   atomic.Int64
	rejected      atomic.Int64
	timeouts      atomic.Int64
	canceled      atomic.Int64
	retries       atomic.Int64
	evictions     atomic.Int64
	connFailures  atomic.Int64
}

func newMetrics() *metrics { return &metrics{} }

func (m *metrics) snapshot() Stats {
	return Stats{
		Conns:          m.conns.Load(),
		IdleConns:      m.idleConns.Load(),
		Inflight:       m.inflight.Load(),
		WaitQueueDepth: m.waitQueue.Load(),
		TotalRequests:  m.totalRequests.Load(),
		ReusedHits:     m.reusedHits.Load(),
		NewConns:       m.newConns.Load(),
		DialsFailed:    m.dialsFailed.Load(),
		Rejected:       m.rejected.Load(),
		Timeouts:       m.timeouts.Load(),
		Canceled:       m.canceled.Load(),
		Retries:        m.retries.Load(),
		ConnEvictions:  m.evictions.Load(),
		ConnFailures:   m.connFailures.Load(),
	}
}
