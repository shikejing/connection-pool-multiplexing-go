package muxpool

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Dialer 建立一条到后端的连接。
type Dialer func(ctx context.Context) (net.Conn, error)

// waiter 是等待可用容量的请求者。
type waiter struct {
	ch chan struct{}
}

// Pool 是多路复用连接池：管理一组可复用的会话，
// 负责连接建立、保活、复用、回收、空闲淘汰与容量控制。
type Pool struct {
	cfg    Config
	dialer Dialer

	mu       sync.Mutex
	sessions []*Session
	closed   bool

	waitMu  sync.Mutex
	waiters []*waiter

	dialing int // 正在拨号中的连接数（避免并发重复创建）

	stats statsAggregator

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewPool 创建连接池。dialer 为 nil 时使用默认 TCP 拨号器。
func NewPool(cfg Config, dialer Dialer) *Pool {
	cfg.normalize()
	if dialer == nil {
		dialer = defaultDialer
	}
	p := &Pool{
		cfg:    cfg,
		dialer: dialer,
		stopCh: make(chan struct{}),
	}
	p.wg.Add(2)
	go p.runEviction()
	go p.runKeepAlive()
	return p
}

// Acquire 获取一个可承载请求的会话。
// 优先复用已有会话；无空闲容量时按等待队列策略排队或拒绝。
func (p *Pool) Acquire(ctx context.Context) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if s := p.tryReuseLocked(); s != nil {
		p.stats.reuseHits.Add(1)
		p.mu.Unlock()
		return s, nil
	}
	if p.canCreateLocked() {
		p.mu.Unlock()
		return p.dialAndAdd(ctx)
	}
	p.mu.Unlock()
	return p.enqueueWaiter(ctx)
}

// Release 释放一个会话引用，使其回到池中供复用（不关闭连接）。
// 未被 Do 消费的预留槽位在此释放。
func (p *Pool) Release(s *Session) {
	if s == nil {
		return
	}
	s.unreserve()
	p.signalWaiter()
}

// Remove 主动摘除异常会话，连接立即关闭且不再复用。
func (p *Pool) Remove(s *Session, reason error) {
	if s == nil {
		return
	}
	p.mu.Lock()
	for i, sess := range p.sessions {
		if sess == s {
			p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
			p.stats.conns.Add(-1)
			break
		}
	}
	p.mu.Unlock()
	p.stats.deadTotal.Add(1)
	s.closeNow(reason)
	p.signalWaiter()
}

// Close 优雅关闭连接池：在途请求要么完成要么被明确拒绝，不悬挂等待方。
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessions := p.sessions
	p.sessions = nil
	p.mu.Unlock()

	// 唤醒所有等待者（它们会看到 closed 并得到 ErrPoolClosed）。
	p.waitMu.Lock()
	ws := p.waiters
	p.waiters = nil
	p.waitMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}

	// 停止后台任务。
	close(p.stopCh)
	p.wg.Wait()

	// 优雅关闭所有会话。
	for _, s := range sessions {
		_ = s.Close()
	}
	return nil
}

// Stats 返回当前可观测指标快照。
func (p *Pool) Stats() Stats {
	st := p.stats.snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	st.Conns = int64(len(p.sessions))
	idle, active := 0, 0
	var inflight int64
	for _, s := range p.sessions {
		n := s.InFlight()
		inflight += int64(n)
		if n > 0 {
			active++
		} else {
			idle++
		}
	}
	st.IdleConns = int64(idle)
	st.ActiveConns = int64(active)
	st.InFlight = inflight
	p.waitMu.Lock()
	st.WaitQueue = int64(len(p.waiters))
	p.waitMu.Unlock()
	return st
}

// canCreateLocked 判断当前是否可新建连接（含正在拨号的名额）。
func (p *Pool) canCreateLocked() bool {
	return p.cfg.MaxConns == 0 || len(p.sessions)+p.dialing < p.cfg.MaxConns
}

// dialAndAdd 拨号并加入池中，占用 dialing 名额以避免并发重复创建。
// 调用时不持有 p.mu。
func (p *Pool) dialAndAdd(ctx context.Context) (*Session, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if !p.canCreateLocked() {
		p.mu.Unlock()
		return nil, ErrInFlightLimit
	}
	p.dialing++
	p.mu.Unlock()

	s, err := p.dial(ctx)

	p.mu.Lock()
	p.dialing--
	if err != nil {
		p.mu.Unlock()
		p.stats.rejectTotal.Add(1)
		p.signalWaiter()
		return nil, err
	}
	if p.closed {
		p.mu.Unlock()
		s.closeNow(ErrPoolClosed)
		p.signalWaiter()
		return nil, ErrPoolClosed
	}
	p.sessions = append(p.sessions, s)
	p.stats.conns.Add(1)
	p.stats.reuseMisses.Add(1)
	if err := s.reserve(); err != nil {
		p.mu.Unlock()
		p.Remove(s, err)
		return nil, err
	}
	p.mu.Unlock()
	return s, nil
}

// dial 建立新会话。
func (p *Pool) dial(ctx context.Context) (*Session, error) {
	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.DialTimeout)
	defer cancel()
	conn, err := p.dialer(dialCtx)
	if err != nil {
		return nil, err
	}
	s := newSession(conn, p.cfg)
	s.onAvailable = p.signalWaiter
	s.onDead = func(reason error) {
		p.Remove(s, reason)
	}
	return s, nil
}

// tryReuseLocked 尝试复用一个在途数未满的会话（优先复用负载最高的，便于空闲连接淘汰）。
// 返前已预留槽位，避免 Acquire 与 Do 之间的竞态导致超额。
func (p *Pool) tryReuseLocked() *Session {
	var best *Session
	bestInFlight := -1
	for _, s := range p.sessions {
		if s.IsDead() {
			continue
		}
		n := s.InFlight()
		if n >= p.cfg.MaxInFlightPerConn {
			continue
		}
		if n > bestInFlight {
			best = s
			bestInFlight = n
		}
	}
	if best == nil {
		return nil
	}
	if err := best.reserve(); err != nil {
		return nil
	}
	return best
}

// enqueueWaiter 加入等待队列，受队列深度与等待时长约束。
func (p *Pool) enqueueWaiter(ctx context.Context) (*Session, error) {
	deadline := time.Now().Add(p.cfg.WaitTimeout)
	for {
		// 每次被唤醒后先尝试复用/新建，避免无效排队。
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if s := p.tryReuseLocked(); s != nil {
			p.mu.Unlock()
			p.stats.reuseHits.Add(1)
			return s, nil
		}
		if p.canCreateLocked() {
			p.mu.Unlock()
			return p.dialAndAdd(ctx)
		}
		p.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			p.stats.rejectTotal.Add(1)
			return nil, ErrWaitTimeout
		}

		p.waitMu.Lock()
		if len(p.waiters) >= p.cfg.WaitQueueDepth {
			p.waitMu.Unlock()
			p.stats.rejectTotal.Add(1)
			return nil, ErrQueueFull
		}
		w := &waiter{ch: make(chan struct{}, 1)}
		p.waiters = append(p.waiters, w)
		p.stats.waitQueue.Add(1)
		p.waitMu.Unlock()

		timer := time.NewTimer(remaining)
		select {
		case <-w.ch:
			timer.Stop()
			p.stats.waitQueue.Add(-1)
			// 容量可能已释放，回到循环顶部重新尝试。
		case <-timer.C:
			p.removeWaiter(w)
			p.stats.waitQueue.Add(-1)
			p.stats.rejectTotal.Add(1)
			return nil, ErrWaitTimeout
		case <-ctx.Done():
			timer.Stop()
			p.removeWaiter(w)
			p.stats.waitQueue.Add(-1)
			p.stats.rejectTotal.Add(1)
			return nil, ctx.Err()
		}
	}
}

// removeWaiter 从队列中移除指定等待者。
func (p *Pool) removeWaiter(target *waiter) {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	for i, w := range p.waiters {
		if w == target {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// signalWaiter 唤醒一个等待者（容量释放时调用）。
func (p *Pool) signalWaiter() {
	p.waitMu.Lock()
	if len(p.waiters) == 0 {
		p.waitMu.Unlock()
		return
	}
	w := p.waiters[0]
	p.waiters = p.waiters[1:]
	p.waitMu.Unlock()
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

// runEviction 周期性淘汰空闲与超龄连接。
func (p *Pool) runEviction() {
	defer p.wg.Done()
	interval := p.cfg.MaxIdleTime / 2
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTimer(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.evictOnce()
			ticker.Reset(interval)
		}
	}
}

// evictOnce 执行一轮淘汰。
func (p *Pool) evictOnce() {
	p.mu.Lock()
	var toClose []*Session

	// 移除已死亡的会话。
	live := make([]*Session, 0, len(p.sessions))
	for _, s := range p.sessions {
		if s.IsDead() {
			p.stats.conns.Add(-1)
			p.stats.deadTotal.Add(1)
			toClose = append(toClose, s)
			continue
		}
		live = append(live, s)
	}
	p.sessions = live

	// 空闲淘汰：空闲数超过 MaxIdleConns 且空闲时长超过 MaxIdleTime。
	idleCount := 0
	for _, s := range p.sessions {
		if s.InFlight() == 0 {
			idleCount++
		}
	}
	if p.cfg.MaxIdleConns > 0 && idleCount > p.cfg.MaxIdleConns {
		for i := len(p.sessions) - 1; i >= 0; i-- {
			s := p.sessions[i]
			if s.InFlight() > 0 {
				continue
			}
			if s.IdleTime() < p.cfg.MaxIdleTime {
				continue
			}
			if idleCount <= p.cfg.MinIdleConns {
				break
			}
			p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
			p.stats.conns.Add(-1)
			p.stats.evictedTotal.Add(1)
			idleCount--
			toClose = append(toClose, s)
		}
	}

	// 超龄淘汰。
	if p.cfg.MaxLifetime > 0 {
		kept := make([]*Session, 0, len(p.sessions))
		for _, s := range p.sessions {
			if s.Age() > p.cfg.MaxLifetime && s.InFlight() == 0 {
				p.stats.conns.Add(-1)
				p.stats.evictedTotal.Add(1)
				toClose = append(toClose, s)
				continue
			}
			kept = append(kept, s)
		}
		p.sessions = kept
	}
	p.mu.Unlock()

	// 释放锁后再关闭，避免 closeNow -> onDead -> Remove 重入 p.mu 导致死锁。
	for _, s := range toClose {
		s.closeNow(ErrPoolClosed)
	}
	p.signalWaiter()
}

// runKeepAlive 周期性保活探测，失败则摘除。
func (p *Pool) runKeepAlive() {
	defer p.wg.Done()
	if p.cfg.KeepAliveInterval <= 0 {
		return
	}
	ticker := time.NewTimer(p.cfg.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.keepAliveOnce()
			ticker.Reset(p.cfg.KeepAliveInterval)
		}
	}
}

// keepAliveOnce 执行一轮保活探测。
func (p *Pool) keepAliveOnce() {
	p.mu.Lock()
	sessions := make([]*Session, len(p.sessions))
	copy(sessions, p.sessions)
	p.mu.Unlock()

	for _, s := range sessions {
		if s.IsDead() {
			continue
		}
		timeout := p.cfg.KeepAliveTimeout
		if timeout <= 0 {
			timeout = p.cfg.DialTimeout
		}
		if err := s.ping(timeout); err != nil {
			p.Remove(s, err)
		}
	}
}

// defaultDialer 是占位拨号器。未配置拨号器时返回明确错误，
// 调用方应通过 NewPool 传入 TCPDialer(addr) 或自定义拨号器。
func defaultDialer(ctx context.Context) (net.Conn, error) {
	return nil, errors.New("muxpool: no dialer configured; use TCPDialer(addr) or provide a custom Dialer")
}
