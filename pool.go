package muxpool

import (
	"context"
	"sync"
	"time"
)

// pool 管理一组可复用的多路复用连接。
//
// 所有连接台账、空闲集合、等待队列的结构性变更都在 pool.mu 下完成；
// pool.mu 与 muxConn.mu 的锁序固定为 pool.mu -> muxConn.mu。
type pool struct {
	cfg     *Config
	metrics *metrics

	mu      sync.Mutex
	all     map[*muxConn]struct{}
	idle    map[*muxConn]struct{}
	closed  bool
	connSeq uint64
	// dialPending 已承诺给建连者、尚未计入 all 的名额。
	dialPending int
	waiters     *waitQueue

	wg       sync.WaitGroup
	stopEvic chan struct{}
}

func newPool(cfg *Config) *pool {
	p := &pool{
		cfg:      cfg,
		metrics:  newMetrics(),
		all:      make(map[*muxConn]struct{}),
		idle:     make(map[*muxConn]struct{}),
		stopEvic: make(chan struct{}),
	}
	p.waiters = newWaitQueue(cfg.MaxWaitQueue, p.metrics)
	p.wg.Add(1)
	go p.reapLoop()
	return p
}

// grant 是一次成功获取：连接 + 已预留的流槽位（已计入 inflight）。
type grant struct {
	mc *muxConn
	s  *stream
}

// acquire 按策略获取一个在途槽位。timeout 为等待的绝对截止点（零值表示不限）。
func (p *pool) acquire(ctx context.Context, timeout time.Time) (grant, error) {
	var (
		w      *waiter
		queued bool
	)
	defer func() {
		if queued {
			p.waiters.remove(w)
		}
	}()

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return grant{}, ErrPoolClosed
		}
		if !queued {
			// 1/2. 复用空闲或任何有容量的活跃连接。
			for mc := range p.idle {
				if s, ok := mc.reserve(); ok {
					delete(p.idle, mc)
					p.metrics.idleConns.Store(int64(len(p.idle)))
					p.metrics.reusedHits.Add(1)
					p.mu.Unlock()
					return grant{mc, s}, nil
				}
				delete(p.idle, mc)
			}
			p.metrics.idleConns.Store(int64(len(p.idle)))
			for mc := range p.all {
				if s, ok := mc.reserve(); ok {
					p.metrics.reusedHits.Add(1)
					p.mu.Unlock()
					return grant{mc, s}, nil
				}
			}
			// 3. 新建连接（名额含在途的建连承诺）。
			if len(p.all)+p.dialPending < p.cfg.MaxConns {
				p.dialPending++
				p.mu.Unlock()
				return p.dialAndGrant()
			}
			// 4. 连接数到顶且在途容量用尽：按策略拒绝或排队。
			if p.cfg.QueuePolicy == QueuePolicyReject {
				p.mu.Unlock()
				p.metrics.rejected.Add(1)
				return grant{}, ErrQueueFull
			}
			// Queue 策略：扫描与入队在同一临界区，无空槽与排队并存的窗口。
			w = newWaiter()
			if !p.waiters.enqueue(w) {
				p.mu.Unlock()
				p.metrics.rejected.Add(1)
				return grant{}, ErrQueueFull
			}
			queued = true
		}

		// 等待唤醒/超时/取消；每次唤醒按绝对截止点重新计算剩余时长。
		var tCh <-chan time.Time
		var t *time.Timer
		if !timeout.IsZero() {
			d := time.Until(timeout)
			if d <= 0 {
				p.mu.Unlock()
				p.metrics.timeouts.Add(1)
				return grant{}, ErrWaitTimeout
			}
			t = time.NewTimer(d)
			tCh = t.C
		}
		p.mu.Unlock()

		if t != nil {
			select {
			case <-w.ch:
			case <-tCh:
				t.Stop()
				p.metrics.timeouts.Add(1)
				return grant{}, ErrWaitTimeout
			case <-ctx.Done():
				t.Stop()
				return grant{}, ctx.Err()
			}
			t.Stop()
		} else {
			select {
			case <-w.ch:
			case <-ctx.Done():
				return grant{}, ctx.Err()
			}
		}

		if w.err != nil {
			return grant{}, w.err
		}
		if w.c != nil {
			return grant{w.c, w.s}, nil
		}
		// 虚假唤醒（连接在交接瞬间失效）：保留队位，重新竞争（通常触发补建）。
		queued = false
	}
}

// dialAndGrant 在锁外建连，锁内登记并预留首个槽位。
func (p *pool) dialAndGrant() (grant, error) {
	raw, err := p.dial()
	p.mu.Lock()
	if err != nil {
		p.dialPending--
		p.mu.Unlock()
		return grant{}, err
	}
	if p.closed {
		p.dialPending--
		p.mu.Unlock()
		_ = raw.Close()
		return grant{}, ErrPoolClosed
	}
	p.dialPending--
	p.connSeq++
	mc := newMuxConn(raw, p, p.connSeq)
	p.all[mc] = struct{}{}
	p.metrics.conns.Store(int64(len(p.all)))
	p.metrics.newConns.Add(1)
	s, ok := mc.reserve()
	if !ok {
		p.mu.Unlock()
		mc.fail(ErrConnClosed)
		return grant{}, ErrConnClosed
	}
	// 新连接还有 MaxInflight-1 个槽位：链式移交给已排队的等待者，
	// 避免"连接有大量空闲容量却只有建连者被放行"的死锁。
	p.handoffWaitersLocked(mc)
	p.mu.Unlock()
	return grant{mc, s}, nil
}

// handoffWaitersLocked 从队首开始，把连接上的空闲在途槽位按 FIFO 全部移交等待者。
// 调用方持有 pool.mu。
func (p *pool) handoffWaitersLocked(mc *muxConn) {
	for p.waiters.len() > 0 {
		s, ok := mc.reserve()
		if !ok {
			return
		}
		w := p.waiters.dequeue()
		w.wake(mc, s, nil)
	}
}

func (p *pool) dial() (Conn, error) {
	if p.cfg.DialTimeout <= 0 {
		return p.cfg.Dial()
	}
	type dr struct {
		c   Conn
		err error
	}
	ch := make(chan dr, 1)
	go func() {
		c, e := p.cfg.Dial()
		ch <- dr{c, e}
	}()
	t := time.NewTimer(p.cfg.DialTimeout)
	defer t.Stop()
	select {
	case r := <-ch:
		if r.err != nil {
			p.metrics.dialsFailed.Add(1)
		}
		return r.c, r.err
	case <-t.C:
		p.metrics.dialsFailed.Add(1)
		return nil, context.DeadlineExceeded
	}
}

// onSlotFreed 在同一临界区把释放的槽位移交队首等待者，否则收为空闲。
func (p *pool) onSlotFreed(mc *muxConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if p.waiters.len() > 0 {
		// 刚释放了至少一个槽位：为队首预留并唤醒，再继续移交其余容量。
		if s, ok := mc.reserve(); ok {
			w := p.waiters.dequeue()
			w.wake(mc, s, nil)
			p.handoffWaitersLocked(mc)
			return
		}
		if w := p.waiters.dequeue(); w != nil {
			w.wake(nil, nil, nil)
		}
		return
	}
	p.markIdleLocked(mc)
}

func (p *pool) markIdleLocked(mc *muxConn) {
	if _, ok := p.all[mc]; !ok {
		return
	}
	mc.mu.Lock()
	usable := mc.state == connStateActive && !mc.drained && !mc.dirty && mc.inflight == 0
	mc.mu.Unlock()
	if !usable {
		return
	}
	p.idle[mc] = struct{}{}
	p.metrics.idleConns.Store(int64(len(p.idle)))
}

// reapLoop 周期回收空闲超时与寿命到期、且没有在途流的连接。
func (p *pool) reapLoop() {
	defer p.wg.Done()
	interval := 50 * time.Millisecond
	if d := p.cfg.IdleTimeout; d > 0 && d/3 < interval {
		interval = d / 3
	}
	if l := p.cfg.MaxConnLife; l > 0 && l/4 < interval {
		interval = l / 4
	}
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.stopEvic:
			return
		case <-t.C:
			p.reapOnce()
		}
	}
}

func (p *pool) reapOnce() {
	idleDeadline := p.cfg.IdleTimeout
	lifeDeadline := p.cfg.MaxConnLife
	var victims []*muxConn
	p.mu.Lock()
	for mc := range p.all {
		mc.mu.Lock()
		n := mc.inflight
		closed := mc.state == connStateClosed
		mc.mu.Unlock()
		if closed || n > 0 {
			continue
		}
		if lifeDeadline > 0 && mc.age() >= lifeDeadline {
			victims = append(victims, mc)
			continue
		}
		if idleDeadline > 0 {
			if _, isIdle := p.idle[mc]; isIdle && mc.idleFor() >= idleDeadline {
				victims = append(victims, mc)
			}
		}
	}
	for _, mc := range victims {
		delete(p.idle, mc)
	}
	p.metrics.idleConns.Store(int64(len(p.idle)))
	p.mu.Unlock()
	for _, mc := range victims {
		mc.evict(ErrConnClosed)
	}
}

// removeConn 从台账移除连接；failed 区分失效与主动摘除计数，并唤醒等待者补位。
func (p *pool) removeConn(mc *muxConn, failed bool) {
	p.mu.Lock()
	if _, ok := p.all[mc]; !ok {
		p.mu.Unlock()
		return
	}
	delete(p.all, mc)
	delete(p.idle, mc)
	p.metrics.conns.Store(int64(len(p.all)))
	p.metrics.idleConns.Store(int64(len(p.idle)))
	var w *waiter
	if !p.closed {
		w = p.waiters.dequeue()
	}
	p.mu.Unlock()
	if failed {
		p.metrics.connFailures.Add(1)
	} else {
		p.metrics.evictions.Add(1)
	}
	if w != nil {
		w.wake(nil, nil, nil)
	}
}

// Evict 按条件主动摘除：无在途流立即关闭；有在途流转半关闭，待其结束后回收。
func (p *pool) Evict(cond func(*ConnInfo) bool) int {
	var targets []*muxConn
	p.mu.Lock()
	for mc := range p.all {
		mc.mu.Lock()
		info := &ConnInfo{
			ID:       mc.id,
			Age:      mc.age(),
			IdleFor:  mc.idleFor(),
			Inflight: mc.inflight,
			Drained:  mc.drained,
		}
		closed := mc.state == connStateClosed
		mc.mu.Unlock()
		if !closed && cond(info) {
			targets = append(targets, mc)
		}
	}
	for _, mc := range targets {
		delete(p.idle, mc)
	}
	p.metrics.idleConns.Store(int64(len(p.idle)))
	p.mu.Unlock()

	n := 0
	for _, mc := range targets {
		mc.mu.Lock()
		inflight := mc.inflight
		mc.drained = true
		mc.mu.Unlock()
		n++
		p.metrics.evictions.Add(1)
		if inflight == 0 {
			mc.evict(ErrConnClosed)
		}
	}
	return n
}

// ConnInfo 暴露给摘除条件的连接信息。
type ConnInfo struct {
	ID       uint64
	Age      time.Duration
	IdleFor  time.Duration
	Inflight int
	Drained  bool
}

func (p *pool) Stats() Stats { return p.metrics.snapshot() }

// beginClose 标记关闭、排空等待者，返回当前连接列表。
func (p *pool) beginClose() []*muxConn {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stopEvic)
	conns := make([]*muxConn, 0, len(p.all))
	for mc := range p.all {
		conns = append(conns, mc)
	}
	ws := p.waiters.drain()
	p.mu.Unlock()
	for _, w := range ws {
		w.wake(nil, nil, ErrPoolClosed)
	}
	return conns
}

func (p *pool) waitBackground() { p.wg.Wait() }
