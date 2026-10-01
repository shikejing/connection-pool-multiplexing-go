package muxpool

import "sync"

// waiter 是等待一个可用连接容量的排队者。
// 通过关闭 ch 唤醒；wake 幂等，保证取消与被选中不会重复唤醒。
type waiter struct {
	ch   chan struct{}
	once sync.Once
	c    *muxConn // 被唤醒时绑定的连接
	s    *stream  // 被唤醒时已代为预留的流槽位
	err  error    // 被唤醒时绑定的结论（排空场景）
}

func newWaiter() *waiter {
	return &waiter{ch: make(chan struct{})}
}

// wake 以连接或错误结论唤醒等待方，恰好生效一次。
func (w *waiter) wake(mc *muxConn, s *stream, err error) {
	w.once.Do(func() {
		w.c = mc
		w.s = s
		w.err = err
		close(w.ch)
	})
}

// waitQueue 是有界 FIFO 等待队列，支持按 waiter 精确移除（取消场景）。
type waitQueue struct {
	mu     sync.Mutex
	q      []*waiter
	max    int
	closed bool
	m      *metrics
}

func newWaitQueue(max int, m *metrics) *waitQueue {
	return &waitQueue{max: max, m: m}
}

func (q *waitQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.q)
}

// enqueue 入队；队列已满或已关闭时返回 false。
func (q *waitQueue) enqueue(w *waiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.q) >= q.max {
		return false
	}
	q.q = append(q.q, w)
	if q.m != nil {
		q.m.waitQueue.Add(1)
	}
	return true
}

// dequeue 取出队首；空队列返回 nil。
func (q *waitQueue) dequeue() *waiter {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.q) == 0 {
		return nil
	}
	w := q.q[0]
	q.q = q.q[1:]
	if q.m != nil {
		q.m.waitQueue.Add(-1)
	}
	return w
}

// remove 从队列中部移除指定 waiter（等待方取消/超时时用）。
func (q *waitQueue) remove(w *waiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.q {
		if x == w {
			q.q = append(q.q[:i], q.q[i+1:]...)
			if q.m != nil {
				q.m.waitQueue.Add(-1)
			}
			return true
		}
	}
	return false
}

// drain 排空并返回全部等待者（池关闭时用），之后拒绝新入队。
func (q *waitQueue) drain() []*waiter {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.q
	q.q = nil
	q.closed = true
	if q.m != nil {
		q.m.waitQueue.Add(-int64(len(out)))
	}
	return out
}
