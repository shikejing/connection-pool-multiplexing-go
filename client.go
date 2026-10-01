package muxpool

import (
	"context"
	"errors"
	"sync"
	"time"
)

// RequestOptions 单次请求的可选项。
type RequestOptions struct {
	// Idempotent 为 true 时，连接级失败与可重试的服务端错误允许有限重试；
	// false（默认）时任何失败都不会导致请求被重复执行。
	Idempotent bool
	// MaxAttempts 含首次在内的最大尝试次数；<=0 用 DefaultMaxAttempts。
	MaxAttempts int
}

// Client 是连接复用 + 多路复用客户端。
type Client struct {
	pool *pool

	closeOnce sync.Once
	closeErr  error
}

// New 创建客户端并启动保活/回收后台协程。
func New(cfg Config) (*Client, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &Client{pool: newPool(&cfg)}, nil
}

// Do 执行一次逻辑请求：获取连接槽位、多路上发送、按流 ID 精确配对响应。
//
// 结论语义：
//   - 成功：返回响应载荷。
//   - 调用方取消/ctx 超时：包装 ErrStreamCanceled；只取消本流，不影响同连接其他请求。
//   - 连接级失效：包装 ErrConnClosed/ErrProtocol 等；仅 Idempotent 请求会在尝试预算内重试。
//   - 服务端拒绝：返回 *ServerError（可 errors.Is ErrServerRejected 或类型断言）。
//   - 容量上界：ErrQueueFull（立即拒绝）或 ErrWaitTimeout（等待超时），不会自动重试。
func (c *Client) Do(ctx context.Context, payload []byte, opts *RequestOptions) ([]byte, error) {
	o := RequestOptions{}
	if opts != nil {
		o = *opts
	}
	maxAttempts := o.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = c.pool.cfg.DefaultMaxAttempts
	}
	c.pool.metrics.totalRequests.Add(1)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			c.pool.metrics.retries.Add(1)
			if ctx.Err() != nil {
				c.pool.metrics.canceled.Add(1)
				lastErr = errors.Join(ErrStreamCanceled, ctx.Err())
				break
			}
		}
		// 等待槽位的绝对截止点：受 MaxWaitTimeout 与 ctx 共同约束。
		var deadline time.Time
		if c.pool.cfg.MaxWaitTimeout > 0 {
			deadline = time.Now().Add(c.pool.cfg.MaxWaitTimeout)
		}
		if dl, ok := ctx.Deadline(); ok && (deadline.IsZero() || dl.Before(deadline)) {
			deadline = dl
		}

		g, err := c.pool.acquire(ctx, deadline)
		if err != nil {
			lastErr = err
			// 等待/拒绝/取消类结论是确定的：不重试。
			break
		}

		if err := g.mc.start(g.s, payload); err != nil {
			// start 内部已拆除连接并释放槽位。
			lastErr = err
			if o.Idempotent && attempt < maxAttempts {
				continue
			}
			break
		}

		select {
		case res := <-g.s.reply:
			if res.err == nil {
				return res.payload, nil
			}
			var se *ServerError
			if errors.As(res.err, &se) {
				// 响应已精确收到，结论确定。仅显式标记可重试且幂等时重试。
				lastErr = errors.Join(ErrServerRejected, se)
				if se.Retryable && o.Idempotent && attempt < maxAttempts {
					continue
				}
				return nil, lastErr
			}
			// 连接级失效：连接已带着全部在途流一起失败。
			lastErr = res.err
			if o.Idempotent && attempt < maxAttempts {
				continue
			}
			return nil, lastErr
		case <-ctx.Done():
			// 只取消本流，绝不影响同连接其他请求。若响应恰在此刻先到则以响应为准。
			select {
			case res := <-g.s.reply:
				if res.err != nil {
					return nil, res.err
				}
				return res.payload, nil
			default:
			}
			g.mc.cancel(g.s)
			c.pool.metrics.canceled.Add(1)
			return nil, errors.Join(ErrStreamCanceled, ctx.Err())
		}
	}
	if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
		c.pool.metrics.canceled.Add(1)
		return nil, errors.Join(ErrStreamCanceled, lastErr)
	}
	if lastErr == nil {
		lastErr = ErrTooManyAttempts
	}
	if maxAttempts > 1 {
		lastErr = errors.Join(ErrTooManyAttempts, lastErr)
	}
	return nil, lastErr
}

// Stats 返回运行时指标快照。
func (c *Client) Stats() Stats { return c.pool.Stats() }

// Evict 按条件主动摘除异常连接。
func (c *Client) Evict(cond func(*ConnInfo) bool) int { return c.pool.Evict(cond) }

// Close 立即关闭：拒绝新请求与等待方，所有在途请求得到 ErrPoolClosed，
// 并等待全部后台协程（读循环、保活、回收）退出，不泄漏连接与协程。
func (c *Client) Close() error {
	return c.Shutdown(0)
}

// Shutdown 优雅关闭：先停止接新并半关闭全部连接；在 grace 期限内等待在途请求
// 自然完成；超时后强制以 ErrPoolClosed 终止剩余在途流，并回收所有资源。
func (c *Client) Shutdown(grace time.Duration) error {
	result := error(nil)
	didRun := false
	c.closeOnce.Do(func() {
		didRun = true
		result = ErrShutdownTimeout
		conns := c.pool.beginClose()
		// 半关闭：有在途流的等待自然结束，无在途流的立即回收。
		for _, mc := range conns {
			mc.markDraining()
		}
		idle := make([]*muxConn, 0, len(conns))
		for _, mc := range conns {
			mc.mu.Lock()
			n := mc.inflight
			mc.mu.Unlock()
			if n == 0 {
				idle = append(idle, mc)
			}
		}
		done := make(chan struct{})
		go func() {
			c.pool.waitBackground()
			close(done)
		}()

		for _, mc := range idle {
			mc.evict(ErrPoolClosed)
		}
		if grace <= 0 {
			// 立即关闭：所有连接（含在途流）得到确定失败结论。
			for _, mc := range conns {
				mc.fail(ErrPoolClosed)
			}
			<-done
			result = nil
			return
		} else {
			t := time.NewTimer(grace)
			select {
			case <-done:
				result = nil
				t.Stop()
				return
			case <-t.C:
				result = ErrShutdownTimeout
			}
		}
		// 强制阶段：剩余连接全部以确定结论失败。
		for _, mc := range conns {
			mc.fail(ErrPoolClosed)
		}
		<-done
	})
	if !didRun {
		return nil // 已经在关闭或已关闭：幂等，无额外错误
	}
	return result
}
