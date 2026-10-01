package muxpool

import (
	"context"
	"math/rand"
	"net"
	"time"
)

// RetryPolicy 重试策略。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），0 表示不重试。
	MaxAttempts int
	// Backoff 初始退避时长。
	Backoff time.Duration
	// MaxBackoff 最大退避时长。
	MaxBackoff time.Duration
	// Idempotent 为 true 时，可重试失败才会被重放；否则失败不重放。
	Idempotent bool
}

// 默认重试策略。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Backoff:     10 * time.Millisecond,
		MaxBackoff:  200 * time.Millisecond,
		Idempotent:  true,
	}
}

// Client 是面向调用方的客户端：在连接池之上提供带重试的请求能力。
type Client struct {
	pool  *Pool
	retry RetryPolicy
}

// NewClient 创建客户端。
func NewClient(pool *Pool, retry RetryPolicy) *Client {
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 1
	}
	if retry.Backoff <= 0 {
		retry.Backoff = 10 * time.Millisecond
	}
	if retry.MaxBackoff <= 0 {
		retry.MaxBackoff = time.Second
	}
	return &Client{pool: pool, retry: retry}
}

// Do 发送一个请求，必要时按重试策略在新会话上重试。
// 重试只在失败可安全重试（不会导致副作用重复）时发生：
// 仅当请求幂等且失败原因为会话失效/写入失败等未确认处理的场景。
func (c *Client) Do(ctx context.Context, payload []byte) ([]byte, error) {
	c.pool.stats.requestsTotal.Add(1)
	maxAttempts := c.retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			c.pool.stats.retriesTotal.Add(1)
			bo := c.backoff(attempt)
			timer := time.NewTimer(bo)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			}
		}

		sess, err := c.pool.Acquire(ctx)
		if err != nil {
			// 池级错误（队列满/等待超时/池关闭）不重放。
			return nil, err
		}

		resp, err := sess.Do(ctx, payload)
		c.pool.Release(sess)
		if err == nil {
			return resp, nil
		}
		lastErr = err

		// 幂等性闸门：非幂等请求绝不重放，避免副作用重复。
		if !c.retry.Idempotent {
			return nil, err
		}
		// 仅重试可安全重试的失败（会话失效/写入失败等未确认处理的场景）。
		if !IsRetryable(err) {
			return nil, err
		}
	}
	return nil, lastErr
}

// Close 关闭底层连接池。
func (c *Client) Close() error {
	return c.pool.Close()
}

// backoff 计算第 attempt 次重试的退避时长（指数退避，带 1/2 抖动上限）。
func (c *Client) backoff(attempt int) time.Duration {
	bo := c.retry.Backoff << uint(attempt-1)
	if bo <= 0 || bo > c.retry.MaxBackoff {
		bo = c.retry.MaxBackoff
	}
	if bo <= 0 {
		return 0
	}
	// 简单抖动：[bo/2, bo]
	jitter := time.Duration(int64(bo) / 2)
	if jitter <= 0 {
		return bo
	}
	return bo - jitter + time.Duration(rand.Int63n(int64(jitter)))
}

// TCPDialer 返回一个拨号到指定地址的 Dialer。
func TCPDialer(addr string) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}
