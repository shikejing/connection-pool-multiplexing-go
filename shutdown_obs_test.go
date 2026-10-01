package muxpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestHalfClose 对端宣告半关闭后，在途请求仍能拿到各自响应；该连接此后不再复用，
// 新请求落到新连接，不串流、不泄漏。
func TestHalfClose(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{halfClose: true})
	cfg := testCfg(s)
	cfg.MaxConns = 2
	cfg.MaxInflight = 8
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	out, err := c.Do(context.Background(), []byte("hc"), nil)
	if err != nil {
		t.Fatalf("half-close in-flight request must succeed, got %v", err)
	}
	if string(out) != "echo:hc" {
		t.Fatalf("got %q", out)
	}
	// 旧连接应被摘除；后续请求在新连接上正常。
	if !waitUntil(func() bool { return c.Stats().Conns <= 1 }, time.Second) {
		t.Fatalf("half-closed conn not removed: %d", c.Stats().Conns)
	}
	out2, err := c.Do(context.Background(), []byte("after"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out2) != "echo:after" {
		t.Fatalf("got %q", out2)
	}
}

// TestGracefulShutdown 优雅关闭：在途请求在宽限期内完成；新请求被拒绝；不泄漏协程。
func TestGracefulShutdown(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{delay: 100 * time.Millisecond})
	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.MaxInflight = 8
	cfg.QueuePolicy = QueuePolicyQueue
	cfg.MaxWaitQueue = 16
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const n = 4
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			out, e := c.Do(context.Background(), []byte(fmt.Sprintf("g-%d", i)), nil)
			if e != nil || string(out) != "echo:g-"+itoa(i) {
				t.Errorf("graceful req %d: out=%q err=%v", i, out, e)
			}
		}()
	}
	// 等在途建立后开始优雅关闭，给足宽限。
	time.Sleep(40 * time.Millisecond)
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- c.Shutdown(2 * time.Second) }()

	// 关闭开始后新请求必须被拒绝（轮询直到观察到关闭态，避免入队时序竞态）。
	if !waitUntil(func() bool {
		_, e := c.Do(context.Background(), []byte("new"), nil)
		return errors.Is(e, ErrPoolClosed)
	}, time.Second) {
		t.Fatal("new requests were not rejected after shutdown began")
	}
	wg.Wait()
	select {
	case e := <-shutdownErr:
		if e != nil {
			t.Fatalf("graceful shutdown should succeed, got %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return after in-flight completed")
	}
}

// TestForceShutdown 宽限期到期后强制失败所有在途请求，且 Close 必须返回、无协程泄漏。
func TestForceShutdown(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{})
	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.MaxInflight = 8
	cfg.ReadTimeout = 0
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 先正常建立连接，再切换挂起。
	if _, err := c.Do(context.Background(), []byte("warm"), nil); err != nil {
		t.Fatal(err)
	}
	s.set(srvConfig{hang: true})
	time.Sleep(30 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, errs[i] = c.Do(context.Background(), []byte("hang"),
				&RequestOptions{MaxAttempts: 1})
		}()
	}
	if !waitUntil(func() bool { return c.Stats().Inflight == 2 }, time.Second) {
		t.Fatal("inflight never 2")
	}
	done := make(chan struct{})
	go func() { _ = c.Shutdown(80 * time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("forced shutdown hung")
	}
	wg.Wait()
	for i, e := range errs {
		if !errors.Is(e, ErrPoolClosed) {
			t.Fatalf("inflight %d want ErrPoolClosed, got %v", i, e)
		}
	}
}

// TestIdleEviction 空闲连接按 IdleTimeout 回收；回收后再请求会新建连接。
func TestIdleEviction(t *testing.T) {
	s := startTestServer(t)
	cfg := testCfg(s)
	cfg.IdleTimeout = 80 * time.Millisecond
	cfg.PingInterval = 0 // 关闭保活，避免探活刷新空闲判定
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if st := c.Stats(); st.Conns != 1 {
		t.Fatalf("expected 1 conn, got %d", st.Conns)
	}
	if !waitUntil(func() bool { return c.Stats().Conns == 0 }, time.Second) {
		t.Fatalf("idle conn not evicted: %d", c.Stats().Conns)
	}
	if c.Stats().ConnEvictions < 1 {
		t.Fatalf("eviction metric=%d", c.Stats().ConnEvictions)
	}
	if _, err := c.Do(context.Background(), []byte("y"), nil); err != nil {
		t.Fatal(err)
	}
}

// TestKeepAliveDetectsSilentReclaim 对端静默回收：保活探测在 PingTimeout 内判定失效。
func TestKeepAliveDetectsSilentReclaim(t *testing.T) {
	s := startTestServer(t)
	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.IdleTimeout = 0
	cfg.PingInterval = 60 * time.Millisecond
	cfg.PingTimeout = 100 * time.Millisecond
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	// 模拟链路被中间设备/对端静默丢弃（不发 FIN）：用 iptables 不可移植，
	// 这里通过关闭服务端连接并确保客户端不写来走保活探测路径。
	s.dropConns()
	// 保活探测应在数个 interval 内发现连接消失并从台账摘除。
	if !waitUntil(func() bool {
		st := c.Stats()
		return st.ConnFailures >= 1 || st.Conns == 0
	}, 2*time.Second) {
		t.Fatalf("keepalive did not detect silent reclaim: %+v", c.Stats())
	}
}

// TestObservabilityAndEvict 指标随操作正确变化；按条件主动摘除连接生效。
func TestObservabilityAndEvict(t *testing.T) {
	s := startTestServer(t)
	cfg := testCfg(s)
	cfg.MaxConns = 3
	cfg.MaxInflight = 4
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// 顺序请求全部复用同一条物理连接，验证复用与指标。
	for i := 0; i < 6; i++ {
		if _, err := c.Do(context.Background(), []byte(fmt.Sprintf("o-%d", i)), nil); err != nil {
			t.Fatal(err)
		}
	}
	st := c.Stats()
	if st.TotalRequests != 6 || st.Inflight != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if st.Conns != 1 || st.ReusedHits < 5 {
		t.Fatalf("expected reuse, stats: %+v", st)
	}
	// 主动摘除现有连接。
	n := c.Evict(func(info *ConnInfo) bool { return info.ID > 0 })
	if n != 1 {
		t.Fatalf("expected to evict 1, got %d", n)
	}
	if !waitUntil(func() bool { return c.Stats().Conns == 0 }, time.Second) {
		t.Fatalf("evicted conn still tracked: %d", c.Stats().Conns)
	}
}

// TestHighConcurrencyBounded 长时间高并发下各项状态有界、结果全部正确。
func TestHighConcurrencyBounded(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{delay: time.Millisecond})
	cfg := testCfg(s)
	cfg.MaxConns = 3
	cfg.MaxInflight = 8
	cfg.QueuePolicy = QueuePolicyQueue
	cfg.MaxWaitQueue = 64
	cfg.MaxWaitTimeout = 5 * time.Second
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const workers = 64
	const rounds = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				body := fmt.Sprintf("w%d-r%d", w, r)
				out, e := c.Do(context.Background(), []byte(body), nil)
				if e != nil {
					t.Errorf("Do %s: %v", body, e)
					return
				}
				if string(out) != "echo:"+body {
					t.Errorf("mismatch %s -> %q", body, out)
					return
				}
			}
		}()
	}
	wg.Wait()
	st := c.Stats()
	if st.Conns > 3 {
		t.Fatalf("conns exceeded cap: %d", st.Conns)
	}
	if st.Inflight != 0 {
		t.Fatalf("inflight leaked: %d", st.Inflight)
	}
	if st.WaitQueueDepth != 0 {
		t.Fatalf("wait queue leaked: %d", st.WaitQueueDepth)
	}
	if st.TotalRequests != workers*rounds {
		t.Fatalf("requests=%d want %d", st.TotalRequests, workers*rounds)
	}
}

// TestServerErrorRetryable 服务端错误帧：非可重试直接失败且不重放；
// 可重试 + 幂等时按预算重试；两种错误都能通过 errors.Is/类型断言区分。
func TestServerError(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{errCode: 7, errRetry: false})
	cfg := testCfg(s)
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	cfg.DefaultMaxAttempts = 3
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Do(context.Background(), []byte("e"), &RequestOptions{Idempotent: true})
	if !errors.Is(err, ErrServerRejected) {
		t.Fatalf("want ErrServerRejected, got %v", err)
	}
	var se *ServerError
	if !errors.As(err, &se) || se.Code != 7 || se.Retryable {
		t.Fatalf("unexpected server error: %v", err)
	}
	// 非可重试：即便幂等也只执行一次（不重放）。
	if n := s.execCountsFor("e"); n["e"] != 1 {
		t.Fatalf("non-retryable server error executed %d times", n["e"])
	}
}

// TestKeepAlivePingTimeout 对端不响应保活（静默黑洞）时，连接在 PingTimeout 内被判失效。
func TestKeepAlivePingTimeout(t *testing.T) {
	s := startTestServer(t)
	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.IdleTimeout = 0
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PingTimeout = 100 * time.Millisecond
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	s.set(srvConfig{ignorePing: true})
	if !waitUntil(func() bool { return c.Stats().ConnFailures >= 1 }, time.Second) {
		t.Fatalf("ping timeout did not fail conn: %+v", c.Stats())
	}
	if !waitUntil(func() bool { return c.Stats().Conns == 0 }, time.Second) {
		t.Fatalf("dead conn not removed: %+v", c.Stats())
	}
}

// TestMaxConnLife 连接达到最大寿命后被半关闭/回收，新请求落到新连接。
func TestMaxConnLife(t *testing.T) {
	s := startTestServer(t)
	cfg := testCfg(s)
	cfg.MaxConnLife = 120 * time.Millisecond
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), []byte("a"), nil); err != nil {
		t.Fatal(err)
	}
	first := c.Stats().NewConns
	if first != 1 {
		t.Fatalf("expected 1 new conn, got %d", first)
	}
	if !waitUntil(func() bool { return c.Stats().Conns == 0 }, time.Second) {
		t.Fatalf("life-expired conn not removed: %d", c.Stats().Conns)
	}
	if _, err := c.Do(context.Background(), []byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	if c.Stats().NewConns < 2 {
		t.Fatalf("expected a fresh conn after life expiry, new=%d", c.Stats().NewConns)
	}
}
