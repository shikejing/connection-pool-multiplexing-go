package muxpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testCfg(s *testServer) Config {
	return Config{
		Dial:               s.dialer(),
		MaxConns:           2,
		MaxInflight:        4,
		QueuePolicy:        QueuePolicyReject,
		DialTimeout:        time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       time.Second,
		IdleTimeout:        200 * time.Millisecond,
		PingInterval:       100 * time.Millisecond,
		PingTimeout:        300 * time.Millisecond,
		DefaultMaxAttempts: 1,
	}
}

// TestBasicReuse 顺序请求必须复用同一条物理连接。
func TestBasicReuse(t *testing.T) {
	s := startTestServer(t)
	c, err := New(testCfg(s))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 5; i++ {
		out, err := c.Do(context.Background(), []byte(fmt.Sprintf("req-%d", i)), nil)
		if err != nil {
			t.Fatalf("Do %d: %v", i, err)
		}
		if string(out) != "echo:req-"+itoa(i) {
			t.Fatalf("got %q", out)
		}
	}
	st := c.Stats()
	if st.Conns != 1 {
		t.Fatalf("expected 1 conn reused, got %d (new=%d)", st.Conns, st.NewConns)
	}
	if st.ReusedHits < 4 {
		t.Fatalf("expected >=4 reuse hits, got %d", st.ReusedHits)
	}
	if st.Inflight != 0 {
		t.Fatalf("inflight leaked: %d", st.Inflight)
	}
}

// TestMultiplexOnOneConn 在单连接上并发多路复用，全部按流 ID 正确配对。
func TestMultiplexOnOneConn(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{delay: 20 * time.Millisecond})

	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.MaxInflight = 16
	cfg.QueuePolicy = QueuePolicyQueue
	cfg.MaxWaitQueue = 32
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			body := []byte(fmt.Sprintf("m-%d", i))
			out, err := c.Do(context.Background(), body, nil)
			if err != nil {
				errs <- err
				return
			}
			if string(out) != "echo:m-"+itoa(i) {
				errs <- fmt.Errorf("mismatch i=%d got=%q", i, out)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if st := c.Stats(); st.Conns != 1 {
		t.Fatalf("expected exactly 1 conn, got %d", st.Conns)
	}
}

// TestOutOfOrder 乱序返回必须仍然精确配对，不得张冠李戴。
func TestOutOfOrder(t *testing.T) {
	s := startTestServer(t)
	s.set(srvConfig{outOrder: true})

	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.MaxInflight = 12
	cfg.QueuePolicy = QueuePolicyQueue
	cfg.MaxWaitQueue = 32
	cfg.ReadTimeout = 5 * time.Second
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 12 // 3 的倍数
	res := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	release := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-release
			out, err := c.Do(context.Background(), []byte(fmt.Sprintf("o-%d", i)), nil)
			if err != nil {
				t.Error(err)
				return
			}
			res[i] = string(out)
		}()
	}
	close(release)
	wg.Wait()
	for i := 0; i < n; i++ {
		want := "echo:o-" + itoa(i)
		if res[i] != want {
			t.Fatalf("stream pairing wrong at %d: got %q want %q", i, res[i], want)
		}
	}
}

// TestInflightCapReject 单连接在途达到上限且连接数到顶：拒绝策略立即返回 ErrQueueFull。
func TestInflightCapReject(t *testing.T) {
	s := startTestServer(t)
	// 先正常应答预热，建立连接；随后切换为挂起模式打满在途槽位。
	cfg := testCfg(s)
	cfg.MaxConns = 1
	cfg.MaxInflight = 2
	cfg.ReadTimeout = 0
	cfg.IdleTimeout = 0
	cfg.PingInterval = 0
	cfg.QueuePolicy = QueuePolicyReject
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), []byte("warm"), nil); err != nil {
		t.Fatal(err)
	}
	s.set(srvConfig{hang: true})
	time.Sleep(30 * time.Millisecond) // 确保服务端下一次读帧读到 hang 配置

	for i := 0; i < 2; i++ {
		go func() { _, _ = c.Do(context.Background(), []byte("hold"), nil) }()
	}
	if !waitUntil(func() bool { return c.Stats().Inflight == 2 }, time.Second) {
		t.Fatalf("expected inflight=2, got inflight=%d exec=%d",
			c.Stats().Inflight, s.execCount())
	}

	_, err = c.Do(context.Background(), []byte("x"), nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	if st := c.Stats(); st.Rejected != 1 {
		t.Fatalf("rejected metric=%d", st.Rejected)
	}
}

func waitUntil(f func() bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return f()
}
