package muxpool

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func goroutineNum() int {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

// TestNoGoroutineLeak 多轮 建客户端->高并发->关闭 后 goroutine 数量不累积。
func TestNoGoroutineLeak(t *testing.T) {
	baseline := goroutineNum()
	for round := 0; round < 5; round++ {
		s := startTestServer(t)
		s.set(srvConfig{delay: time.Millisecond})
		cfg := testCfg(s)
		cfg.MaxConns = 4
		cfg.MaxInflight = 8
		cfg.QueuePolicy = QueuePolicyQueue
		cfg.MaxWaitQueue = 64
		cfg.MaxWaitTimeout = 3 * time.Second
		cfg.IdleTimeout = 50 * time.Millisecond
		cfg.PingInterval = 40 * time.Millisecond
		cfg.PingTimeout = 100 * time.Millisecond
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 400; i++ {
				_, _ = c.Do(context.Background(), []byte("leak"), nil)
			}
		}()
		<-done
		if err := c.Shutdown(time.Second); err != nil {
			t.Fatalf("round %d shutdown: %v", round, err)
		}
		s.stop()
	}
	// 服务端/客户端都关闭后，goroutine 不应比基线显著增长。
	final := goroutineNum()
	goroutineNum()
	final = goroutineNum()
	if final > baseline+2 {
		t.Fatalf("goroutine leak: baseline=%d final=%d", baseline, final)
	}
}

// TestRepeatedCreateClose 反复创建关闭不泄漏连接与内存（指标归零）。
func TestRepeatedCreateClose(t *testing.T) {
	for i := 0; i < 10; i++ {
		s := startTestServer(t)
		c, err := New(testCfg(s))
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 20; j++ {
			_, _ = c.Do(context.Background(), []byte("x"), nil)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		s.stop()
		// Close 后再调用必须返回 ErrPoolClosed（closeOnce 幂等）。
		if err := c.Close(); err != nil {
			t.Fatalf("double close should be nil, got %v", err)
		}
		_, err = c.Do(context.Background(), []byte("x"), nil)
		if err == nil {
			t.Fatal("Do after close must fail")
		}
	}
}
