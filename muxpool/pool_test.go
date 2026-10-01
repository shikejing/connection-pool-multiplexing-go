package muxpool_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shikejing/connection-pool-multiplexing-go/muxpool"
	"github.com/shikejing/connection-pool-multiplexing-go/testserver"
)

// newTestPool 创建连接到测试服务器的连接池。
func newTestPool(t *testing.T, cfg muxpool.Config, srv *testserver.Server) *muxpool.Pool {
	t.Helper()
	return muxpool.NewPool(cfg, muxpool.TCPDialer(srv.Addr()))
}

func echoHandler(_ context.Context, req []byte) ([]byte, error) {
	return append([]byte("echo:"), req...), nil
}

// TestMultiplexOutOfOrder 验证乱序响应按请求标识正确配对。
func TestMultiplexOutOfOrder(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithReverseOrder(), testserver.WithDelay(50*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 10
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 1})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("req-%d", i))
			resp, err := client.Do(context.Background(), payload)
			if err != nil {
				t.Errorf("req-%d: %v", i, err)
				return
			}
			want := "echo:" + string(payload)
			if string(resp) != want {
				t.Errorf("req-%d: got %q, want %q", i, resp, want)
			}
		}(i)
	}
	wg.Wait()
}

// TestConnDropAfter 验证连接中途失效时在途请求得到一致结论，幂等请求可重试。
func TestConnDropAfter(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDropAfter(2))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 10
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	// 幂等客户端：连接失效后应在新连接上重试成功。
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())
	for i := 0; i < 6; i++ {
		resp, err := client.Do(context.Background(), []byte(fmt.Sprintf("r-%d", i)))
		if err != nil {
			t.Fatalf("req-%d: %v", i, err)
		}
		want := fmt.Sprintf("echo:r-%d", i)
		if string(resp) != want {
			t.Fatalf("req-%d: got %q, want %q", i, resp, want)
		}
	}
	st := pool.Stats()
	if st.DeadTotal < 1 {
		t.Fatalf("DeadTotal = %d, want >= 1", st.DeadTotal)
	}
}

// TestConnDropAfterNonIdempotent 验证非幂等请求失败不重放。
func TestConnDropAfterNonIdempotent(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDropAfter(0))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 3, Idempotent: false})
	_, err := client.Do(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("expected error for non-idempotent request on dead conn")
	}
}

// TestHalfClose 验证半关闭后在途请求继续完成。
func TestHalfClose(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithHalfClose(), testserver.WithDelay(30*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	sess, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 并发发送多个请求，然后半关闭。
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := sess.Do(context.Background(), []byte(fmt.Sprintf("h-%d", i)))
			if err != nil {
				t.Errorf("h-%d: %v", i, err)
				return
			}
			if string(resp) != fmt.Sprintf("echo:h-%d", i) {
				t.Errorf("h-%d: got %q", i, resp)
			}
		}(i)
	}
	// 等待请求发出后再半关闭。
	time.Sleep(10 * time.Millisecond)
	if err := sess.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	pool.Release(sess)
}

// TestBackpressureQueueFull 验证等待队列满时行为确定（ErrQueueFull）。
func TestBackpressureQueueFull(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(200*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 1
	cfg.WaitQueueDepth = 2
	cfg.WaitTimeout = 5 * time.Second
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	// 占用唯一连接。
	holder, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = holder.Do(context.Background(), []byte("hold"))
	}()
	time.Sleep(20 * time.Millisecond)

	// 2 个排队，其余应被拒绝。
	var wg sync.WaitGroup
	var rejected int64
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pool.Acquire(context.Background())
			if errors.Is(err, muxpool.ErrQueueFull) {
				atomic.AddInt64(&rejected, 1)
			}
		}()
	}
	wg.Wait()
	if rejected != 4 {
		t.Fatalf("rejected = %d, want 4", rejected)
	}
}

// TestBackpressureWaitTimeout 验证等待超时返回 ErrWaitTimeout。
func TestBackpressureWaitTimeout(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(500*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 1
	cfg.WaitTimeout = 100 * time.Millisecond
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	holder, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = holder.Do(context.Background(), []byte("hold")) }()
	time.Sleep(20 * time.Millisecond)

	_, err = pool.Acquire(context.Background())
	if !errors.Is(err, muxpool.ErrWaitTimeout) {
		t.Fatalf("err = %v, want ErrWaitTimeout", err)
	}
}

// TestCancel 验证取消不影响同连接上的其他请求。
func TestCancel(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(100*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 5
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 1})

	// 一个会被取消的请求。
	cancelCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := client.Do(cancelCtx, []byte("canceled"))
		if !errors.Is(err, muxpool.ErrRequestCanceled) {
			t.Errorf("err = %v, want ErrRequestCanceled", err)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	// 同连接上的其他请求应正常完成。
	resp, err := client.Do(context.Background(), []byte("ok"))
	if err != nil {
		t.Fatalf("other request failed: %v", err)
	}
	if string(resp) != "echo:ok" {
		t.Fatalf("resp = %q", resp)
	}
}

// TestTimeout 验证请求超时返回 ErrTimeout。
func TestTimeout(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(300*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.RequestTimeout = 100 * time.Millisecond
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 1})

	_, err := client.Do(context.Background(), []byte("slow"))
	if !errors.Is(err, muxpool.ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}

// TestGracefulShutdown 验证优雅关闭时在途请求完成、无悬挂等待方。
func TestGracefulShutdown(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(100*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.MaxInFlightPerConn = 5
	cfg.CloseTimeout = 2 * time.Second
	pool := newTestPool(t, cfg, srv)
	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 1})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = client.Do(context.Background(), []byte(fmt.Sprintf("g-%d", i)))
		}(i)
	}
	time.Sleep(20 * time.Millisecond)

	// 优雅关闭：在途请求应完成。
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// 关闭后新请求应被拒绝。
	_, err := client.Do(context.Background(), []byte("after"))
	if !errors.Is(err, muxpool.ErrPoolClosed) {
		t.Fatalf("err = %v, want ErrPoolClosed", err)
	}
}

// TestRemoveConn 验证主动摘除异常连接后不再复用。
func TestRemoveConn(t *testing.T) {
	srv := testserver.New(echoHandler)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 2
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()

	sess, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Do(context.Background(), []byte("a")); err != nil {
		t.Fatal(err)
	}
	// 主动摘除。
	pool.Remove(sess, muxpool.ErrBadConn)
	if !sess.IsDead() {
		t.Fatal("session should be dead after Remove")
	}
	st := pool.Stats()
	if st.Conns != 0 {
		t.Fatalf("Conns = %d, want 0", st.Conns)
	}
}

// TestStatsNoUnboundedGrowth 验证长时间高并发下指标无界增长。
func TestStatsNoUnboundedGrowth(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithDelay(5*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 4
	cfg.MaxInFlightPerConn = 10
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	for i := 0; i < 200; i++ {
		if _, err := client.Do(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	st := pool.Stats()
	if st.Conns > int64(cfg.MaxConns) {
		t.Fatalf("Conns = %d, want <= %d", st.Conns, cfg.MaxConns)
	}
	if st.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", st.InFlight)
	}
}

// TestServerErrorResponse 验证服务端错误响应不被重试。
func TestServerErrorResponse(t *testing.T) {
	srv := testserver.New(echoHandler, testserver.WithErrorRate(1.0))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	_, err := client.Do(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("expected error from server")
	}
}

// TestBadFrameNotReused 验证协议错位（坏帧）的连接被摘除且不再复用。
func TestBadFrameNotReused(t *testing.T) {
	// 原始 TCP 服务器：第一个连接发送坏帧，后续连接正常回显。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var connCount int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// 读取请求帧。
				hdr := make([]byte, 16)
				if _, err := io.ReadFull(c, hdr); err != nil {
					return
				}
				payloadLen := binary.BigEndian.Uint32(hdr[12:16])
				payload := make([]byte, payloadLen)
				if _, err := io.ReadFull(c, payload); err != nil {
					return
				}
				if atomic.AddInt32(&connCount, 1) == 1 {
					// 第一个连接：发送坏帧（错误 magic）。
					_, _ = c.Write([]byte{0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
					return
				}
				// 后续连接：正常回显合法帧。
				_ = muxpool.EncodeFrame(c, &muxpool.Frame{
					ID:      binary.BigEndian.Uint64(hdr[4:12]),
					Flags:   muxpool.FlagResponse,
					Payload: append([]byte("echo:"), payload...),
				})
			}(conn)
		}
	}()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	pool := muxpool.NewPool(cfg, muxpool.TCPDialer(ln.Addr().String()))
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	resp, err := client.Do(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if string(resp) != "echo:x" {
		t.Fatalf("resp = %q", resp)
	}
	// 坏连接应被摘除。
	st := pool.Stats()
	if st.DeadTotal < 1 {
		t.Fatalf("DeadTotal = %d, want >= 1", st.DeadTotal)
	}
}

// TestIdleEviction 验证空闲连接按策略淘汰。
func TestIdleEviction(t *testing.T) {
	srv := testserver.New(echoHandler)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 4
	cfg.MaxInFlightPerConn = 1
	cfg.MaxIdleConns = 1
	cfg.MinIdleConns = 0
	cfg.MaxIdleTime = 100 * time.Millisecond
	pool := newTestPool(t, cfg, srv)
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.RetryPolicy{MaxAttempts: 1})

	// 建立 4 条连接（每连接在途上限 1，确保并发请求各自建连）。
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.Do(context.Background(), []byte("x"))
		}()
	}
	wg.Wait()
	if pool.Stats().Conns != 4 {
		t.Fatalf("Conns = %d, want 4", pool.Stats().Conns)
	}

	// 等待空闲淘汰。
	time.Sleep(400 * time.Millisecond)
	st := pool.Stats()
	if st.Conns > 1 {
		t.Fatalf("Conns = %d, want <= 1 after eviction", st.Conns)
	}
	if st.EvictedTotal < 3 {
		t.Fatalf("EvictedTotal = %d, want >= 3", st.EvictedTotal)
	}
}

// TestKeepAlive 验证保活探测能发现静默死亡的连接。
func TestKeepAlive(t *testing.T) {
	// 服务器：接受连接后静默关闭（不响应保活）。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// 静默关闭，不响应任何帧。
		_ = conn.Close()
	}()

	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 1
	cfg.KeepAliveInterval = 100 * time.Millisecond
	cfg.KeepAliveTimeout = 100 * time.Millisecond
	pool := muxpool.NewPool(cfg, muxpool.TCPDialer(ln.Addr().String()))
	defer pool.Close()

	// 手动获取一个会话（不发请求），等待保活探测发现其死亡。
	sess, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(sess)

	time.Sleep(500 * time.Millisecond)
	st := pool.Stats()
	if st.DeadTotal < 1 {
		t.Fatalf("DeadTotal = %d, want >= 1 (keepalive should detect)", st.DeadTotal)
	}
}

// TestCloseIdempotent 验证 Close 可安全重复调用。
func TestCloseIdempotent(t *testing.T) {
	srv := testserver.New(echoHandler)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	pool := newTestPool(t, muxpool.DefaultConfig(), srv)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

// TestAcquireAfterClose 验证关闭后 Acquire 返回 ErrPoolClosed。
func TestAcquireAfterClose(t *testing.T) {
	srv := testserver.New(echoHandler)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	pool := newTestPool(t, muxpool.DefaultConfig(), srv)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Acquire(context.Background())
	if !errors.Is(err, muxpool.ErrPoolClosed) {
		t.Fatalf("err = %v, want ErrPoolClosed", err)
	}
}
