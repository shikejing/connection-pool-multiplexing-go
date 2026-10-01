// Command 演示本地连接复用与请求多路复用。
// 启动一个本地测试后端，然后通过连接池并发发起请求，展示复用、乱序配对与可观测指标。
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/shikejing/connection-pool-multiplexing-go/muxpool"
	"github.com/shikejing/connection-pool-multiplexing-go/testserver"
)

func main() {
	// 1. 启动本地后端（乱序响应 + 每请求延迟）。
	srv := testserver.New(
		func(_ context.Context, req []byte) ([]byte, error) {
			return append([]byte("echo:"), req...), nil
		},
		testserver.WithReverseOrder(),
		testserver.WithDelay(20*time.Millisecond),
	)
	if err := srv.Listen(); err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// 2. 创建连接池：最多 2 条连接，每条连接最多 8 个在途请求。
	cfg := muxpool.DefaultConfig()
	cfg.MaxConns = 2
	cfg.MaxInFlightPerConn = 8
	cfg.WaitQueueDepth = 64
	cfg.WaitTimeout = 5 * time.Second
	pool := muxpool.NewPool(cfg, muxpool.TCPDialer(srv.Addr()))
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	// 3. 并发发起 32 个请求（乱序到达，按 ID 配对）。
	var wg sync.WaitGroup
	ok := 0
	var mu sync.Mutex
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("req-%02d", i))
			resp, err := client.Do(context.Background(), payload)
			if err != nil {
				log.Printf("req-%02d failed: %v", i, err)
				return
			}
			if string(resp) != "echo:"+string(payload) {
				log.Printf("req-%02d mismatch: %q", i, resp)
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// 4. 输出可观测指标。
	st := pool.Stats()
	fmt.Printf("成功请求: %d/32\n", ok)
	fmt.Printf("连接数: %d (空闲: %d, 活跃: %d)\n", st.Conns, st.IdleConns, st.ActiveConns)
	fmt.Printf("在途请求: %d\n", st.InFlight)
	fmt.Printf("复用命中: %d, 复用未命中: %d\n", st.ReuseHits, st.ReuseMisses)
	fmt.Printf("拒绝: %d, 淘汰: %d, 异常死亡: %d\n", st.RejectTotal, st.EvictedTotal, st.DeadTotal)
	fmt.Printf("请求总数: %d, 重试: %d\n", st.RequestsTotal, st.RetriesTotal)
}
