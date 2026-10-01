package muxpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/shikejing/connection-pool-multiplexing-go/muxpool"
	"github.com/shikejing/connection-pool-multiplexing-go/testserver"
)

func TestSmokeBasicRequest(t *testing.T) {
	srv := testserver.New(func(ctx context.Context, req []byte) ([]byte, error) {
		return append([]byte("echo:"), req...), nil
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	pool := muxpool.NewPool(muxpool.DefaultConfig(), muxpool.TCPDialer(srv.Addr()))
	defer pool.Close()

	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())
	resp, err := client.Do(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp) != "echo:hello" {
		t.Fatalf("resp = %q", resp)
	}

	st := pool.Stats()
	if st.Conns != 1 {
		t.Fatalf("conns = %d, want 1", st.Conns)
	}
	if st.ReuseMisses != 1 {
		t.Fatalf("reuseMisses = %d, want 1", st.ReuseMisses)
	}
}

func TestSmokeReuse(t *testing.T) {
	srv := testserver.New(func(ctx context.Context, req []byte) ([]byte, error) {
		return append([]byte("echo:"), req...), nil
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	pool := muxpool.NewPool(muxpool.DefaultConfig(), muxpool.TCPDialer(srv.Addr()))
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	for i := 0; i < 5; i++ {
		if _, err := client.Do(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	st := pool.Stats()
	if st.Conns != 1 {
		t.Fatalf("conns = %d, want 1 (reuse)", st.Conns)
	}
	if st.ReuseHits != 4 {
		t.Fatalf("reuseHits = %d, want 4", st.ReuseHits)
	}
}

func TestSmokeConcurrent(t *testing.T) {
	srv := testserver.New(func(ctx context.Context, req []byte) ([]byte, error) {
		return append([]byte("echo:"), req...), nil
	}, testserver.WithDelay(20*time.Millisecond))
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cfg := muxpool.DefaultConfig()
	cfg.MaxInFlightPerConn = 10
	cfg.MaxConns = 2
	pool := muxpool.NewPool(cfg, muxpool.TCPDialer(srv.Addr()))
	defer pool.Close()
	client := muxpool.NewClient(pool, muxpool.DefaultRetryPolicy())

	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			_, err := client.Do(context.Background(), []byte("x"))
			done <- err
		}()
	}
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	st := pool.Stats()
	if st.Conns > 2 {
		t.Fatalf("conns = %d, want <= 2 (mux)", st.Conns)
	}
}
