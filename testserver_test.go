package muxpool

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// srvConfig 是单连接生效的后端编排快照（连接建立时取一次）。
type srvConfig struct {
	outOrder   bool          // 每 3 个请求逆序响应
	delay      time.Duration // 每个请求处理延迟
	hang       bool          // 收请求后挂起不响应
	closeNow   bool          // 收到请求立即静默断连
	closeAfter bool          // 记录执行后、发响应前断连（副作用已发生、响应丢失）
	halfClose  bool          // 回应后置 halfClose 并关闭读循环
	errCode    uint8         // >0 时返回错误响应
	errRetry   bool          // 错误响应是否标记可重试
	ignorePing bool          // 收到 ping 不回 pong（模拟静默黑洞）
}

// testServer 是一个可编排的本地 TCP 后端，说本包私有帧协议。
type testServer struct {
	ln  net.Listener
	cfg srvConfig

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	execLog []string
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &testServer{ln: ln, conns: map[net.Conn]struct{}{}}
	go s.serve()
	t.Cleanup(s.stop)
	return s
}

func (s *testServer) addr() string { return s.ln.Addr().String() }

func (s *testServer) set(cfg srvConfig) {
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
}

func (s *testServer) snapshot() srvConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *testServer) dialer() Dialer {
	return func() (Conn, error) {
		return net.Dial("tcp", s.addr())
	}
}

func (s *testServer) stop() {
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
	_ = s.ln.Close()
}

func (s *testServer) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// dropConns 模拟对端悄悄回收：直接 RST/FIN 全部现存连接。
func (s *testServer) dropConns() {
	s.mu.Lock()
	cs := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		cs = append(cs, c)
	}
	s.mu.Unlock()
	for _, c := range cs {
		_ = c.Close()
	}
}

func (s *testServer) serve() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.conns == nil {
			s.mu.Unlock()
			_ = c.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(c)
	}
}

func (s *testServer) handleConn(c net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()
	var pending []frame
	for {
		f, err := decodeFrame(c)
		if err != nil {
			return
		}
		cfg := s.snapshot()
		switch f.typ {
		case framePing:
			if cfg.ignorePing {
				continue
			}
			if writeRaw(c, frame{typ: framePong}) != nil {
				return
			}
		case frameCancel:
			// 取消：不再为该流产生响应。
		case frameRequest:
			if cfg.hang {
				continue
			}
			if cfg.closeNow {
				_ = c.Close()
				return
			}
			s.mu.Lock()
			s.execLog = append(s.execLog, string(f.payload))
			s.mu.Unlock()
			if cfg.closeAfter {
				_ = c.Close()
				return
			}
			if cfg.delay > 0 {
				time.Sleep(cfg.delay)
			}
			resp := buildResponse(f, cfg)
			if cfg.halfClose {
				resp.flags |= flagHalfClose
			}
			if cfg.outOrder {
				pending = append(pending, resp)
				if len(pending) == 3 {
					for i := len(pending) - 1; i >= 0; i-- {
						if writeRaw(c, pending[i]) != nil {
							return
						}
					}
					pending = nil
				}
				continue
			}
			if writeRaw(c, resp) != nil {
				return
			}
			if cfg.halfClose {
				return
			}
		}
	}
}

func buildResponse(f frame, cfg srvConfig) frame {
	if cfg.errCode > 0 {
		flags := flagError
		if cfg.errRetry {
			flags |= flagRetryable
		}
		return frame{
			typ:      frameResponse,
			streamID: f.streamID,
			flags:    flags,
			payload:  encodeServerError(cfg.errCode, "boom"),
		}
	}
	return frame{typ: frameResponse, streamID: f.streamID, payload: append([]byte("echo:"), f.payload...)}
}

func (s *testServer) execCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.execLog)
}

func writeRaw(w io.Writer, f frame) error {
	buf, err := encodeFrame(f, nil)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// execCountsFor 返回每个载荷被执行的次数，用于断言重试没有重复副作用。
func (s *testServer) execCountsFor(payloads ...string) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(payloads))
	for _, p := range payloads {
		out[p] = 0
	}
	for _, p := range s.execLog {
		if _, ok := out[p]; ok {
			out[p]++
		}
	}
	return out
}
