// Package testserver 是一个可本地复现的多路复用后端：
// 支持乱序响应、延迟、断连、半关闭、错误注入等行为，用于验证 muxpool。
package testserver

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/shikejing/connection-pool-multiplexing-go/muxpool"
)

// Handler 处理一个请求并返回响应。
type Handler func(ctx context.Context, req []byte) ([]byte, error)

// Server 是测试后端服务器。
type Server struct {
	handler Handler
	addr    string
	ln      net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	delay        time.Duration
	reverseOrder bool
	dropAfter    int
	halfClose    bool
	errRate      float64
}

// Option 配置服务器行为。
type Option func(*Server)

// WithDelay 每个请求处理前的固定延迟。
func WithDelay(d time.Duration) Option {
	return func(s *Server) { s.delay = d }
}

// WithReverseOrder 乱序响应：后到的请求先回（通过随机延迟实现）。
func WithReverseOrder() Option {
	return func(s *Server) { s.reverseOrder = true }
}

// WithDropAfter 处理 N 个请求后断连（不响应在途请求）。
func WithDropAfter(n int) Option {
	return func(s *Server) { s.dropAfter = n }
}

// WithHalfClose 处理完请求后半关闭（发送 stream-end 再关连接）。
func WithHalfClose() Option {
	return func(s *Server) { s.halfClose = true }
}

// WithErrorRate 按比例返回错误响应。
func WithErrorRate(rate float64) Option {
	return func(s *Server) { s.errRate = rate }
}

// New 创建测试服务器。
func New(handler Handler, opts ...Option) *Server {
	s := &Server{
		handler:   handler,
		conns:     make(map[net.Conn]struct{}),
		dropAfter: -1,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Listen 启动监听。
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	s.ln = ln
	s.addr = ln.Addr().String()
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Addr 返回监听地址。
func (s *Server) Addr() string {
	return s.addr
}

// Close 关闭服务器与所有连接。
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	var reqWg sync.WaitGroup
	var handled int64
	maxFrame := uint32(1 << 20)

	for {
		f, err := muxpool.DecodeFrame(conn, maxFrame)
		if err != nil {
			if errors.Is(err, io.EOF) {
				reqWg.Wait()
				if s.halfClose {
					writeMu.Lock()
					_ = muxpool.EncodeFrame(conn, &muxpool.Frame{ID: 0, Flags: muxpool.FlagStreamEnd})
					writeMu.Unlock()
				}
				return
			}
			return
		}

		if f.Flags&muxpool.FlagPing != 0 {
			writeMu.Lock()
			_ = muxpool.EncodeFrame(conn, &muxpool.Frame{ID: f.ID, Flags: muxpool.FlagPong})
			writeMu.Unlock()
			continue
		}

		if f.ID == 0 && f.Flags&muxpool.FlagStreamEnd != 0 {
			reqWg.Wait()
			writeMu.Lock()
			_ = muxpool.EncodeFrame(conn, &muxpool.Frame{ID: 0, Flags: muxpool.FlagStreamEnd})
			writeMu.Unlock()
			return
		}

		if f.Flags&muxpool.FlagRequest == 0 {
			continue
		}

		if s.dropAfter >= 0 {
			s.mu.Lock()
			handled++
			n := handled
			s.mu.Unlock()
			if n > int64(s.dropAfter) {
				return
			}
		}

		reqWg.Add(1)
		go func(reqID uint64, payload []byte) {
			defer reqWg.Done()
			s.processRequest(conn, &writeMu, reqID, payload)
		}(f.ID, f.Payload)
	}
}

func (s *Server) processRequest(conn net.Conn, writeMu *sync.Mutex, reqID uint64, payload []byte) {
	delay := s.delay
	if s.reverseOrder {
		delay = time.Duration(rand.Int63n(int64(s.delay) + 1))
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	var resp *muxpool.Frame
	if s.errRate > 0 && rand.Float64() < s.errRate {
		resp = &muxpool.Frame{ID: reqID, Flags: muxpool.FlagError, Payload: []byte("injected error")}
	} else {
		out, err := s.handler(context.Background(), payload)
		if err != nil {
			resp = &muxpool.Frame{ID: reqID, Flags: muxpool.FlagError, Payload: []byte(err.Error())}
		} else {
			resp = &muxpool.Frame{ID: reqID, Flags: muxpool.FlagResponse, Payload: out}
		}
	}
	writeMu.Lock()
	_ = muxpool.EncodeFrame(conn, resp)
	writeMu.Unlock()
}
