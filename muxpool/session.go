package muxpool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// response 是一次请求的异步结果。
type response struct {
	payload []byte
	err     error
}

// pending 是一个在途请求的等待句柄。
type pending struct {
	id    uint64
	resp  chan *response
	ctx   context.Context
	added time.Time
}

// Session 是一条承载多路复用请求的连接会话。
// 多个 goroutine 可并发调用 Do，响应按请求标识正确配对。
type Session struct {
	conn net.Conn
	cfg  Config

	mu         sync.Mutex
	closed     bool
	dead       bool
	halfClosed bool

	nextID   uint64
	inflight map[uint64]*pending
	reserved int // 已被 Acquire 预留、尚未被 Do 消费的槽位

	writeMu sync.Mutex

	done     chan struct{}
	created  time.Time
	lastUsed time.Time

	wg sync.WaitGroup // 跟踪在途 Do，用于优雅关闭

	pongCh chan struct{} // 保活回应通道

	onAvailable func()             // 在途数下降时回调（供连接池唤醒等待者）
	onDead      func(reason error) // 会话死亡时回调（供连接池摘除）
}

// newSession 在已建立的连接上创建会话并启动读循环。
func newSession(conn net.Conn, cfg Config) *Session {
	cfg.normalize()
	s := &Session{
		conn:     conn,
		cfg:      cfg,
		inflight: make(map[uint64]*pending),
		done:     make(chan struct{}),
		created:  time.Now(),
		lastUsed: time.Now(),
		pongCh:   make(chan struct{}, 1),
	}
	go s.readLoop()
	return s
}

// Do 在该会话上发送一个请求并等待响应。
// 多个 goroutine 可并发调用；在途请求数受 MaxInFlightPerConn 约束。
func (s *Session) Do(ctx context.Context, payload []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 应用默认请求超时（不缩短调用方已有的更短 deadline）。
	if s.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.RequestTimeout)
		defer cancel()
	}

	id := s.allocateID()
	p := &pending{
		id:    id,
		resp:  make(chan *response, 1),
		ctx:   ctx,
		added: time.Now(),
	}

	s.mu.Lock()
	if s.dead || s.closed {
		s.mu.Unlock()
		s.unreserve()
		return nil, ErrSessionClosed
	}
	if s.halfClosed {
		s.mu.Unlock()
		s.unreserve()
		return nil, ErrHalfClosed
	}
	if s.reserved > 0 {
		// 消费 Acquire 时预留的槽位。
		s.reserved--
	} else if s.cfg.MaxInFlightPerConn > 0 && len(s.inflight) >= s.cfg.MaxInFlightPerConn {
		s.mu.Unlock()
		return nil, ErrInFlightLimit
	}
	s.inflight[id] = p
	s.wg.Add(1)
	s.lastUsed = time.Now()
	s.mu.Unlock()

	// 清理：无论从哪个返回路径退出，都注销在途并通知池层容量释放。
	defer func() {
		s.wg.Done()
		s.unregister(id)
		if s.onAvailable != nil {
			s.onAvailable()
		}
	}()

	// 串行写入请求帧。
	if err := s.writeFrame(&Frame{ID: id, Flags: FlagRequest, Payload: payload}); err != nil {
		s.markDead(err)
		return nil, &RequestError{ID: id, Err: err, Retryable: true}
	}

	select {
	case resp := <-p.resp:
		if resp.err != nil {
			return nil, &RequestError{ID: id, Err: resp.err, Retryable: false}
		}
		return resp.payload, nil
	case <-ctx.Done():
		//  Best effort 通知对端取消（请求可能已被处理，故取消不可重试）。
		_ = s.writeFrame(&Frame{ID: id, Flags: FlagRequest | FlagStreamEnd})
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, &RequestError{ID: id, Err: ErrTimeout, Retryable: false}
		}
		return nil, &RequestError{ID: id, Err: ErrRequestCanceled, Retryable: false}
	case <-s.done:
		return nil, &RequestError{ID: id, Err: ErrSessionDead, Retryable: true}
	}
}

// CloseWrite 半关闭：通知对端本方向不再发送新请求，已有在途请求继续等待响应。
func (s *Session) CloseWrite() error {
	s.mu.Lock()
	if s.halfClosed {
		s.mu.Unlock()
		return nil
	}
	s.halfClosed = true
	s.mu.Unlock()
	return s.writeFrame(&Frame{ID: 0, Flags: FlagStreamEnd})
}

// Close 优雅关闭会话：停止接受新请求，等待在途请求完成（或超时），再关闭连接。
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// 通知对端本方向不再发送。
	_ = s.CloseWrite()

	// 等待在途请求完成，带超时。
	drained := make(chan struct{})
	go func() { s.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(s.cfg.CloseTimeout):
	}

	s.markDead(ErrSessionClosed)
	return nil
}

// closeNow 立即关闭并让所有在途请求得到一致结论。
func (s *Session) closeNow(reason error) {
	s.markDead(reason)
}

// Done 返回会话生命周期通道，会话死亡或关闭时被关闭。
func (s *Session) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// InFlight 返回当前在途请求数（含已预留未消费的槽位）。
func (s *Session) InFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight) + s.reserved
}

// reserve 预留一个槽位（Acquire 时调用），受在途上限约束。
func (s *Session) reserve() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead || s.closed {
		return ErrSessionClosed
	}
	if s.halfClosed {
		return ErrHalfClosed
	}
	if s.cfg.MaxInFlightPerConn > 0 && len(s.inflight)+s.reserved >= s.cfg.MaxInFlightPerConn {
		return ErrInFlightLimit
	}
	s.reserved++
	return nil
}

// unreserve 释放一个预留槽位（Do 未消费时由 Release 调用）。
func (s *Session) unreserve() {
	s.mu.Lock()
	if s.reserved > 0 {
		s.reserved--
	}
	s.mu.Unlock()
}

// consume 将一个预留槽位转为在途请求（Do 注册时调用）。
func (s *Session) consume(id uint64, p *pending) {
	s.mu.Lock()
	if s.reserved > 0 {
		s.reserved--
	}
	s.inflight[id] = p
	s.wg.Add(1)
	s.lastUsed = time.Now()
	s.mu.Unlock()
}

// IsDead 报告会话是否已失效。
func (s *Session) IsDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

// Age 返回会话已存活时长。
func (s *Session) Age() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.created)
}

// IdleTime 返回会话空闲时长。
func (s *Session) IdleTime() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastUsed)
}

// 读循环：从连接读取帧并按标识分发。
func (s *Session) readLoop() {
	for {
		f, err := DecodeFrame(s.conn, s.cfg.MaxFrameSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.handlePeerEOF()
			} else {
				// 协议错位/帧非法：连接不可再复用。
				s.markDead(err)
			}
			return
		}
		s.dispatch(f)
	}
}

// handlePeerEOF 处理对端关闭写侧（TCP FIN 或 stream-end 帧）。
func (s *Session) handlePeerEOF() {
	s.mu.Lock()
	pending := s.inflight
	s.mu.Unlock()
	if len(pending) == 0 {
		s.markDead(io.EOF)
		return
	}
	// 对端不再返回响应，在途请求无法完成 -> 一致失败。
	s.markDead(ErrSessionDead)
}

// dispatch 处理一帧。
func (s *Session) dispatch(f *Frame) {
	if f.Flags&FlagPong != 0 {
		select {
		case s.pongCh <- struct{}{}:
		default:
		}
		return
	}
	if f.Flags&FlagPing != 0 {
		// 服务端不应收到 ping；若收到则回应 pong。
		_ = s.writeFrame(&Frame{ID: f.ID, Flags: FlagPong})
		return
	}
	if f.Flags&FlagStreamEnd != 0 && f.ID == 0 {
		s.handlePeerEOF()
		return
	}

	s.mu.Lock()
	p := s.inflight[f.ID]
	s.mu.Unlock()
	if p == nil {
		// 已取消/已移除请求的迟到响应，丢弃。
		return
	}
	switch {
	case f.Flags&FlagError != 0:
		select {
		case p.resp <- &response{err: errors.New(string(f.Payload))}:
		default:
		}
	default:
		select {
		case p.resp <- &response{payload: f.Payload}:
		default:
		}
	}
}

// failAll 让所有在途请求得到一致结论。
func (s *Session) failAll(reason error) {
	s.mu.Lock()
	ps := s.inflight
	s.inflight = make(map[uint64]*pending)
	s.mu.Unlock()
	for _, p := range ps {
		select {
		case p.resp <- &response{err: reason}:
		default:
		}
	}
}

// markDead 标记会话死亡，唤醒所有在途请求，并关闭底层连接。
func (s *Session) markDead(reason error) {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	s.dead = true
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	s.failAll(reason)
	_ = s.conn.Close()
	if s.onDead != nil {
		s.onDead(reason)
	}
}

// allocateID 分配下一个请求标识（从 1 开始，0 保留给保活/控制帧）。
func (s *Session) allocateID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	if s.nextID == 0 {
		s.nextID = 1
	}
	return s.nextID
}

// unregister 移除一个在途请求。
func (s *Session) unregister(id uint64) {
	s.mu.Lock()
	delete(s.inflight, id)
	s.mu.Unlock()
}

// writeFrame 串行写入一帧。
func (s *Session) writeFrame(f *Frame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.DialTimeout))
	return EncodeFrame(s.conn, f)
}

// ping 发送保活探测并等待回应。
func (s *Session) ping(timeout time.Duration) error {
	if err := s.writeFrame(&Frame{ID: 0, Flags: FlagPing}); err != nil {
		return err
	}
	select {
	case <-s.pongCh:
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	case <-s.done:
		return ErrSessionDead
	}
}
