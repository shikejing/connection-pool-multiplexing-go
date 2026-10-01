package muxpool

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type connState int

const (
	connStateActive connState = iota
	connStateClosed
)

// errSlowRead 表示在途请求读取超时（慢读取背压），连接整体失效。
var errSlowRead = errors.New("muxpool: read deadline exceeded while stream in flight")

// stream 是一条连接上的一个在途逻辑请求。
//
// 流的终态（结果或取消）通过 reply 通道（cap=1）恰好传递一次；
// 槽位释放（inflight--、移交等待者/回收连接）也恰好发生一次，
// 由 deliver/cancel/shutdown 三条路径竞争 finishStream 完成。
type stream struct {
	id    uint64
	reply chan streamResult
}

// streamResult 是读循环派发给流的确定结论（成功响应或失败原因）。
type streamResult struct {
	payload []byte
	err     error
}

// muxConn 在单条底层 Conn 上承载多路复用。
//
// 锁序固定为 pool.mu -> mc.mu（池在持锁时调用 reserve）；
// mc.writeMu 独立，仅串行化写帧。
type muxConn struct {
	raw  Conn
	pool *pool
	id   uint64

	createdAt  time.Time
	lastUsedNs atomic.Int64

	mu       sync.Mutex
	state    connState
	streams  map[uint64]*stream
	nextID   uint64
	inflight int
	drained  bool // 半关闭：不再接受新流，在途流仍可收到响应
	dirty    bool // 协议错位/残留数据：永不复用
	// canceled 记录本连接上已被调用方取消的流 ID（有界），用于安全丢弃迟到响应。
	canceled map[uint64]struct{}
	closeErr error

	writeMu sync.Mutex

	pongMu sync.Mutex
	pongCh chan struct{}

	closeOnce sync.Once
	done      chan struct{}
}

func newMuxConn(raw Conn, p *pool, id uint64) *muxConn {
	mc := &muxConn{
		raw:       raw,
		pool:      p,
		id:        id,
		createdAt: time.Now(),
		streams:   make(map[uint64]*stream),
		canceled:  make(map[uint64]struct{}),
		done:      make(chan struct{}),
	}
	mc.lastUsedNs.Store(mc.createdAt.UnixNano())
	p.wg.Add(2)
	go mc.readLoop()
	go mc.keepAliveLoop()
	return mc
}

func (mc *muxConn) touch() { mc.lastUsedNs.Store(time.Now().UnixNano()) }

func (mc *muxConn) idleFor() time.Duration {
	return time.Since(time.Unix(0, mc.lastUsedNs.Load()))
}

func (mc *muxConn) age() time.Duration { return time.Since(mc.createdAt) }

// reserve 占用一个在途槽位并登记新流。ok=false 表示不可用或容量已满。
// 与 finishStream 互斥：新流不可能顶替一个尚未终态的流 ID。
func (mc *muxConn) reserve() (s *stream, ok bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.state != connStateActive || mc.drained || mc.dirty ||
		mc.inflight >= mc.pool.cfg.MaxInflight {
		return nil, false
	}
	mc.nextID++
	s = &stream{id: mc.nextID, reply: make(chan streamResult, 1)}
	mc.streams[s.id] = s
	mc.inflight++
	mc.touch()
	mc.pool.metrics.inflight.Add(1)
	return s, true
}

// start 写出请求帧。写失败即拆除整条连接（流由 shutdown 统一失败）。
func (mc *muxConn) start(s *stream, payload []byte) error {
	if err := mc.writeFrame(frame{typ: frameRequest, streamID: s.id, payload: payload},
		mc.pool.cfg.WriteTimeout); err != nil {
		mc.fail(errors.Join(ErrConnClosed, err))
		return errors.Join(ErrConnClosed, err)
	}
	mc.touch()
	mc.armReadDeadline()
	return nil
}

// cancel 处理调用方取消。若流仍在途（响应尚未到达），则终态化本流、
// 尽力发送取消帧并释放槽位；取消仅影响本流，不影响同连接其他请求。
// 返回 true 表示确为调用方取消（调用方将收到 ErrStreamCanceled）。
func (mc *muxConn) cancel(s *stream) {
	if mc.finishStream(s) {
		mc.noteCanceled(s.id)
		// 本流此前仍在途：告知对端停止（失败只影响连接探活，不影响取消结论）。
		_ = mc.writeFrame(frame{typ: frameCancel, streamID: s.id}, mc.pool.cfg.WriteTimeout)
		mc.pool.onSlotFreed(mc)
	}
}

// finishStream 将一个仍在途的流终态化：从 map 删除并 inflight--。
// 返回 false 表示该流已有终态（响应已由读循环处理，或连接整体关闭已接管），
// 调用方不得再释放槽位或重复发送结论。持/取 mc.mu。
func (mc *muxConn) finishStream(s *stream) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if _, ok := mc.streams[s.id]; !ok {
		return false
	}
	delete(mc.streams, s.id)
	mc.inflight--
	mc.touch()
	mc.pool.metrics.inflight.Add(-1)
	return true
}

// noteCanceled 有界记录已取消流 ID；超过 2*MaxInflight 则整体清空。
// 清空后若该连接再收到对应迟到响应会被判脏——这是保守且安全的边界，
// 正常情况下取消帧已让对端停止响应，不会命中。
func (mc *muxConn) noteCanceled(id uint64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.canceled) >= 2*mc.pool.cfg.MaxInflight {
		mc.canceled = make(map[uint64]struct{})
	}
	mc.canceled[id] = struct{}{}
}

// inflightCount 返回当前在途数。
func (mc *muxConn) inflightCount() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.inflight
}

func (mc *muxConn) readLoop() {
	defer mc.pool.wg.Done()
	for {
		f, err := decodeFrame(mc.raw)
		if err != nil {
			mc.onReadError(err)
			return
		}
		if mc.handleFrame(f) {
			return
		}
	}
}

// handleFrame 处理一帧；返回 true 表示读循环应退出。
func (mc *muxConn) handleFrame(f frame) bool {
	switch f.typ {
	case framePong:
		mc.pongMu.Lock()
		ch := mc.pongCh
		mc.pongMu.Unlock()
		if ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		mc.armReadDeadline()
		return false
	case framePing:
		_ = mc.writeFrame(frame{typ: framePong}, mc.pool.cfg.WriteTimeout)
		return false
	case frameResponse:
		mc.deliverResponse(f)
		return false
	case frameRequest, frameCancel:
		mc.poison(errors.Join(ErrProtocol, errors.New("unexpected frame type on client conn")))
		return true
	default:
		mc.poison(errors.Join(ErrProtocol, errors.New("unknown frame type")))
		return true
	}
}

func (mc *muxConn) deliverResponse(f frame) {
	if f.flags&flagHalfClose != 0 {
		mc.mu.Lock()
		mc.drained = true
		mc.mu.Unlock()
	}
	res := streamResult{}
	if f.flags&flagError != 0 {
		se := decodeServerError(f.payload)
		se.Retryable = f.flags&flagRetryable != 0
		res.err = se
	} else {
		res.payload = f.payload
	}

	mc.mu.Lock()
	s, ok := mc.streams[f.streamID]
	if !ok {
		// 未知流的响应帧：调用方已取消则属于正常迟到响应，丢弃即可；
		// 否则说明帧与本地流状态错位，连接带毒，绝不复用。
		_, canceled := mc.canceled[f.streamID]
		if !canceled {
			mc.dirty = true
		}
		mc.mu.Unlock()
		if !canceled {
			mc.poison(errors.Join(ErrProtocol, errors.New("response for unknown stream")))
			return
		}
		// 迟到响应：安全丢弃。连接仍然干净可复用。
		mc.armReadDeadline()
		return
	}
	delete(mc.streams, f.streamID)
	mc.inflight--
	remaining := mc.inflight
	willDrain := mc.drained
	mc.touch()
	mc.pool.metrics.inflight.Add(-1)
	mc.mu.Unlock()

	// cap=1：入队不阻塞读循环。若调用方恰在此刻取消，缓冲保证结果不丢，
	// 调用方通过 finishStream 的 false 返回值得知响应已先到，不会误杀。
	s.reply <- res

	if willDrain && remaining == 0 {
		// 半关闭连接的最后一个在途流结束：优雅回收，不再复用。
		mc.evict(ErrConnClosed)
		return
	}
	mc.armReadDeadline()
	mc.pool.onSlotFreed(mc)
}

func (mc *muxConn) onReadError(err error) {
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		if mc.inflightCount() == 0 {
			// 空闲连接读截止到期：直接作为空闲回收（由 reaper 也会处理，这里兜底）。
			mc.evict(ErrConnClosed)
			return
		}
		mc.pool.metrics.timeouts.Add(1)
		mc.fail(errSlowRead)
	case errors.Is(err, io.EOF):
		mc.fail(ErrConnClosed)
	case errors.Is(err, ErrProtocol), errors.Is(err, ErrFrameTooLarge):
		mc.poison(err)
	default:
		mc.fail(errors.Join(ErrConnClosed, err))
	}
}

// armReadDeadline 仅在存在在途流时给慢读取上保险；空闲时不设限（保活负责探活）。
func (mc *muxConn) armReadDeadline() {
	n := mc.inflightCount()
	if n > 0 && mc.pool.cfg.ReadTimeout > 0 {
		_ = mc.raw.SetReadDeadline(time.Now().Add(mc.pool.cfg.ReadTimeout))
	} else {
		_ = mc.raw.SetReadDeadline(time.Time{})
	}
}

func (mc *muxConn) writeFrame(f frame, timeout time.Duration) error {
	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()
	if timeout > 0 {
		_ = mc.raw.SetWriteDeadline(time.Now().Add(timeout))
	} else {
		_ = mc.raw.SetWriteDeadline(time.Time{})
	}
	buf, err := encodeFrame(f, nil)
	if err != nil {
		return err
	}
	for len(buf) > 0 {
		n, werr := mc.raw.Write(buf)
		if werr != nil {
			return werr
		}
		buf = buf[n:]
	}
	return nil
}

func (mc *muxConn) keepAliveLoop() {
	defer mc.pool.wg.Done()
	interval := mc.pool.cfg.PingInterval
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-mc.done:
			return
		case <-t.C:
		}
		if mc.isClosed() {
			return
		}
		// 有在途流时读截止时间已在兜底，空闲时才主动 ping。
		if mc.inflightCount() > 0 {
			continue
		}
		if err := mc.ping(); err != nil {
			mc.fail(errors.Join(ErrConnClosed, err))
			return
		}
	}
}

func (mc *muxConn) ping() error {
	timeout := mc.pool.cfg.PingTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ch := make(chan struct{}, 1)
	mc.pongMu.Lock()
	mc.pongCh = ch
	mc.pongMu.Unlock()
	defer func() {
		mc.pongMu.Lock()
		mc.pongCh = nil
		mc.pongMu.Unlock()
	}()
	if err := mc.writeFrame(frame{typ: framePing}, timeout); err != nil {
		return err
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-t.C:
		return errors.New("muxpool: ping timeout")
	case <-mc.done:
		return ErrConnClosed
	}
}

// markDraining 半关闭：不再接受新流。
func (mc *muxConn) markDraining() {
	mc.mu.Lock()
	mc.drained = true
	mc.mu.Unlock()
}

// poison 标记连接带毒并拆除：协议错位/残留数据的连接绝不复用。
func (mc *muxConn) poison(err error) {
	mc.mu.Lock()
	mc.dirty = true
	mc.mu.Unlock()
	mc.fail(err)
}

// evict 用于空闲/寿命/主动回收，计入摘除指标。
func (mc *muxConn) evict(err error) { mc.shutdown(err, false) }

// fail 用于对端关闭/IO 错误等被动失效，计入连接失效指标。
func (mc *muxConn) fail(err error) { mc.shutdown(err, true) }

// shutdown 关闭连接并让其上所有仍在途的流得到同一结论；每个流恰好一次。
// 已经终态（响应先到或已取消）的流不在 streams 中，不会被重复结论。
func (mc *muxConn) shutdown(err error, failed bool) {
	mc.mu.Lock()
	if mc.state == connStateClosed {
		mc.mu.Unlock()
		return
	}
	mc.state = connStateClosed
	mc.closeErr = err
	streams := mc.streams
	mc.streams = make(map[uint64]*stream)
	mc.inflight = 0
	mc.pool.metrics.inflight.Add(-int64(len(streams)))
	mc.mu.Unlock()

	mc.closeOnce.Do(func() { close(mc.done) })
	_ = mc.raw.Close()

	for _, s := range streams {
		select {
		case s.reply <- streamResult{err: err}:
		default: // 调用方可能已放弃读取（缓冲已满）；不阻塞关闭、不泄漏协程。
		}
	}
	mc.pool.removeConn(mc, failed)
}

func (mc *muxConn) isClosed() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.state == connStateClosed
}
