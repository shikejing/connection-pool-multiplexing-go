package muxpool

import (
	"errors"
	"fmt"
)

// 连接池相关错误。
var (
	// ErrPoolClosed 连接池已关闭。
	ErrPoolClosed = errors.New("muxpool: pool closed")
	// ErrQueueFull 等待队列已满（背压触发）。
	ErrQueueFull = errors.New("muxpool: wait queue full")
	// ErrWaitTimeout 等待连接/会话超时。
	ErrWaitTimeout = errors.New("muxpool: wait timed out")
	// ErrInFlightLimit 单连接在途请求数达到上限。
	ErrInFlightLimit = errors.New("muxpool: in-flight limit reached")
	// ErrSessionDead 会话已失效（连接中断或被对端回收）。
	ErrSessionDead = errors.New("muxpool: session dead")
	// ErrSessionClosed 会话已被显式关闭。
	ErrSessionClosed = errors.New("muxpool: session closed")
	// ErrBadConn 连接状态异常（残留数据/协议错位），不可复用。
	ErrBadConn = errors.New("muxpool: bad connection")
	// ErrRequestCanceled 请求被调用方取消。
	ErrRequestCanceled = errors.New("muxpool: request canceled")
	// ErrTimeout 请求超时。
	ErrTimeout = errors.New("muxpool: request timed out")
	// ErrHalfClosed 会话处于半关闭状态，不再接受新请求。
	ErrHalfClosed = errors.New("muxpool: session half-closed")
)

// RequestError 描述一次请求的失败，携带是否可重试信息。
type RequestError struct {
	ID        uint64 // 请求标识
	Err       error  // 底层错误
	Retryable bool   // 是否可安全重试（不会导致副作用重复）
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("muxpool: request %d failed: %v (retryable=%v)", e.ID, e.Err, e.Retryable)
}

func (e *RequestError) Unwrap() error { return e.Err }

// IsRetryable 判断错误是否可安全重试。
func IsRetryable(err error) bool {
	var re *RequestError
	if errors.As(err, &re) {
		return re.Retryable
	}
	return errors.Is(err, ErrSessionDead)
}
