package muxpool

import "errors"

// 哨兵错误：调用方可通过 errors.Is 区分各类确定性行为。
var (
	// ErrPoolClosed 客户端/连接池已关闭。
	ErrPoolClosed = errors.New("muxpool: client is closed")
	// ErrQueueFull 拒绝策略下无可用容量，或排队策略下等待队列已满。
	ErrQueueFull = errors.New("muxpool: no connection capacity and queue is full")
	// ErrWaitTimeout 在等待队列中等待可用连接超时。
	ErrWaitTimeout = errors.New("muxpool: timed out waiting for a connection slot")
	// ErrConnClosed 连接已关闭或在请求执行过程中失效。
	ErrConnClosed = errors.New("muxpool: connection closed")
	// ErrStreamCanceled 请求被调用方主动取消。
	ErrStreamCanceled = errors.New("muxpool: request canceled")
	// ErrProtocol 协议错位（非法帧、未知流帧等），连接被标记为脏且不得复用。
	ErrProtocol = errors.New("muxpool: protocol error, connection is poisoned")
	// ErrFrameTooLarge 帧长度超过上限。
	ErrFrameTooLarge = errors.New("muxpool: frame payload exceeds 16MiB")
	// ErrServerRejected 服务端以错误响应帧拒绝了请求。
	ErrServerRejected = errors.New("muxpool: server rejected request")
	// ErrTooManyAttempts 重试次数耗尽（外层包装，最后一次错误被 wrap）。
	ErrTooManyAttempts = errors.New("muxpool: max attempts exhausted")
	// ErrShutdownTimeout 优雅关闭期限内仍有在途请求未结束。
	ErrShutdownTimeout = errors.New("muxpool: graceful shutdown timed out")
)

// ServerError 是服务端通过响应帧显式返回的应用错误，对应 flagError。
type ServerError struct {
	// Code 业务错误码。
	Code uint8
	// Retryable 表示该错误是否可以安全重试。
	Retryable bool
	// Message 错误描述。
	Message string
}

func (e *ServerError) Error() string {
	if e == nil {
		return "muxpool: server error"
	}
	return "muxpool: server error(" + itoa(int(e.Code)) + "): " + e.Message
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
