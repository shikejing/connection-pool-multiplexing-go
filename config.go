package muxpool

import (
	"errors"
	"time"
)

// QueuePolicy 等待容量不足时的策略。
type QueuePolicy int

const (
	// QueuePolicyReject 立即拒绝（Do 返回 ErrQueueFull）。
	QueuePolicyReject QueuePolicy = iota
	// QueuePolicyQueue 进入有界等待队列，受队列深度与等待时长上界约束。
	QueuePolicyQueue
)

// Dialer 建立底层连接，返回的连接必须是一条全新的、干净的连接。
type Dialer func() (Conn, error)

// Conn 是多路复用所依赖的底层全双工流。
type Conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// Config 连接池与多路复用配置。
type Config struct {
	// Dial 必需：建立一条全新的底层连接。
	Dial Dialer
	// MaxConns 连接数硬上限；<=0 表示默认 1（单连接多路复用）。
	MaxConns int
	// MaxInflight 单连接在途请求数硬上限；<=0 表示默认 8。
	MaxInflight int

	// QueuePolicy 容量不足时排队或拒绝。
	QueuePolicy QueuePolicy
	// MaxWaitQueue 等待队列深度上界（仅 Queue 策略生效）；<=0 默认 MaxConns*MaxInflight。
	MaxWaitQueue int
	// MaxWaitTimeout 等待时长上界；<=0 表示不超时（仅受 ctx 约束）。
	MaxWaitTimeout time.Duration

	// IdleTimeout 空闲连接淘汰时间；<=0 表示不按空闲淘汰。
	IdleTimeout time.Duration
	// MaxConnLife 连接最大寿命（到期半关闭，新流不再分发）；<=0 表示不限。
	MaxConnLife time.Duration
	// DialTimeout 建连超时；<=0 不限制。
	DialTimeout time.Duration
	// ReadTimeout 单个响应帧读取超时（慢响应背压）；<=0 不限制。
	ReadTimeout time.Duration
	// WriteTimeout 单个请求帧写出超时（慢写入背压）；<=0 不限制。
	WriteTimeout time.Duration
	// PingInterval 保活探测间隔；<=0 不主动保活。
	PingInterval time.Duration
	// PingTimeout 保活响应等待超时，超时判定连接失效；<=0 取 ReadTimeout。
	PingTimeout time.Duration

	// DefaultMaxAttempts 默认最大尝试次数（含首次）；<=0 默认 1（不重试）。
	DefaultMaxAttempts int
}

func (c *Config) normalize() error {
	if c.Dial == nil {
		return errors.New("muxpool: Config.Dial is required")
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 1
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 8
	}
	if c.MaxWaitQueue <= 0 {
		c.MaxWaitQueue = c.MaxConns * c.MaxInflight
	}
	if c.PingInterval > 0 && c.PingTimeout <= 0 {
		c.PingTimeout = 3 * time.Second
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 1
	}
	return nil
}
