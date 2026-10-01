package muxpool

import "time"

// Config 连接池与会话的配置项。
type Config struct {
	// MaxConns 最大连接（会话）数，0 表示不限制。
	MaxConns int
	// MinIdleConns 最小空闲连接数，低于该值时不主动淘汰空闲连接。
	MinIdleConns int
	// MaxIdleConns 最大空闲连接数，超过该值的空闲连接会被淘汰。
	MaxIdleConns int
	// MaxIdleTime 空闲连接最长存活时间，超时被淘汰。
	MaxIdleTime time.Duration
	// MaxLifetime 连接最长存活时间，超时被强制回收。
	MaxLifetime time.Duration
	// DialTimeout 建立连接的超时。
	DialTimeout time.Duration
	// MaxInFlightPerConn 单条连接上允许的最大在途请求数，0 表示不限制。
	MaxInFlightPerConn int
	// WaitQueueDepth 等待队列深度，达到上限时新请求被拒绝（ErrQueueFull）。
	WaitQueueDepth int
	// WaitTimeout 等待可用连接/会话的最长时间，超时返回 ErrWaitTimeout。
	WaitTimeout time.Duration
	// KeepAliveInterval 保活探测间隔，0 表示关闭保活。
	KeepAliveInterval time.Duration
	// KeepAliveTimeout 保活探测超时，超时判定连接已死。
	KeepAliveTimeout time.Duration
	// RequestTimeout 单个请求的默认超时，0 表示不限制。
	RequestTimeout time.Duration
	// CloseTimeout 优雅关闭时等待在途请求完成的最长时间，超时强制关闭。
	CloseTimeout time.Duration
	// MaxFrameSize 单帧载荷最大字节数，0 使用默认值。
	MaxFrameSize uint32
}

// 默认配置。
func DefaultConfig() Config {
	return Config{
		MaxConns:           64,
		MinIdleConns:       1,
		MaxIdleConns:       16,
		MaxIdleTime:        5 * time.Minute,
		MaxLifetime:        30 * time.Minute,
		DialTimeout:        5 * time.Second,
		MaxInFlightPerConn: 64,
		WaitQueueDepth:     256,
		WaitTimeout:        30 * time.Second,
		KeepAliveInterval:  30 * time.Second,
		KeepAliveTimeout:   5 * time.Second,
		RequestTimeout:     30 * time.Second,
		MaxFrameSize:       1 << 20, // 1 MiB
	}
}

// 归一化配置，填充零值默认项。
func (c *Config) normalize() {
	if c.MaxConns == 0 {
		c.MaxConns = 64
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 16
	}
	if c.MaxIdleTime == 0 {
		c.MaxIdleTime = 5 * time.Minute
	}
	if c.MaxLifetime == 0 {
		c.MaxLifetime = 30 * time.Minute
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.MaxInFlightPerConn == 0 {
		c.MaxInFlightPerConn = 64
	}
	if c.WaitQueueDepth == 0 {
		c.WaitQueueDepth = 256
	}
	if c.WaitTimeout == 0 {
		c.WaitTimeout = 30 * time.Second
	}
	if c.MaxFrameSize == 0 {
		c.MaxFrameSize = 1 << 20
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = 10 * time.Second
	}
}
