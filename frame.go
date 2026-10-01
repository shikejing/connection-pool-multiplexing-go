package muxpool

import (
	"encoding/binary"
	"fmt"
	"io"
)

// 帧类型。
const (
	frameRequest  byte = 1
	frameResponse byte = 2
	frameCancel   byte = 3
	framePing     byte = 4
	framePong     byte = 5
)

const (
	frameHeaderLen = 15
	frameMagic     = 0x6D // 'm'
	// MaxFramePayload 单帧载荷上限 16MiB。
	MaxFramePayload = 16 << 20
)

// frame 是私有协议帧：
//
//	magic(1=0x6D) type(1) streamID(8 BE) flags(1) length(4 BE) payload(length)
type frame struct {
	typ      byte
	streamID uint64
	flags    byte
	payload  []byte
}

// 响应帧 flag 位。
const (
	flagError     byte = 1 << 0 // payload 是编码后的服务端错误
	flagRetryable byte = 1 << 1 // 服务端错误可重试
	flagHalfClose byte = 1 << 2 // 对端宣告半关闭，此后不再发响应
)

// encodeFrame 把帧追加编码到 buf 尾部返回；payload 长度不得超过 MaxFramePayload。
func encodeFrame(f frame, buf []byte) ([]byte, error) {
	if len(f.payload) > MaxFramePayload {
		return nil, ErrFrameTooLarge
	}
	var h [frameHeaderLen]byte
	h[0] = frameMagic
	h[1] = f.typ
	binary.BigEndian.PutUint64(h[2:10], f.streamID)
	h[10] = f.flags
	binary.BigEndian.PutUint32(h[11:15], uint32(len(f.payload)))
	buf = append(buf, h[:]...)
	buf = append(buf, f.payload...)
	return buf, nil
}

// encodeServerError 将服务端错误编入响应帧载荷：code(1) msgLen(2) msg。
func encodeServerError(code uint8, msg string) []byte {
	b := make([]byte, 3+len(msg))
	b[0] = code
	binary.BigEndian.PutUint16(b[1:3], uint16(len(msg)))
	copy(b[3:], msg)
	return b
}

// decodeServerError 解析错误响应载荷。
func decodeServerError(p []byte) *ServerError {
	if len(p) < 3 {
		return &ServerError{Code: 0, Message: "malformed error frame"}
	}
	n := int(binary.BigEndian.Uint16(p[1:3]))
	if 3+n > len(p) {
		return &ServerError{Code: p[0], Message: "malformed error frame"}
	}
	return &ServerError{Code: p[0], Message: string(p[3 : 3+n])}
}

// decodeFrame 从 r 精确读取一个完整帧。
// 任何读取错误（含 EOF）原样返回；协议非法返回包装了 ErrProtocol 的错误。
func decodeFrame(r io.Reader) (frame, error) {
	var h [frameHeaderLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return frame{}, err
	}
	if h[0] != frameMagic {
		return frame{}, fmt.Errorf("%w: bad magic 0x%02x", ErrProtocol, h[0])
	}
	n := binary.BigEndian.Uint32(h[11:15])
	if n > MaxFramePayload {
		return frame{}, fmt.Errorf("%w: declared length %d", ErrFrameTooLarge, n)
	}
	f := frame{
		typ:      h[1],
		streamID: binary.BigEndian.Uint64(h[2:10]),
		flags:    h[10],
	}
	if n > 0 {
		f.payload = make([]byte, n)
		if _, err := io.ReadFull(r, f.payload); err != nil {
			return frame{}, err
		}
	}
	return f, nil
}
