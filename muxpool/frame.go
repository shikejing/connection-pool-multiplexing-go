package muxpool

import (
	"encoding/binary"
	"errors"
	"io"
)

// 帧协议（16 字节定长头 + 变长载荷）：
//
//	+--------+--------+--------+--------+
//	| Magic  | Ver    | Flags  |  ...   |
//	+--------+--------+--------+--------+
//	|        RequestID (uint64)         |
//	+-----------------------------------+
//	|        PayloadLen (uint32)        |
//	+-----------------------------------+
//	|            Payload (N)            |
//	+-----------------------------------+
const (
	frameMagic   uint16 = 0x4D58 // "MX"
	frameVersion uint8  = 1
	headerSize          = 16
)

// 帧标志位。
type FrameFlags uint8

const (
	FlagRequest   FrameFlags = 1 << 0 // 客户端 -> 服务端
	FlagResponse  FrameFlags = 1 << 1 // 服务端 -> 客户端
	FlagError     FrameFlags = 1 << 2 // 响应载荷为错误描述
	FlagStreamEnd FrameFlags = 1 << 3 // 半关闭：本方向不再有数据
	FlagPing      FrameFlags = 1 << 4 // 保活探测
	FlagPong      FrameFlags = 1 << 5 // 保活回应
)

// 一帧数据。
type Frame struct {
	ID      uint64
	Flags   FrameFlags
	Payload []byte
}

// 帧相关错误。
var (
	errBadMagic    = errors.New("muxpool: bad Frame magic")
	errBadVersion  = errors.New("muxpool: bad Frame version")
	errFrameTooBig = errors.New("muxpool: Frame payload too large")
)

// EncodeFrame 将一帧写入 w。
func EncodeFrame(w io.Writer, f *Frame) error {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint16(hdr[0:2], frameMagic)
	hdr[2] = frameVersion
	hdr[3] = byte(f.Flags)
	binary.BigEndian.PutUint64(hdr[4:12], f.ID)
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}

// DecodeFrame 从 r 读取并解析一帧，并对头部做合法性校验。
func DecodeFrame(r io.Reader, maxSize uint32) (*Frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(hdr[0:2]) != frameMagic {
		return nil, errBadMagic
	}
	if hdr[2] != frameVersion {
		return nil, errBadVersion
	}
	length := binary.BigEndian.Uint32(hdr[12:16])
	if maxSize > 0 && length > maxSize {
		return nil, errFrameTooBig
	}
	f := &Frame{
		ID:    binary.BigEndian.Uint64(hdr[4:12]),
		Flags: FrameFlags(hdr[3]),
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, err
		}
	}
	return f, nil
}
