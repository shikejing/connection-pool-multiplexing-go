package muxpool_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/shikejing/connection-pool-multiplexing-go/muxpool"
)

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("hello-mux")
	var buf bytes.Buffer
	f := &muxpool.Frame{ID: 42, Flags: muxpool.FlagRequest | muxpool.FlagStreamEnd, Payload: payload}
	if err := muxpool.EncodeFrame(&buf, f); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 16+len(payload) {
		t.Fatalf("len = %d, want %d", buf.Len(), 16+len(payload))
	}
	got, err := muxpool.DecodeFrame(&buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != f.ID || got.Flags != f.Flags || !bytes.Equal(got.Payload, f.Payload) {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestFrameRejectsBadMagic(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	_, err := muxpool.DecodeFrame(&buf, 0)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestFrameRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	f := &muxpool.Frame{ID: 1, Flags: muxpool.FlagRequest, Payload: make([]byte, 100)}
	_ = muxpool.EncodeFrame(&buf, f)
	_, err := muxpool.DecodeFrame(&buf, 50)
	if err == nil {
		t.Fatal("expected error for oversize frame")
	}
}

func TestFrameEOF(t *testing.T) {
	_, err := muxpool.DecodeFrame(bytes.NewReader(nil), 0)
	if err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}
