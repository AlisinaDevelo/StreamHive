package p2p

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkFrameRoundTrip1KB(b *testing.B) {
	benchmarkFrameRoundTrip(b, 1024)
}

func BenchmarkFrameRoundTrip64KB(b *testing.B) {
	benchmarkFrameRoundTrip(b, 64*1024)
}

func benchmarkFrameRoundTrip(b *testing.B, size int) {
	payload := bytes.Repeat([]byte("x"), size)
	b.SetBytes(int64(size))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, payload, DefaultMaxFrameBytes); err != nil {
			b.Fatal(err)
		}
		got, err := ReadFrame(bufio.NewReader(&buf), DefaultMaxFrameBytes)
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(payload) {
			b.Fatalf("got %d bytes, want %d", len(got), len(payload))
		}
	}
}
