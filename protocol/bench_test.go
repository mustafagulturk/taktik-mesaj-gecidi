package protocol

import (
	"bytes"
	"testing"
)

var benchFrame = &Frame{
	Header: Header{Priority: Immediate, Src: 10, Dst: 20, Seq: 1},
	Msg: &Position{Time: 43200, Pos: CoordFromDegrees(39.92077, 32.85411),
		Alt: 938, Speed: 42, Heading: 270, Status: StatusEngaged},
}

func BenchmarkEncodePosition(b *testing.B) {
	buf := make([]byte, 0, MaxFrameLen)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, _ = AppendFrame(buf[:0], benchFrame)
	}
	b.SetBytes(int64(len(buf)))
}

func BenchmarkDecodePosition(b *testing.B) {
	wire, _ := Encode(benchFrame)
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for i := 0; i < b.N; i++ {
		if _, err := Decode(wire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamReader(b *testing.B) {
	wire, _ := Encode(benchFrame)
	stream := bytes.Repeat(wire, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))
	for i := 0; i < b.N; i++ {
		r := NewReader(bytes.NewReader(stream))
		for {
			if _, err := r.ReadFrame(); err != nil {
				break
			}
		}
	}
}
