package bitio

import (
	"bytes"
	"errors"
	"testing"
)

func TestWriterKnownLayout(t *testing.T) {
	w := NewWriter(4)
	w.WriteBits(0b101, 3)
	w.WriteBits(0b11110000, 8)
	w.WriteBool(true)
	w.WriteBits(0xF, 4)
	got, err := w.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	// 101 11110 | 000 1 1111
	want := []byte{0b10111110, 0b00011111}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %08b want %08b", got, want)
	}
}

func TestPadding(t *testing.T) {
	w := NewWriter(1)
	w.WriteBits(1, 1)
	got, _ := w.Bytes()
	if !bytes.Equal(got, []byte{0x80}) {
		t.Fatalf("got %x", got)
	}
}

func TestSignedRoundTrip(t *testing.T) {
	cases := []struct {
		v     int64
		width int
	}{{0, 1}, {-1, 1}, {-1, 24}, {8388607, 24}, {-8388608, 24}, {-12345, 17}, {-1 << 63, 64}}
	for _, c := range cases {
		w := NewWriter(8)
		w.WriteSigned(c.v, c.width)
		b, err := w.Bytes()
		if err != nil {
			t.Fatalf("%d/%d: %v", c.v, c.width, err)
		}
		r := NewReader(b)
		if got := r.ReadSigned(c.width); got != c.v || r.Err() != nil {
			t.Fatalf("%d/%d: got %d err %v", c.v, c.width, got, r.Err())
		}
	}
}

func TestOverflow(t *testing.T) {
	w := NewWriter(1)
	w.WriteBits(8, 3)
	if _, err := w.Bytes(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("want overflow, got %v", err)
	}
	w = NewWriter(1)
	w.WriteSigned(4, 3)
	if _, err := w.Bytes(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("want overflow, got %v", err)
	}
}

func TestShortRead(t *testing.T) {
	r := NewReader([]byte{0xFF})
	r.ReadBits(6)
	r.ReadBits(3)
	if !errors.Is(r.Err(), ErrShortBuffer) {
		t.Fatalf("want short buffer, got %v", r.Err())
	}
	if r.ReadBits(1) != 0 {
		t.Fatal("reads after error must return 0")
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(uint64(0), uint8(1), int64(0), uint8(1))
	f.Add(uint64(0xDEADBEEF), uint8(32), int64(-42), uint8(13))
	f.Fuzz(func(t *testing.T, u uint64, uw uint8, s int64, sw uint8) {
		uwidth := int(uw%64) + 1
		swidth := int(sw%64) + 1
		u &= mask(uwidth)
		if swidth < 64 {
			s = s << uint(64-swidth) >> uint(64-swidth) // sign-truncate into range
		}
		w := NewWriter(16)
		w.WriteBits(u, uwidth)
		w.WriteSigned(s, swidth)
		b, err := w.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != (uwidth+swidth+7)/8 {
			t.Fatalf("len %d for %d bits", len(b), uwidth+swidth)
		}
		r := NewReader(b)
		if got := r.ReadBits(uwidth); got != u {
			t.Fatalf("unsigned: got %d want %d", got, u)
		}
		if got := r.ReadSigned(swidth); got != s {
			t.Fatalf("signed: got %d want %d", got, s)
		}
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
	})
}
