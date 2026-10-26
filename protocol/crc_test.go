package protocol

import "testing"

func TestCRC16CheckValue(t *testing.T) {
	if got := CRC16([]byte("123456789")); got != 0x29B1 {
		t.Fatalf("CRC16(\"123456789\") = %#04x, want 0x29b1", got)
	}
}

func TestCRC16Empty(t *testing.T) {
	// No input: the register keeps its initial value (no xorout).
	if got := CRC16(nil); got != 0xFFFF {
		t.Fatalf("CRC16(nil) = %#04x, want 0xffff", got)
	}
}

func TestCRC16MatchesBitwise(t *testing.T) {
	bitwise := func(b []byte) uint16 {
		c := uint16(0xFFFF)
		for _, x := range b {
			c ^= uint16(x) << 8
			for i := 0; i < 8; i++ {
				if c&0x8000 != 0 {
					c = c<<1 ^ 0x1021
				} else {
					c <<= 1
				}
			}
		}
		return c
	}
	buf := make([]byte, 0, 300)
	for i := 0; i < 300; i++ {
		buf = append(buf, byte(i*131+7))
		if got, want := CRC16(buf), bitwise(buf); got != want {
			t.Fatalf("len %d: table CRC %#04x != bitwise %#04x", len(buf), got, want)
		}
	}
}
