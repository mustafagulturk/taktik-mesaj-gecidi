package protocol

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"testing/iotest"
)

// seedFrames returns the encodings of the sample frames.
func seedFrames(f *testing.F) [][]byte {
	var seeds [][]byte
	for _, fr := range sampleFrames() {
		b, err := Encode(fr)
		if err != nil {
			f.Fatal(err)
		}
		seeds = append(seeds, b)
	}
	return seeds
}

func FuzzDecode(f *testing.F) {
	seeds := seedFrames(f)
	for _, s := range seeds {
		f.Add(s)
		f.Add(s[:len(s)-1])
		bad := append([]byte(nil), s...)
		bad[len(bad)/2] ^= 0x10
		f.Add(bad)
	}
	f.Add([]byte{})
	f.Add([]byte{syncHi, syncLo})
	f.Fuzz(func(t *testing.T, b []byte) {
		fr, err := Decode(b)
		if err != nil {
			if fr != nil {
				t.Fatalf("non-nil frame with error %v", err)
			}
			return
		}
		enc, err := Encode(fr)
		if err != nil {
			t.Fatalf("decoded frame does not re-encode: %v (%v)", err, fr)
		}
		if !bytes.Equal(enc, b) {
			t.Fatalf("non-canonical: input % x re-encodes to % x", b, enc)
		}
	})
}

func FuzzStreamReader(f *testing.F) {
	seeds := seedFrames(f)
	var all []byte
	for _, s := range seeds {
		all = append(all, s...)
		f.Add(s, false)
	}
	f.Add(all, false)
	f.Add(all, true)
	f.Add(append([]byte{0, syncHi, syncHi, syncLo, 1, 2}, all...), true)
	f.Add(all[:len(all)-3], false)
	f.Fuzz(func(t *testing.T, in []byte, oneByte bool) {
		var src io.Reader = bytes.NewReader(in)
		if oneByte {
			src = iotest.OneByteReader(src)
		}
		r := NewReader(src)
		var got []*Frame
		for i := 0; ; i++ {
			// Each success consumes at least MinFrameLen bytes.
			if i > len(in)/MinFrameLen+1 {
				t.Fatalf("too many ReadFrame calls (%d) for %d bytes", i, len(in))
			}
			fr, err := r.ReadFrame()
			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					t.Fatalf("unexpected error %v", err)
				}
				break
			}
			enc, err := Encode(fr)
			if err != nil {
				t.Fatalf("returned frame does not re-encode: %v", err)
			}
			if !bytes.Contains(in, enc) {
				t.Fatalf("frame % x not in input", enc)
			}
			got = append(got, fr)
		}
		checkAccounting(t, r, got, len(in))
	})
}

// FuzzStreamResync corrupts bytes of one frame in a train of valid frames
// and checks that every other frame is still delivered, in order.
func FuzzStreamResync(f *testing.F) {
	f.Add(uint8(5), uint8(2), []byte{10, 0xFF})        // length byte
	f.Add(uint8(17), uint8(11), []byte{10, 0x01})      // FreeText length grows
	f.Add(uint8(17), uint8(15), []byte{10, 0x30})      // big FreeText, near EOF
	f.Add(uint8(12), uint8(11), []byte{10, 0x20})      // FreeText length runs past EOF
	f.Add(uint8(3), uint8(0), []byte{0, 0x01})         // SYNC
	f.Add(uint8(8), uint8(7), []byte{2, 0x20, 3, 0x7}) // version, reserved bits
	f.Add(uint8(16), uint8(15), []byte{255, 0x80})     // CRC of the last frame
	f.Add(uint8(1), uint8(0), []byte{})
	f.Fuzz(func(t *testing.T, n, target uint8, muts []byte) {
		frames := sampleFrames()
		count := int(n)%24 + 1
		var in []byte
		var spans [][2]int
		for i := 0; i < count; i++ {
			fr := *frames[i%len(frames)]
			fr.Seq = uint16(i)
			b := mustEncode(t, &fr)
			spans = append(spans, [2]int{len(in), len(in) + len(b)})
			in = append(in, b...)
		}
		orig := append([]byte(nil), in...)
		tgt := spans[int(target)%count]
		for i := 0; i+1 < len(muts); i += 2 {
			off := tgt[0] + int(muts[i])%(tgt[1]-tgt[0])
			in[off] ^= muts[i+1]
		}
		var want [][]byte
		for _, s := range spans {
			if bytes.Equal(in[s[0]:s[1]], orig[s[0]:s[1]]) {
				want = append(want, orig[s[0]:s[1]])
			}
		}

		r := NewReader(bytes.NewReader(in))
		var got [][]byte
		for i := 0; ; i++ {
			if i > count+len(in)/MinFrameLen+1 {
				t.Fatal("reader does not terminate")
			}
			fr, err := r.ReadFrame()
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(err)
				}
				break
			}
			got = append(got, mustEncode(t, fr))
		}
		if !isSubsequence(want, got) {
			t.Fatalf("untouched frames lost: want %d, got %d frames (input % x)", len(want), len(got), in)
		}
	})
}

// FuzzFrameRoundTrip builds frames from arbitrary field values folded into
// their valid ranges and checks Encode/Decode is the identity.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint16(0), uint16(0), uint32(0), int32(0), int32(0), uint16(0), uint16(0), uint16(0), uint8(0), uint8(0), "")
	f.Add(uint8(255), uint16(0xFFFF), uint16(0xFFFF), uint16(0xFFFF), uint32(0xFFFFFFFF), int32(-1<<31), int32(1<<31-1),
		uint16(0xFFFF), uint16(0xFFFF), uint16(0xFFFF), uint8(255), uint8(255), "Düşman ŞİMDİ ğüç")
	f.Add(uint8(1), uint16(0x0102), uint16(0), uint16(7), uint32(3600), int32(0x1A2B3C), int32(-0x123456),
		uint16(850), uint16(42), uint16(270), uint8(2), uint8(5), "CONTACT 3 KM NORTH")
	f.Fuzz(func(t *testing.T, hdrBits uint8, src, dst, seq uint16, tm uint32, lat, lon int32,
		alt, speed, heading uint16, small1, small2 uint8, text string) {
		h := Header{
			Priority:       Priority(hdrBits & 3),
			Classification: Classification(hdrBits >> 2 & 3),
			AckRequired:    hdrBits&0x10 != 0,
			Src:            src,
			Dst:            dst,
			Seq:            seq,
		}
		pos := Coord{
			Lat: lat % (1 << 23), // keeps the sign: |Lat| < 2^23
			Lon: lon % (1 << 24),
		}
		// Map an ASCII-or-not string onto the alphabet, then truncate.
		var ft []byte
		for i := 0; i < len(text) && len(ft) < MaxTextLen; i++ {
			ft = append(ft, sixbitAlphabet[text[i]%64])
		}
		msgs := []Message{
			&Position{
				Time: tm % SecondsPerDay, Pos: pos, Alt: int16(alt),
				Speed: speed % 1024, Heading: heading % 360, Status: UnitStatus(small1 % uint8(numUnitStatus)),
			},
			&Contact{
				Time: tm % SecondsPerDay, Pos: pos, Category: ContactCategory(small1 % uint8(numContactCategory)),
				Count: small2 % 128, Heading: heading % 360, Speed: speed % 1024, Confidence: small2 >> 5,
			},
			&FreeText{Text: string(ft)},
			NewFreeText(text),
		}
		for i, m := range msgs {
			fr := &Frame{Header: h, Msg: m}
			b, err := Encode(fr)
			if err != nil {
				if i == len(msgs)-1 && errors.Is(err, ErrInvalidField) {
					continue // NewFreeText does not validate arbitrary input
				}
				t.Fatalf("Encode(%v): %v", fr, err)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode(% x): %v", b, err)
			}
			if exp := decodedForm(fr, len(b)); !reflect.DeepEqual(got, exp) {
				t.Fatalf("mismatch:\n got %+v\nwant %+v", got, exp)
			}
		}
	})
}
