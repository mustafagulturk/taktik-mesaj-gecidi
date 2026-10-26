package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// sampleFrames returns one valid frame per message type plus FreeText edge
// cases. Header fields vary so every header bit position is exercised.
func sampleFrames() []*Frame {
	hdr := func(i int) Header {
		return Header{
			Priority:       Priority(i % 4),
			Classification: Classification((i / 2) % 4),
			AckRequired:    i%2 == 1,
			Src:            uint16(0x0100 + i),
			Dst:            []uint16{AddrGateway, AddrBroadcast, 0x0203}[i%3],
			Seq:            uint16(i*7919 + 1),
		}
	}
	msgs := []Message{
		&Heartbeat{Time: 0, Status: StatusOperational, Battery: 100},
		&Heartbeat{Time: SecondsPerDay - 1, Status: StatusOutOfAction, Battery: 0},
		&Position{Time: 43200, Pos: CoordFromDegrees(39.92077, 32.85411), Alt: 938, Speed: 60, Heading: 359, Status: StatusEngaged},
		&Position{Time: 1, Pos: Coord{Lat: latMin, Lon: lonMin}, Alt: math.MinInt16, Speed: 1023, Heading: 0, Status: StatusWithdrawing},
		&Position{Time: 86399, Pos: Coord{Lat: latMax, Lon: lonMax}, Alt: math.MaxInt16, Speed: 0, Heading: 180, Status: StatusOutOfAction},
		&Contact{Time: 3600, Pos: CoordFromDegrees(-33.8688, 151.2093), Category: ContactArmor, Count: 12, Heading: 90, Speed: 25, Confidence: 5},
		&Contact{Time: 86399, Pos: Coord{Lat: -1, Lon: -1}, Category: ContactUAV, Count: 127, Heading: 359, Speed: 1023, Confidence: 7},
		&Order{Time: 100, Code: OrderAdvance, Target: CoordFromDegrees(41.0082, 28.9784), ExecuteAt: 200},
		&Order{Time: 86399, Code: OrderMedevac, Target: Coord{Lat: latMax, Lon: lonMin}, ExecuteAt: 86399},
		&Ack{AckedSeq: 0, Status: AckDelivered},
		&Ack{AckedSeq: 0xFFFF, Status: AckQueueFull},
		&FreeText{Text: ""},
		&FreeText{Text: "A"},
		&FreeText{Text: "CONTACT 3 KM NORTH, REQUEST FIRE @ GRID 37S DB 1234 5678"},
		&FreeText{Text: strings.Repeat("Z", MaxTextLen)},
		&FreeText{Text: sixbitAlphabet[1:]}, // 63 chars: whole alphabet minus space
		NewFreeText("Düşman ŞİMDİ ğüç"),
	}
	out := make([]*Frame, len(msgs))
	for i, m := range msgs {
		out[i] = &Frame{Header: hdr(i), Msg: m}
	}
	return out
}

// decodedForm returns what Decode is expected to yield for f: the header
// fields derived during encoding filled in.
func decodedForm(f *Frame, wireLen int) *Frame {
	g := *f
	g.Version = Version
	g.Type = f.Msg.Type()
	g.PayloadLen = uint8(wireLen - MinFrameLen)
	return &g
}

func mustEncode(t testing.TB, f *Frame) []byte {
	t.Helper()
	b, err := Encode(f)
	if err != nil {
		t.Fatalf("Encode(%v): %v", f, err)
	}
	return b
}

// resealCRC recomputes the trailing CRC of a (possibly tampered) frame.
func resealCRC(b []byte) []byte {
	end := len(b) - CRCLen
	binary.BigEndian.PutUint16(b[end:], CRC16(b[SyncLen:end]))
	return b
}

func TestRoundTripAllTypes(t *testing.T) {
	for _, f := range sampleFrames() {
		t.Run(f.Msg.Type().String(), func(t *testing.T) {
			b := mustEncode(t, f)
			_, want := newMessage(f.Msg.Type())
			if ft, ok := f.Msg.(*FreeText); ok {
				want = FreeTextLen(len(ft.Text))
			}
			if got := len(b) - MinFrameLen; got != want {
				t.Fatalf("payload length %d, want %d", got, want)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if exp := decodedForm(f, len(b)); !reflect.DeepEqual(got, exp) {
				t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, exp)
			}
			// AppendFrame must append, not overwrite.
			prefix := []byte{1, 2, 3}
			ab, err := AppendFrame(append([]byte(nil), prefix...), f)
			if err != nil || !bytes.Equal(ab[:3], prefix) || !bytes.Equal(ab[3:], b) {
				t.Fatalf("AppendFrame = %x, %v; want %x%x", ab, err, prefix, b)
			}
		})
	}
}

func TestFreeTextLengths(t *testing.T) {
	for n := 0; n <= MaxTextLen; n++ {
		f := &Frame{Msg: &FreeText{Text: strings.Repeat("?", n)}}
		b := mustEncode(t, f)
		if len(b) != MinFrameLen+FreeTextLen(n) {
			t.Fatalf("n=%d: frame length %d", n, len(b))
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got.Msg.(*FreeText).Text != f.Msg.(*FreeText).Text {
			t.Fatalf("n=%d: text %q", n, got.Msg.(*FreeText).Text)
		}
	}
}

func TestSixbitAlphabet(t *testing.T) {
	if len(sixbitAlphabet) != 64 {
		t.Fatalf("alphabet has %d symbols, want 64", len(sixbitAlphabet))
	}
	// Duplicates would make decoding non-canonical.
	seen := map[byte]bool{}
	for i := 0; i < len(sixbitAlphabet); i++ {
		c := sixbitAlphabet[i]
		if seen[c] || c >= 0x80 || sixbitIndex[c] != int8(i) {
			t.Fatalf("symbol %q at %d duplicated or misindexed", c, i)
		}
		seen[c] = true
	}
}

func TestNormalizeTurkish(t *testing.T) {
	cases := map[string]string{
		"Düşman ŞİMDİ ğüç":    "DUSMAN SIMDI GUC",
		"ıiIİ çÇ öÖ şŞ üÜ ğĞ": "IIII CC OO SS UU GG",
		"Hedef: 3 tank, 2 km": "HEDEF: 3 TANK, 2 KM",
	}
	for in, want := range cases {
		if got := NewFreeText(in).Text; got != want {
			t.Errorf("NewFreeText(%q) = %q, want %q", in, got, want)
		}
		got, err := Decode(mustEncode(t, &Frame{Msg: NewFreeText(in)}))
		if err != nil || got.Msg.(*FreeText).Text != want {
			t.Errorf("round trip of %q: %v %v", in, got, err)
		}
	}
}

// TestGoldenPosition pins the wire format of a POSREP. Bit breakdown:
//
//	c3 5a                     SYNC
//	22 = 001 00010            ver=1 type=2 (POSREP)
//	98 = 10 01 1 000          pri=IMMEDIATE cls=RESTRICTED ack=1 rsv=0
//	01 02 | 00 00 | 00 07     src=0x0102 dst=0x0000 seq=7
//	0d                        len=13
//	payload (104 bits):
//	  time=3600     0 0000 1110 0001 0000           (17)
//	  lat=0x1a2b3c  0001 1010 0010 1011 0011 1100   (24)
//	  lon=-0x123456 1 1110 1101 1100 1011 1010 1010 (25)
//	  alt=850       0000 0011 0101 0010             (16)
//	  speed=42      00 0010 1010                    (10)
//	  heading=270   1 0000 1110                     (9)
//	  status=2      010                             (3)
//	  -> 07 08 0d 15 9e 7b 72 ea 80 d4 82 a8 72
//	b6 96                     CRC-16/CCITT-FALSE over header+payload
func TestGoldenPosition(t *testing.T) {
	want, _ := hex.DecodeString("c35a" + "2298" + "0102" + "0000" + "0007" + "0d" +
		"07080d159e7b72ea80d482a872" + "b696")
	f := &Frame{
		Header: Header{Priority: Immediate, Classification: Restricted, AckRequired: true, Src: 0x0102, Dst: AddrGateway, Seq: 7},
		Msg: &Position{Time: 3600, Pos: Coord{Lat: 0x1A2B3C, Lon: -0x123456},
			Alt: 850, Speed: 42, Heading: 270, Status: StatusEngaged},
	}
	checkGolden(t, f, want)
}

// TestGoldenAck pins the wire format of an ACK. Bit breakdown:
//
//	c3 5a                     SYNC
//	25 = 001 00101            ver=1 type=5 (ACK)
//	c0 = 11 00 0 000          pri=FLASH cls=UNCLASSIFIED ack=0 rsv=0
//	00 00 | 01 02 | 00 01     src=0x0000 dst=0x0102 seq=1
//	03                        len=3
//	12 34 | 20 = 001 00000    acked_seq=0x1234 status=NO_ROUTE rsv=0
//	ff 0a                     CRC
func TestGoldenAck(t *testing.T) {
	want, _ := hex.DecodeString("c35a" + "25c0" + "0000" + "0102" + "0001" + "03" + "123420" + "ff0a")
	f := &Frame{
		Header: Header{Priority: Flash, Classification: Unclassified, Src: AddrGateway, Dst: 0x0102, Seq: 1},
		Msg:    &Ack{AckedSeq: 0x1234, Status: AckNoRoute},
	}
	checkGolden(t, f, want)
}

func checkGolden(t *testing.T, f *Frame, want []byte) {
	t.Helper()
	got := mustEncode(t, f)
	if !bytes.Equal(got, want) {
		t.Fatalf("Encode:\n got % x\nwant % x", got, want)
	}
	dec, err := Decode(want)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if exp := decodedForm(f, len(want)); !reflect.DeepEqual(dec, exp) {
		t.Fatalf("Decode:\n got %+v\nwant %+v", dec, exp)
	}
}

func TestCoordQuantisation(t *testing.T) {
	const tol = 1.1e-5
	check := func(lat, lon float64) {
		t.Helper()
		c := CoordFromDegrees(lat, lon)
		// Survive the wire as well.
		b := mustEncode(t, &Frame{Msg: &Position{Pos: c}})
		f, err := Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		gl, gn := f.Msg.(*Position).Pos.Degrees()
		if math.Abs(gl-lat) >= tol || math.Abs(gn-lon) >= tol {
			t.Fatalf("(%v,%v) -> (%v,%v): error (%g,%g)", lat, lon, gl, gn, gl-lat, gn-lon)
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		check(rng.Float64()*180-90, rng.Float64()*360-180)
	}
	for _, lat := range []float64{-90, -45.5, 0, 1e-9, 39.92077, 89.99999, 90} {
		for _, lon := range []float64{-180, -0.000001, 0, 32.85411, 179.99999, 180} {
			check(lat, lon)
		}
	}
}

func TestCoordClamping(t *testing.T) {
	cases := []struct {
		lat, lon float64
		want     Coord
	}{
		{90, 180, Coord{latMax, lonMax}},
		{-90, -180, Coord{latMin, lonMin}},
		{1000, -1000, Coord{latMax, lonMin}},
		{-90.0001, 180.0001, Coord{latMin, lonMax}},
		{math.Inf(1), math.Inf(-1), Coord{latMax, lonMin}},
		{0, 0, Coord{0, 0}},
	}
	for _, c := range cases {
		if got := CoordFromDegrees(c.lat, c.lon); got != c.want {
			t.Errorf("CoordFromDegrees(%v,%v) = %+v, want %+v", c.lat, c.lon, got, c.want)
		}
	}
}

func TestEncodeRejectsOutOfRange(t *testing.T) {
	ok := func() *Position { return &Position{Time: 1, Heading: 1} }
	cases := map[string]*Frame{
		"nil msg":              {},
		"priority 4":           {Header: Header{Priority: 4}, Msg: &Ack{}},
		"classification 4":     {Header: Header{Classification: 4}, Msg: &Ack{}},
		"heartbeat time":       {Msg: &Heartbeat{Time: SecondsPerDay}},
		"heartbeat battery":    {Msg: &Heartbeat{Battery: 101}},
		"heartbeat status":     {Msg: &Heartbeat{Status: numUnitStatus}},
		"position time":        {Msg: &Position{Time: SecondsPerDay}},
		"position heading":     {Msg: &Position{Heading: 360}},
		"position speed":       {Msg: &Position{Speed: 1024}},
		"position status":      {Msg: &Position{Status: numUnitStatus}},
		"position lat":         {Msg: func() *Position { p := ok(); p.Pos.Lat = latMax + 1; return p }()},
		"position lon":         {Msg: func() *Position { p := ok(); p.Pos.Lon = lonMin - 1; return p }()},
		"contact time":         {Msg: &Contact{Time: SecondsPerDay}},
		"contact heading":      {Msg: &Contact{Heading: 360}},
		"contact category":     {Msg: &Contact{Category: numContactCategory}},
		"contact count":        {Msg: &Contact{Count: 128}},
		"contact speed":        {Msg: &Contact{Speed: 1024}},
		"contact confidence":   {Msg: &Contact{Confidence: 8}},
		"order time":           {Msg: &Order{Time: SecondsPerDay}},
		"order execute_at":     {Msg: &Order{ExecuteAt: SecondsPerDay}},
		"order code":           {Msg: &Order{Code: numOrderCode}},
		"ack status":           {Msg: &Ack{Status: numAckStatus}},
		"text too long":        {Msg: &FreeText{Text: strings.Repeat("A", MaxTextLen+1)}},
		"text lower case":      {Msg: &FreeText{Text: "hello"}},
		"text turkish raw":     {Msg: &FreeText{Text: "DÜŞMAN"}},
		"text tilde":           {Msg: &FreeText{Text: "A~B"}},
		"text newline":         {Msg: &FreeText{Text: "A\nB"}},
		"text NUL":             {Msg: &FreeText{Text: "A\x00"}},
		"normalized not ascii": {Msg: NewFreeText("café")},
	}
	for name, f := range cases {
		dst := []byte{0xAA}
		got, err := AppendFrame(dst, f)
		if !errors.Is(err, ErrInvalidField) {
			t.Errorf("%s: err = %v, want ErrInvalidField", name, err)
		}
		if !bytes.Equal(got, dst) {
			t.Errorf("%s: dst modified on error: % x", name, got)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	pos := mustEncode(t, &Frame{Msg: &Position{Time: 5, Heading: 10}})
	cp := func(b []byte) []byte { return append([]byte(nil), b...) }
	ftA := mustEncode(t, &Frame{Msg: &FreeText{Text: "A"}}) // 12 bits + 4 pad

	cases := []struct {
		name string
		b    []byte
		want error
	}{
		{"empty", nil, ErrTruncated},
		{"min-1", pos[:MinFrameLen-1], ErrTruncated},
		{"bad sync hi", func() []byte { b := cp(pos); b[0] = 0xC2; return b }(), ErrBadSync},
		{"bad sync lo", func() []byte { b := cp(pos); b[1] = 0x5B; return b }(), ErrBadSync},
		{"version 0", func() []byte { b := cp(pos); b[2] &^= 0xE0; return resealCRC(b) }(), ErrBadVersion},
		{"version 2", func() []byte { b := cp(pos); b[2] = b[2]&0x1F | 2<<5; return resealCRC(b) }(), ErrBadVersion},
		{"version 7", func() []byte { b := cp(pos); b[2] |= 0xE0; return resealCRC(b) }(), ErrBadVersion},
		{"type 0", func() []byte { b := cp(pos); b[2] &^= 0x1F; return resealCRC(b) }(), ErrUnknownType},
		{"type 7", func() []byte { b := cp(pos); b[2] = b[2]&0xE0 | 7; return resealCRC(b) }(), ErrUnknownType},
		{"type 31", func() []byte { b := cp(pos); b[2] |= 0x1F; return resealCRC(b) }(), ErrUnknownType},
		{"header rsv bit 0", func() []byte { b := cp(pos); b[3] |= 1; return resealCRC(b) }(), ErrReservedBits},
		{"header rsv bit 2", func() []byte { b := cp(pos); b[3] |= 4; return resealCRC(b) }(), ErrReservedBits},
		{"position len 12", func() []byte {
			b := cp(pos[:len(pos)-1])
			b[10] = 12
			return resealCRC(b)
		}(), ErrBadLength},
		{"position len 14", func() []byte {
			b := append(cp(pos), 0)
			b[10] = 14
			return resealCRC(b)
		}(), ErrBadLength},
		{"position as ack", func() []byte { b := cp(pos); b[2] = b[2]&0xE0 | byte(TypeAck); return resealCRC(b) }(), ErrBadLength},
		{"freetext len 0", func() []byte {
			b := cp(ftA[:MinFrameLen])
			b[10] = 0
			return resealCRC(b)
		}(), ErrBadLength},
		{"freetext len 49", func() []byte {
			b := append(cp(ftA[:MinFrameLen-CRCLen]), make([]byte, 49+CRCLen)...)
			b[10] = 49
			return resealCRC(b)
		}(), ErrBadLength},
		{"freetext count exceeds payload", func() []byte {
			b := cp(ftA)
			b[11] = 10 << 2 // claims 10 chars in a 2-byte payload
			return resealCRC(b)
		}(), ErrBadLength},
		{"freetext extra byte", func() []byte {
			b := append(cp(ftA), 0)
			b[10] = 3
			return resealCRC(b)
		}(), ErrBadLength},
		{"freetext nonzero pad", func() []byte { b := cp(ftA); b[12] |= 1; return resealCRC(b) }(), ErrReservedBits},
		{"freetext nonzero pad hi", func() []byte { b := cp(ftA); b[12] |= 8; return resealCRC(b) }(), ErrReservedBits},
		{"trailing byte", append(cp(pos), 0), ErrTrailingBytes},
		{"trailing frame", append(cp(pos), pos...), ErrTrailingBytes},
		{"truncated payload", pos[:len(pos)-3], ErrTruncated},
		{"truncated crc", pos[:len(pos)-1], ErrTruncated},
		{"crc swapped", func() []byte {
			b := cp(pos)
			n := len(b)
			b[n-1], b[n-2] = b[n-2], b[n-1]
			return b
		}(), ErrCRC},
	}
	// Reserved payload bits, one case per type that has them.
	rsv := []struct {
		name string
		msg  Message
		mask byte // low bits of the last payload byte that are reserved
	}{
		{"heartbeat rsv", &Heartbeat{Time: 7, Battery: 50}, 0x0F},
		{"contact rsv", &Contact{Time: 7}, 0x1F},
		{"order rsv", &Order{Time: 7}, 0x7F},
		{"ack rsv", &Ack{AckedSeq: 7}, 0x1F},
	}
	for _, r := range rsv {
		base := mustEncode(t, &Frame{Msg: r.msg})
		last := len(base) - CRCLen - 1
		for bit := byte(1); bit != 0 && bit <= r.mask; bit <<= 1 {
			b := cp(base)
			b[last] |= bit
			cases = append(cases, struct {
				name string
				b    []byte
				want error
			}{r.name, resealCRC(b), ErrReservedBits})
		}
	}
	// Out-of-range fields that fit their bit width must also be refused.
	payloadOff := SyncLen + HeaderLen
	oor := []struct {
		name string
		msg  Message
		set  func(p []byte)
	}{
		// time occupies the first 17 bits of every timed payload.
		{"heartbeat time 86400", &Heartbeat{}, func(p []byte) { setBits(p, 0, 17, SecondsPerDay) }},
		{"heartbeat time max", &Heartbeat{}, func(p []byte) { setBits(p, 0, 17, 1<<17-1) }},
		{"heartbeat status 5", &Heartbeat{}, func(p []byte) { setBits(p, 17, 4, 5) }},
		{"heartbeat battery 101", &Heartbeat{}, func(p []byte) { setBits(p, 21, 7, 101) }},
		{"position heading 360", &Position{}, func(p []byte) { setBits(p, 92, 9, 360) }},
		{"position status 7", &Position{}, func(p []byte) { setBits(p, 101, 3, 7) }},
		{"contact category 8", &Contact{}, func(p []byte) { setBits(p, 66, 4, 8) }},
		{"contact heading 511", &Contact{}, func(p []byte) { setBits(p, 77, 9, 511) }},
		{"order code 9", &Order{}, func(p []byte) { setBits(p, 17, 6, 9) }},
		{"order execute_at 86400", &Order{}, func(p []byte) { setBits(p, 72, 17, SecondsPerDay) }},
		{"ack status 5", &Ack{}, func(p []byte) { setBits(p, 16, 3, 5) }},
	}
	for _, o := range oor {
		b := mustEncode(t, &Frame{Msg: o.msg})
		o.set(b[payloadOff : len(b)-CRCLen])
		cases = append(cases, struct {
			name string
			b    []byte
			want error
		}{o.name, resealCRC(b), ErrInvalidField})
	}

	for _, c := range cases {
		f, err := Decode(c.b)
		if !errors.Is(err, c.want) || f != nil {
			t.Errorf("%s: Decode(% x) = %v, %v; want %v", c.name, c.b, f, err, c.want)
		}
	}
}

// setBits overwrites width bits of p starting at bit offset off (MSB first).
func setBits(p []byte, off, width int, v uint64) {
	for i := 0; i < width; i++ {
		pos := off + i
		m := byte(0x80) >> (pos % 8)
		if v>>(width-1-i)&1 == 1 {
			p[pos/8] |= m
		} else {
			p[pos/8] &^= m
		}
	}
}

func TestEveryBitFlipRejected(t *testing.T) {
	for _, f := range sampleFrames() {
		b := mustEncode(t, f)
		for i := 0; i < len(b)*8; i++ {
			m := append([]byte(nil), b...)
			m[i/8] ^= 0x80 >> (i % 8)
			got, err := Decode(m)
			if err == nil {
				t.Fatalf("%v: flip of bit %d accepted: %v", f, i, got)
			}
			switch byteIdx := i / 8; {
			case byteIdx < SyncLen:
				if !errors.Is(err, ErrBadSync) {
					t.Fatalf("%v: sync bit %d: %v", f, i, err)
				}
			case byteIdx >= SyncLen+HeaderLen:
				// Payload and CRC bits: header still valid, so the CRC catches it.
				if !errors.Is(err, ErrCRC) {
					t.Fatalf("%v: bit %d: %v, want ErrCRC", f, i, err)
				}
			}
		}
	}
}

func TestMsgStrings(t *testing.T) {
	// Smoke test: String must not panic for any value.
	for i := 0; i < 256; i++ {
		_ = MsgType(i).String()
		_ = Priority(i).String()
		_ = Classification(i).String()
		_ = UnitStatus(i).String()
		_ = ContactCategory(i).String()
		_ = OrderCode(i).String()
		_ = AckStatus(i).String()
	}
	for _, f := range sampleFrames() {
		if s := f.String(); !strings.Contains(s, f.Msg.Type().String()) {
			t.Errorf("Frame.String() = %q", s)
		}
	}
}
