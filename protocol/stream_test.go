package protocol

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"testing/iotest"
)

// readAll drains r, returning the frames and the terminating error.
func readAll(t testing.TB, r *Reader) ([]*Frame, error) {
	t.Helper()
	var out []*Frame
	for i := 0; ; i++ {
		if i > 1e6 {
			t.Fatal("reader does not terminate")
		}
		f, err := r.ReadFrame()
		if err != nil {
			return out, err
		}
		out = append(out, f)
	}
}

// encodeAll concatenates the encodings of fs and returns each one too.
func encodeAll(t testing.TB, fs []*Frame) ([]byte, [][]byte) {
	var all []byte
	var each [][]byte
	for _, f := range fs {
		b := mustEncode(t, f)
		each = append(each, b)
		all = append(all, b...)
	}
	return all, each
}

func expectFrames(t *testing.T, got []*Frame, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i, f := range got {
		b, err := Encode(f)
		if err != nil || !bytes.Equal(b, want[i]) {
			t.Fatalf("frame %d: got % x (%v), want % x", i, b, err, want[i])
		}
	}
}

// garbage returns n random bytes that contain no 0xC3, so they can never
// start a candidate SYNC.
func garbage(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		for b[i] = byte(rng.Intn(256)); b[i] == syncHi; b[i] = byte(rng.Intn(256)) {
		}
	}
	return b
}

func TestReaderBackToBack(t *testing.T) {
	var fs []*Frame
	for i := 0; i < 50; i++ {
		fs = append(fs, sampleFrames()...)
	}
	all, each := encodeAll(t, fs)
	r := NewReader(bytes.NewReader(all))
	got, err := readAll(t, r)
	if err != io.EOF {
		t.Fatalf("terminating error %v, want io.EOF", err)
	}
	expectFrames(t, got, each)
	for i, f := range got {
		if exp := decodedForm(fs[i], len(each[i])); !reflect.DeepEqual(f, exp) {
			t.Fatalf("frame %d: %+v != %+v", i, f, exp)
		}
	}
	if s := r.Stats(); s != (ReaderStats{Frames: uint64(len(fs))}) {
		t.Fatalf("stats %+v", s)
	}
}

func TestReaderCleanGarbage(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	var in []byte
	var want [][]byte
	skipped := 0
	for _, f := range sampleFrames() {
		g := garbage(rng, rng.Intn(40))
		skipped += len(g)
		in = append(in, g...)
		b := mustEncode(t, f)
		want = append(want, b)
		in = append(in, b...)
	}
	g := garbage(rng, 17)
	skipped += len(g)
	in = append(in, g...)

	r := NewReader(bytes.NewReader(in))
	got, err := readAll(t, r)
	if err != io.EOF {
		t.Fatalf("err = %v", err)
	}
	expectFrames(t, got, want)
	s := r.Stats()
	if s.Frames != uint64(len(want)) || s.SkippedBytes != uint64(skipped) ||
		s.BadHeaders != 0 || s.CRCErrors != 0 || s.BadPayloads != 0 {
		t.Fatalf("stats %+v, want %d frames %d skipped", s, len(want), skipped)
	}
}

func TestReaderHostileGarbage(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	sampleBytes, _ := encodeAll(t, sampleFrames())
	junk := func() []byte {
		var b []byte
		for i := rng.Intn(8); i > 0; i-- {
			switch rng.Intn(6) {
			case 0:
				b = append(b, syncHi)
			case 1:
				b = append(b, syncHi, syncHi, syncLo)
			case 2: // fake SYNC followed by random bytes
				b = append(b, syncHi, syncLo)
				b = append(b, garbage(rng, rng.Intn(20))...)
			case 3: // a real frame cut short
				fb := mustEncode(t, sampleFrames()[rng.Intn(len(sampleFrames()))])
				b = append(b, fb[:rng.Intn(len(fb)-1)+1]...)
			case 4: // a real frame with one corrupted byte
				fb := mustEncode(t, sampleFrames()[rng.Intn(len(sampleFrames()))])
				fb[SyncLen+rng.Intn(len(fb)-SyncLen)] ^= byte(rng.Intn(255) + 1)
				b = append(b, fb...)
			default:
				b = append(b, sampleBytes[rng.Intn(len(sampleBytes))])
			}
		}
		return b
	}
	for iter := 0; iter < 200; iter++ {
		var in []byte
		var want [][]byte
		for _, f := range sampleFrames() {
			in = append(in, junk()...)
			b := mustEncode(t, f)
			want = append(want, b)
			in = append(in, b...)
		}
		// Junk may by itself contain whole valid frames (case 3/4 rarely
		// leave one intact); the wanted frames must be a subsequence.
		r := NewReader(bytes.NewReader(in))
		got, err := readAll(t, r)
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			t.Fatalf("iter %d: err %v", iter, err)
		}
		var enc [][]byte
		for _, f := range got {
			enc = append(enc, mustEncode(t, f))
		}
		if !isSubsequence(want, enc) {
			t.Fatalf("iter %d: lost frames; got %d want %d", iter, len(enc), len(want))
		}
		checkAccounting(t, r, got, len(in))
	}
}

func isSubsequence(want, got [][]byte) bool {
	j := 0
	for _, g := range got {
		if j < len(want) && bytes.Equal(g, want[j]) {
			j++
		}
	}
	return j == len(want)
}

func checkAccounting(t testing.TB, r *Reader, got []*Frame, inLen int) {
	t.Helper()
	s := r.Stats()
	used := s.SkippedBytes
	for _, f := range got {
		b, _ := Encode(f)
		used += uint64(len(b))
	}
	if used > uint64(inLen) || s.Frames != uint64(len(got)) {
		t.Fatalf("accounting: stats %+v, %d frames, %d bytes input", s, len(got), inLen)
	}
}

func TestReaderCorruptLengthThenValid(t *testing.T) {
	ack := mustEncode(t, &Frame{Msg: &Ack{AckedSeq: 42}})
	hb := mustEncode(t, &Frame{Msg: &Heartbeat{Time: 9, Battery: 9}})
	cases := map[string][]byte{}

	// FreeText whose length grows past the end of the stream: the claimed
	// frame cannot be read in full, but the frame behind it must still be
	// found and must not be reported as a truncation.
	ft := mustEncode(t, NewFrameText("HELLO"))
	ft[10] = 48
	cases["freetext len grows past EOF"] = append(append([]byte(nil), ft...), ack...)

	// Same, but the claimed length lands inside the following frames.
	ft = mustEncode(t, NewFrameText("HELLO"))
	ft[10] = 20
	cases["freetext len grows into next"] = append(append([]byte(nil), ft...), ack...)

	// Length shrinks: CRC check fails at the wrong position.
	ft = mustEncode(t, NewFrameText("HELLO WORLD"))
	ft[10] = 2
	cases["freetext len shrinks"] = append(append([]byte(nil), ft...), ack...)

	// Fixed-size type: any other length is a bad header.
	p := mustEncode(t, &Frame{Msg: &Position{Time: 1}})
	p[10] = 200
	cases["position len"] = append(append([]byte(nil), p...), ack...)

	for name, in := range cases {
		in = append(in, hb...)
		for _, oneByte := range []bool{false, true} {
			var src io.Reader = bytes.NewReader(in)
			if oneByte {
				src = iotest.OneByteReader(src)
			}
			r := NewReader(src)
			got, err := readAll(t, r)
			if err != io.EOF {
				t.Errorf("%s: err = %v, want io.EOF", name, err)
			}
			if len(got) != 2 {
				t.Errorf("%s: got %d frames, want 2", name, len(got))
				continue
			}
			expectFrames(t, got, [][]byte{ack, hb})
			// A candidate cut by EOF is only visible as skipped bytes.
			if s := r.Stats(); s.SkippedBytes == 0 {
				t.Errorf("%s: corruption not counted: %+v", name, s)
			}
		}
	}
}

// NewFrameText is a test shorthand for a FreeText frame.
func NewFrameText(s string) *Frame { return &Frame{Msg: NewFreeText(s)} }

func TestReaderOneByteAtATime(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	var in []byte
	var want [][]byte
	for _, f := range sampleFrames() {
		in = append(in, garbage(rng, 5)...)
		in = append(in, syncHi, syncLo, 0xFF)
		b := mustEncode(t, f)
		want = append(want, b)
		in = append(in, b...)
	}
	for _, wrap := range []func(io.Reader) io.Reader{iotest.OneByteReader, iotest.HalfReader, iotest.DataErrReader} {
		r := NewReader(wrap(bytes.NewReader(in)))
		got, err := readAll(t, r)
		if err != io.EOF {
			t.Fatalf("err = %v", err)
		}
		expectFrames(t, got, want)
	}
}

func TestReaderTruncatedFinalFrame(t *testing.T) {
	a := mustEncode(t, &Frame{Msg: &Ack{AckedSeq: 1}})
	p := mustEncode(t, &Frame{Msg: &Position{Time: 1}})
	for cut := 1; cut < len(p); cut++ {
		in := append(append([]byte(nil), a...), p[:cut]...)
		r := NewReader(iotest.OneByteReader(bytes.NewReader(in)))
		f, err := r.ReadFrame()
		if err != nil || !bytes.Equal(mustEncode(t, f), a) {
			t.Fatalf("cut %d: first frame %v %v", cut, f, err)
		}
		if _, err := r.ReadFrame(); err != io.ErrUnexpectedEOF {
			t.Fatalf("cut %d: err = %v, want io.ErrUnexpectedEOF", cut, err)
		}
		checkAccounting(t, r, []*Frame{f}, len(in))
	}
}

func TestReaderEmptyAndPureGarbage(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	if _, err := r.ReadFrame(); err != io.EOF {
		t.Fatalf("empty: %v", err)
	}
	g := garbage(rand.New(rand.NewSource(5)), 1000)
	r = NewReader(bytes.NewReader(g))
	if _, err := r.ReadFrame(); err != io.EOF {
		t.Fatalf("garbage: %v", err)
	}
	if s := r.Stats(); s.SkippedBytes != 1000 || s.Frames != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestReaderIOError(t *testing.T) {
	boom := errors.New("boom")
	a := mustEncode(t, &Frame{Msg: &Ack{}})
	r := NewReader(io.MultiReader(bytes.NewReader(a), bytes.NewReader(a[:5]), iotest.ErrReader(boom)))
	if _, err := r.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestReaderMaxGarbage(t *testing.T) {
	a := mustEncode(t, &Frame{Msg: &Ack{AckedSeq: 5}})
	for _, tc := range []struct {
		junk    []byte
		limit   int
		wantErr error
	}{
		{bytes.Repeat([]byte{0}, 10), 10, nil},
		{bytes.Repeat([]byte{0}, 11), 10, ErrTooMuchGarbage},
		{bytes.Repeat([]byte{0}, 5000), 100, ErrTooMuchGarbage},
		{bytes.Repeat([]byte{syncHi, syncLo, 0}, 100), 50, ErrTooMuchGarbage},
		{bytes.Repeat([]byte{0}, 5000), 0, nil}, // unlimited
	} {
		in := append(append([]byte(nil), tc.junk...), a...)
		r := NewReader(bytes.NewReader(in))
		r.MaxGarbage = tc.limit
		f, err := r.ReadFrame()
		if err != tc.wantErr {
			t.Fatalf("junk %d limit %d: err = %v, want %v", len(tc.junk), tc.limit, err, tc.wantErr)
		}
		if err == nil && !bytes.Equal(mustEncode(t, f), a) {
			t.Fatalf("wrong frame %v", f)
		}
		if err != nil && r.Stats().SkippedBytes > uint64(tc.limit)+2 {
			t.Fatalf("overshot the garbage budget: %+v", r.Stats())
		}
	}

	// The budget is per frame, not cumulative.
	var in []byte
	for i := 0; i < 20; i++ {
		in = append(in, bytes.Repeat([]byte{0}, 8)...)
		in = append(in, a...)
	}
	r := NewReader(bytes.NewReader(in))
	r.MaxGarbage = 8
	got, err := readAll(t, r)
	if err != io.EOF || len(got) != 20 || r.Stats().SkippedBytes != 160 {
		t.Fatalf("per-frame budget: %d frames, %v, %+v", len(got), err, r.Stats())
	}
}

func TestReaderStatsCategories(t *testing.T) {
	a := mustEncode(t, &Frame{Msg: &Ack{AckedSeq: 5}})
	badHdr := append([]byte(nil), a...)
	badHdr[2] ^= 0x80 // version
	badCRC := append([]byte(nil), a...)
	badCRC[12] ^= 1
	badPayload := append([]byte(nil), a...)
	badPayload[13] = 7 << 5 // ack status 7, CRC resealed
	resealCRC(badPayload)

	var in []byte
	for _, b := range [][]byte{badHdr, badCRC, badPayload, a} {
		in = append(in, b...)
	}
	r := NewReader(bytes.NewReader(in))
	got, err := readAll(t, r)
	if err != io.EOF || len(got) != 1 {
		t.Fatalf("got %d frames, %v", len(got), err)
	}
	s := r.Stats()
	if s.Frames != 1 || s.BadHeaders != 1 || s.CRCErrors != 1 || s.BadPayloads != 1 ||
		s.SkippedBytes != uint64(3*len(a)) {
		t.Fatalf("stats %+v", s)
	}
}

func TestWriterRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fs := sampleFrames()
	_, each := encodeAll(t, fs)
	for _, f := range fs {
		if err := w.WriteFrame(f); err != nil {
			t.Fatal(err)
		}
	}
	// Invalid frames are refused and write nothing.
	n := buf.Len()
	if err := w.WriteFrame(&Frame{Msg: &Heartbeat{Battery: 200}}); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("err = %v", err)
	}
	if buf.Len() != n {
		t.Fatal("invalid frame wrote bytes")
	}
	got, err := readAll(t, NewReader(&buf))
	if err != io.EOF {
		t.Fatal(err)
	}
	expectFrames(t, got, each)
}

func TestWriterPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	w := NewWriter(errWriter{boom})
	if err := w.WriteFrame(&Frame{Msg: &Ack{}}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

type errWriter struct{ err error }

func (e errWriter) Write([]byte) (int, error) { return 0, e.err }
