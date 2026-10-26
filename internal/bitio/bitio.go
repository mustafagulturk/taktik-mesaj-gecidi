// Package bitio implements MSB-first bit-level readers and writers used to
// pack tactical message fields into fixed bit layouts.
package bitio

import "errors"

// ErrShortBuffer is returned when a read runs past the end of the input.
var ErrShortBuffer = errors.New("bitio: short buffer")

// ErrFieldWidth is returned when a field width is outside 1..64.
var ErrFieldWidth = errors.New("bitio: invalid field width")

// ErrOverflow is returned when a value does not fit in the requested width.
var ErrOverflow = errors.New("bitio: value overflows field")

// Writer appends bit fields, most significant bit first.
type Writer struct {
	buf  []byte
	nbit int // total bits written
	err  error
}

// NewWriter returns a Writer with capacity for sizeHint bytes.
func NewWriter(sizeHint int) *Writer {
	return &Writer{buf: make([]byte, 0, sizeHint)}
}

// WriteBits writes the low `width` bits of v. Values that do not fit set a
// sticky error instead of being silently truncated.
func (w *Writer) WriteBits(v uint64, width int) {
	if w.err != nil {
		return
	}
	if width < 1 || width > 64 {
		w.err = ErrFieldWidth
		return
	}
	if width < 64 && v>>uint(width) != 0 {
		w.err = ErrOverflow
		return
	}
	for i := width - 1; i >= 0; i-- {
		if w.nbit%8 == 0 {
			w.buf = append(w.buf, 0)
		}
		if v>>uint(i)&1 == 1 {
			w.buf[len(w.buf)-1] |= 0x80 >> uint(w.nbit%8)
		}
		w.nbit++
	}
}

// WriteSigned writes v in two's complement using `width` bits.
func (w *Writer) WriteSigned(v int64, width int) {
	if w.err != nil {
		return
	}
	if width < 1 || width > 64 {
		w.err = ErrFieldWidth
		return
	}
	if width < 64 {
		lo, hi := -(int64(1) << uint(width-1)), int64(1)<<uint(width-1)-1
		if v < lo || v > hi {
			w.err = ErrOverflow
			return
		}
	}
	w.WriteBits(uint64(v)&mask(width), width)
}

// WriteBool writes a single bit.
func (w *Writer) WriteBool(b bool) {
	if b {
		w.WriteBits(1, 1)
	} else {
		w.WriteBits(0, 1)
	}
}

// Bits reports how many bits have been written.
func (w *Writer) Bits() int { return w.nbit }

// Bytes returns the written data, zero-padded to a byte boundary, and the
// first error encountered.
func (w *Writer) Bytes() ([]byte, error) { return w.buf, w.err }

// Reader consumes bit fields, most significant bit first.
type Reader struct {
	buf []byte
	pos int // bit position
	err error
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{buf: b} }

// ReadBits reads an unsigned field of `width` bits. After an error all reads
// return 0; check Err once at the end.
func (r *Reader) ReadBits(width int) uint64 {
	if r.err != nil {
		return 0
	}
	if width < 1 || width > 64 {
		r.err = ErrFieldWidth
		return 0
	}
	if r.pos+width > len(r.buf)*8 {
		r.err = ErrShortBuffer
		return 0
	}
	var v uint64
	for i := 0; i < width; i++ {
		bit := r.buf[r.pos/8] >> uint(7-r.pos%8) & 1
		v = v<<1 | uint64(bit)
		r.pos++
	}
	return v
}

// ReadSigned reads a two's complement field of `width` bits.
func (r *Reader) ReadSigned(width int) int64 {
	v := r.ReadBits(width)
	if r.err != nil {
		return 0
	}
	if width < 64 && v>>uint(width-1)&1 == 1 {
		v |= ^mask(width)
	}
	return int64(v)
}

// ReadBool reads a single bit.
func (r *Reader) ReadBool() bool { return r.ReadBits(1) == 1 }

// Remaining reports the number of unread bits.
func (r *Reader) Remaining() int { return len(r.buf)*8 - r.pos }

// Err returns the first error encountered.
func (r *Reader) Err() error { return r.err }

func mask(width int) uint64 {
	if width >= 64 {
		return ^uint64(0)
	}
	return uint64(1)<<uint(width) - 1
}
