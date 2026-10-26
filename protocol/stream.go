package protocol

import (
	"bufio"
	"errors"
	"io"
)

// ErrTooMuchGarbage is returned by Reader when the configured garbage budget
// is exhausted without a valid frame, so callers can drop hostile peers.
var ErrTooMuchGarbage = errors.New("protocol: too much garbage on stream")

// ReaderStats counts what the stream reader had to throw away.
type ReaderStats struct {
	Frames       uint64 // valid frames returned
	SkippedBytes uint64 // bytes discarded while hunting for SYNC
	BadHeaders   uint64 // SYNC found but header invalid
	CRCErrors    uint64 // header plausible but CRC failed
	BadPayloads  uint64 // CRC ok but payload fields invalid
}

// Reader extracts frames from a byte stream that may contain corruption.
//
// On any error it discards a single byte after the candidate SYNC and hunts
// again, so a corrupted length field can never swallow the frame after it.
type Reader struct {
	br    *bufio.Reader
	stats ReaderStats
	// MaxGarbage is the number of bytes that may be discarded between two
	// valid frames before ReadFrame fails with ErrTooMuchGarbage. Zero means
	// unlimited.
	MaxGarbage int
}

// NewReader wraps r. The internal buffer always holds a full max-size frame.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 4*MaxFrameLen)}
}

// Stats returns a snapshot of the reader's counters.
func (r *Reader) Stats() ReaderStats { return r.stats }

// ReadFrame returns the next valid frame. Corrupt data is skipped silently
// (and counted); only I/O errors and ErrTooMuchGarbage are returned. A frame
// cut off by EOF is reported as io.ErrUnexpectedEOF.
func (r *Reader) ReadFrame() (*Frame, error) {
	garbage := 0
	truncated := false // a candidate frame ran past EOF
	discard := func(n int) error {
		r.br.Discard(n)
		r.stats.SkippedBytes += uint64(n)
		garbage += n
		if r.MaxGarbage > 0 && garbage > r.MaxGarbage {
			return ErrTooMuchGarbage
		}
		return nil
	}
	for {
		if err := r.huntSync(discard); err != nil {
			if err == io.EOF && truncated {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		hdr, err := r.br.Peek(SyncLen + HeaderLen)
		if err != nil {
			return nil, eofToUnexpected(err)
		}
		h, err := parseHeader(hdr[SyncLen:])
		if err != nil {
			r.stats.BadHeaders++
			if err := discard(1); err != nil {
				return nil, err
			}
			continue
		}
		n := MinFrameLen + int(h.PayloadLen)
		buf, err := r.br.Peek(n)
		if err == io.EOF {
			// The length byte may be corrupt and hide complete frames
			// behind it: keep hunting instead of giving up.
			truncated = true
			if err := discard(1); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		f, err := decodeBody(h, buf)
		if err != nil {
			if errors.Is(err, ErrCRC) {
				r.stats.CRCErrors++
			} else {
				r.stats.BadPayloads++
			}
			if err := discard(1); err != nil {
				return nil, err
			}
			continue
		}
		r.br.Discard(n)
		r.stats.Frames++
		return f, nil
	}
}

// huntSync advances until the buffered input starts with the SYNC word.
func (r *Reader) huntSync(discard func(int) error) error {
	for {
		b, err := r.br.Peek(SyncLen)
		if err != nil {
			if len(b) == 1 && b[0] != syncHi {
				discard(1)
			} else if len(b) == 1 && err == io.EOF {
				// Possibly the first byte of a cut-off frame.
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if b[0] == syncHi && b[1] == syncLo {
			return nil
		}
		// Skip quickly over the buffered bytes that cannot start SYNC.
		skip := 1
		if b[1] != syncHi {
			skip = 2
		}
		if err := discard(skip); err != nil {
			return err
		}
	}
}

func eofToUnexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// Writer serialises frames onto an io.Writer.
type Writer struct {
	w   io.Writer
	buf []byte
}

// NewWriter returns a frame writer on w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteFrame encodes and writes f in a single Write call.
func (w *Writer) WriteFrame(f *Frame) error {
	var err error
	w.buf, err = AppendFrame(w.buf[:0], f)
	if err != nil {
		return err
	}
	_, err = w.w.Write(w.buf)
	return err
}
