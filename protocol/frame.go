package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/internal/bitio"
)

// Frame is a header plus its decoded message.
type Frame struct {
	Header
	Msg Message
}

func (f *Frame) String() string {
	return fmt.Sprintf("%s %s/%s %04x->%04x seq=%d ack=%t %+v",
		f.Msg.Type(), f.Priority, f.Classification, f.Src, f.Dst, f.Seq, f.AckRequired, f.Msg)
}

// AppendFrame encodes f and appends it to dst. Header.Type, Version and
// PayloadLen are derived from the message and need not be set by callers.
func AppendFrame(dst []byte, f *Frame) ([]byte, error) {
	if f.Msg == nil {
		return dst, fmt.Errorf("%w: nil message", ErrInvalidField)
	}
	pw := bitio.NewWriter(16)
	if err := f.Msg.encode(pw); err != nil {
		return dst, err
	}
	payload, err := pw.Bytes()
	if err != nil {
		return dst, fmt.Errorf("%w: %v", ErrInvalidField, err)
	}
	if len(payload) > MaxPayload {
		return dst, ErrBadLength
	}
	if f.Priority > Flash || f.Classification > Secret {
		return dst, fmt.Errorf("%w: priority/classification", ErrInvalidField)
	}

	hw := bitio.NewWriter(HeaderLen)
	hw.WriteBits(Version, 3)
	hw.WriteBits(uint64(f.Msg.Type()), 5)
	hw.WriteBits(uint64(f.Priority), 2)
	hw.WriteBits(uint64(f.Classification), 2)
	hw.WriteBool(f.AckRequired)
	hw.WriteBits(0, 3)
	hw.WriteBits(uint64(f.Src), 16)
	hw.WriteBits(uint64(f.Dst), 16)
	hw.WriteBits(uint64(f.Seq), 16)
	hw.WriteBits(uint64(len(payload)), 8)
	header, err := hw.Bytes()
	if err != nil {
		return dst, err
	}

	start := len(dst)
	dst = binary.BigEndian.AppendUint16(dst, Sync)
	dst = append(dst, header...)
	dst = append(dst, payload...)
	crc := CRC16(dst[start+SyncLen:])
	dst = binary.BigEndian.AppendUint16(dst, crc)
	return dst, nil
}

// Encode returns the wire encoding of f.
func Encode(f *Frame) ([]byte, error) { return AppendFrame(make([]byte, 0, 32), f) }

// parseHeader decodes and sanity-checks the 9 header bytes following SYNC.
func parseHeader(b []byte) (Header, error) {
	r := bitio.NewReader(b[:HeaderLen])
	h := Header{
		Version:        uint8(r.ReadBits(3)),
		Type:           MsgType(r.ReadBits(5)),
		Priority:       Priority(r.ReadBits(2)),
		Classification: Classification(r.ReadBits(2)),
		AckRequired:    r.ReadBool(),
	}
	rsv := r.ReadBits(3)
	h.Src = uint16(r.ReadBits(16))
	h.Dst = uint16(r.ReadBits(16))
	h.Seq = uint16(r.ReadBits(16))
	h.PayloadLen = uint8(r.ReadBits(8))
	switch {
	case r.Err() != nil:
		return h, r.Err()
	case h.Version != Version:
		return h, ErrBadVersion
	case rsv != 0:
		return h, ErrReservedBits
	}
	if m, _ := newMessage(h.Type); m == nil {
		return h, ErrUnknownType
	}
	if !validPayloadLen(h.Type, int(h.PayloadLen)) {
		return h, ErrBadLength
	}
	return h, nil
}

// Decode parses exactly one frame from b. It is strict: trailing bytes,
// reserved bits, out-of-range fields and CRC errors are all rejected.
func Decode(b []byte) (*Frame, error) {
	if len(b) < MinFrameLen {
		return nil, ErrTruncated
	}
	if binary.BigEndian.Uint16(b) != Sync {
		return nil, ErrBadSync
	}
	h, err := parseHeader(b[SyncLen:])
	if err != nil {
		return nil, err
	}
	n := MinFrameLen + int(h.PayloadLen)
	switch {
	case len(b) < n:
		return nil, ErrTruncated
	case len(b) > n:
		return nil, ErrTrailingBytes
	}
	return decodeBody(h, b)
}

// decodeBody verifies the CRC of the complete frame b and decodes the payload.
func decodeBody(h Header, b []byte) (*Frame, error) {
	end := SyncLen + HeaderLen + int(h.PayloadLen)
	if CRC16(b[SyncLen:end]) != binary.BigEndian.Uint16(b[end:]) {
		return nil, ErrCRC
	}
	msg, _ := newMessage(h.Type)
	r := bitio.NewReader(b[SyncLen+HeaderLen : end])
	if err := msg.decode(r); err != nil {
		return nil, err
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	// Fixed layouts may carry trailing pad bits in the last byte only.
	if r.Remaining() >= 8 {
		return nil, ErrBadLength
	}
	return &Frame{Header: h, Msg: msg}, nil
}
