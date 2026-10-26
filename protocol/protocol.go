// Package protocol defines the TMG (Tactical Message Gateway) wire format:
// fixed bit-layout tactical messages wrapped in a CRC-protected frame.
//
// Frame layout (big-endian, MSB-first):
//
//	+--------+----------------------------------------------+---------+--------+
//	| SYNC   | HEADER (72 bits)                             | PAYLOAD | CRC16  |
//	| 0xC35A | ver:3 type:5 pri:2 cls:2 ack:1 rsv:3         | 0..255  | CCITT  |
//	| 16 bit | src:16 dst:16 seq:16 len:8                   | bytes   | 16 bit |
//	+--------+----------------------------------------------+---------+--------+
//
// The CRC covers HEADER and PAYLOAD. Every message type has a fixed bit
// layout (FreeText is fixed per character), so the payload length is fully
// determined by the header and is validated before the CRC is checked.
package protocol

import (
	"errors"
	"fmt"
)

const (
	// Sync is the two-byte frame delimiter used for resynchronisation.
	Sync uint16 = 0xC35A
	// Version is the only protocol version this package speaks.
	Version = 1

	SyncLen        = 2
	HeaderLen      = 9
	CRCLen         = 2
	MaxPayload     = 255
	MinFrameLen    = SyncLen + HeaderLen + CRCLen
	MaxFrameLen    = MinFrameLen + MaxPayload
	headerBits     = HeaderLen * 8
	syncHi, syncLo = byte(Sync >> 8), byte(Sync & 0xFF)
)

// Well-known addresses.
const (
	AddrGateway   uint16 = 0x0000
	AddrBroadcast uint16 = 0xFFFF
)

// MsgType identifies the payload layout.
type MsgType uint8

const (
	TypeHeartbeat MsgType = 1
	TypePosition  MsgType = 2
	TypeContact   MsgType = 3
	TypeOrder     MsgType = 4
	TypeAck       MsgType = 5
	TypeFreeText  MsgType = 6
)

func (t MsgType) String() string {
	switch t {
	case TypeHeartbeat:
		return "HEARTBEAT"
	case TypePosition:
		return "POSREP"
	case TypeContact:
		return "CONTACT"
	case TypeOrder:
		return "ORDER"
	case TypeAck:
		return "ACK"
	case TypeFreeText:
		return "FREETEXT"
	}
	return fmt.Sprintf("TYPE(%d)", uint8(t))
}

// Priority follows the classic precedence ladder; higher is more urgent.
type Priority uint8

const (
	Routine Priority = iota
	PriorityPrec
	Immediate
	Flash
)

func (p Priority) String() string {
	return [...]string{"ROUTINE", "PRIORITY", "IMMEDIATE", "FLASH"}[p&3]
}

// Classification marking carried in every header.
type Classification uint8

const (
	Unclassified Classification = iota
	Restricted
	Confidential
	Secret
)

func (c Classification) String() string {
	return [...]string{"UNCLASSIFIED", "RESTRICTED", "CONFIDENTIAL", "SECRET"}[c&3]
}

// Header is the decoded fixed header of a frame.
type Header struct {
	Version        uint8
	Type           MsgType
	Priority       Priority
	Classification Classification
	AckRequired    bool
	Src            uint16
	Dst            uint16
	Seq            uint16
	PayloadLen     uint8
}

// Errors returned by the codec. Use errors.Is to test for them.
var (
	ErrBadSync       = errors.New("protocol: bad sync word")
	ErrBadVersion    = errors.New("protocol: unsupported version")
	ErrUnknownType   = errors.New("protocol: unknown message type")
	ErrReservedBits  = errors.New("protocol: reserved bits set")
	ErrBadLength     = errors.New("protocol: payload length invalid for type")
	ErrCRC           = errors.New("protocol: CRC mismatch")
	ErrTruncated     = errors.New("protocol: truncated frame")
	ErrTrailingBytes = errors.New("protocol: trailing bytes after frame")
	ErrInvalidField  = errors.New("protocol: field out of range")
)
