package protocol

import (
	"fmt"
	"math"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/internal/bitio"
)

// Message is a payload with a fixed bit layout.
type Message interface {
	Type() MsgType
	encode(w *bitio.Writer) error
	decode(r *bitio.Reader) error
}

// Field widths. Changing any of these is a wire-incompatible change.
const (
	bitsTime    = 17 // seconds since 00:00Z, 0..86399
	bitsLat     = 24 // signed, LSB = 90/2^23 deg (~1.2 m)
	bitsLon     = 25 // signed, LSB = 180/2^24 deg (~1.2 m at equator)
	bitsAlt     = 16 // signed metres
	bitsSpeed   = 10 // km/h, 0..1023
	bitsHeading = 9  // degrees, 0..359
	bitsTextLen = 6  // FreeText characters, 0..63

	MaxTextLen    = 1<<bitsTextLen - 1
	SecondsPerDay = 86400
)

// Fixed payload sizes in bytes.
const (
	HeartbeatLen = 4
	PositionLen  = 13
	ContactLen   = 13
	OrderLen     = 12
	AckLen       = 3
)

// Coord is a quantised WGS-84 position.
type Coord struct {
	Lat int32 // units of 90/2^23 degrees
	Lon int32 // units of 180/2^24 degrees
}

const (
	latScale = float64(1<<23) / 90
	lonScale = float64(1<<24) / 180
	latMax   = 1<<23 - 1
	latMin   = -(1 << 23)
	lonMax   = 1<<24 - 1
	lonMin   = -(1 << 24)
)

// CoordFromDegrees quantises lat/lon. Values are clamped to the
// representable range (+90 and +180 map to the largest code); NaN maps to 0.
func CoordFromDegrees(lat, lon float64) Coord {
	if math.IsNaN(lat) {
		lat = 0
	}
	if math.IsNaN(lon) {
		lon = 0
	}
	return Coord{
		Lat: int32(clamp(math.Round(lat*latScale), latMin, latMax)),
		Lon: int32(clamp(math.Round(lon*lonScale), lonMin, lonMax)),
	}
}

// Degrees returns the coordinate in decimal degrees.
func (c Coord) Degrees() (lat, lon float64) {
	return float64(c.Lat) / latScale, float64(c.Lon) / lonScale
}

func (c Coord) String() string {
	lat, lon := c.Degrees()
	return fmt.Sprintf("%.5f,%.5f", lat, lon)
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

func (c Coord) encode(w *bitio.Writer) {
	w.WriteSigned(int64(c.Lat), bitsLat)
	w.WriteSigned(int64(c.Lon), bitsLon)
}

func (c *Coord) decode(r *bitio.Reader) {
	c.Lat = int32(r.ReadSigned(bitsLat))
	c.Lon = int32(r.ReadSigned(bitsLon))
}

func checkTime(t uint32) error {
	if t >= SecondsPerDay {
		return fmt.Errorf("%w: time %d", ErrInvalidField, t)
	}
	return nil
}

func checkHeading(h uint16) error {
	if h >= 360 {
		return fmt.Errorf("%w: heading %d", ErrInvalidField, h)
	}
	return nil
}

func reserved(r *bitio.Reader, width int) error {
	if r.ReadBits(width) != 0 {
		return ErrReservedBits
	}
	return nil
}

// UnitStatus is the operational state reported in heartbeats and POSREPs.
type UnitStatus uint8

const (
	StatusOperational UnitStatus = iota
	StatusDegraded
	StatusEngaged
	StatusWithdrawing
	StatusOutOfAction
	numUnitStatus
)

func (s UnitStatus) String() string {
	if s < numUnitStatus {
		return [...]string{"OPERATIONAL", "DEGRADED", "ENGAGED", "WITHDRAWING", "OUT_OF_ACTION"}[s]
	}
	return fmt.Sprintf("STATUS(%d)", uint8(s))
}

// Heartbeat (32 bits): time:17 status:4 battery:7 rsv:4
type Heartbeat struct {
	Time    uint32
	Status  UnitStatus
	Battery uint8 // percent, 0..100
}

func (*Heartbeat) Type() MsgType { return TypeHeartbeat }

func (m *Heartbeat) encode(w *bitio.Writer) error {
	if err := checkTime(m.Time); err != nil {
		return err
	}
	if m.Status >= numUnitStatus || m.Battery > 100 {
		return fmt.Errorf("%w: heartbeat status/battery", ErrInvalidField)
	}
	w.WriteBits(uint64(m.Time), bitsTime)
	w.WriteBits(uint64(m.Status), 4)
	w.WriteBits(uint64(m.Battery), 7)
	w.WriteBits(0, 4)
	return nil
}

func (m *Heartbeat) decode(r *bitio.Reader) error {
	m.Time = uint32(r.ReadBits(bitsTime))
	m.Status = UnitStatus(r.ReadBits(4))
	m.Battery = uint8(r.ReadBits(7))
	if err := reserved(r, 4); err != nil {
		return err
	}
	return m.encode(bitio.NewWriter(HeartbeatLen)) // reuse validation
}

// Position report (104 bits):
// time:17 lat:24 lon:25 alt:16 speed:10 heading:9 status:3
type Position struct {
	Time    uint32
	Pos     Coord
	Alt     int16 // metres
	Speed   uint16
	Heading uint16
	Status  UnitStatus
}

func (*Position) Type() MsgType { return TypePosition }

func (m *Position) encode(w *bitio.Writer) error {
	if err := checkTime(m.Time); err != nil {
		return err
	}
	if err := checkHeading(m.Heading); err != nil {
		return err
	}
	if m.Status >= numUnitStatus {
		return fmt.Errorf("%w: status %d", ErrInvalidField, m.Status)
	}
	w.WriteBits(uint64(m.Time), bitsTime)
	m.Pos.encode(w)
	w.WriteSigned(int64(m.Alt), bitsAlt)
	w.WriteBits(uint64(m.Speed), bitsSpeed)
	w.WriteBits(uint64(m.Heading), bitsHeading)
	w.WriteBits(uint64(m.Status), 3)
	return nil
}

func (m *Position) decode(r *bitio.Reader) error {
	m.Time = uint32(r.ReadBits(bitsTime))
	m.Pos.decode(r)
	m.Alt = int16(r.ReadSigned(bitsAlt))
	m.Speed = uint16(r.ReadBits(bitsSpeed))
	m.Heading = uint16(r.ReadBits(bitsHeading))
	m.Status = UnitStatus(r.ReadBits(3))
	return m.encode(bitio.NewWriter(PositionLen))
}

// ContactCategory classifies an observed contact.
type ContactCategory uint8

const (
	ContactUnknown ContactCategory = iota
	ContactInfantry
	ContactArmor
	ContactArtillery
	ContactAir
	ContactNaval
	ContactVehicle
	ContactUAV
	numContactCategory
)

func (c ContactCategory) String() string {
	if c < numContactCategory {
		return [...]string{"UNKNOWN", "INFANTRY", "ARMOR", "ARTILLERY", "AIR", "NAVAL", "VEHICLE", "UAV"}[c]
	}
	return fmt.Sprintf("CATEGORY(%d)", uint8(c))
}

// Contact report (104 bits):
// time:17 lat:24 lon:25 category:4 count:7 heading:9 speed:10 confidence:3 rsv:5
type Contact struct {
	Time       uint32
	Pos        Coord
	Category   ContactCategory
	Count      uint8 // 0..127
	Heading    uint16
	Speed      uint16
	Confidence uint8 // 0..7
}

func (*Contact) Type() MsgType { return TypeContact }

func (m *Contact) encode(w *bitio.Writer) error {
	if err := checkTime(m.Time); err != nil {
		return err
	}
	if err := checkHeading(m.Heading); err != nil {
		return err
	}
	if m.Category >= numContactCategory {
		return fmt.Errorf("%w: category %d", ErrInvalidField, m.Category)
	}
	w.WriteBits(uint64(m.Time), bitsTime)
	m.Pos.encode(w)
	w.WriteBits(uint64(m.Category), 4)
	w.WriteBits(uint64(m.Count), 7)
	w.WriteBits(uint64(m.Heading), bitsHeading)
	w.WriteBits(uint64(m.Speed), bitsSpeed)
	w.WriteBits(uint64(m.Confidence), 3)
	w.WriteBits(0, 5)
	return nil
}

func (m *Contact) decode(r *bitio.Reader) error {
	m.Time = uint32(r.ReadBits(bitsTime))
	m.Pos.decode(r)
	m.Category = ContactCategory(r.ReadBits(4))
	m.Count = uint8(r.ReadBits(7))
	m.Heading = uint16(r.ReadBits(bitsHeading))
	m.Speed = uint16(r.ReadBits(bitsSpeed))
	m.Confidence = uint8(r.ReadBits(3))
	if err := reserved(r, 5); err != nil {
		return err
	}
	return m.encode(bitio.NewWriter(ContactLen))
}

// OrderCode is the action requested by an order.
type OrderCode uint8

const (
	OrderHold OrderCode = iota
	OrderAdvance
	OrderWithdraw
	OrderRegroup
	OrderObserve
	OrderEngage
	OrderCeaseFire
	OrderResupply
	OrderMedevac
	numOrderCode
)

func (o OrderCode) String() string {
	if o < numOrderCode {
		return [...]string{"HOLD", "ADVANCE", "WITHDRAW", "REGROUP", "OBSERVE", "ENGAGE", "CEASE_FIRE", "RESUPPLY", "MEDEVAC"}[o]
	}
	return fmt.Sprintf("ORDER(%d)", uint8(o))
}

// Order (96 bits): time:17 code:6 target_lat:24 target_lon:25 execute_at:17 rsv:7
type Order struct {
	Time      uint32
	Code      OrderCode
	Target    Coord
	ExecuteAt uint32
}

func (*Order) Type() MsgType { return TypeOrder }

func (m *Order) encode(w *bitio.Writer) error {
	if err := checkTime(m.Time); err != nil {
		return err
	}
	if err := checkTime(m.ExecuteAt); err != nil {
		return err
	}
	if m.Code >= numOrderCode {
		return fmt.Errorf("%w: order code %d", ErrInvalidField, m.Code)
	}
	w.WriteBits(uint64(m.Time), bitsTime)
	w.WriteBits(uint64(m.Code), 6)
	m.Target.encode(w)
	w.WriteBits(uint64(m.ExecuteAt), bitsTime)
	w.WriteBits(0, 7)
	return nil
}

func (m *Order) decode(r *bitio.Reader) error {
	m.Time = uint32(r.ReadBits(bitsTime))
	m.Code = OrderCode(r.ReadBits(6))
	m.Target.decode(r)
	m.ExecuteAt = uint32(r.ReadBits(bitsTime))
	if err := reserved(r, 7); err != nil {
		return err
	}
	return m.encode(bitio.NewWriter(OrderLen))
}

// AckStatus tells the sender what the gateway did with a frame.
type AckStatus uint8

const (
	AckDelivered AckStatus = iota // handed to every addressed live session
	AckNoRoute                    // destination not connected
	AckDuplicate                  // (src,seq) already seen; not re-delivered
	AckRejected                   // valid frame, refused by policy
	AckQueueFull                  // destination too slow, dropped
	numAckStatus
)

func (s AckStatus) String() string {
	if s < numAckStatus {
		return [...]string{"DELIVERED", "NO_ROUTE", "DUPLICATE", "REJECTED", "QUEUE_FULL"}[s]
	}
	return fmt.Sprintf("ACK(%d)", uint8(s))
}

// Ack (24 bits): acked_seq:16 status:3 rsv:5
type Ack struct {
	AckedSeq uint16
	Status   AckStatus
}

func (*Ack) Type() MsgType { return TypeAck }

func (m *Ack) encode(w *bitio.Writer) error {
	if m.Status >= numAckStatus {
		return fmt.Errorf("%w: ack status %d", ErrInvalidField, m.Status)
	}
	w.WriteBits(uint64(m.AckedSeq), 16)
	w.WriteBits(uint64(m.Status), 3)
	w.WriteBits(0, 5)
	return nil
}

func (m *Ack) decode(r *bitio.Reader) error {
	m.AckedSeq = uint16(r.ReadBits(16))
	m.Status = AckStatus(r.ReadBits(3))
	if err := reserved(r, 5); err != nil {
		return err
	}
	return m.encode(bitio.NewWriter(AckLen))
}

// FreeText (6 + 6n bits, zero-padded): len:6 then n 6-bit characters.
type FreeText struct {
	Text string
}

// NewFreeText normalises s (upper case, Turkish letters folded) for sending.
func NewFreeText(s string) *FreeText { return &FreeText{Text: NormalizeText(s)} }

func (*FreeText) Type() MsgType { return TypeFreeText }

// FreeTextLen returns the payload size for n characters.
func FreeTextLen(n int) int { return (bitsTextLen + 6*n + 7) / 8 }

func (m *FreeText) encode(w *bitio.Writer) error {
	if len(m.Text) > MaxTextLen {
		return fmt.Errorf("%w: text longer than %d", ErrInvalidField, MaxTextLen)
	}
	codes, err := sixbitCodes(m.Text)
	if err != nil {
		return err
	}
	w.WriteBits(uint64(len(codes)), bitsTextLen)
	for _, c := range codes {
		w.WriteBits(uint64(c), 6)
	}
	return nil
}

func (m *FreeText) decode(r *bitio.Reader) error {
	n := int(r.ReadBits(bitsTextLen))
	if r.Err() != nil {
		return r.Err()
	}
	// The character count fixes the payload size exactly.
	if (r.Remaining()+bitsTextLen)/8 != FreeTextLen(n) {
		return ErrBadLength
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = sixbitAlphabet[r.ReadBits(6)]
	}
	if r.Err() != nil {
		return r.Err()
	}
	// Padding (under 8 bits, given the length check) must be zero.
	if pad := r.Remaining(); pad > 0 && r.ReadBits(pad) != 0 {
		return ErrReservedBits
	}
	m.Text = string(buf)
	return nil
}

// newMessage returns an empty message for t and the payload length it
// requires (-1 when the length is variable).
func newMessage(t MsgType) (Message, int) {
	switch t {
	case TypeHeartbeat:
		return &Heartbeat{}, HeartbeatLen
	case TypePosition:
		return &Position{}, PositionLen
	case TypeContact:
		return &Contact{}, ContactLen
	case TypeOrder:
		return &Order{}, OrderLen
	case TypeAck:
		return &Ack{}, AckLen
	case TypeFreeText:
		return &FreeText{}, -1
	}
	return nil, 0
}

// validPayloadLen reports whether n bytes is a legal payload size for t.
func validPayloadLen(t MsgType, n int) bool {
	_, want := newMessage(t)
	if want >= 0 {
		return n == want
	}
	return n >= 1 && n <= FreeTextLen(MaxTextLen)
}
