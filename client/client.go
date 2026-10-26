// Package client is a field-unit client for the TMG gateway. It keeps a
// connection alive across network failures (exponential backoff with
// jitter), sends heartbeats for liveness in both directions, and retransmits
// frames that require acknowledgement until the gateway confirms them.
package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

// ErrNoAck is returned by Send when every retransmission went unanswered.
var ErrNoAck = errors.New("client: no acknowledgement from gateway")

// Config tunes a client. Zero values fall back to the defaults below.
type Config struct {
	Addr              string        // gateway address, required
	Unit              uint16        // this unit's address, required (1..0xFFFE)
	HeartbeatInterval time.Duration // default 5s
	ReadTimeout       time.Duration // silence before reconnecting (default 3x heartbeat)
	WriteTimeout      time.Duration // default 3s
	DialTimeout       time.Duration // default 3s
	AckTimeout        time.Duration // wait per attempt (default 1s)
	MaxRetries        int           // retransmissions after the first try (default 5)
	MinBackoff        time.Duration // default 100ms
	MaxBackoff        time.Duration // default 5s
	InboxSize         int           // buffered incoming frames (default 256)
	Logger            *slog.Logger
}

func (c *Config) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.HeartbeatInterval, 5*time.Second)
	def(&c.ReadTimeout, 3*c.HeartbeatInterval)
	def(&c.WriteTimeout, 3*time.Second)
	def(&c.DialTimeout, 3*time.Second)
	def(&c.AckTimeout, time.Second)
	def(&c.MinBackoff, 100*time.Millisecond)
	def(&c.MaxBackoff, 5*time.Second)
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.InboxSize <= 0 {
		c.InboxSize = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Options are per-message header settings.
type Options struct {
	Priority       protocol.Priority
	Classification protocol.Classification
}

// Stats are cumulative client counters.
type Stats struct {
	Connects       uint64
	Retransmits    uint64
	InboxDropped   uint64
	SkippedBytes   uint64
	CorruptFrames  uint64
	FramesReceived uint64
	AcksUnmatched  uint64
}

// Client is a reconnecting connection to the gateway. Run must be running
// for Send to make progress.
type Client struct {
	cfg Config
	log *slog.Logger
	seq atomic.Uint32

	mu      sync.Mutex
	conn    net.Conn
	up      chan struct{} // closed while connected
	pending map[uint16]chan protocol.AckStatus

	writeMu sync.Mutex // keeps deadline+write pairs atomic

	inbox  chan *protocol.Frame
	status atomic.Uint32

	connects, retransmits, inboxDropped, acksUnmatched atomic.Uint64
	skipped, corrupt, received                         atomic.Uint64
}

// New validates cfg and returns a client. Call Run to connect.
func New(cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		return nil, errors.New("client: Addr is required")
	}
	if cfg.Unit == protocol.AddrGateway || cfg.Unit == protocol.AddrBroadcast {
		return nil, errors.New("client: Unit must be in 1..0xFFFE")
	}
	cfg.setDefaults()
	c := &Client{
		cfg:     cfg,
		log:     cfg.Logger.With("unit", cfg.Unit),
		up:      make(chan struct{}),
		pending: make(map[uint16]chan protocol.AckStatus),
		inbox:   make(chan *protocol.Frame, cfg.InboxSize),
	}
	// Start at a random sequence number so a restarted unit does not collide
	// with the gateway's duplicate window.
	c.seq.Store(rand.Uint32N(1 << 16))
	return c, nil
}

// Incoming delivers frames addressed to this unit (and broadcasts). If the
// consumer falls behind, the oldest undelivered frames are dropped.
func (c *Client) Incoming() <-chan *protocol.Frame { return c.inbox }

// SetStatus sets the unit status reported in heartbeats.
func (c *Client) SetStatus(s protocol.UnitStatus) { c.status.Store(uint32(s)) }

// Stats returns a snapshot of the client counters.
func (c *Client) Stats() Stats {
	return Stats{
		Connects:       c.connects.Load(),
		Retransmits:    c.retransmits.Load(),
		InboxDropped:   c.inboxDropped.Load(),
		SkippedBytes:   c.skipped.Load(),
		CorruptFrames:  c.corrupt.Load(),
		FramesReceived: c.received.Load(),
		AcksUnmatched:  c.acksUnmatched.Load(),
	}
}

// WaitConnected blocks until a connection is up or ctx is done.
func (c *Client) WaitConnected(ctx context.Context) error {
	c.mu.Lock()
	up := c.up
	c.mu.Unlock()
	select {
	case <-up:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run connects and keeps reconnecting until ctx is cancelled.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.cfg.MinBackoff
	d := net.Dialer{Timeout: c.cfg.DialTimeout, KeepAlive: 15 * time.Second}
	for {
		conn, err := d.DialContext(ctx, "tcp", c.cfg.Addr)
		if err == nil {
			c.connects.Add(1)
			c.log.Info("connected", "addr", c.cfg.Addr)
			backoff = c.cfg.MinBackoff
			err = c.serve(ctx, conn)
			c.log.Warn("disconnected", "err", err)
		} else {
			c.log.Warn("dial failed", "err", err, "retry_in", backoff)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Full jitter keeps a fleet of units from reconnecting in lockstep.
		wait := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(2*backoff, c.cfg.MaxBackoff)
	}
}

// serve runs one connection until it fails or ctx is cancelled.
func (c *Client) serve(ctx context.Context, conn net.Conn) error {
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c.mu.Lock()
	c.conn = conn
	close(c.up)
	c.mu.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		conn.Close()
		wg.Wait()
		c.mu.Lock()
		c.conn = nil
		c.up = make(chan struct{})
		c.mu.Unlock()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// The first heartbeat registers the unit with the gateway.
		t := time.NewTicker(c.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			if err := c.writeFrame(conn, protocol.Flash, protocol.Unclassified, protocol.AddrGateway, c.heartbeat(), true, c.nextSeq()); err != nil {
				conn.Close()
				return
			}
			select {
			case <-t.C:
			case <-stop:
				return
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}()

	r := protocol.NewReader(conn)
	var prev protocol.ReaderStats
	for {
		conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		f, err := r.ReadFrame()
		st := r.Stats()
		c.skipped.Add(st.SkippedBytes - prev.SkippedBytes)
		c.corrupt.Add(st.BadHeaders + st.CRCErrors + st.BadPayloads - prev.BadHeaders - prev.CRCErrors - prev.BadPayloads)
		prev = st
		if err != nil {
			return err
		}
		c.received.Add(1)
		if ack, ok := f.Msg.(*protocol.Ack); ok && f.Src == protocol.AddrGateway {
			c.resolve(ack)
			continue
		}
		c.deliver(f)
	}
}

func (c *Client) heartbeat() *protocol.Heartbeat {
	now := time.Now().UTC()
	return &protocol.Heartbeat{
		Time:    uint32(now.Hour()*3600 + now.Minute()*60 + now.Second()),
		Status:  protocol.UnitStatus(c.status.Load()),
		Battery: 100,
	}
}

func (c *Client) deliver(f *protocol.Frame) {
	for {
		select {
		case c.inbox <- f:
			return
		default:
		}
		select {
		case <-c.inbox:
			c.inboxDropped.Add(1)
		default:
		}
	}
}

func (c *Client) resolve(a *protocol.Ack) {
	c.mu.Lock()
	ch := c.pending[a.AckedSeq]
	c.mu.Unlock()
	if ch == nil {
		c.acksUnmatched.Add(1) // heartbeat acks, or a late ack after giving up
		return
	}
	select {
	case ch <- a.Status:
	default:
	}
}

func (c *Client) nextSeq() uint16 { return uint16(c.seq.Add(1)) }

func (c *Client) writeFrame(conn net.Conn, p protocol.Priority, cls protocol.Classification, dst uint16, m protocol.Message, ack bool, seq uint16) error {
	f := &protocol.Frame{
		Header: protocol.Header{Priority: p, Classification: cls, AckRequired: ack, Src: c.cfg.Unit, Dst: dst, Seq: seq},
		Msg:    m,
	}
	wire, err := protocol.Encode(f)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	_, err = conn.Write(wire)
	return err
}

// current returns the live connection, waiting for one if necessary.
func (c *Client) current(ctx context.Context) (net.Conn, error) {
	for {
		c.mu.Lock()
		conn, up := c.conn, c.up
		c.mu.Unlock()
		if conn != nil {
			return conn, nil
		}
		select {
		case <-up:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Post sends m once without requesting an acknowledgement. It waits for a
// connection if there is none.
func (c *Client) Post(ctx context.Context, dst uint16, m protocol.Message, o Options) error {
	conn, err := c.current(ctx)
	if err != nil {
		return err
	}
	return c.writeFrame(conn, o.Priority, o.Classification, dst, m, false, c.nextSeq())
}

// Send transmits m with AckRequired set and retransmits it, with the same
// sequence number, until the gateway acknowledges it or retries run out.
// Retransmissions survive reconnects; the gateway's duplicate filter ensures
// the message is delivered at most once. A DUPLICATE ack therefore means an
// earlier attempt got through.
func (c *Client) Send(ctx context.Context, dst uint16, m protocol.Message, o Options) (protocol.AckStatus, error) {
	seq := c.nextSeq()
	ch := make(chan protocol.AckStatus, 1)
	c.mu.Lock()
	c.pending[seq] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			c.retransmits.Add(1)
		}
		conn, err := c.current(ctx)
		if err != nil {
			return 0, err
		}
		if err := c.writeFrame(conn, o.Priority, o.Classification, dst, m, true, seq); err != nil {
			// Validation errors will never succeed; I/O errors are retried.
			if errors.Is(err, protocol.ErrInvalidField) || errors.Is(err, protocol.ErrBadLength) {
				return 0, err
			}
			conn.Close()
		}
		timer.Reset(c.cfg.AckTimeout)
		select {
		case st := <-ch:
			return st, nil
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, ErrNoAck
}
