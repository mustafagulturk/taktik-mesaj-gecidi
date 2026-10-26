// Package gateway implements the TMG distribution server: it accepts TCP
// connections from field units, decodes framed tactical messages, and routes
// them to a single unit or broadcasts them to all connected units.
//
// Resilience properties:
//   - Corrupt bytes are skipped by the resynchronising protocol.Reader; a
//     peer that sends only garbage is dropped once its garbage budget runs out.
//   - Each session has its own read idle timeout and write deadline, so a
//     dead or stalled peer cannot hold resources or block other sessions.
//   - Outbound traffic goes through a bounded per-session priority queue; a
//     slow consumer loses ROUTINE traffic first and never stalls the router.
//   - Duplicate (src, seq) pairs are suppressed server-wide, so retransmits
//     after a reconnect are acknowledged but not delivered twice.
//   - A unit that reconnects takes over its old session immediately.
package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

// ErrServerClosed is returned by Serve after Shutdown.
var ErrServerClosed = errors.New("gateway: server closed")

// Config tunes the server. Zero values fall back to the defaults below.
type Config struct {
	IdleTimeout  time.Duration // max time without a valid frame (default 30s)
	WriteTimeout time.Duration // per-write deadline (default 5s)
	QueueLimit   int           // frames per priority level per session (default 256)
	MaxGarbage   int           // bytes of junk tolerated between frames (default 64 KiB)
	DedupWindow  int           // remembered sequence numbers per unit (default 4096)
	DedupTTL     time.Duration // how long a (src, seq) pair is remembered (default 2m)
	MaxConns     int           // concurrent connections (default 1024)
	Logger       *slog.Logger  // default: discard
}

func (c *Config) setDefaults() {
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.QueueLimit <= 0 {
		c.QueueLimit = 256
	}
	if c.MaxGarbage <= 0 {
		c.MaxGarbage = 64 << 10
	}
	if c.DedupWindow <= 0 {
		c.DedupWindow = 4096
	}
	if c.DedupTTL <= 0 {
		c.DedupTTL = 2 * time.Minute
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Server routes frames between connected units.
type Server struct {
	cfg     Config
	log     *slog.Logger
	metrics Metrics
	dedup   *dedup
	seq     atomic.Uint32 // sequence numbers for gateway-originated frames

	mu       sync.Mutex
	units    map[uint16]*session
	sessions map[*session]struct{}
	ln       net.Listener
	closing  bool

	wg sync.WaitGroup
}

// New returns a server with the given configuration.
func New(cfg Config) *Server {
	cfg.setDefaults()
	return &Server{
		cfg:      cfg,
		log:      cfg.Logger,
		dedup:    newDedup(cfg.DedupWindow, cfg.DedupTTL),
		units:    make(map[uint16]*session),
		sessions: make(map[*session]struct{}),
	}
}

// Metrics returns a snapshot of the server counters.
func (s *Server) Metrics() Snapshot { return s.metrics.Snapshot() }

// Units returns the unit IDs that currently have a bound session.
func (s *Server) Units() []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uint16, 0, len(s.units))
	for id := range s.units {
		ids = append(ids, id)
	}
	return ids
}

// ListenAndServe listens on addr and serves until Shutdown.
func (s *Server) ListenAndServe(addr string) error {
	lc := net.ListenConfig{KeepAlive: 15 * time.Second}
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("gateway listening", "addr", ln.Addr().String())

	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosing() {
				return ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() || errors.Is(err, syscallEMFILE) {
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				s.log.Warn("accept error, backing off", "err", err, "backoff", backoff)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		s.accept(conn)
	}
}

func (s *Server) accept(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	s.mu.Lock()
	if s.closing || len(s.sessions) >= s.cfg.MaxConns {
		s.mu.Unlock()
		s.log.Warn("connection refused", "remote", conn.RemoteAddr().String(), "reason", "limit or shutdown")
		conn.Close()
		return
	}
	sess := newSession(s, conn)
	s.sessions[sess] = struct{}{}
	s.wg.Add(2)
	s.mu.Unlock()

	s.metrics.ConnsAccepted.Add(1)
	s.metrics.ConnsActive.Add(1)
	s.log.Info("connection opened", "remote", sess.remote)
	go sess.readLoop()
	go sess.writeLoop()
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Shutdown stops accepting, closes every session and waits for their
// goroutines to exit or ctx to expire.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	if s.ln != nil {
		s.ln.Close()
	}
	live := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		live = append(live, sess)
	}
	s.mu.Unlock()

	for _, sess := range live {
		sess.close("shutdown")
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// bind associates sess with unit, evicting any older session for that unit.
func (s *Server) bind(sess *session, unit uint16) {
	s.mu.Lock()
	if _, live := s.sessions[sess]; !live {
		s.mu.Unlock()
		return
	}
	old := s.units[unit]
	s.units[unit] = sess
	sess.unit = unit
	s.mu.Unlock()
	s.log.Info("unit registered", "unit", unit, "remote", sess.remote)
	if old != nil && old != sess {
		s.metrics.ConnsTakenOver.Add(1)
		old.close("taken over by new connection")
	}
}

// unbind forgets sess and returns the unit it was bound to (0 if none).
func (s *Server) unbind(sess *session) uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sess)
	if sess.unit != 0 && s.units[sess.unit] == sess {
		delete(s.units, sess.unit)
	}
	return sess.unit
}

// handle processes one valid frame received on sess.
func (s *Server) handle(sess *session, f *protocol.Frame) {
	s.metrics.FramesIn.Add(1)
	if f.Src == protocol.AddrGateway || f.Src == protocol.AddrBroadcast {
		s.metrics.FramesRejected.Add(1)
		return
	}
	switch {
	case sess.unit == 0:
		s.bind(sess, f.Src)
	case f.Src != sess.unit:
		// A session may only speak for the unit it registered as.
		s.metrics.FramesRejected.Add(1)
		s.log.Warn("spoofed source", "unit", sess.unit, "claimed", f.Src)
		return
	}

	if f.Dst == protocol.AddrGateway {
		switch f.Type {
		case protocol.TypeHeartbeat:
			// The read deadline is already refreshed; acking lets the unit
			// detect a dead gateway as quickly as we detect a dead unit.
			s.ack(sess, f, protocol.AckDelivered)
		case protocol.TypeAck:
		default:
			s.metrics.FramesRejected.Add(1)
			s.ack(sess, f, protocol.AckRejected)
		}
		return
	}

	if s.dedup.seen(f.Src, f.Seq, time.Now()) {
		s.metrics.FramesDuplicate.Add(1)
		s.ack(sess, f, protocol.AckDuplicate)
		return
	}
	s.ack(sess, f, s.route(sess, f))
}

// route fans a frame out and returns the status to acknowledge with.
func (s *Server) route(from *session, f *protocol.Frame) protocol.AckStatus {
	wire, err := protocol.Encode(f)
	if err != nil { // cannot happen for a frame that just decoded
		s.metrics.FramesRejected.Add(1)
		return protocol.AckRejected
	}

	s.mu.Lock()
	var targets []*session
	if f.Dst == protocol.AddrBroadcast {
		targets = make([]*session, 0, len(s.units))
		for _, t := range s.units {
			if t != from {
				targets = append(targets, t)
			}
		}
	} else if t := s.units[f.Dst]; t != nil {
		targets = []*session{t}
	}
	s.mu.Unlock()

	if len(targets) == 0 && f.Dst != protocol.AddrBroadcast {
		s.metrics.FramesNoRoute.Add(1)
		return protocol.AckNoRoute
	}
	delivered := 0
	for _, t := range targets {
		if t.enqueue(f.Priority, wire) {
			delivered++
		}
	}
	s.metrics.FramesRouted.Add(1)
	if len(targets) > 0 && delivered == 0 {
		return protocol.AckQueueFull
	}
	return protocol.AckDelivered
}

func (s *Server) ack(sess *session, f *protocol.Frame, status protocol.AckStatus) {
	if !f.AckRequired {
		return
	}
	reply := &protocol.Frame{
		Header: protocol.Header{
			Priority:       f.Priority,
			Classification: f.Classification,
			Src:            protocol.AddrGateway,
			Dst:            f.Src,
			Seq:            uint16(s.seq.Add(1)),
		},
		Msg: &protocol.Ack{AckedSeq: f.Seq, Status: status},
	}
	wire, err := protocol.Encode(reply)
	if err != nil {
		return
	}
	// Acks travel at FLASH so they are never starved by the data they confirm.
	sess.enqueue(protocol.Flash, wire)
}
