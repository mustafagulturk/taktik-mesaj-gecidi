package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

func startServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	srv := New(cfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		if err := <-errc; !errors.Is(err, ErrServerClosed) {
			t.Errorf("serve returned %v", err)
		}
	})
	return srv, ln.Addr().String()
}

// unit is a raw protocol-level test peer.
type unit struct {
	t    *testing.T
	id   uint16
	conn net.Conn
	r    *protocol.Reader
	seq  uint16
}

func dialUnit(t *testing.T, addr string, id uint16) *unit {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	u := &unit{t: t, id: id, conn: conn, r: protocol.NewReader(conn)}
	u.send(protocol.AddrGateway, &protocol.Heartbeat{Time: 1}, protocol.Flash, true)
	if a := u.expectAck(); a.Status != protocol.AckDelivered {
		t.Fatalf("register: %v", a.Status)
	}
	return u
}

func (u *unit) frame(dst uint16, m protocol.Message, p protocol.Priority, ack bool) *protocol.Frame {
	u.seq++
	return &protocol.Frame{
		Header: protocol.Header{Priority: p, AckRequired: ack, Src: u.id, Dst: dst, Seq: u.seq},
		Msg:    m,
	}
}

func (u *unit) write(f *protocol.Frame) []byte {
	u.t.Helper()
	wire, err := protocol.Encode(f)
	if err != nil {
		u.t.Fatal(err)
	}
	if _, err := u.conn.Write(wire); err != nil {
		u.t.Fatal(err)
	}
	return wire
}

func (u *unit) send(dst uint16, m protocol.Message, p protocol.Priority, ack bool) *protocol.Frame {
	f := u.frame(dst, m, p, ack)
	u.write(f)
	return f
}

func (u *unit) recv() *protocol.Frame {
	u.t.Helper()
	u.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := u.r.ReadFrame()
	if err != nil {
		u.t.Fatalf("unit %d recv: %v", u.id, err)
	}
	return f
}

func (u *unit) expectAck() *protocol.Ack {
	u.t.Helper()
	f := u.recv()
	a, ok := f.Msg.(*protocol.Ack)
	if !ok || f.Src != protocol.AddrGateway {
		u.t.Fatalf("unit %d: expected ack, got %v", u.id, f)
	}
	return a
}

func (u *unit) expectSilence(d time.Duration) {
	u.t.Helper()
	u.conn.SetReadDeadline(time.Now().Add(d))
	if f, err := u.r.ReadFrame(); err == nil {
		u.t.Fatalf("unit %d: unexpected frame %v", u.id, f)
	}
	// The reader stays usable: a deadline mid-Peek keeps buffered bytes.
}

func (u *unit) expectClosed() {
	u.t.Helper()
	u.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, err := u.r.ReadFrame()
		if err == nil {
			continue // drain whatever was queued before the close
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			u.t.Fatalf("unit %d: connection still open", u.id)
		}
		return
	}
}

var samplePos = &protocol.Position{
	Time: 43200, Pos: protocol.CoordFromDegrees(39.92077, 32.85411),
	Alt: 938, Speed: 42, Heading: 270, Status: protocol.StatusEngaged,
}

func TestUnicastWithAck(t *testing.T) {
	_, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 10)
	b := dialUnit(t, addr, 20)

	sent := a.send(20, samplePos, protocol.Immediate, true)
	if ack := a.expectAck(); ack.Status != protocol.AckDelivered || ack.AckedSeq != sent.Seq {
		t.Fatalf("ack %+v", ack)
	}
	got := b.recv()
	want, _ := protocol.Encode(sent)
	gotWire, _ := protocol.Encode(got)
	if !bytes.Equal(want, gotWire) {
		t.Fatalf("frame altered in transit:\n got %x\nwant %x", gotWire, want)
	}
}

func TestBroadcast(t *testing.T) {
	srv, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 1)
	b := dialUnit(t, addr, 2)
	c := dialUnit(t, addr, 3)
	a.send(protocol.AddrBroadcast, protocol.NewFreeText("tüm birlikler dikkat"), protocol.Flash, false)
	for _, u := range []*unit{b, c} {
		f := u.recv()
		if txt := f.Msg.(*protocol.FreeText).Text; txt != "TUM BIRLIKLER DIKKAT" {
			t.Fatalf("unit %d got %q", u.id, txt)
		}
	}
	a.expectSilence(100 * time.Millisecond)
	if m := srv.Metrics(); m.FramesRouted != 1 {
		t.Fatalf("routed %d", m.FramesRouted)
	}
}

func TestNoRoute(t *testing.T) {
	srv, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 1)
	a.send(99, samplePos, protocol.Routine, true)
	if ack := a.expectAck(); ack.Status != protocol.AckNoRoute {
		t.Fatalf("status %v", ack.Status)
	}
	if srv.Metrics().FramesNoRoute != 1 {
		t.Fatal("no-route not counted")
	}
}

func TestDuplicateSuppressed(t *testing.T) {
	_, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 1)
	b := dialUnit(t, addr, 2)
	f := a.frame(2, samplePos, protocol.Routine, true)
	a.write(f)
	a.write(f) // retransmission with the same seq
	if s := a.expectAck().Status; s != protocol.AckDelivered {
		t.Fatalf("first ack %v", s)
	}
	if s := a.expectAck().Status; s != protocol.AckDuplicate {
		t.Fatalf("second ack %v", s)
	}
	b.recv()
	b.expectSilence(150 * time.Millisecond)
}

func TestDuplicateAcrossReconnect(t *testing.T) {
	_, addr := startServer(t, Config{})
	b := dialUnit(t, addr, 2)
	a := dialUnit(t, addr, 1)
	f := a.frame(2, samplePos, protocol.Routine, true)
	a.write(f)
	a.expectAck()
	b.recv()

	// The unit lost the ack, reconnects and retransmits.
	a2 := dialUnit(t, addr, 1)
	a2.write(f)
	if s := a2.expectAck().Status; s != protocol.AckDuplicate {
		t.Fatalf("status %v", s)
	}
	b.expectSilence(150 * time.Millisecond)
}

func TestCorruptionResync(t *testing.T) {
	srv, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 1)
	b := dialUnit(t, addr, 2)

	bad, _ := protocol.Encode(a.frame(2, samplePos, protocol.Routine, false))
	bad[len(bad)/2] ^= 0x10
	good := a.frame(2, &protocol.Ack{AckedSeq: 7}, protocol.Routine, false)
	goodWire, _ := protocol.Encode(good)

	var stream []byte
	stream = append(stream, 0x00, 0xC3, 0xC3, 0x5A, 0xFF, 0x13) // junk with a fake sync
	stream = append(stream, bad...)
	stream = append(stream, goodWire...)
	if _, err := a.conn.Write(stream); err != nil {
		t.Fatal(err)
	}
	f := b.recv()
	if f.Seq != good.Seq {
		t.Fatalf("got seq %d want %d", f.Seq, good.Seq)
	}
	m := srv.Metrics()
	if m.CRCErrors == 0 || m.BytesSkipped == 0 {
		t.Fatalf("corruption not accounted: %+v", m)
	}
}

func TestIdleTimeout(t *testing.T) {
	srv, addr := startServer(t, Config{IdleTimeout: 150 * time.Millisecond})
	u := dialUnit(t, addr, 5)
	start := time.Now()
	u.expectClosed()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("idle close took %v", d)
	}
	waitFor(t, func() bool { return srv.Metrics().ConnsIdle == 1 && srv.Metrics().ConnsActive == 0 })
}

func TestGarbageFloodDropped(t *testing.T) {
	srv, addr := startServer(t, Config{MaxGarbage: 512})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write(bytes.Repeat([]byte{0xC3}, 4096))
	u := &unit{t: t, conn: conn, r: protocol.NewReader(conn)}
	u.expectClosed()
	waitFor(t, func() bool { return srv.Metrics().ConnsGarbage == 1 })
}

func TestTakeover(t *testing.T) {
	srv, addr := startServer(t, Config{})
	old := dialUnit(t, addr, 7)
	fresh := dialUnit(t, addr, 7)
	old.expectClosed()
	sender := dialUnit(t, addr, 8)
	sender.send(7, samplePos, protocol.Routine, true)
	if s := sender.expectAck().Status; s != protocol.AckDelivered {
		t.Fatalf("status %v", s)
	}
	fresh.recv()
	if srv.Metrics().ConnsTakenOver != 1 {
		t.Fatal("takeover not counted")
	}
}

func TestSpoofedSourceRejected(t *testing.T) {
	srv, addr := startServer(t, Config{})
	a := dialUnit(t, addr, 1)
	b := dialUnit(t, addr, 2)
	a.id = 3 // pretend to be someone else
	a.send(2, samplePos, protocol.Routine, false)
	b.expectSilence(150 * time.Millisecond)
	if srv.Metrics().FramesRejected != 1 {
		t.Fatal("spoof not rejected")
	}
}

func TestSlowConsumerIsolated(t *testing.T) {
	srv, addr := startServer(t, Config{QueueLimit: 8, WriteTimeout: 200 * time.Millisecond})
	slow := dialUnit(t, addr, 1) // never reads again
	fast := dialUnit(t, addr, 2)
	src := dialUnit(t, addr, 3)

	big := protocol.NewFreeText("SPAM SPAM SPAM SPAM SPAM SPAM SPAM SPAM SPAM SPAM SPAM SPAM 123")
	for i := 0; i < 5000; i++ {
		src.send(1, big, protocol.Routine, false)
	}
	start := time.Now()
	src.send(2, samplePos, protocol.Flash, true)
	// Acks and the fast unit's traffic must not wait behind the slow unit.
	if s := src.expectAck().Status; s != protocol.AckDelivered {
		t.Fatalf("status %v", s)
	}
	fast.recv()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("fast path took %v", d)
	}
	_ = slow
	waitFor(t, func() bool { return srv.Metrics().FramesDropped > 0 || srv.Metrics().ConnsActive == 2 })
}

func TestShutdownClosesSessions(t *testing.T) {
	srv := New(Config{})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(ln)
	u := dialUnit(t, ln.Addr().String(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	u.expectClosed()
	if n := srv.Metrics().ConnsActive; n != 0 {
		t.Fatalf("active %d", n)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
