package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/gateway"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

func startGateway(t *testing.T, addr string) (*gateway.Server, string) {
	t.Helper()
	srv := gateway.New(gateway.Config{})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { stopGateway(srv) })
	return srv, ln.Addr().String()
}

func stopGateway(srv *gateway.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

func runClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// registered waits until the gateway knows unit id.
func registered(t *testing.T, srv *gateway.Server, id uint16) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, u := range srv.Units() {
			if u == id {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("unit %d never registered", id)
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{Unit: 1}); err == nil {
		t.Fatal("missing addr accepted")
	}
	for _, id := range []uint16{protocol.AddrGateway, protocol.AddrBroadcast} {
		if _, err := New(Config{Addr: "x:1", Unit: id}); err == nil {
			t.Fatalf("unit %#x accepted", id)
		}
	}
}

func TestSendReceive(t *testing.T) {
	srv, addr := startGateway(t, "127.0.0.1:0")
	a := runClient(t, Config{Addr: addr, Unit: 100})
	b := runClient(t, Config{Addr: addr, Unit: 200})
	registered(t, srv, 200)

	order := &protocol.Order{Time: 3600, Code: protocol.OrderAdvance, Target: protocol.CoordFromDegrees(41.0151, 28.9795), ExecuteAt: 3900}
	st, err := a.Send(ctxT(t, 3*time.Second), 200, order, Options{Priority: protocol.Flash, Classification: protocol.Secret})
	if err != nil || st != protocol.AckDelivered {
		t.Fatalf("send: %v %v", st, err)
	}
	select {
	case f := <-b.Incoming():
		got, ok := f.Msg.(*protocol.Order)
		if !ok || *got != *order || f.Src != 100 || f.Classification != protocol.Secret {
			t.Fatalf("got %v", f)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("order not received")
	}
}

func TestSendBlocksUntilConnected(t *testing.T) {
	c := runClient(t, Config{Addr: "127.0.0.1:1", Unit: 1, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	_, err := c.Send(ctxT(t, 200*time.Millisecond), 2, &protocol.Ack{}, Options{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestReconnectAfterGatewayRestart(t *testing.T) {
	srv, addr := startGateway(t, "127.0.0.1:0")
	a := runClient(t, Config{Addr: addr, Unit: 1, MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond})
	registered(t, srv, 1)

	stopGateway(srv)
	srv2, _ := startGateway(t, addr)
	registered(t, srv2, 1)
	b := runClient(t, Config{Addr: addr, Unit: 2})
	registered(t, srv2, 2)

	st, err := a.Send(ctxT(t, 3*time.Second), 2, samplePos, Options{})
	if err != nil || st != protocol.AckDelivered {
		t.Fatalf("send after restart: %v %v", st, err)
	}
	<-b.Incoming()
	if n := a.Stats().Connects; n < 2 {
		t.Fatalf("connects %d", n)
	}
}

func TestDeadGatewayDetected(t *testing.T) {
	// A listener that accepts and then says nothing, like a hung peer.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()
	c := runClient(t, Config{
		Addr: ln.Addr().String(), Unit: 1,
		HeartbeatInterval: 20 * time.Millisecond, ReadTimeout: 80 * time.Millisecond,
		MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
	})
	deadline := time.Now().Add(3 * time.Second)
	for c.Stats().Connects < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("silent gateway not detected; connects=%d", c.Stats().Connects)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNoAckAfterRetries(t *testing.T) {
	// Accepts and swallows frames, never acknowledges anything.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1024)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	c := runClient(t, Config{Addr: ln.Addr().String(), Unit: 1, AckTimeout: 20 * time.Millisecond, MaxRetries: 3})
	_, err = c.Send(ctxT(t, 3*time.Second), 2, samplePos, Options{})
	if !errors.Is(err, ErrNoAck) {
		t.Fatalf("got %v", err)
	}
	if n := c.Stats().Retransmits; n != 3 {
		t.Fatalf("retransmits %d", n)
	}
}

func TestInvalidMessageFailsFast(t *testing.T) {
	_, addr := startGateway(t, "127.0.0.1:0")
	c := runClient(t, Config{Addr: addr, Unit: 1})
	_, err := c.Send(ctxT(t, 3*time.Second), 2, &protocol.Position{Heading: 400}, Options{})
	if !errors.Is(err, protocol.ErrInvalidField) {
		t.Fatalf("got %v", err)
	}
}

var samplePos = &protocol.Position{Time: 100, Pos: protocol.CoordFromDegrees(37.0, 35.3), Heading: 90}
