// Package e2e runs the gateway and clients over a fault-injecting proxy to
// check end-to-end delivery guarantees on a hostile network.
package e2e

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/chaos"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/client"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/gateway"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

func setup(t *testing.T, cfg chaos.Config) (*gateway.Server, string, *chaos.Proxy) {
	t.Helper()
	srv := gateway.New(gateway.Config{IdleTimeout: 2 * time.Second})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	cfg.Target = ln.Addr().String()
	px, err := chaos.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		px.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, ln.Addr().String(), px
}

func run(t *testing.T, cfg client.Config) *client.Client {
	t.Helper()
	cfg.HeartbeatInterval = 200 * time.Millisecond
	cfg.MinBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 100 * time.Millisecond
	c, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

func waitUnit(t *testing.T, srv *gateway.Server, id uint16) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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

// TestExactlyOnceOverHostileUplink sends acknowledged orders from a unit
// behind a proxy that corrupts bits, fragments writes, adds jitter and
// resets the connection. Every Send must be acknowledged and the receiver,
// on a clean link, must see every order exactly once.
func TestExactlyOnceOverHostileUplink(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	srv, direct, px := setup(t, chaos.Config{
		Latency:     5 * time.Millisecond,
		Jitter:      3 * time.Millisecond,
		CorruptRate: 0.002,
		ChunkMax:    7,
		ResetAfter:  400 * time.Millisecond,
		Seed:        42,
	})
	rx := run(t, client.Config{Addr: direct, Unit: 2})
	tx := run(t, client.Config{Addr: px.Addr(), Unit: 1, AckTimeout: 150 * time.Millisecond, MaxRetries: 40})
	waitUnit(t, srv, 2)

	const n = 150
	received := make(map[uint32]int)
	var mu sync.Mutex
	stop := make(chan struct{})
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for {
			select {
			case f := <-rx.Incoming():
				if o, ok := f.Msg.(*protocol.Order); ok && f.Src == 1 {
					mu.Lock()
					received[o.ExecuteAt]++
					mu.Unlock()
				}
			case <-stop:
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := uint32(0); i < n; i++ {
		o := &protocol.Order{Time: i, Code: protocol.OrderObserve, Target: protocol.CoordFromDegrees(36.2, 36.16), ExecuteAt: i}
		st, err := tx.Send(ctx, 2, o, client.Options{Priority: protocol.Immediate})
		if err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
		if st != protocol.AckDelivered && st != protocol.AckDuplicate {
			t.Fatalf("order %d: status %v", i, st)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := len(received)
		mu.Unlock()
		if got == n || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	<-collected
	for i := uint32(0); i < n; i++ {
		if c := received[i]; c != 1 {
			t.Errorf("order %d delivered %d times", i, c)
		}
	}

	ps, ms, cs := px.Stats(), srv.Metrics(), tx.Stats()
	t.Logf("proxy: %+v", ps)
	t.Logf("gateway: %+v", ms)
	t.Logf("sender: %+v", cs)
	if ps.BytesCorrupted == 0 || ps.Resets == 0 {
		t.Fatalf("chaos did not happen: %+v", ps)
	}
}

// TestBroadcastSurvivesNoisyDownlinks checks that receivers behind a noisy
// downlink never see a corrupted or duplicated frame, only (rarely) a lost one.
func TestBroadcastSurvivesNoisyDownlinks(t *testing.T) {
	srv, direct, px := setup(t, chaos.Config{CorruptRate: 0.001, ChunkMax: 5, Direction: chaos.Downstream, Seed: 7})
	tx := run(t, client.Config{Addr: direct, Unit: 1})
	rxs := []*client.Client{
		run(t, client.Config{Addr: px.Addr(), Unit: 11, InboxSize: 1024}),
		run(t, client.Config{Addr: px.Addr(), Unit: 12, InboxSize: 1024}),
	}
	waitUnit(t, srv, 11)
	waitUnit(t, srv, 12)

	const n = 300
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		p := &protocol.Position{Time: uint32(i), Pos: protocol.CoordFromDegrees(39.9, 32.8), Heading: uint16(i % 360)}
		if err := tx.Post(ctx, protocol.AddrBroadcast, p, client.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, rx := range rxs {
		seen := make(map[uint32]bool)
		timeout := time.After(2 * time.Second)
	collect:
		for len(seen) < n {
			select {
			case f := <-rx.Incoming():
				p, ok := f.Msg.(*protocol.Position)
				if !ok {
					continue
				}
				if seen[p.Time] {
					t.Fatalf("duplicate position %d", p.Time)
				}
				if p.Heading != uint16(p.Time%360) {
					t.Fatalf("corrupted frame passed validation: %+v", p)
				}
				seen[p.Time] = true
			case <-timeout:
				break collect
			}
		}
		if len(seen) < n*8/10 {
			t.Fatalf("only %d/%d positions arrived", len(seen), n)
		}
		t.Logf("received %d/%d, stats %+v", len(seen), n, rx.Stats())
	}
}
