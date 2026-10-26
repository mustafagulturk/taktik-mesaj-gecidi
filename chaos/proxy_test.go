package chaos

import (
	"bytes"
	"errors"
	"io"
	"math/bits"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// echoServer echoes every connection and half-closes after EOF.
type echoServer struct {
	ln    net.Listener
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns []net.Conn
}

func startEcho(t *testing.T) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &echoServer{ln: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				_, _ = io.Copy(c, c)
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.CloseWrite()
				}
			}()
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *echoServer) addr() string { return s.ln.Addr().String() }

func (s *echoServer) close() {
	s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func newProxy(t *testing.T, cfg Config) *Proxy {
	t.Helper()
	cfg.Listen = "127.0.0.1:0"
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func dial(t *testing.T, p *Proxy) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c.(*net.TCPConn)
}

// roundTrip writes payload, half-closes and returns everything echoed back.
func roundTrip(t *testing.T, c *net.TCPConn, payload []byte) []byte {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		if err == nil {
			err = c.CloseWrite()
		}
		errc <- err
	}()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
	return got
}

func randBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// checkGoroutines waits for the goroutine count to fall back to base.
func checkGoroutines(t *testing.T, base int) {
	t.Helper()
	const slack = 2
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= base+slack {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutine leak: %d now, %d before\n%s", n, base, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewValidates(t *testing.T) {
	bad := []Config{
		{},
		{Target: "x:1", CorruptRate: 1.5},
		{Target: "x:1", DropRate: -0.1},
		{Target: "x:1", Latency: -time.Second},
		{Target: "x:1", ChunkMax: -1},
		{Target: "x:1", Direction: 7},
	}
	for i, cfg := range bad {
		if p, err := New(cfg); err == nil {
			p.Close()
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestParseDirection(t *testing.T) {
	for in, want := range map[string]Direction{"both": Both, "up": Upstream, "down": Downstream} {
		got, err := ParseDirection(in)
		if err != nil || got != want {
			t.Errorf("ParseDirection(%q) = %v, %v", in, got, err)
		}
		if got.String() != in {
			t.Errorf("%v.String() = %q", got, got.String())
		}
	}
	if _, err := ParseDirection("sideways"); err == nil {
		t.Error("expected error")
	}
}

func TestPassThrough(t *testing.T) {
	echo := startEcho(t)
	p := newProxy(t, Config{Target: echo.addr()})
	payload := randBytes(1<<20, 1)

	got := roundTrip(t, dial(t, p), payload)
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	st := p.Stats()
	if st.Conns != 1 || st.BytesForwarded != 2<<20 || st.BytesCorrupted != 0 || st.ChunksDropped != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestLatencyAdded(t *testing.T) {
	echo := startEcho(t)
	const lat = 100 * time.Millisecond
	p := newProxy(t, Config{Target: echo.addr(), Latency: lat})
	c := dial(t, p)

	buf := make([]byte, 4)
	for i := 0; i < 3; i++ {
		start := time.Now()
		if _, err := c.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatal(err)
		}
		rtt := time.Since(start)
		// Latency applies once per direction.
		if rtt < 2*lat || rtt > 2*lat+time.Second {
			t.Fatalf("rtt %v, want about %v", rtt, 2*lat)
		}
	}
}

func TestLatencyUpstreamOnly(t *testing.T) {
	echo := startEcho(t)
	const lat = 100 * time.Millisecond
	p := newProxy(t, Config{Target: echo.addr(), Latency: lat, Direction: Upstream})
	c := dial(t, p)

	start := time.Now()
	got := roundTrip(t, c, []byte("ping"))
	rtt := time.Since(start)
	if string(got) != "ping" {
		t.Fatalf("got %q", got)
	}
	if rtt < lat || rtt >= 2*lat {
		t.Fatalf("rtt %v, want in [%v, %v)", rtt, lat, 2*lat)
	}
}

func TestJitterPreservesOrder(t *testing.T) {
	echo := startEcho(t)
	p := newProxy(t, Config{
		Target:   echo.addr(),
		Latency:  5 * time.Millisecond,
		Jitter:   5 * time.Millisecond,
		ChunkMax: 512,
		Seed:     42,
	})
	payload := randBytes(256<<10, 2)
	got := roundTrip(t, dial(t, p), payload)
	if !bytes.Equal(got, payload) {
		t.Fatal("jitter reordered or lost bytes")
	}
}

func TestCorruptEveryByte(t *testing.T) {
	echo := startEcho(t)
	// Corrupt only upstream so every byte gets exactly one bit flipped.
	p := newProxy(t, Config{Target: echo.addr(), CorruptRate: 1, Direction: Upstream, Seed: 7})
	payload := randBytes(64<<10, 3)
	got := roundTrip(t, dial(t, p), payload)
	if len(got) != len(payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
	for i := range got {
		if n := bits.OnesCount8(got[i] ^ payload[i]); n != 1 {
			t.Fatalf("byte %d: %d bits differ, want 1", i, n)
		}
	}
	if st := p.Stats(); st.BytesCorrupted != uint64(len(payload)) {
		t.Fatalf("BytesCorrupted = %d, want %d", st.BytesCorrupted, len(payload))
	}
}

func TestCorruptDeterministicWithSeed(t *testing.T) {
	echo := startEcho(t)
	payload := randBytes(16<<10, 4)
	run := func() []byte {
		p := newProxy(t, Config{Target: echo.addr(), CorruptRate: 0.3, Direction: Upstream, Seed: 99})
		defer p.Close()
		return roundTrip(t, dial(t, p), payload)
	}
	a, b := run(), run()
	if bytes.Equal(a, payload) {
		t.Fatal("nothing corrupted")
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same seed produced different corruption")
	}
}

func TestDropAll(t *testing.T) {
	echo := startEcho(t)
	p := newProxy(t, Config{Target: echo.addr(), DropRate: 1, Direction: Upstream})
	got := roundTrip(t, dial(t, p), []byte("never arrives"))
	if len(got) != 0 {
		t.Fatalf("got %q, want nothing", got)
	}
	if st := p.Stats(); st.ChunksDropped == 0 {
		t.Fatal("ChunksDropped = 0")
	}
}

func TestFragmentationPreservesContent(t *testing.T) {
	echo := startEcho(t)
	p := newProxy(t, Config{Target: echo.addr(), ChunkMax: 3, Seed: 5})
	payload := randBytes(32<<10, 5)
	got := roundTrip(t, dial(t, p), payload)
	if !bytes.Equal(got, payload) {
		t.Fatal("fragmentation altered content")
	}
}

func TestFragmentationSplitsWrites(t *testing.T) {
	// A target that records the size of each read.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sizes := make(chan []int, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var s []int
		buf := make([]byte, 4096)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				s = append(s, n)
			}
			if err != nil {
				break
			}
		}
		sizes <- s
	}()

	p := newProxy(t, Config{Target: ln.Addr().String(), ChunkMax: 4, Latency: time.Millisecond, Jitter: time.Millisecond})
	c := dial(t, p)
	if _, err := c.Write(randBytes(64, 6)); err != nil {
		t.Fatal(err)
	}
	c.CloseWrite()

	select {
	case s := <-sizes:
		total := 0
		for _, n := range s {
			total += n
		}
		if total != 64 || len(s) < 2 {
			t.Fatalf("reads %v: want 64 bytes over several reads", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestResetKillsConnection(t *testing.T) {
	echo := startEcho(t)
	const after = 100 * time.Millisecond
	p := newProxy(t, Config{Target: echo.addr(), ResetAfter: after})
	c := dial(t, p)

	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := c.Read(make([]byte, 1))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("read succeeded, want error")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("connection was not reset")
	}
	if elapsed < after/2-10*time.Millisecond || elapsed > after*3/2+time.Second {
		t.Fatalf("reset after %v, want in [%v, %v]", elapsed, after/2, after*3/2)
	}
	deadline := time.Now().Add(time.Second)
	for p.Stats().Resets != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("Resets = %d, want 1", p.Stats().Resets)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCloseCleansUp(t *testing.T) {
	base := runtime.NumGoroutine()

	echo := startEcho(t)
	p, err := New(Config{Listen: "127.0.0.1:0", Target: echo.addr(), Latency: 50 * time.Millisecond, ChunkMax: 16})
	if err != nil {
		t.Fatal(err)
	}

	var clients []net.Conn
	for i := 0; i < 5; i++ {
		c, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		// Leave data in flight inside the latency queue.
		if _, err := c.Write(randBytes(4096, uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().Conns < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("Conns = %d, want 5", p.Stats().Conns)
		}
		time.Sleep(5 * time.Millisecond)
	}

	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	for _, c := range clients {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.Copy(io.Discard, c); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("client still open after Close")
			}
		}
		c.Close()
	}
	if _, err := net.DialTimeout("tcp", p.Addr(), time.Second); err == nil {
		t.Fatal("listener still accepting after Close")
	}

	echo.close()
	checkGoroutines(t, base)
}
