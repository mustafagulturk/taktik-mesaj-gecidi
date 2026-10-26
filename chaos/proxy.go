// Package chaos provides a TCP fault-injection proxy used to verify that the
// gateway survives hostile networks: added latency and jitter, bit
// corruption, dropped chunks, fragmentation and abrupt connection resets.
//
// Each proxied connection runs one pipeline per direction. A reader
// goroutine applies faults, stamps every chunk with a release time and
// pushes it onto a FIFO queue; a writer goroutine waits for each release
// time in order. Because the queue is FIFO, latency and jitter never
// reorder bytes within a direction.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Direction selects which traffic direction faults apply to.
type Direction int

const (
	// Both applies faults to client->target and target->client traffic.
	Both Direction = iota
	// Upstream applies faults only to client->target traffic.
	Upstream
	// Downstream applies faults only to target->client traffic.
	Downstream
)

func (d Direction) String() string {
	switch d {
	case Both:
		return "both"
	case Upstream:
		return "up"
	case Downstream:
		return "down"
	}
	return fmt.Sprintf("Direction(%d)", int(d))
}

// ParseDirection parses "both", "up" or "down".
func ParseDirection(s string) (Direction, error) {
	switch s {
	case "both":
		return Both, nil
	case "up", "upstream":
		return Upstream, nil
	case "down", "downstream":
		return Downstream, nil
	}
	return 0, fmt.Errorf("chaos: unknown direction %q (want both, up or down)", s)
}

// Config describes the faults a Proxy injects.
type Config struct {
	Listen      string        // listen address, e.g. "127.0.0.1:0"
	Target      string        // upstream address
	Latency     time.Duration // added per chunk, in each faulted direction
	Jitter      time.Duration // uniform extra delay in [-Jitter, +Jitter]
	CorruptRate float64       // per-byte probability of flipping one random bit
	DropRate    float64       // per-chunk probability of silently dropping the chunk
	ResetAfter  time.Duration // if >0, reset each connection after a random time in [ResetAfter/2, ResetAfter*3/2]
	ChunkMax    int           // if >0, split data into random chunks of 1..ChunkMax bytes
	Seed        int64         // deterministic RNG when nonzero
	Direction   Direction     // which directions faults apply to
}

func (c *Config) validate() error {
	switch {
	case c.Target == "":
		return errors.New("chaos: Target is required")
	case c.Latency < 0 || c.Jitter < 0 || c.ResetAfter < 0:
		return errors.New("chaos: durations must be non-negative")
	case c.CorruptRate < 0 || c.CorruptRate > 1:
		return errors.New("chaos: CorruptRate must be in [0, 1]")
	case c.DropRate < 0 || c.DropRate > 1:
		return errors.New("chaos: DropRate must be in [0, 1]")
	case c.ChunkMax < 0:
		return errors.New("chaos: ChunkMax must be non-negative")
	case c.Direction < Both || c.Direction > Downstream:
		return fmt.Errorf("chaos: invalid direction %d", c.Direction)
	}
	return nil
}

// Stats is a snapshot of the proxy counters.
type Stats struct {
	Conns          uint64 // connections proxied to the target
	Resets         uint64 // connections killed by ResetAfter
	BytesCorrupted uint64 // bytes that had a bit flipped
	ChunksDropped  uint64 // chunks silently discarded
	BytesForwarded uint64 // bytes written to either peer
}

// Proxy is a running fault-injection proxy. It starts accepting in New and
// stops in Close.
type Proxy struct {
	cfg    Config
	ln     net.Listener
	ctx    context.Context // cancelled by Close; aborts pending dials
	cancel context.CancelFunc

	wg        sync.WaitGroup
	mu        sync.Mutex
	links     map[*link]struct{}
	closed    bool
	closeOnce sync.Once
	nextID    atomic.Uint64

	conns, resets, corrupted, dropped, forwarded atomic.Uint64
}

// New validates cfg, starts listening on cfg.Listen and begins accepting
// connections in the background.
func New(cfg Config) (*Proxy, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("chaos: listen: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{cfg: cfg, ln: ln, ctx: ctx, cancel: cancel, links: make(map[*link]struct{})}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Addr returns the address the proxy listens on.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Stats returns a snapshot of the counters.
func (p *Proxy) Stats() Stats {
	return Stats{
		Conns:          p.conns.Load(),
		Resets:         p.resets.Load(),
		BytesCorrupted: p.corrupted.Load(),
		ChunksDropped:  p.dropped.Load(),
		BytesForwarded: p.forwarded.Load(),
	}
}

// Close stops the listener, kills every live connection and waits for all
// goroutines to exit. It is safe to call more than once.
func (p *Proxy) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		live := make([]*link, 0, len(p.links))
		for l := range p.links {
			live = append(live, l)
		}
		p.mu.Unlock()

		p.cancel()
		err = p.ln.Close()
		for _, l := range live {
			l.kill(false)
		}
	})
	p.wg.Wait()
	return err
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Transient accept error (e.g. EMFILE): back off briefly.
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		p.wg.Add(1)
		go p.handle(c, p.nextID.Add(1))
	}
}

// rngFor returns an independent generator for one stream of one connection.
func (p *Proxy) rngFor(connID, stream uint64) *rand.Rand {
	if p.cfg.Seed != 0 {
		return rand.New(rand.NewPCG(uint64(p.cfg.Seed), connID<<2|stream))
	}
	return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
}

func (p *Proxy) handle(client net.Conn, id uint64) {
	defer p.wg.Done()

	var d net.Dialer
	dctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	upstream, err := d.DialContext(dctx, "tcp", p.cfg.Target)
	cancel()
	if err != nil {
		client.Close()
		return
	}

	l := &link{client: client, upstream: upstream, done: make(chan struct{})}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		l.kill(false)
		return
	}
	p.links[l] = struct{}{}
	p.mu.Unlock()
	p.conns.Add(1)

	defer func() {
		p.mu.Lock()
		delete(p.links, l)
		p.mu.Unlock()
		l.kill(false)
	}()

	if p.cfg.ResetAfter > 0 {
		r := p.rngFor(id, 0)
		half := p.cfg.ResetAfter / 2
		after := half + time.Duration(r.Int64N(int64(p.cfg.ResetAfter)+1))
		t := time.AfterFunc(after, func() {
			if l.kill(true) {
				p.resets.Add(1)
			}
		})
		defer t.Stop()
	}

	up := p.cfg.Direction == Both || p.cfg.Direction == Upstream
	down := p.cfg.Direction == Both || p.cfg.Direction == Downstream

	var wg sync.WaitGroup
	wg.Add(2)
	go p.pipe(l, client, upstream, up, p.rngFor(id, 1), &wg)
	go p.pipe(l, upstream, client, down, p.rngFor(id, 2), &wg)
	wg.Wait()
}

// link is one proxied connection pair.
type link struct {
	client, upstream net.Conn
	done             chan struct{}
	once             sync.Once
}

// kill closes both sides exactly once. With rst set, it sets SO_LINGER 0 so
// peers see a TCP RST instead of a FIN. It reports whether this call did it.
func (l *link) kill(rst bool) bool {
	killed := false
	l.once.Do(func() {
		killed = true
		close(l.done)
		for _, c := range []net.Conn{l.client, l.upstream} {
			if tc, ok := c.(*net.TCPConn); ok && rst {
				_ = tc.SetLinger(0)
			}
			c.Close()
		}
	})
	return killed
}

type packet struct {
	data    []byte
	release time.Time
}

// pipe forwards src to dst, applying faults when faulty is set. It returns
// once both its reader and writer have finished.
func (p *Proxy) pipe(l *link, src, dst net.Conn, faulty bool, r *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()

	q := make(chan packet, 256)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		p.writeLoop(l, dst, q)
	}()

	buf := make([]byte, 32*1024)
	var readErr error
read:
	for {
		n, err := src.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			for _, pk := range p.inject(data, faulty, r) {
				select {
				case q <- pk:
				case <-l.done:
					break read
				}
			}
		}
		if err != nil {
			readErr = err
			break
		}
	}

	if readErr != nil && !errors.Is(readErr, io.EOF) {
		// Abnormal end: tear down without flushing.
		l.kill(false)
	}
	close(q)
	<-writerDone
}

// inject applies corruption, fragmentation, drops and delay to one read.
func (p *Proxy) inject(data []byte, faulty bool, r *rand.Rand) []packet {
	now := time.Now()
	if !faulty {
		return []packet{{data: data, release: now}}
	}
	if rate := p.cfg.CorruptRate; rate > 0 {
		var n uint64
		for i := range data {
			if r.Float64() < rate {
				data[i] ^= 1 << r.IntN(8)
				n++
			}
		}
		p.corrupted.Add(n)
	}

	chunks := [][]byte{data}
	if cm := p.cfg.ChunkMax; cm > 0 && len(data) > 1 {
		chunks = chunks[:0]
		for len(data) > 0 {
			n := min(1+r.IntN(cm), len(data))
			chunks = append(chunks, data[:n:n])
			data = data[n:]
		}
	}

	out := make([]packet, 0, len(chunks))
	for _, c := range chunks {
		if p.cfg.DropRate > 0 && r.Float64() < p.cfg.DropRate {
			p.dropped.Add(1)
			continue
		}
		out = append(out, packet{data: c, release: now.Add(p.delay(r))})
	}
	return out
}

func (p *Proxy) delay(r *rand.Rand) time.Duration {
	d := p.cfg.Latency
	if j := p.cfg.Jitter; j > 0 {
		d += time.Duration(r.Int64N(2*int64(j)+1)) - j
	}
	return max(d, 0)
}

// writeLoop writes packets in FIFO order, each no earlier than its release
// time. On a clean end of input it half-closes dst so EOF propagates.
func (p *Proxy) writeLoop(l *link, dst net.Conn, q <-chan packet) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for pk := range q {
		if wait := time.Until(pk.release); wait > 0 {
			timer.Reset(wait)
			select {
			case <-timer.C:
			case <-l.done:
				return
			}
		}
		n, err := dst.Write(pk.data)
		p.forwarded.Add(uint64(n))
		if err != nil {
			l.kill(false)
			return
		}
	}
	select {
	case <-l.done:
	default:
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}
}
