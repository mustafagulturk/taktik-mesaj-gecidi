package gateway

import (
	"sync"
	"time"
)

// dedup remembers recently seen (src, seq) pairs. It lives on the server,
// not the session, so a unit that reconnects and retransmits an
// unacknowledged frame is acknowledged but not delivered twice.
//
// Entries expire after ttl so that a unit which restarts and reuses
// sequence numbers is not silenced, and at most `window` entries are kept
// per unit so memory stays bounded.
type dedup struct {
	mu     sync.Mutex
	window int
	ttl    time.Duration
	units  map[uint16]*seqWindow
}

type seqEntry struct {
	seq uint16
	at  time.Time
}

type seqWindow struct {
	seen map[uint16]time.Time
	ring []seqEntry
	next int
}

func newDedup(window int, ttl time.Duration) *dedup {
	return &dedup{window: window, ttl: ttl, units: make(map[uint16]*seqWindow)}
}

// seen records (src, seq) at time now and reports whether it was already
// recorded within the TTL.
func (d *dedup) seen(src, seq uint16, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.units[src]
	if w == nil {
		w = &seqWindow{seen: make(map[uint16]time.Time), ring: make([]seqEntry, 0, 64)}
		d.units[src] = w
	}
	if at, ok := w.seen[seq]; ok && now.Sub(at) < d.ttl {
		return true
	}
	e := seqEntry{seq, now}
	if len(w.ring) < d.window {
		w.ring = append(w.ring, e)
	} else {
		old := w.ring[w.next]
		if w.seen[old.seq].Equal(old.at) {
			delete(w.seen, old.seq)
		}
		w.ring[w.next] = e
		w.next = (w.next + 1) % d.window
	}
	w.seen[seq] = now
	return false
}
