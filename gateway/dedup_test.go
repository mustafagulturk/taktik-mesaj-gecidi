package gateway

import (
	"testing"
	"time"
)

func TestDedup(t *testing.T) {
	d := newDedup(3, time.Minute)
	now := time.Now()
	if d.seen(1, 100, now) {
		t.Fatal("first sighting reported as duplicate")
	}
	if !d.seen(1, 100, now) {
		t.Fatal("repeat not detected")
	}
	if d.seen(2, 100, now) {
		t.Fatal("sources must be independent")
	}
	// Push 100 out of the 3-entry window.
	d.seen(1, 101, now)
	d.seen(1, 102, now)
	d.seen(1, 103, now)
	if d.seen(1, 100, now) {
		t.Fatal("evicted entry still remembered")
	}
}

func TestDedupTTL(t *testing.T) {
	d := newDedup(16, time.Second)
	now := time.Now()
	d.seen(1, 7, now)
	if !d.seen(1, 7, now.Add(500*time.Millisecond)) {
		t.Fatal("within TTL must be duplicate")
	}
	if d.seen(1, 7, now.Add(2*time.Second)) {
		t.Fatal("after TTL must be fresh")
	}
}
