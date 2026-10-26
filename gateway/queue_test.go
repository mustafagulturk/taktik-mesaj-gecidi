package gateway

import (
	"testing"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

func TestQueuePriorityOrder(t *testing.T) {
	q := newOutQueue(4)
	q.push(protocol.Routine, []byte{0})
	q.push(protocol.Flash, []byte{3})
	q.push(protocol.Immediate, []byte{2})
	q.push(protocol.Routine, []byte{1})
	want := []byte{3, 2, 0, 1}
	for i, w := range want {
		got := q.pop()
		if got == nil || got[0] != w {
			t.Fatalf("pop %d: got %v want %d", i, got, w)
		}
	}
	if q.pop() != nil {
		t.Fatal("queue should be empty")
	}
}

func TestQueueEvictsLowerPriority(t *testing.T) {
	q := newOutQueue(2)
	q.push(protocol.Routine, []byte{1})
	q.push(protocol.Flash, []byte{10})
	q.push(protocol.Flash, []byte{11})
	ok, evicted := q.push(protocol.Flash, []byte{12})
	if !ok || evicted != 1 {
		t.Fatalf("flash push: ok=%v evicted=%d", ok, evicted)
	}
	if ok, _ := q.push(protocol.Flash, []byte{13}); ok {
		t.Fatal("full queue with nothing lower must refuse")
	}
	if ok, _ := q.push(protocol.Routine, []byte{2}); !ok {
		t.Fatal("routine level has room")
	}
	if n := q.len(); n != 4 {
		t.Fatalf("len %d", n)
	}
}

func TestQueueClosedRefuses(t *testing.T) {
	q := newOutQueue(2)
	q.close()
	if ok, _ := q.push(protocol.Flash, []byte{1}); ok {
		t.Fatal("closed queue accepted a frame")
	}
}
