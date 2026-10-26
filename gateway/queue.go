package gateway

import (
	"sync"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

const numPriorities = int(protocol.Flash) + 1

// outQueue is a bounded, priority-ordered outbound queue for one session.
// When a priority level is full, the oldest frame of the lowest non-empty
// lower priority is evicted to make room; if nothing lower exists the new
// frame is refused. Routine chatter can therefore never block FLASH traffic.
type outQueue struct {
	mu     sync.Mutex
	levels [numPriorities][][]byte
	limit  int
	closed bool
	notify chan struct{}
}

func newOutQueue(limit int) *outQueue {
	return &outQueue{limit: limit, notify: make(chan struct{}, 1)}
}

// push enqueues an encoded frame. It reports whether the frame was accepted
// and how many lower-priority frames were evicted for it.
func (q *outQueue) push(p protocol.Priority, frame []byte) (ok bool, evicted int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, 0
	}
	if len(q.levels[p]) >= q.limit {
		victim := -1
		for lp := 0; lp < int(p); lp++ {
			if len(q.levels[lp]) > 0 {
				victim = lp
				break
			}
		}
		if victim < 0 {
			return false, 0
		}
		q.levels[victim][0] = nil
		q.levels[victim] = q.levels[victim][1:]
		evicted = 1
	}
	q.levels[p] = append(q.levels[p], frame)
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true, evicted
}

// pop returns the highest-priority frame, or nil if the queue is empty.
func (q *outQueue) pop() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	for p := numPriorities - 1; p >= 0; p-- {
		if l := q.levels[p]; len(l) > 0 {
			f := l[0]
			l[0] = nil
			q.levels[p] = l[1:]
			return f
		}
	}
	return nil
}

func (q *outQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, l := range q.levels {
		n += len(l)
	}
	return n
}

func (q *outQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.levels = [numPriorities][][]byte{}
	q.mu.Unlock()
}
