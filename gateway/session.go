package gateway

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

// session is one TCP connection. It has a reader goroutine (decode + route)
// and a writer goroutine (drain the priority queue); either may close it.
type session struct {
	srv    *Server
	conn   net.Conn
	remote string
	unit   uint16 // written only by the read loop, under srv.mu
	q      *outQueue

	done      chan struct{}
	closeOnce sync.Once
}

func newSession(srv *Server, conn net.Conn) *session {
	return &session{
		srv:    srv,
		conn:   conn,
		remote: conn.RemoteAddr().String(),
		q:      newOutQueue(srv.cfg.QueueLimit),
		done:   make(chan struct{}),
	}
}

func (s *session) enqueue(p protocol.Priority, wire []byte) bool {
	ok, evicted := s.q.push(p, wire)
	if !ok || evicted > 0 {
		s.srv.metrics.FramesDropped.Add(uint64(evicted))
		if !ok {
			s.srv.metrics.FramesDropped.Add(1)
		}
	}
	return ok
}

func (s *session) close(reason string) {
	s.closeOnce.Do(func() {
		close(s.done)
		s.conn.Close()
		s.q.close()
		unit := s.srv.unbind(s)
		s.srv.metrics.ConnsActive.Add(-1)
		s.srv.log.Info("connection closed", "remote", s.remote, "unit", unit, "reason", reason)
	})
}

func (s *session) readLoop() {
	defer s.srv.wg.Done()
	m := &s.srv.metrics
	r := protocol.NewReader(s.conn)
	r.MaxGarbage = s.srv.cfg.MaxGarbage
	var prev protocol.ReaderStats
	for {
		s.conn.SetReadDeadline(time.Now().Add(s.srv.cfg.IdleTimeout))
		f, err := r.ReadFrame()

		st := r.Stats()
		m.BytesSkipped.Add(st.SkippedBytes - prev.SkippedBytes)
		m.BadHeaders.Add(st.BadHeaders - prev.BadHeaders)
		m.CRCErrors.Add(st.CRCErrors - prev.CRCErrors)
		m.BadPayloads.Add(st.BadPayloads - prev.BadPayloads)
		prev = st

		if err != nil {
			reason := err.Error()
			switch {
			case errors.Is(err, os.ErrDeadlineExceeded):
				m.ConnsIdle.Add(1)
				reason = "idle timeout"
			case errors.Is(err, protocol.ErrTooMuchGarbage):
				m.ConnsGarbage.Add(1)
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
				reason = "peer closed"
			}
			s.close(reason)
			return
		}
		s.srv.handle(s, f)
	}
}

func (s *session) writeLoop() {
	defer s.srv.wg.Done()
	bw := bufio.NewWriterSize(s.conn, 8<<10)
	for {
		select {
		case <-s.done:
			return
		case <-s.q.notify:
		}
		// Drain everything queued, then flush once: one syscall per burst.
		n := 0
		for wire := s.q.pop(); wire != nil; wire = s.q.pop() {
			s.conn.SetWriteDeadline(time.Now().Add(s.srv.cfg.WriteTimeout))
			if _, err := bw.Write(wire); err != nil {
				s.close("write: " + err.Error())
				return
			}
			n++
		}
		if err := bw.Flush(); err != nil {
			s.close("write: " + err.Error())
			return
		}
		s.srv.metrics.FramesOut.Add(uint64(n))
	}
}
