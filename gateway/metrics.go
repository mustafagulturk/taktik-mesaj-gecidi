package gateway

import "sync/atomic"

// Metrics are cumulative server counters, safe for concurrent use.
type Metrics struct {
	ConnsAccepted   atomic.Uint64
	ConnsActive     atomic.Int64
	ConnsIdle       atomic.Uint64 // closed by idle timeout
	ConnsTakenOver  atomic.Uint64 // replaced by a newer connection for the same unit
	ConnsGarbage    atomic.Uint64 // dropped for exceeding the garbage budget
	FramesIn        atomic.Uint64
	FramesOut       atomic.Uint64
	FramesRouted    atomic.Uint64
	FramesDuplicate atomic.Uint64
	FramesNoRoute   atomic.Uint64
	FramesRejected  atomic.Uint64
	FramesDropped   atomic.Uint64 // refused or evicted by a full outbound queue
	BytesSkipped    atomic.Uint64
	BadHeaders      atomic.Uint64
	CRCErrors       atomic.Uint64
	BadPayloads     atomic.Uint64
}

// Snapshot is a plain copy of Metrics, convenient for JSON.
type Snapshot struct {
	ConnsAccepted   uint64 `json:"conns_accepted"`
	ConnsActive     int64  `json:"conns_active"`
	ConnsIdle       uint64 `json:"conns_idle_timeout"`
	ConnsTakenOver  uint64 `json:"conns_taken_over"`
	ConnsGarbage    uint64 `json:"conns_garbage"`
	FramesIn        uint64 `json:"frames_in"`
	FramesOut       uint64 `json:"frames_out"`
	FramesRouted    uint64 `json:"frames_routed"`
	FramesDuplicate uint64 `json:"frames_duplicate"`
	FramesNoRoute   uint64 `json:"frames_no_route"`
	FramesRejected  uint64 `json:"frames_rejected"`
	FramesDropped   uint64 `json:"frames_dropped"`
	BytesSkipped    uint64 `json:"bytes_skipped"`
	BadHeaders      uint64 `json:"bad_headers"`
	CRCErrors       uint64 `json:"crc_errors"`
	BadPayloads     uint64 `json:"bad_payloads"`
}

// Snapshot returns the current counter values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		ConnsAccepted:   m.ConnsAccepted.Load(),
		ConnsActive:     m.ConnsActive.Load(),
		ConnsIdle:       m.ConnsIdle.Load(),
		ConnsTakenOver:  m.ConnsTakenOver.Load(),
		ConnsGarbage:    m.ConnsGarbage.Load(),
		FramesIn:        m.FramesIn.Load(),
		FramesOut:       m.FramesOut.Load(),
		FramesRouted:    m.FramesRouted.Load(),
		FramesDuplicate: m.FramesDuplicate.Load(),
		FramesNoRoute:   m.FramesNoRoute.Load(),
		FramesRejected:  m.FramesRejected.Load(),
		FramesDropped:   m.FramesDropped.Load(),
		BytesSkipped:    m.BytesSkipped.Load(),
		BadHeaders:      m.BadHeaders.Load(),
		CRCErrors:       m.CRCErrors.Load(),
		BadPayloads:     m.BadPayloads.Load(),
	}
}
