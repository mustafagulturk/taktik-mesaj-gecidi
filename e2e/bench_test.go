package e2e

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/chaos"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/client"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

// BenchmarkDelivery measures sender-to-receiver latency of acknowledged
// orders, with the sender behind the chaos proxy. Run with a fixed count:
//
//	go test ./e2e -run '^$' -bench Delivery -benchtime 300x
func BenchmarkDelivery(b *testing.B) {
	cases := []struct {
		name string
		cfg  chaos.Config
	}{
		{"clean", chaos.Config{}},
		{"latency20ms", chaos.Config{Latency: 20 * time.Millisecond, Jitter: 10 * time.Millisecond}},
		{"latency20ms+corrupt", chaos.Config{Latency: 20 * time.Millisecond, Jitter: 10 * time.Millisecond, CorruptRate: 0.001, ChunkMax: 7}},
		{"latency20ms+corrupt+reset", chaos.Config{Latency: 20 * time.Millisecond, Jitter: 10 * time.Millisecond, CorruptRate: 0.001, ChunkMax: 7, ResetAfter: time.Second}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			srv, direct, px := setup(b, tc.cfg)
			rx := run(b, client.Config{Addr: direct, Unit: 2, InboxSize: 4096})
			tx := run(b, client.Config{Addr: px.Addr(), Unit: 1, AckTimeout: 200 * time.Millisecond, MaxRetries: 50})
			waitUnit(b, srv, 2)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			lat := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o := &protocol.Order{Time: uint32(i % protocol.SecondsPerDay), Code: protocol.OrderHold}
				start := time.Now()
				if _, err := tx.Send(ctx, 2, o, client.Options{Priority: protocol.Immediate}); err != nil {
					b.Fatal(err)
				}
				for {
					f := <-rx.Incoming()
					if m, ok := f.Msg.(*protocol.Order); ok && m.Time == o.Time {
						break
					}
				}
				lat = append(lat, time.Since(start))
			}
			b.StopTimer()
			slices.Sort(lat)
			pct := func(p float64) float64 {
				return float64(lat[int(p*float64(len(lat)-1))]) / float64(time.Millisecond)
			}
			b.ReportMetric(pct(0.50), "p50-ms")
			b.ReportMetric(pct(0.99), "p99-ms")
			b.ReportMetric(float64(tx.Stats().Retransmits), "retransmits")
			b.ReportMetric(0, "ns/op")
			if testing.Verbose() {
				fmt.Printf("%s: proxy %+v\n", tc.name, px.Stats())
			}
		})
	}
}
