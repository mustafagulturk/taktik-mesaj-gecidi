// Command chaosproxy runs a TCP fault-injection proxy in front of a target,
// adding latency, jitter, corruption, drops, fragmentation and resets.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/chaos"
)

func main() {
	if err := run(); err != nil {
		slog.Error("chaosproxy failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen  = flag.String("listen", "127.0.0.1:9100", "address to listen on")
		target  = flag.String("target", "", "upstream address to forward to (required)")
		latency = flag.Duration("latency", 0, "delay added to each chunk in faulted directions, e.g. 150ms")
		jitter  = flag.Duration("jitter", 0, "uniform random extra delay in [-jitter, +jitter]")
		corrupt = flag.Float64("corrupt", 0, "per-byte probability of flipping one random bit (0..1)")
		drop    = flag.Float64("drop", 0, "per-chunk probability of silently dropping the chunk (0..1)")
		reset   = flag.Duration("reset", 0, "if >0, reset each connection after a random time in [reset/2, reset*3/2]")
		chunk   = flag.Int("chunk", 0, "if >0, fragment data into random chunks of 1..chunk bytes")
		seed    = flag.Int64("seed", 0, "RNG seed for reproducible faults (0 = random)")
		dir     = flag.String("dir", "both", "directions faults apply to: both, up (client->target) or down (target->client)")
		every   = flag.Duration("stats", 5*time.Second, "interval between stats log lines")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `Usage: chaosproxy -target host:port [options]

chaosproxy is a TCP proxy that injects network faults between clients and a
target, to test that the gateway survives bad links.

Example:
  chaosproxy -listen :9100 -target 127.0.0.1:9000 -latency 200ms -jitter 50ms -corrupt 0.001 -chunk 7

Options:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *target == "" {
		flag.Usage()
		return fmt.Errorf("-target is required")
	}
	d, err := chaos.ParseDirection(*dir)
	if err != nil {
		return err
	}
	if *every <= 0 {
		return fmt.Errorf("-stats must be positive")
	}

	p, err := chaos.New(chaos.Config{
		Listen:      *listen,
		Target:      *target,
		Latency:     *latency,
		Jitter:      *jitter,
		CorruptRate: *corrupt,
		DropRate:    *drop,
		ResetAfter:  *reset,
		ChunkMax:    *chunk,
		Seed:        *seed,
		Direction:   d,
	})
	if err != nil {
		return err
	}
	slog.Info("chaosproxy listening",
		"listen", p.Addr(), "target", *target, "dir", d,
		"latency", *latency, "jitter", *jitter, "corrupt", *corrupt, "drop", *drop,
		"reset", *reset, "chunk", *chunk, "seed", *seed)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tick := time.NewTicker(*every)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			logStats("stats", p.Stats())
		case <-ctx.Done():
			slog.Info("shutting down")
			err := p.Close()
			logStats("final stats", p.Stats())
			return err
		}
	}
}

func logStats(msg string, s chaos.Stats) {
	slog.Info(msg,
		"conns", s.Conns,
		"resets", s.Resets,
		"bytes_forwarded", s.BytesForwarded,
		"bytes_corrupted", s.BytesCorrupted,
		"chunks_dropped", s.ChunksDropped)
}
