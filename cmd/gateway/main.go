// Command gateway runs the TMG tactical message distribution server.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/gateway"
)

func main() {
	var (
		listen     = flag.String("listen", ":7400", "TCP address for field units")
		httpAddr   = flag.String("http", "127.0.0.1:7401", "HTTP address for /metrics, /units and /healthz (empty disables)")
		idle       = flag.Duration("idle", 30*time.Second, "drop a unit after this long without a valid frame")
		writeTO    = flag.Duration("write-timeout", 5*time.Second, "per-write deadline towards a unit")
		queue      = flag.Int("queue", 256, "outbound frames per priority level per unit")
		maxGarbage = flag.Int("max-garbage", 64<<10, "junk bytes tolerated between valid frames")
		dedupTTL   = flag.Duration("dedup-ttl", 2*time.Minute, "how long (src,seq) pairs are remembered")
		maxConns   = flag.Int("max-conns", 1024, "maximum concurrent connections")
		logLevel   = flag.String("log-level", "info", "debug, info, warn or error")
		logJSON    = flag.Bool("log-json", false, "log as JSON")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	log := slog.New(h)

	srv := gateway.New(gateway.Config{
		IdleTimeout:  *idle,
		WriteTimeout: *writeTO,
		QueueLimit:   *queue,
		MaxGarbage:   *maxGarbage,
		DedupTTL:     *dedupTTL,
		MaxConns:     *maxConns,
		Logger:       log,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var hs *http.Server
	if *httpAddr != "" {
		hs = &http.Server{Addr: *httpAddr, Handler: adminMux(srv), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("admin http", "err", err)
			}
		}()
		log.Info("admin http listening", "addr", *httpAddr)
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(*listen) }()

	select {
	case err := <-errc:
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if hs != nil {
		hs.Shutdown(sctx)
	}
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("shutdown", "err", err)
	}
	m, _ := json.Marshal(srv.Metrics())
	log.Info("final metrics", "metrics", string(m))
}

func adminMux(srv *gateway.Server) *http.ServeMux {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, srv.Metrics())
	})
	mux.HandleFunc("GET /units", func(w http.ResponseWriter, _ *http.Request) {
		units := srv.Units()
		slices.Sort(units)
		writeJSON(w, units)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	return mux
}
