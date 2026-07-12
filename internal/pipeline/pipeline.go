// Package pipeline wires receiver -> WAL -> dispatcher -> batcher ->
// adapter into a running whole.
package pipeline

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/adapter/metronome"
	"github.com/tallyd/tallyd/adapter/stdout"
	"github.com/tallyd/tallyd/internal/batcher"
	"github.com/tallyd/tallyd/internal/dispatcher"
	"github.com/tallyd/tallyd/internal/dlq"
	"github.com/tallyd/tallyd/internal/dlqreplay"
	"github.com/tallyd/tallyd/internal/grpcapi"
	"github.com/tallyd/tallyd/internal/grpcserver"
	"github.com/tallyd/tallyd/internal/metrics"
	"github.com/tallyd/tallyd/internal/receiver"
	"github.com/tallyd/tallyd/internal/wal"
)

// Pipeline holds every component wired together and ready to serve.
type Pipeline struct {
	Config     *Config
	WAL        *wal.WAL
	DLQ        *dlq.DLQ
	Metrics    *metrics.Metrics
	Batchers   map[string]*batcher.Batcher
	Dispatcher *dispatcher.Dispatcher
	Receiver   *receiver.Receiver
	DLQReplay  *dlqreplay.Handler
	// GRPCServer is nil when Config.Listen.GRPC is empty — the gRPC
	// listener is entirely optional, off by default. When non-nil, the
	// caller (cmd/tallyd) owns starting/stopping it, same as the HTTP
	// server built from Handler().
	GRPCServer *grpc.Server
}

// walDispatchSink durably appends an event to the WAL and, only once
// durable, hands it to the Dispatcher for live delivery. This is the
// receiver.Sink the HTTP layer actually talks to — it's what makes the
// WAL the durability boundary while still delivering in real time on the
// happy path (crash-recovery redelivery is handled separately by
// Dispatcher.ReplayPending at startup).
type walDispatchSink struct {
	wal  *wal.WAL
	disp *dispatcher.Dispatcher
}

func (s *walDispatchSink) Append(event adapter.Event, providers []string) error {
	if err := s.wal.Append(event, providers); err != nil {
		return err
	}
	return s.disp.Dispatch(event, providers)
}

// Build constructs a Pipeline from cfg: opens the WAL (replaying any
// unresolved entries from a prior crash), opens the DLQ, starts one
// Batcher per configured provider, and re-enqueues replayed entries
// before returning — so the returned Pipeline is safe to start serving
// traffic on immediately.
func Build(cfg *Config) (*Pipeline, error) {
	cfg.applyDefaults()

	if cfg.Buffer.OnFull != "reject" {
		return nil, fmt.Errorf("pipeline: buffer.on_full %q not implemented (only \"reject\" is supported)", cfg.Buffer.OnFull)
	}

	if err := validateRouting(cfg); err != nil {
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	w, err := wal.Open(cfg.Buffer.Dir, wal.WithMaxBytes(cfg.Buffer.MaxBytes))
	if err != nil {
		return nil, fmt.Errorf("pipeline: open wal: %w", err)
	}

	dq, err := dlq.Open(filepath.Join(cfg.Buffer.Dir, "dlq"))
	if err != nil {
		return nil, fmt.Errorf("pipeline: open dlq: %w", err)
	}

	m := metrics.New()

	enqueuers := make(map[string]dispatcher.Enqueuer, len(cfg.Providers))
	batchers := make(map[string]*batcher.Batcher, len(cfg.Providers))
	for name, pc := range cfg.Providers {
		ad, err := buildAdapter(pc)
		if err != nil {
			return nil, fmt.Errorf("pipeline: provider %q: %w", name, err)
		}
		retry := batcher.RetryPolicy{
			MaxBackoff: pc.Retry.MaxInterval.Duration,
			MaxElapsed: pc.Retry.MaxElapsed.Duration,
		}
		b := batcher.New(name, ad, pc.Batch.Linger.Duration, w, dq, retry)
		b.Metrics = m
		b.Logger = log.Default()
		enqueuers[name] = b
		batchers[name] = b
	}

	disp := dispatcher.New(enqueuers)

	if err := disp.ReplayPending(w.Pending()); err != nil {
		return nil, fmt.Errorf("pipeline: replay pending wal entries: %w", err)
	}

	router := &receiver.StaticRouter{Default: cfg.Routing.Default}
	for _, rule := range cfg.Routing.Rules {
		router.Rules = append(router.Rules, receiver.Rule{
			EventName: rule.Match.EventName,
			Providers: rule.Route,
		})
	}

	sink := &walDispatchSink{wal: w, disp: disp}

	recv := receiver.New(sink, router)
	recv.Metrics = m

	knownProviders := make(map[string]bool, len(cfg.Providers))
	for name := range cfg.Providers {
		knownProviders[name] = true
	}
	replay := &dlqreplay.Handler{Sink: sink, DLQ: dq, KnownProviders: knownProviders}

	var grpcServer *grpc.Server
	if cfg.Listen.GRPC != "" {
		grpcServer = grpc.NewServer()
		grpcapi.RegisterEventsServer(grpcServer, grpcserver.New(recv))
	}

	return &Pipeline{
		Config:     cfg,
		WAL:        w,
		DLQ:        dq,
		Metrics:    m,
		Batchers:   batchers,
		Dispatcher: disp,
		Receiver:   recv,
		DLQReplay:  replay,
		GRPCServer: grpcServer,
	}, nil
}

// validateRouting fails fast at startup if routing.default or any
// routing.rules entry names a provider that doesn't exist in
// cfg.Providers — otherwise the mistake would only surface later, as a
// 503 on whichever request first happened to match the bad rule, instead
// of an immediate, clear startup error.
func validateRouting(cfg *Config) error {
	for _, name := range cfg.Routing.Default {
		if _, ok := cfg.Providers[name]; !ok {
			return fmt.Errorf("routing.default references unknown provider %q", name)
		}
	}
	for i, rule := range cfg.Routing.Rules {
		for _, name := range rule.Route {
			if _, ok := cfg.Providers[name]; !ok {
				return fmt.Errorf("routing.rules[%d] (event_name=%q) references unknown provider %q", i, rule.Match.EventName, name)
			}
		}
	}
	return nil
}

func buildAdapter(pc ProviderConfig) (adapter.Adapter, error) {
	switch pc.Type {
	case "", "stdout":
		a := stdout.New()
		if pc.Batch.MaxEvents > 0 {
			a.MaxBatch = pc.Batch.MaxEvents
		}
		return a, nil
	case "metronome":
		token := os.Getenv(pc.TokenEnv)
		if token == "" {
			return nil, fmt.Errorf("metronome: token_env %q is unset or empty", pc.TokenEnv)
		}
		a := metronome.New(pc.Endpoint, token)
		if pc.Batch.MaxEvents > 0 {
			a.MaxBatch = pc.Batch.MaxEvents
		}
		return a, nil
	default:
		return nil, fmt.Errorf("unsupported adapter type %q (only \"stdout\" and \"metronome\" are implemented so far; Orb is the next unit of work)", pc.Type)
	}
}

// Handler returns the top-level HTTP handler: POST /v1/events,
// GET /metrics, and POST /v1/dlq/replay?provider=X[&include_poison=true].
func (p *Pipeline) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", p.Receiver.Handler())
	mux.Handle("/metrics", p.Metrics.Handler())
	mux.Handle("/v1/dlq/replay", p.DLQReplay)
	return mux
}

// RunGauges refreshes the wal_unacked_entries and dlq_depth gauges on a
// fixed interval until ctx is cancelled. Run it in its own goroutine.
func (p *Pipeline) RunGauges(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	refresh := func() {
		p.Metrics.SetWALUnacked(p.WAL.UnackedCount())
		for name := range p.Batchers {
			p.Metrics.SetDLQDepth(name, p.DLQ.Depth(name))
		}
	}

	for {
		select {
		case <-ticker.C:
			refresh()
		case <-ctx.Done():
			return
		}
	}
}

// Close stops accepting new work in every batcher (flushing whatever is
// already queued), then closes the DLQ, then the WAL. Order matters:
// batcher shutdown still needs the WAL/DLQ to be open to record the
// outcome of its final in-flight flush.
func (p *Pipeline) Close() error {
	for _, b := range p.Batchers {
		b.Close()
	}
	if err := p.DLQ.Close(); err != nil {
		return err
	}
	return p.WAL.Close()
}
