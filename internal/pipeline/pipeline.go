// Package pipeline wires receiver -> WAL -> dispatcher -> batcher ->
// adapter into a running whole.
package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/adapter/stdout"
	"github.com/earthy1024/tallyd/internal/batcher"
	"github.com/earthy1024/tallyd/internal/dispatcher"
	"github.com/earthy1024/tallyd/internal/dlq"
	"github.com/earthy1024/tallyd/internal/metrics"
	"github.com/earthy1024/tallyd/internal/receiver"
	"github.com/earthy1024/tallyd/internal/wal"
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

	w, err := wal.Open(cfg.Buffer.Dir)
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

	recv := receiver.New(&walDispatchSink{wal: w, disp: disp}, router)
	recv.Metrics = m

	return &Pipeline{
		Config:     cfg,
		WAL:        w,
		DLQ:        dq,
		Metrics:    m,
		Batchers:   batchers,
		Dispatcher: disp,
		Receiver:   recv,
	}, nil
}

func buildAdapter(pc ProviderConfig) (adapter.Adapter, error) {
	switch pc.Type {
	case "", "stdout":
		a := stdout.New()
		if pc.Batch.MaxEvents > 0 {
			a.MaxBatch = pc.Batch.MaxEvents
		}
		return a, nil
	default:
		return nil, fmt.Errorf("unsupported adapter type %q (only \"stdout\" is implemented so far; Orb/Metronome adapters are the next unit of work)", pc.Type)
	}
}

// Handler returns the top-level HTTP handler: POST /v1/events plus
// GET /metrics.
func (p *Pipeline) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", p.Receiver.Handler())
	mux.Handle("/metrics", p.Metrics.Handler())
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
