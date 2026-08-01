// Package dispatcher fans events out to one queue per target provider, so
// a slow or failing provider can never head-of-line-block delivery to a
// healthy one.
package dispatcher

import (
	"fmt"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/wal"
)

// Enqueuer accepts events for eventual delivery to one provider. Satisfied
// by *batcher.Batcher.
type Enqueuer interface {
	Enqueue(event adapter.Event)
}

// Logger is optional; a nil Logger means skipped replay entries (unknown
// provider) are dropped silently. *log.Logger satisfies this structurally.
type Logger interface {
	Printf(format string, args ...any)
}

// Dispatcher routes events to the per-provider Enqueuers named in each
// event's provider list.
type Dispatcher struct {
	batchers map[string]Enqueuer
	Logger   Logger // optional
}

// New builds a Dispatcher over a provider-name -> Enqueuer map (normally
// one *batcher.Batcher per configured provider).
func New(batchers map[string]Enqueuer) *Dispatcher {
	return &Dispatcher{batchers: batchers}
}

// Dispatch enqueues event onto every provider queue named in providers.
func (d *Dispatcher) Dispatch(event adapter.Event, providers []string) error {
	for _, p := range providers {
		b, ok := d.batchers[p]
		if !ok {
			return fmt.Errorf("dispatcher: unknown provider %q", p)
		}
		b.Enqueue(event)
	}
	return nil
}

// ReplayPending re-enqueues every unresolved WAL entry onto its still-
// pending providers' queues. Call once at startup, right after the WAL
// has replayed and before the receiver starts accepting new traffic, so
// nothing acked to a caller before a crash is ever silently dropped.
//
// Unlike Dispatch, a pending provider that no longer exists in the config
// is tolerated: the unknown provider is skipped (and logged, if a Logger
// is set) rather than aborting replay. Refusing to boot over such an
// entry would wedge the daemon permanently across every restart — the
// exact failure a WAL written by an older or misconfigured build (or a
// pre-fix client-supplied route) could otherwise cause. The entry stays
// pending for that provider, visible via wal_unacked_entries, instead of
// taking the whole process down.
func (d *Dispatcher) ReplayPending(entries []wal.Entry) error {
	for _, e := range entries {
		for _, p := range e.Pending {
			b, ok := d.batchers[p]
			if !ok {
				if d.Logger != nil {
					d.Logger.Printf("dispatcher: skipping replay of event %q for unknown provider %q; leaving it pending in the WAL", e.Event.ID, p)
				}
				continue
			}
			b.Enqueue(e.Event)
		}
	}
	return nil
}
