// Package dispatcher fans events out to one queue per target provider, so
// a slow or failing provider can never head-of-line-block delivery to a
// healthy one.
package dispatcher

import (
	"fmt"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/wal"
)

// Enqueuer accepts events for eventual delivery to one provider. Satisfied
// by *batcher.Batcher.
type Enqueuer interface {
	Enqueue(event adapter.Event)
}

// Dispatcher routes events to the per-provider Enqueuers named in each
// event's provider list.
type Dispatcher struct {
	batchers map[string]Enqueuer
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
func (d *Dispatcher) ReplayPending(entries []wal.Entry) error {
	for _, e := range entries {
		if err := d.Dispatch(e.Event, e.Pending); err != nil {
			return err
		}
	}
	return nil
}
