// Package batcher accumulates events for a single provider and flushes
// them through that provider's Adapter on a max-size-or-linger basis. It
// owns retry-with-backoff for the Retry disposition and handoff to the
// DLQ for DeadLetter / retry-exhausted events.
package batcher

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/earthy1024/tallyd/adapter"
)

// Acker durably records a terminal disposition (Ok or DeadLetter) for one
// event/provider pair, e.g. *wal.WAL. Batcher never calls Ack with Retry;
// retries are handled internally via re-enqueue with backoff.
type Acker interface {
	Ack(eventID, provider string, disposition adapter.Disposition) error
}

// DeadLetterSink durably parks an event that a provider permanently
// rejected or that exhausted its retry budget, e.g. *dlq.DLQ.
type DeadLetterSink interface {
	Put(provider string, event adapter.Event, reason string) error
}

// RetryPolicy controls exponential backoff + jitter, and caps total retry
// duration below the provider's dedup window (see ARCHITECTURE.md
// "Delivery guarantees") so a redelivered event can never double-count.
type RetryPolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxElapsed     time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = time.Second
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 2 * time.Minute
	}
	if p.MaxElapsed <= 0 {
		p.MaxElapsed = 30 * time.Minute
	}
	return p
}

// backoff returns a full-jitter exponential delay for the given attempt
// count (1-indexed).
func (p RetryPolicy) backoff(attempt int) time.Duration {
	p = p.withDefaults()
	d := p.InitialBackoff
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

type queued struct {
	event      adapter.Event
	attempt    int
	firstQueue time.Time
}

// Batcher is the per-provider queue + flush + retry engine. Network
// coalescing only: raw events reach the provider intact, batching never
// aggregates them (see ARCHITECTURE.md Goals).
type Batcher struct {
	Provider string
	Adapter  adapter.Adapter
	Linger   time.Duration
	Acker    Acker
	DLQ      DeadLetterSink
	Retry    RetryPolicy

	in      chan queued
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// New creates and starts a Batcher for one provider.
func New(provider string, ad adapter.Adapter, linger time.Duration, acker Acker, dlq DeadLetterSink, retry RetryPolicy) *Batcher {
	b := &Batcher{
		Provider: provider,
		Adapter:  ad,
		Linger:   linger,
		Acker:    acker,
		DLQ:      dlq,
		Retry:    retry,
		in:       make(chan queued, 1024),
		closeCh:  make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// Enqueue adds event to this provider's pending queue.
//
// TODO: this blocks once the 1024-deep channel buffer fills. A production
// backpressure story (reject vs. block vs. spill to disk) belongs here;
// v1 keeps it simple since the WAL upstream is the durability boundary.
func (b *Batcher) Enqueue(event adapter.Event) {
	b.enqueue(queued{event: event, firstQueue: time.Now()})
}

func (b *Batcher) enqueue(q queued) {
	select {
	case b.in <- q:
	case <-b.closeCh:
	}
}

// Close stops accepting new work, flushes whatever is already queued, and
// waits for the flush loop to exit. In-flight retries scheduled via
// time.AfterFunc that fire after Close has returned are dropped rather
// than delivered — acceptable because the event remains pending in the
// WAL and will be redelivered on the next process start via
// dispatcher.ReplayPending.
func (b *Batcher) Close() {
	close(b.closeCh)
	b.wg.Wait()
}

func (b *Batcher) run() {
	defer b.wg.Done()

	maxBatch := b.Adapter.MaxBatchSize()
	if maxBatch <= 0 {
		maxBatch = 500
	}

	var pending []queued
	timer := time.NewTimer(b.Linger)
	defer timer.Stop()

	flush := func() {
		if len(pending) == 0 {
			return
		}
		b.flush(pending)
		pending = nil
	}

	for {
		select {
		case q := <-b.in:
			pending = append(pending, q)
			if len(pending) >= maxBatch {
				flush()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(b.Linger)
			}
		case <-timer.C:
			flush()
			timer.Reset(b.Linger)
		case <-b.closeCh:
			for {
				select {
				case q := <-b.in:
					pending = append(pending, q)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (b *Batcher) flush(batch []queued) {
	events := make([]adapter.Event, len(batch))
	for i, q := range batch {
		events[i] = q.event
	}

	body, err := b.Adapter.Encode(events)
	if err != nil {
		for _, q := range batch {
			b.deadLetter(q, fmt.Sprintf("encode error: %v", err))
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result, sendErr := b.Adapter.Send(ctx, body)
	cancel()

	if sendErr != nil {
		disposition := b.Adapter.Classify(sendErr, 0)
		for _, q := range batch {
			b.resolve(q, disposition, sendErr)
		}
		return
	}

	byID := make(map[string]adapter.EventResult, len(result.Results))
	for _, r := range result.Results {
		byID[r.EventID] = r
	}

	for _, q := range batch {
		r, ok := byID[q.event.ID]
		if !ok {
			// Adapter didn't report this event explicitly, but Send didn't
			// error either: treat as accepted.
			r = adapter.EventResult{EventID: q.event.ID, Disposition: adapter.Ok}
		}
		b.resolve(q, r.Disposition, r.Err)
	}
}

func (b *Batcher) resolve(q queued, disposition adapter.Disposition, cause error) {
	switch disposition {
	case adapter.Ok:
		b.ack(q, adapter.Ok)
	case adapter.DeadLetter:
		reason := "dead-lettered by provider"
		if cause != nil {
			reason = cause.Error()
		}
		b.deadLetter(q, reason)
	default: // Retry
		if time.Since(q.firstQueue) >= b.Retry.withDefaults().MaxElapsed {
			reason := "retry budget exhausted"
			if cause != nil {
				reason = fmt.Sprintf("retry budget exhausted: %v", cause)
			}
			b.deadLetter(q, reason)
			return
		}
		b.scheduleRetry(q)
	}
}

func (b *Batcher) ack(q queued, disposition adapter.Disposition) {
	// A durable-ack failure here leaves the WAL entry pending, which is
	// safe under at-least-once delivery (it will simply be redelivered)
	// but currently invisible to operators.
	// TODO: surface via metrics/logging instead of silently dropping.
	_ = b.Acker.Ack(q.event.ID, b.Provider, disposition)
}

func (b *Batcher) deadLetter(q queued, reason string) {
	// Same best-effort caveat as ack above.
	_ = b.DLQ.Put(b.Provider, q.event, reason)
	b.ack(q, adapter.DeadLetter)
}

func (b *Batcher) scheduleRetry(q queued) {
	next := q
	next.attempt++
	delay := b.Retry.backoff(next.attempt)
	time.AfterFunc(delay, func() {
		b.enqueue(next)
	})
}
