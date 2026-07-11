// Package wal implements tallyd's durability guarantee: a segmented,
// disk-backed, append-only write-ahead log. A successful Append blocks
// until its record is fsync'd, so once it returns, the event is guaranteed
// to survive a crash — this is what makes it safe to ack the caller
// immediately afterward. Each event tracks its own per-provider ack state,
// so under dual-write an event is only garbage-collected once every target
// provider has acked or dead-lettered it.
package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/earthy1024/tallyd/adapter"
)

const defaultMaxSegmentBytes = 64 * 1024 * 1024 // 64MiB

// Entry is a snapshot of one unresolved WAL entry: the event and the set
// of providers that have not yet acked or dead-lettered it.
type Entry struct {
	Event   adapter.Event
	Pending []string
}

type entryState struct {
	event   adapter.Event
	pending map[string]struct{}
	segSeq  uint64
}

type eventPayload struct {
	Event     adapter.Event `json:"event"`
	Providers []string      `json:"providers"`
}

type ackPayload struct {
	EventID     string `json:"event_id"`
	Provider    string `json:"provider"`
	Disposition int    `json:"disposition"`
}

type writeRequest struct {
	frame []byte
	// apply mutates in-memory state and is invoked only after the frame's
	// segment has been durably fsync'd. It receives the sequence number of
	// the segment the frame actually landed in (which may differ from the
	// WAL's current active segment if earlier requests in the same batch
	// triggered a rotation).
	apply func(segSeq uint64)
	done  chan error
}

// WAL is a segmented, disk-backed, append-only write-ahead log for billing
// events.
//
// This first pass optimizes for correctness over throughput: group commit
// batches whatever is already queued when the writer wakes, but there is no
// linger/tuning knob yet. TODO: add a small linger window if fsync syscall
// overhead becomes a bottleneck under low concurrency.
//
// TODO: Close() has a narrow shutdown race — a concurrent Append/Ack that
// passes the w.closed check just as Close() runs could still block on a
// channel send after the writer loop has drained and exited. Acceptable for
// v1 because Close() is expected to run only after callers have stopped
// submitting (e.g. after the HTTP receiver has stopped accepting), but a
// fully airtight shutdown would need a submitter WaitGroup too.
type WAL struct {
	dir             string
	maxSegmentBytes int64

	mu       sync.RWMutex
	index    map[string]*entryState
	segments []*segment // oldest first; last is always the active segment

	nextSeq uint64
	closed  atomic.Bool

	writeCh chan *writeRequest
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// Option configures a WAL at Open time.
type Option func(*WAL)

// WithMaxSegmentBytes overrides the default segment rotation threshold.
func WithMaxSegmentBytes(n int64) Option {
	return func(w *WAL) { w.maxSegmentBytes = n }
}

// Open opens (or creates) a WAL rooted at dir, replaying any existing
// segments to reconstruct the set of unresolved entries before accepting
// new writes.
func Open(dir string, opts ...Option) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	w := &WAL{
		dir:             dir,
		maxSegmentBytes: defaultMaxSegmentBytes,
		index:           make(map[string]*entryState),
		writeCh:         make(chan *writeRequest, 256),
		closeCh:         make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}

	if err := w.replay(); err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.writerLoop()

	return w, nil
}

func (w *WAL) replay() error {
	seqs, err := listSegmentSeqs(w.dir)
	if err != nil {
		return fmt.Errorf("wal: list segments: %w", err)
	}

	for _, seq := range seqs {
		path := segmentPath(w.dir, seq)
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("wal: open segment %s: %w", path, err)
		}
		records, err := decodeRecords(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("wal: decode segment %s: %w", path, err)
		}

		for _, rec := range records {
			switch rec.recType {
			case recTypeEvent:
				var p eventPayload
				if err := json.Unmarshal(rec.payload, &p); err != nil {
					return fmt.Errorf("wal: decode event record in %s: %w", path, err)
				}
				pending := make(map[string]struct{}, len(p.Providers))
				for _, prov := range p.Providers {
					pending[prov] = struct{}{}
				}
				w.index[p.Event.ID] = &entryState{event: p.Event, pending: pending, segSeq: seq}
			case recTypeAck:
				var a ackPayload
				if err := json.Unmarshal(rec.payload, &a); err != nil {
					return fmt.Errorf("wal: decode ack record in %s: %w", path, err)
				}
				if entry, ok := w.index[a.EventID]; ok {
					delete(entry.pending, a.Provider)
					if len(entry.pending) == 0 {
						delete(w.index, a.EventID)
					}
				}
			}
		}

		if seq >= w.nextSeq {
			w.nextSeq = seq + 1
		}
	}

	// Now that the full index reflects every ack replayed, compute each
	// segment's real refcount: the number of still-unresolved entries whose
	// event record originally landed there.
	segRefCount := make(map[uint64]int)
	for _, entry := range w.index {
		segRefCount[entry.segSeq]++
	}

	var activeSeq uint64
	if len(seqs) > 0 {
		activeSeq = seqs[len(seqs)-1]
	}

	for _, seq := range seqs {
		if seq != activeSeq && segRefCount[seq] == 0 {
			if err := os.Remove(segmentPath(w.dir, seq)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal: gc segment %d: %w", seq, err)
			}
			continue
		}
		w.segments = append(w.segments, &segment{
			seq:      seq,
			path:     segmentPath(w.dir, seq),
			refCount: segRefCount[seq],
		})
	}

	if len(w.segments) == 0 {
		seg, err := createSegment(w.dir, w.nextSeq)
		if err != nil {
			return fmt.Errorf("wal: create initial segment: %w", err)
		}
		w.nextSeq++
		w.segments = append(w.segments, seg)
		return nil
	}

	last := w.segments[len(w.segments)-1]
	seg, err := openSegmentForAppend(w.dir, last.seq)
	if err != nil {
		return fmt.Errorf("wal: reopen active segment: %w", err)
	}
	seg.refCount = last.refCount
	w.segments[len(w.segments)-1] = seg
	return nil
}

// Append durably writes a new event with the set of providers it must be
// delivered to. It blocks until the record is fsync'd; a nil return means
// the event will survive a crash from this point on.
func (w *WAL) Append(event adapter.Event, providers []string) error {
	if w.closed.Load() {
		return fmt.Errorf("wal: closed")
	}

	payload, err := json.Marshal(eventPayload{Event: event, Providers: providers})
	if err != nil {
		return fmt.Errorf("wal: marshal event: %w", err)
	}

	req := &writeRequest{
		frame: encodeFrame(recTypeEvent, payload),
		done:  make(chan error, 1),
	}
	req.apply = func(segSeq uint64) {
		pending := make(map[string]struct{}, len(providers))
		for _, p := range providers {
			pending[p] = struct{}{}
		}
		w.index[event.ID] = &entryState{event: event, pending: pending, segSeq: segSeq}
		w.increfSegment(segSeq)
	}

	return w.submit(req)
}

// Ack records that provider has resolved (accepted or permanently failed)
// eventID. Once every provider originally routed for an event has acked,
// the entry becomes GC-able and its segment's refcount drops accordingly.
func (w *WAL) Ack(eventID, provider string, disposition adapter.Disposition) error {
	if w.closed.Load() {
		return fmt.Errorf("wal: closed")
	}

	payload, err := json.Marshal(ackPayload{EventID: eventID, Provider: provider, Disposition: int(disposition)})
	if err != nil {
		return fmt.Errorf("wal: marshal ack: %w", err)
	}

	req := &writeRequest{
		frame: encodeFrame(recTypeAck, payload),
		done:  make(chan error, 1),
	}
	req.apply = func(_ uint64) {
		entry, ok := w.index[eventID]
		if !ok {
			return // already resolved (duplicate ack) or unknown event.
		}
		delete(entry.pending, provider)
		if len(entry.pending) == 0 {
			delete(w.index, eventID)
			w.decrefSegment(entry.segSeq)
		}
	}

	return w.submit(req)
}

func (w *WAL) submit(req *writeRequest) error {
	w.writeCh <- req
	return <-req.done
}

// Pending returns a snapshot of every unresolved entry, e.g. for the
// dispatcher to re-enqueue at startup after replay.
func (w *WAL) Pending() []Entry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	entries := make([]Entry, 0, len(w.index))
	for _, e := range w.index {
		providers := make([]string, 0, len(e.pending))
		for p := range e.pending {
			providers = append(providers, p)
		}
		entries = append(entries, Entry{Event: e.event, Pending: providers})
	}
	return entries
}

// UnackedCount reports the number of entries not yet resolved by every
// target provider. Feeds the wal_unacked_entries metric.
func (w *WAL) UnackedCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.index)
}

// Close stops accepting new writes, flushes and fsyncs the active segment,
// and closes its file handle.
func (w *WAL) Close() error {
	w.closed.Store(true)
	close(w.closeCh)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	active := w.segments[len(w.segments)-1]
	if err := active.sync(); err != nil {
		return err
	}
	return active.close()
}

func (w *WAL) writerLoop() {
	defer w.wg.Done()
	for {
		select {
		case req := <-w.writeCh:
			w.commitBatch(w.drainBatch(req))
		case <-w.closeCh:
			for {
				select {
				case req := <-w.writeCh:
					w.commitBatch(w.drainBatch(req))
				default:
					return
				}
			}
		}
	}
}

func (w *WAL) drainBatch(first *writeRequest) []*writeRequest {
	batch := []*writeRequest{first}
	for {
		select {
		case r := <-w.writeCh:
			batch = append(batch, r)
		default:
			return batch
		}
	}
}

// commitBatch writes every request's frame to disk (rotating segments as
// needed), fsyncs, then applies each request's in-memory state update and
// wakes its caller — in that order, so a caller never observes an ack for
// data that isn't durable yet.
func (w *WAL) commitBatch(batch []*writeRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()

	writeErrs := make([]error, len(batch))
	segSeqs := make([]uint64, len(batch))
	synced := make([]bool, len(batch))

	active := w.segments[len(w.segments)-1]
	for i, req := range batch {
		if active.size+int64(len(req.frame)) > w.maxSegmentBytes {
			if err := w.rotate(active); err != nil {
				writeErrs[i] = err
				synced[i] = true // terminal; no further sync will help.
				continue
			}
			// Rotation already synced everything written to the old
			// segment, so every prior batch entry that succeeded its
			// write is now known-durable regardless of what happens next.
			for j := 0; j < i; j++ {
				if writeErrs[j] == nil {
					synced[j] = true
				}
			}
			active = w.segments[len(w.segments)-1]
		}
		writeErrs[i] = active.append(req.frame)
		segSeqs[i] = active.seq
	}

	finalSyncErr := active.sync()

	for i, req := range batch {
		err := writeErrs[i]
		if err == nil && !synced[i] {
			err = finalSyncErr
		}
		if err == nil && req.apply != nil {
			req.apply(segSeqs[i])
		}
		req.done <- err
	}
}

func (w *WAL) rotate(active *segment) error {
	if err := active.sync(); err != nil {
		return err
	}
	if err := active.close(); err != nil {
		return err
	}
	seg, err := createSegment(w.dir, w.nextSeq)
	if err != nil {
		return err
	}
	w.nextSeq++
	w.segments = append(w.segments, seg)
	return nil
}

// increfSegment and decrefSegment are only ever called from within
// commitBatch (via req.apply), which holds w.mu for the duration.
func (w *WAL) increfSegment(seq uint64) {
	for _, seg := range w.segments {
		if seg.seq == seq {
			seg.refCount++
			return
		}
	}
}

func (w *WAL) decrefSegment(seq uint64) {
	for i, seg := range w.segments {
		if seg.seq != seq {
			continue
		}
		seg.refCount--
		if seg.refCount == 0 && i != len(w.segments)-1 {
			if seg.f != nil {
				seg.close()
			}
			os.Remove(seg.path)
			w.segments = append(w.segments[:i], w.segments[i+1:]...)
		}
		return
	}
}
