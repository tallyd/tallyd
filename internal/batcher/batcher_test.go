package batcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/batcher"
)

type fakeAdapter struct {
	maxBatch       int
	dispositionFor func(eventID string, attempt int) adapter.Disposition

	mu        sync.Mutex
	sendCalls [][]adapter.Event
	attempts  map[string]int
}

func newFakeAdapter(maxBatch int, dispositionFor func(string, int) adapter.Disposition) *fakeAdapter {
	return &fakeAdapter{maxBatch: maxBatch, dispositionFor: dispositionFor, attempts: make(map[string]int)}
}

func (f *fakeAdapter) Encode(events []adapter.Event) ([]byte, error) {
	return json.Marshal(events)
}

func (f *fakeAdapter) Send(_ context.Context, body []byte) (adapter.BatchResult, error) {
	var events []adapter.Event
	if err := json.Unmarshal(body, &events); err != nil {
		return adapter.BatchResult{}, err
	}

	f.mu.Lock()
	f.sendCalls = append(f.sendCalls, events)
	f.mu.Unlock()

	results := make([]adapter.EventResult, len(events))
	for i, e := range events {
		f.mu.Lock()
		f.attempts[e.ID]++
		attempt := f.attempts[e.ID]
		f.mu.Unlock()

		d := adapter.Ok
		if f.dispositionFor != nil {
			d = f.dispositionFor(e.ID, attempt)
		}
		results[i] = adapter.EventResult{EventID: e.ID, Disposition: d}
	}
	return adapter.BatchResult{Results: results}, nil
}

func (f *fakeAdapter) Classify(_ error, _ int) adapter.Disposition { return adapter.Retry }
func (f *fakeAdapter) MaxBatchSize() int                           { return f.maxBatch }

func (f *fakeAdapter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sendCalls)
}

type ackCall struct {
	eventID, provider string
	disposition       adapter.Disposition
}

type fakeAcker struct {
	mu    sync.Mutex
	acked []ackCall
}

func (f *fakeAcker) Ack(eventID, provider string, disposition adapter.Disposition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ackCall{eventID, provider, disposition})
	return nil
}

func (f *fakeAcker) find(eventID string) (ackCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.acked {
		if a.eventID == eventID {
			return a, true
		}
	}
	return ackCall{}, false
}

type dlqCall struct {
	provider, eventID, reason string
}

type fakeDLQ struct {
	mu   sync.Mutex
	puts []dlqCall
}

func (f *fakeDLQ) Put(provider string, event adapter.Event, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, dlqCall{provider, event.ID, reason})
	return nil
}

func (f *fakeDLQ) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestCloseFlushesQueuedEventsBeforeLingerElapses proves the shutdown
// path a graceful signal (SIGINT/SIGTERM) relies on: events still sitting
// in the queue, well before their linger window or max-batch size would
// have triggered a flush on their own, must still be sent before Close()
// returns — not silently dropped.
func TestCloseFlushesQueuedEventsBeforeLingerElapses(t *testing.T) {
	ad := newFakeAdapter(100, nil) // always Ok; batch size never reached
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	// Linger is deliberately huge so only Close()'s drain-and-flush could
	// possibly resolve these before the test's own timeout would.
	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})

	b.Enqueue(testEvent("evt-1"))
	b.Enqueue(testEvent("evt-2"))

	b.Close()

	if got := ad.callCount(); got != 1 {
		t.Fatalf("adapter Send called %d times, want exactly 1 (one final flush on Close)", got)
	}
	for _, id := range []string{"evt-1", "evt-2"} {
		call, ok := acker.find(id)
		if !ok || call.disposition != adapter.Ok {
			t.Errorf("%s: expected Ok ack after Close, got %+v (ok=%v)", id, call, ok)
		}
	}
}

func TestFlushesOnMaxBatchSize(t *testing.T) {
	ad := newFakeAdapter(2, nil) // always Ok
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))
	b.Enqueue(testEvent("evt-2"))

	waitFor(t, time.Second, func() bool { return ad.callCount() == 1 })

	if _, ok := acker.find("evt-1"); !ok {
		t.Errorf("evt-1 not acked")
	}
	if _, ok := acker.find("evt-2"); !ok {
		t.Errorf("evt-2 not acked")
	}
}

func TestFlushesOnLinger(t *testing.T) {
	ad := newFakeAdapter(100, nil) // always Ok, batch size never reached
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, 20*time.Millisecond, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, time.Second, func() bool {
		_, ok := acker.find("evt-1")
		return ok
	})
}

func TestDeadLetterDisposition(t *testing.T) {
	ad := newFakeAdapter(10, func(string, int) adapter.Disposition { return adapter.DeadLetter })
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, 5*time.Millisecond, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, time.Second, func() bool { return dlq.count() == 1 })

	call, ok := acker.find("evt-1")
	if !ok || call.disposition != adapter.DeadLetter {
		t.Errorf("expected evt-1 acked DeadLetter, got %+v (ok=%v)", call, ok)
	}
}

func TestRetryThenOk(t *testing.T) {
	ad := newFakeAdapter(10, func(_ string, attempt int) adapter.Disposition {
		if attempt < 3 {
			return adapter.Retry
		}
		return adapter.Ok
	})
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	retry := batcher.RetryPolicy{InitialBackoff: 2 * time.Millisecond, MaxBackoff: 5 * time.Millisecond, MaxElapsed: time.Second}
	b := batcher.New("orb", ad, 2*time.Millisecond, acker, dlq, retry)
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, 2*time.Second, func() bool {
		call, ok := acker.find("evt-1")
		return ok && call.disposition == adapter.Ok
	})
	if dlq.count() != 0 {
		t.Errorf("expected no dead-lettering, got %d", dlq.count())
	}
}

// slowAdapter tracks how many Send calls are concurrently in-flight, to
// prove a single Batcher never fires two provider requests at once even
// when new events queue up faster than a slow provider responds.
type slowAdapter struct {
	maxBatch  int
	sendDelay time.Duration

	mu            sync.Mutex
	sendCalls     int
	concurrent    int
	maxConcurrent int
	batchSizes    []int
}

func (a *slowAdapter) Encode(events []adapter.Event) ([]byte, error) {
	return json.Marshal(events)
}

func (a *slowAdapter) Send(_ context.Context, body []byte) (adapter.BatchResult, error) {
	var events []adapter.Event
	if err := json.Unmarshal(body, &events); err != nil {
		return adapter.BatchResult{}, err
	}

	a.mu.Lock()
	a.sendCalls++
	a.concurrent++
	if a.concurrent > a.maxConcurrent {
		a.maxConcurrent = a.concurrent
	}
	a.batchSizes = append(a.batchSizes, len(events))
	a.mu.Unlock()

	time.Sleep(a.sendDelay)

	a.mu.Lock()
	a.concurrent--
	a.mu.Unlock()

	results := make([]adapter.EventResult, len(events))
	for i, e := range events {
		results[i] = adapter.EventResult{EventID: e.ID, Disposition: adapter.Ok}
	}
	return adapter.BatchResult{Results: results}, nil
}

func (a *slowAdapter) Classify(_ error, _ int) adapter.Disposition { return adapter.Retry }
func (a *slowAdapter) MaxBatchSize() int                           { return a.maxBatch }

func (a *slowAdapter) snapshot() (calls, maxConcurrent int, sizes []int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sendCalls, a.maxConcurrent, append([]int(nil), a.batchSizes...)
}

// TestConcurrentSendsAreBounded answers a specific question about the
// design: if many events arrive while sends to a provider are already in
// flight, does the Batcher pipeline multiple requests concurrently
// instead of serializing behind one slow provider response? Yes, but not
// unbounded — up to defaultMaxInFlight requests can be in flight to one
// provider at once; a saturated provider back-pressures further sends
// rather than piling on unlimited concurrent requests.
//
// maxBatch=1 means every event becomes its own single-event flush, so the
// number of concurrent Send calls is determined purely by the
// concurrency limit, not by incidental batch-vs-linger timing.
func TestConcurrentSendsAreBounded(t *testing.T) {
	const wantMaxInFlight = 4 // must match batcher.defaultMaxInFlight
	const n = 12

	ad := &slowAdapter{maxBatch: 1, sendDelay: 50 * time.Millisecond}
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	for i := 0; i < n; i++ {
		b.Enqueue(testEvent(fmt.Sprintf("evt-%d", i)))
	}

	waitFor(t, 3*time.Second, func() bool {
		for i := 0; i < n; i++ {
			if _, ok := acker.find(fmt.Sprintf("evt-%d", i)); !ok {
				return false
			}
		}
		return true
	})

	calls, maxConcurrent, sizes := ad.snapshot()
	if calls != n {
		t.Errorf("Send called %d time(s), want %d (one per single-event batch)", calls, n)
	}
	for _, size := range sizes {
		if size != 1 {
			t.Errorf("batch size = %d, want 1 for every call (maxBatch=1)", size)
		}
	}
	if maxConcurrent <= 1 {
		t.Errorf("max concurrent Send calls = %d, want > 1 (sends should overlap, not serialize behind one slow request)", maxConcurrent)
	}
	if maxConcurrent > wantMaxInFlight {
		t.Errorf("max concurrent Send calls = %d, want <= %d (concurrency must stay bounded)", maxConcurrent, wantMaxInFlight)
	}
	t.Logf("max concurrent Send calls: %d", maxConcurrent)
}

// TestCloseWaitsForConcurrentInFlightFlushes guards the specific risk
// introduced by making flush() concurrent: Close() must wait for every
// in-flight Send, not just the run() loop's own exit, or a graceful
// shutdown could return while sends are still running — and the caller
// (pipeline.Close) would then close the DLQ/WAL out from under them.
func TestCloseWaitsForConcurrentInFlightFlushes(t *testing.T) {
	ad := &slowAdapter{maxBatch: 1, sendDelay: 100 * time.Millisecond}
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})

	// Enqueue exactly enough to saturate the concurrency limit, so all of
	// them are genuinely in flight (not just queued) at the moment Close
	// is called.
	const n = 4
	for i := 0; i < n; i++ {
		b.Enqueue(testEvent(fmt.Sprintf("evt-%d", i)))
	}
	time.Sleep(10 * time.Millisecond) // let all n sends actually start

	b.Close() // must block until all n in-flight 100ms sends complete

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("evt-%d", i)
		if _, ok := acker.find(id); !ok {
			t.Errorf("%s not acked by the time Close() returned", id)
		}
	}
}

func TestRetryBudgetExhaustion(t *testing.T) {
	ad := newFakeAdapter(10, func(string, int) adapter.Disposition { return adapter.Retry })
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	retry := batcher.RetryPolicy{InitialBackoff: 2 * time.Millisecond, MaxBackoff: 3 * time.Millisecond, MaxElapsed: 15 * time.Millisecond}
	b := batcher.New("orb", ad, 2*time.Millisecond, acker, dlq, retry)
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, 2*time.Second, func() bool { return dlq.count() == 1 })

	call, ok := acker.find("evt-1")
	if !ok || call.disposition != adapter.DeadLetter {
		t.Errorf("expected evt-1 eventually acked DeadLetter after retry exhaustion, got %+v (ok=%v)", call, ok)
	}
}
