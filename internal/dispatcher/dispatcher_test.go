package dispatcher_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/dispatcher"
	"github.com/earthy1024/tallyd/internal/wal"
)

type fakeEnqueuer struct {
	mu       sync.Mutex
	enqueued []adapter.Event
}

func (f *fakeEnqueuer) Enqueue(event adapter.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, event)
}

func (f *fakeEnqueuer) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for _, e := range f.enqueued {
		ids = append(ids, e.ID)
	}
	return ids
}

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func TestDispatchFansOutToProviders(t *testing.T) {
	orb := &fakeEnqueuer{}
	metronome := &fakeEnqueuer{}
	d := dispatcher.New(map[string]dispatcher.Enqueuer{"orb": orb, "metronome": metronome})

	evt := testEvent("evt-1")
	if err := d.Dispatch(evt, []string{"orb", "metronome"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if ids := orb.ids(); len(ids) != 1 || ids[0] != "evt-1" {
		t.Errorf("orb got %v, want [evt-1]", ids)
	}
	if ids := metronome.ids(); len(ids) != 1 || ids[0] != "evt-1" {
		t.Errorf("metronome got %v, want [evt-1]", ids)
	}
}

func TestDispatchUnknownProviderErrors(t *testing.T) {
	d := dispatcher.New(map[string]dispatcher.Enqueuer{"orb": &fakeEnqueuer{}})

	err := d.Dispatch(testEvent("evt-1"), []string{"orb", "does-not-exist"})
	if err == nil || !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("expected unknown-provider error, got %v", err)
	}
}

func TestReplayPendingReenqueuesOnlyPendingProviders(t *testing.T) {
	orb := &fakeEnqueuer{}
	metronome := &fakeEnqueuer{}
	d := dispatcher.New(map[string]dispatcher.Enqueuer{"orb": orb, "metronome": metronome})

	entries := []wal.Entry{
		{Event: testEvent("evt-1"), Pending: []string{"orb", "metronome"}},
		{Event: testEvent("evt-2"), Pending: []string{"metronome"}}, // already acked by orb
	}

	if err := d.ReplayPending(entries); err != nil {
		t.Fatalf("replay pending: %v", err)
	}

	if ids := orb.ids(); len(ids) != 1 || ids[0] != "evt-1" {
		t.Errorf("orb got %v, want [evt-1]", ids)
	}
	if ids := metronome.ids(); len(ids) != 2 {
		t.Errorf("metronome got %v, want 2 entries", ids)
	}
}
