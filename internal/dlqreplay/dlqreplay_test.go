package dlqreplay_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/dlq"
	"github.com/tallyd/tallyd/internal/dlqreplay"
)

type fakeSink struct {
	failIDs   map[string]bool
	appended  []adapter.Event
	providers [][]string
}

func (f *fakeSink) Append(event adapter.Event, providers []string) error {
	if f.failIDs[event.ID] {
		return errors.New("simulated append failure")
	}
	f.appended = append(f.appended, event)
	f.providers = append(f.providers, providers)
	return nil
}

type fakeStore struct {
	regular map[string][]dlq.Record
	poison  map[string][]dlq.Record
	removed map[string][]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		regular: make(map[string][]dlq.Record),
		poison:  make(map[string][]dlq.Record),
		removed: make(map[string][]string),
	}
}

func (f *fakeStore) List(provider string) ([]dlq.Record, error)       { return f.regular[provider], nil }
func (f *fakeStore) ListPoison(provider string) ([]dlq.Record, error) { return f.poison[provider], nil }
func (f *fakeStore) Remove(provider string, eventIDs []string) error {
	f.removed[provider] = append(f.removed[provider], eventIDs...)
	return nil
}

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func doPost(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReplaySuccessRemovesFromStore(t *testing.T) {
	store := newFakeStore()
	store.regular["orb"] = []dlq.Record{
		{Provider: "orb", Event: testEvent("evt-1")},
		{Provider: "orb", Event: testEvent("evt-2")},
	}
	sink := &fakeSink{}
	h := &dlqreplay.Handler{Sink: sink, DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	rec := doPost(t, h, "/v1/dlq/replay?provider=orb")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var result dlqreplay.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(result.Replayed) != 2 || len(result.Failed) != 0 {
		t.Errorf("result = %+v, want 2 replayed, 0 failed", result)
	}
	if len(sink.appended) != 2 {
		t.Errorf("sink.appended = %+v, want 2 events", sink.appended)
	}
	for _, providers := range sink.providers {
		if len(providers) != 1 || providers[0] != "orb" {
			t.Errorf("Append called with providers=%v, want [orb] only", providers)
		}
	}
	if len(store.removed["orb"]) != 2 {
		t.Errorf("store.removed[orb] = %v, want 2 IDs removed", store.removed["orb"])
	}
}

func TestReplayPartialFailureOnlyRemovesSucceeded(t *testing.T) {
	store := newFakeStore()
	store.regular["orb"] = []dlq.Record{
		{Provider: "orb", Event: testEvent("evt-ok")},
		{Provider: "orb", Event: testEvent("evt-fail")},
	}
	sink := &fakeSink{failIDs: map[string]bool{"evt-fail": true}}
	h := &dlqreplay.Handler{Sink: sink, DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	rec := doPost(t, h, "/v1/dlq/replay?provider=orb")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var result dlqreplay.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(result.Replayed) != 1 || result.Replayed[0] != "evt-ok" {
		t.Errorf("Replayed = %v, want [evt-ok]", result.Replayed)
	}
	if len(result.Failed) != 1 || result.Failed[0] != "evt-fail" {
		t.Errorf("Failed = %v, want [evt-fail]", result.Failed)
	}
	if len(store.removed["orb"]) != 1 || store.removed["orb"][0] != "evt-ok" {
		t.Errorf("store.removed[orb] = %v, want only [evt-ok] removed", store.removed["orb"])
	}
}

func TestReplayExcludesPoisonByDefault(t *testing.T) {
	store := newFakeStore()
	store.regular["orb"] = []dlq.Record{{Provider: "orb", Event: testEvent("evt-1")}}
	store.poison["orb"] = []dlq.Record{{Provider: "orb", Event: testEvent("evt-poisoned")}}
	sink := &fakeSink{}
	h := &dlqreplay.Handler{Sink: sink, DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	doPost(t, h, "/v1/dlq/replay?provider=orb")

	if len(sink.appended) != 1 || sink.appended[0].ID != "evt-1" {
		t.Errorf("sink.appended = %+v, want only evt-1 (poisoned entry excluded)", sink.appended)
	}
}

func TestReplayIncludePoison(t *testing.T) {
	store := newFakeStore()
	store.regular["orb"] = []dlq.Record{{Provider: "orb", Event: testEvent("evt-1")}}
	store.poison["orb"] = []dlq.Record{{Provider: "orb", Event: testEvent("evt-poisoned")}}
	sink := &fakeSink{}
	h := &dlqreplay.Handler{Sink: sink, DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	doPost(t, h, "/v1/dlq/replay?provider=orb&include_poison=true")

	if len(sink.appended) != 2 {
		t.Errorf("sink.appended = %+v, want 2 events (poisoned entry included)", sink.appended)
	}
}

func TestReplayRejectsUnknownProviderWithoutTouchingSinkOrStore(t *testing.T) {
	store := newFakeStore()
	store.regular["metronome"] = []dlq.Record{{Provider: "metronome", Event: testEvent("evt-1")}}
	sink := &fakeSink{}
	h := &dlqreplay.Handler{Sink: sink, DLQ: store, KnownProviders: map[string]bool{"orb": true}} // metronome not known

	rec := doPost(t, h, "/v1/dlq/replay?provider=metronome")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if len(sink.appended) != 0 {
		t.Errorf("sink.appended = %+v, want none — must reject before ever calling Sink.Append", sink.appended)
	}
	if len(store.removed["metronome"]) != 0 {
		t.Errorf("store.removed[metronome] = %v, want none", store.removed["metronome"])
	}
}

func TestReplayRequiresProviderParam(t *testing.T) {
	h := &dlqreplay.Handler{Sink: &fakeSink{}, DLQ: newFakeStore(), KnownProviders: map[string]bool{"orb": true}}
	rec := doPost(t, h, "/v1/dlq/replay")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestReplayOnlyAcceptsPost(t *testing.T) {
	h := &dlqreplay.Handler{Sink: &fakeSink{}, DLQ: newFakeStore(), KnownProviders: map[string]bool{"orb": true}}
	req := httptest.NewRequest(http.MethodGet, "/v1/dlq/replay?provider=orb", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
