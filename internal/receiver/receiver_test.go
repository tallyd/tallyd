package receiver_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/receiver"
)

type fakeSink struct {
	mu       sync.Mutex
	appended []adapter.Event
	failFrom int // fail every Append from this call index onward (-1 = never)
	calls    int
}

func (f *fakeSink) Append(event adapter.Event, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failFrom >= 0 && f.calls > f.failFrom {
		return errors.New("simulated durable write failure")
	}
	f.appended = append(f.appended, event)
	return nil
}

func newTestReceiver(sink *fakeSink) *receiver.Receiver {
	r := receiver.New(sink, &receiver.StaticRouter{Default: []string{"orb"}})
	r.Now = func() time.Time { return time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC) }
	return r
}

func doPost(t *testing.T, h http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSingleEventAccepted(t *testing.T) {
	sink := &fakeSink{failFrom: -1}
	r := newTestReceiver(sink)

	evt := adapter.Event{
		ID:         "evt-1",
		CustomerID: "cust_1",
		EventName:  "api_call",
		Timestamp:  time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC),
	}

	rec := doPost(t, r.Handler(), evt)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if len(sink.appended) != 1 || sink.appended[0].ID != "evt-1" {
		t.Fatalf("sink.appended = %+v, want one event evt-1", sink.appended)
	}
}

func TestArrayOfEventsAccepted(t *testing.T) {
	sink := &fakeSink{failFrom: -1}
	r := newTestReceiver(sink)

	events := []adapter.Event{
		{ID: "evt-1", CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)},
		{ID: "evt-2", CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Date(2026, 7, 11, 11, 5, 0, 0, time.UTC)},
	}

	rec := doPost(t, r.Handler(), events)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if len(sink.appended) != 2 {
		t.Fatalf("sink.appended has %d events, want 2", len(sink.appended))
	}
}

func TestMissingCustomerIDRejected(t *testing.T) {
	sink := &fakeSink{failFrom: -1}
	r := newTestReceiver(sink)

	evt := adapter.Event{ID: "evt-1", EventName: "api_call", Timestamp: time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)}

	rec := doPost(t, r.Handler(), evt)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if len(sink.appended) != 0 {
		t.Fatalf("sink.appended = %+v, want none (should reject before append)", sink.appended)
	}
}

func TestFarFutureTimestampRejected(t *testing.T) {
	sink := &fakeSink{failFrom: -1}
	r := newTestReceiver(sink)

	// Now() is stubbed to 2026-07-11T12:00:00Z; 48h ahead exceeds the 24h cap.
	evt := adapter.Event{
		ID:         "evt-1",
		CustomerID: "cust_1",
		EventName:  "api_call",
		Timestamp:  time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
	}

	rec := doPost(t, r.Handler(), evt)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSinkFailureReturns503WithoutPartialAck(t *testing.T) {
	sink := &fakeSink{failFrom: 1} // second Append call fails
	r := newTestReceiver(sink)

	events := []adapter.Event{
		{ID: "evt-1", CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)},
		{ID: "evt-2", CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)},
	}

	rec := doPost(t, r.Handler(), events)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
