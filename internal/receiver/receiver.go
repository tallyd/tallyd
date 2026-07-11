// Package receiver implements tallyd's HTTP ingress: POST /v1/events.
package receiver

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/earthy1024/tallyd/adapter"
)

// maxFutureTimestamp rejects events timestamped further in the future than
// this. Metronome itself rejects timestamps >24h ahead; enforcing the same
// bound here at ingress means a bad event never wastes a WAL append.
const maxFutureTimestamp = 24 * time.Hour

// Sink durably accepts events. *wal.WAL satisfies this interface without
// the receiver needing to import the wal package directly, keeping the two
// packages independently testable.
type Sink interface {
	Append(event adapter.Event, providers []string) error
}

// Router decides which providers an event should be routed to when the
// event itself doesn't specify a Route.
type Router interface {
	Route(event adapter.Event) []string
}

// MetricsRecorder is optional; a nil Metrics field records nothing.
// *metrics.Metrics satisfies this structurally.
type MetricsRecorder interface {
	RecordEventsReceived(n int)
}

// Receiver handles POST /v1/events: validate, enrich, hand off to the
// durable Sink, and only then ack the caller with 2xx.
type Receiver struct {
	Sink    Sink
	Router  Router
	Metrics MetricsRecorder  // optional
	Now     func() time.Time // overridable for tests
}

// New returns a Receiver ready to be mounted as an http.Handler.
func New(sink Sink, router Router) *Receiver {
	return &Receiver{Sink: sink, Router: router, Now: time.Now}
}

func (r *Receiver) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", r.handleEvents)
	return mux
}

func (r *Receiver) handleEvents(w http.ResponseWriter, req *http.Request) {
	events, err := decodeEvents(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(events) == 0 {
		http.Error(w, "no events in request body", http.StatusBadRequest)
		return
	}

	now := r.Now()
	for i, e := range events {
		if err := validate(e, now); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		events[i] = enrich(e, now)
	}

	// Durably append every event before acking the caller. If any append
	// fails partway through, the caller must retry the whole request: it
	// has no way to know which prefix already landed, and retrying a
	// mixture of new and already-durable events is safe because the
	// event ID is the provider-side idempotency key.
	for _, e := range events {
		providers := e.Route
		if len(providers) == 0 {
			providers = r.Router.Route(e)
		}
		if err := r.Sink.Append(e, providers); err != nil {
			http.Error(w, "failed to durably persist event", http.StatusServiceUnavailable)
			return
		}
	}

	if r.Metrics != nil {
		r.Metrics.RecordEventsReceived(len(events))
	}
	w.WriteHeader(http.StatusAccepted)
}

// decodeEvents accepts either a single event object or a JSON array of
// events in the request body.
func decodeEvents(body io.Reader) ([]adapter.Event, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		return nil, errors.New("invalid JSON body")
	}

	var events []adapter.Event
	if err := json.Unmarshal(raw, &events); err == nil {
		return events, nil
	}

	var single adapter.Event
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, errors.New("body must be a single event object or an array of events")
	}
	return []adapter.Event{single}, nil
}

func validate(e adapter.Event, now time.Time) error {
	if e.ID == "" {
		return errors.New("event.id is required")
	}
	if e.CustomerID == "" {
		return errors.New("event.customer_id is required")
	}
	if e.EventName == "" {
		return errors.New("event.event_name is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("event.timestamp is required")
	}
	if e.Timestamp.After(now.Add(maxFutureTimestamp)) {
		return errors.New("event.timestamp is too far in the future")
	}
	return nil
}

// enrich tags an event with anything the daemon itself is responsible for
// filling in. Currently a no-op placeholder for receive-time tagging;
// kept as a separate step so future enrichment doesn't get tangled into
// validation.
func enrich(e adapter.Event, _ time.Time) adapter.Event {
	return e
}
