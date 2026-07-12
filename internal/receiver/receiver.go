// Package receiver implements tallyd's HTTP ingress: POST /v1/events.
package receiver

import (
	"encoding/json"
	"errors"
	"fmt"
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

// ValidationError means the request itself is malformed or unroutable —
// retrying it unchanged will never succeed. Transports map this to their
// own "bad request" status (HTTP 400, gRPC InvalidArgument).
type ValidationError struct{ msg string }

func (e *ValidationError) Error() string { return e.msg }

// UnavailableError means ingest failed for a transient, infrastructure
// reason (e.g. the WAL append failed) — the caller should retry as-is.
// Transports map this to their own "unavailable" status (HTTP 503, gRPC
// Unavailable).
type UnavailableError struct{ msg string }

func (e *UnavailableError) Error() string { return e.msg }

// Ingest runs the transport-agnostic core: validate, enrich, route, then
// durably append every event before returning. Both the HTTP handler and
// any other transport (e.g. gRPC) call this so behavior — validation
// rules, routing, durability — is identical regardless of how an event
// arrived.
func (r *Receiver) Ingest(events []adapter.Event) error {
	if len(events) == 0 {
		return &ValidationError{msg: "no events in request"}
	}

	now := r.Now()
	providers := make([][]string, len(events))
	for i, e := range events {
		if err := validate(e, now); err != nil {
			return &ValidationError{msg: err.Error()}
		}
		events[i] = enrich(e, now)

		p := events[i].Route
		if len(p) == 0 {
			p = r.Router.Route(events[i])
		}
		if len(p) == 0 {
			// An event with nowhere to go must not be durably accepted:
			// nothing would ever Ack it, so it would sit in the WAL
			// unresolved forever instead of just failing loudly now.
			return &ValidationError{msg: fmt.Sprintf(
				"event %q (event_name=%q) matches no provider route and no default route is configured",
				events[i].ID, events[i].EventName,
			)}
		}
		providers[i] = p
	}

	// Durably append every event before acking the caller. If any append
	// fails partway through, the caller must retry the whole request: it
	// has no way to know which prefix already landed, and retrying a
	// mixture of new and already-durable events is safe because the
	// event ID is the provider-side idempotency key.
	for i, e := range events {
		if err := r.Sink.Append(e, providers[i]); err != nil {
			return &UnavailableError{msg: fmt.Sprintf("failed to durably persist event: %v", err)}
		}
	}

	if r.Metrics != nil {
		r.Metrics.RecordEventsReceived(len(events))
	}
	return nil
}

func (r *Receiver) handleEvents(w http.ResponseWriter, req *http.Request) {
	events, err := decodeEvents(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := r.Ingest(events); err != nil {
		status := http.StatusInternalServerError
		switch err.(type) {
		case *ValidationError:
			status = http.StatusBadRequest
		case *UnavailableError:
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
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
