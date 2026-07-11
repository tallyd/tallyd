package adapter

import "context"

// Disposition is an adapter's verdict on the outcome of a send attempt for
// a single event within a batch.
type Disposition int

const (
	// Ok means the provider accepted the event; it is safe to GC from the WAL.
	Ok Disposition = iota
	// Retry means the send should be retried with backoff (e.g. 5xx, 429,
	// network error). Callers must cap total retry duration below the
	// provider's dedup window to avoid double-counting.
	Retry
	// DeadLetter means the event cannot succeed on retry (e.g. 4xx
	// validation error) and should be parked in the DLQ.
	DeadLetter
)

func (d Disposition) String() string {
	switch d {
	case Ok:
		return "Ok"
	case Retry:
		return "Retry"
	case DeadLetter:
		return "DeadLetter"
	default:
		return "Unknown"
	}
}

// EventResult carries the per-event outcome of a batch send, keyed by the
// event's position in the batch that was passed to Encode/Send.
type EventResult struct {
	EventID     string
	Disposition Disposition
	Err         error
}

// BatchResult is the outcome of a single Send call across all events in
// the batch. A batch send can partially succeed: some events may be Ok
// while others need Retry or DeadLetter.
type BatchResult struct {
	Results []EventResult
}

// Adapter is implemented once per billing provider. It owns wire-format
// encoding, transport, and classification of provider responses.
type Adapter interface {
	// Encode serializes a batch of events into the provider's wire format.
	Encode(events []Event) ([]byte, error)

	// Send transmits an already-encoded batch. It returns a BatchResult
	// describing the outcome; transport-level errors (e.g. connection
	// refused) should be surfaced via err, and classified by Classify.
	Send(ctx context.Context, body []byte) (BatchResult, error)

	// Classify maps a transport error and/or HTTP status code to a
	// Disposition, e.g. 5xx/429/network -> Retry, 4xx -> DeadLetter.
	Classify(err error, status int) Disposition

	// MaxBatchSize returns the maximum number of events this provider
	// accepts in a single batch request.
	MaxBatchSize() int
}
