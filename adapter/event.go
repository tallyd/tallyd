// Package adapter defines the vendor-agnostic event shape and the interface
// that per-provider billing adapters implement.
package adapter

import "time"

// Event is the canonical, vendor-neutral representation of a billable
// occurrence. Adapters translate it into a provider's wire format; tallyd
// itself never aggregates or reshapes it.
type Event struct {
	// ID uniquely identifies the event and doubles as the idempotency key
	// passed to providers. Should be a UUIDv7 (time-ordered) string.
	ID string `json:"id"`

	CustomerID string    `json:"customer_id"`
	EventName  string    `json:"event_name"`
	Timestamp  time.Time `json:"timestamp"`

	// Properties carries arbitrary event payload data. Adapters are
	// responsible for any provider-specific type coercion (e.g. Metronome
	// requires all property values to be stringified).
	Properties map[string]any `json:"properties,omitempty"`

	// Route lists the provider names this event should be dispatched to.
	// Empty means "use the daemon's default routing rule."
	Route []string `json:"route,omitempty"`
}
