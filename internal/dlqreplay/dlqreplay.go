// Package dlqreplay serves an admin endpoint for re-injecting
// dead-lettered events back into the pipeline for a single provider.
package dlqreplay

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/dlq"
)

// Sink durably re-injects a replayed event, targeting only the given
// providers. *wal.WAL-backed sinks (e.g. pipeline's walDispatchSink)
// satisfy this: the event gets a fresh WAL entry and is dispatched live,
// exactly like a brand-new POST /v1/events call.
type Sink interface {
	Append(event adapter.Event, providers []string) error
}

// Store is the subset of *dlq.DLQ this handler needs.
type Store interface {
	List(provider string) ([]dlq.Record, error)
	ListPoison(provider string) ([]dlq.Record, error)
	Remove(provider string, eventIDs []string) error
}

// Handler serves POST /v1/dlq/replay?provider=X[&include_poison=true].
type Handler struct {
	Sink Sink
	DLQ  Store

	// KnownProviders gates which provider names are accepted. Without
	// this check, a misspelled provider query param would durably
	// re-append every replayed event to the WAL before Sink.Append's
	// dispatch step failed on "unknown provider" — leaving permanently
	// unresolvable WAL entries behind, the same failure mode the
	// receiver's own routing validation guards against on the normal
	// ingest path.
	KnownProviders map[string]bool
}

// Result is the JSON response body.
type Result struct {
	Provider string   `json:"provider"`
	Replayed []string `json:"replayed"`
	Failed   []string `json:"failed,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	provider := r.URL.Query().Get("provider")
	if provider == "" {
		http.Error(w, "provider query parameter is required", http.StatusBadRequest)
		return
	}
	if !h.KnownProviders[provider] {
		http.Error(w, fmt.Sprintf("unknown provider %q", provider), http.StatusBadRequest)
		return
	}

	records, err := h.DLQ.List(provider)
	if err != nil {
		http.Error(w, fmt.Sprintf("list dlq: %v", err), http.StatusInternalServerError)
		return
	}

	if r.URL.Query().Get("include_poison") == "true" {
		poisoned, err := h.DLQ.ListPoison(provider)
		if err != nil {
			http.Error(w, fmt.Sprintf("list poisoned dlq: %v", err), http.StatusInternalServerError)
			return
		}
		records = append(records, poisoned...)
	}

	result := Result{Provider: provider, Replayed: []string{}}
	for _, rec := range records {
		// Only re-target the provider being replayed — other providers
		// this event was originally routed to (if any) either already
		// succeeded or are being replayed independently via their own
		// DLQ, not re-triggered here.
		if err := h.Sink.Append(rec.Event, []string{provider}); err != nil {
			result.Failed = append(result.Failed, rec.Event.ID)
			continue
		}
		result.Replayed = append(result.Replayed, rec.Event.ID)
	}

	// A successful Sink.Append means the event is durably back in the
	// WAL as a fresh pending entry — the same "accepted" guarantee a
	// normal POST /v1/events gives, not confirmation of delivery. The old
	// DLQ record is now superseded by that fresh attempt, so it's safe to
	// remove regardless of how the new attempt eventually resolves: if it
	// fails again, it'll produce a brand new DLQ record with the
	// attempt/poison counter continuing from where it left off.
	if len(result.Replayed) > 0 {
		if err := h.DLQ.Remove(provider, result.Replayed); err != nil {
			http.Error(w, fmt.Sprintf("remove replayed entries: %v", err), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
