// Package stdout provides a no-op Adapter that prints batches to stdout.
// It requires no vendor credentials, so the full receiver -> WAL ->
// dispatcher -> batcher -> adapter pipeline can be exercised end-to-end
// without a real billing provider.
package stdout

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/earthy1024/tallyd/adapter"
)

const defaultMaxBatchSize = 500

// Adapter implements adapter.Adapter by encoding events as JSON and
// printing each batch to stdout. Every event is always accepted (Ok).
type Adapter struct {
	MaxBatch int
}

// New returns a stdout Adapter with the default max batch size.
func New() *Adapter {
	return &Adapter{MaxBatch: defaultMaxBatchSize}
}

func (a *Adapter) Encode(events []adapter.Event) ([]byte, error) {
	return json.Marshal(events)
}

func (a *Adapter) Send(_ context.Context, body []byte) (adapter.BatchResult, error) {
	var events []adapter.Event
	if err := json.Unmarshal(body, &events); err != nil {
		return adapter.BatchResult{}, err
	}

	fmt.Printf("[stdout adapter] forwarding batch of %d event(s):\n%s\n", len(events), body)

	results := make([]adapter.EventResult, len(events))
	for i, e := range events {
		results[i] = adapter.EventResult{EventID: e.ID, Disposition: adapter.Ok}
	}
	return adapter.BatchResult{Results: results}, nil
}

func (a *Adapter) Classify(_ error, _ int) adapter.Disposition {
	return adapter.Ok
}

func (a *Adapter) MaxBatchSize() int {
	if a.MaxBatch <= 0 {
		return defaultMaxBatchSize
	}
	return a.MaxBatch
}
