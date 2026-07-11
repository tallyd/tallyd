// Package dlq provides durable on-disk parking for events a provider has
// permanently rejected or that exhausted their retry budget.
package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/earthy1024/tallyd/adapter"
)

type entry struct {
	Provider  string        `json:"provider"`
	Event     adapter.Event `json:"event"`
	Reason    string        `json:"reason"`
	Timestamp time.Time     `json:"timestamp"`
}

// DLQ appends dead-lettered events as JSON lines to a per-provider file.
// Simple and auditable; this first pass does not support replaying or
// requeuing from the DLQ, only recording it durably and counting depth.
type DLQ struct {
	dir string

	mu    sync.Mutex
	files map[string]*os.File
	depth map[string]int
}

// Open opens (creating if needed) a DLQ rooted at dir.
func Open(dir string) (*DLQ, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("dlq: mkdir %s: %w", dir, err)
	}
	return &DLQ{dir: dir, files: make(map[string]*os.File), depth: make(map[string]int)}, nil
}

// Put durably records that event was dead-lettered for provider, with a
// human-readable reason.
func (d *DLQ) Put(provider string, event adapter.Event, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, ok := d.files[provider]
	if !ok {
		path := filepath.Join(d.dir, provider+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("dlq: open %s: %w", path, err)
		}
		d.files[provider] = f
	}

	line, err := json.Marshal(entry{Provider: provider, Event: event, Reason: reason, Timestamp: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("dlq: marshal: %w", err)
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("dlq: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("dlq: sync: %w", err)
	}

	d.depth[provider]++
	return nil
}

// Depth returns the number of events dead-lettered for provider during
// this process's lifetime. Feeds the dlq_depth{provider} metric.
func (d *DLQ) Depth(provider string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.depth[provider]
}

// Close closes every per-provider file handle opened so far.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var firstErr error
	for _, f := range d.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
