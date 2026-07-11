package dlq_test

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/dlq"
)

func TestPutAppendsAndTracksDepth(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	evt := adapter.Event{ID: "evt-1", CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
	if err := d.Put("orb", evt, "4xx from provider"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := d.Put("orb", evt, "retry budget exhausted"); err != nil {
		t.Fatalf("put: %v", err)
	}

	if got := d.Depth("orb"); got != 2 {
		t.Errorf("Depth(orb) = %d, want 2", got)
	}
	if got := d.Depth("metronome"); got != 0 {
		t.Errorf("Depth(metronome) = %d, want 0", got)
	}

	f, err := os.Open(filepath.Join(dir, "orb.jsonl"))
	if err != nil {
		t.Fatalf("open dlq file: %v", err)
	}
	defer f.Close()

	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines++
	}
	if lines != 2 {
		t.Errorf("orb.jsonl has %d lines, want 2", lines)
	}
}
