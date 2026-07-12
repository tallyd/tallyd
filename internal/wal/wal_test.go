package wal_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/wal"
)

func testEvent(id string) adapter.Event {
	return adapter.Event{
		ID:         id,
		CustomerID: "cust_1",
		EventName:  "api_call",
		Timestamp:  time.Now().UTC(),
		Properties: map[string]any{"endpoint": "/charge"},
	}
}

func TestAppendAckReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	events := []string{"evt-0", "evt-1", "evt-2"}
	for _, id := range events {
		if err := w.Append(testEvent(id), []string{"orb", "metronome"}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	// evt-0: fully acked by both providers -> should be fully resolved.
	if err := w.Ack("evt-0", "orb", adapter.Ok); err != nil {
		t.Fatalf("ack evt-0/orb: %v", err)
	}
	if err := w.Ack("evt-0", "metronome", adapter.Ok); err != nil {
		t.Fatalf("ack evt-0/metronome: %v", err)
	}

	// evt-1: only one of two providers acked -> should remain pending.
	if err := w.Ack("evt-1", "orb", adapter.Ok); err != nil {
		t.Fatalf("ack evt-1/orb: %v", err)
	}

	// evt-2: untouched -> should remain pending on both providers.

	if got := w.UnackedCount(); got != 2 {
		t.Fatalf("before close: UnackedCount() = %d, want 2", got)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen to force a replay from disk, simulating a process restart.
	w2, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()

	pending := w2.Pending()
	if len(pending) != 2 {
		t.Fatalf("after replay: got %d pending entries, want 2", len(pending))
	}

	byID := make(map[string][]string)
	for _, e := range pending {
		sort.Strings(e.Pending)
		byID[e.Event.ID] = e.Pending
	}

	if _, ok := byID["evt-0"]; ok {
		t.Errorf("evt-0 should have been fully resolved and absent from Pending()")
	}
	if got := byID["evt-1"]; len(got) != 1 || got[0] != "metronome" {
		t.Errorf("evt-1 pending providers = %v, want [metronome]", got)
	}
	if got := byID["evt-2"]; len(sortedCopy(got)) != 2 {
		t.Errorf("evt-2 pending providers = %v, want 2 providers", got)
	}
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

// TestAppendRejectedWhenBufferFull proves Append enforces WithMaxBytes:
// once the WAL's total on-disk size would exceed the cap, new Appends
// fail with ErrBufferFull rather than growing the WAL unbounded.
func TestAppendRejectedWhenBufferFull(t *testing.T) {
	dir := t.TempDir()

	// A cap far smaller than even one event's on-disk frame guarantees
	// the very first Append already exceeds it, without needing to know
	// the exact framing/JSON overhead.
	w, err := wal.Open(dir, wal.WithMaxBytes(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w.Close() }()

	err = w.Append(testEvent("evt-0"), []string{"orb"})
	if !errors.Is(err, wal.ErrBufferFull) {
		t.Fatalf("append with maxBytes=1: got %v, want ErrBufferFull", err)
	}
}

// TestBufferSpaceFreesAfterSegmentGC proves the backpressure is not
// permanent: once enough events are fully acked that a whole segment
// becomes garbage-collectable, TotalBytes drops and further Appends
// succeed again. Ack records themselves consume WAL space too (written
// to whichever segment is currently active), so sizes are probed
// empirically rather than assumed.
func TestBufferSpaceFreesAfterSegmentGC(t *testing.T) {
	probeDir := t.TempDir()
	probe, err := wal.Open(probeDir)
	if err != nil {
		t.Fatalf("open (probe): %v", err)
	}
	if err := probe.Append(testEvent("evt-0"), []string{"orb"}); err != nil {
		t.Fatalf("probe append: %v", err)
	}
	oneEventSize := probe.TotalBytes()
	if err := probe.Ack("evt-0", "orb", adapter.Ok); err != nil {
		t.Fatalf("probe ack: %v", err)
	}
	oneAckSize := probe.TotalBytes() - oneEventSize
	if err := probe.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if oneAckSize >= oneEventSize {
		t.Fatalf("test assumption violated: ack record (%d bytes) not smaller than event record (%d bytes)", oneAckSize, oneEventSize)
	}

	dir := t.TempDir()
	// Segment holds exactly 2 events before rotating; buffer fits 3
	// events' worth of data plus one ack record — enough to reject a 4th
	// event before any GC, but not after freeing one 2-event segment.
	maxSegmentBytes := oneEventSize*2 + 1
	maxBytes := oneEventSize*3 + oneAckSize
	w, err := wal.Open(dir, wal.WithMaxSegmentBytes(maxSegmentBytes), wal.WithMaxBytes(maxBytes))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w.Close() }()

	// evt-0 and evt-1 share one segment; evt-2 rotates into a second.
	if err := w.Append(testEvent("evt-0"), []string{"orb"}); err != nil {
		t.Fatalf("append evt-0: %v", err)
	}
	if err := w.Append(testEvent("evt-1"), []string{"orb"}); err != nil {
		t.Fatalf("append evt-1: %v", err)
	}
	if err := w.Append(testEvent("evt-2"), []string{"orb"}); err != nil {
		t.Fatalf("append evt-2: %v", err)
	}

	if err := w.Append(testEvent("evt-3"), []string{"orb"}); !errors.Is(err, wal.ErrBufferFull) {
		t.Fatalf("append evt-3 while full: got %v, want ErrBufferFull", err)
	}

	// Fully resolve both evt-0 and evt-1 — every entry originally written
	// to their shared segment — freeing it via GC.
	if err := w.Ack("evt-0", "orb", adapter.Ok); err != nil {
		t.Fatalf("ack evt-0: %v", err)
	}
	if err := w.Ack("evt-1", "orb", adapter.Ok); err != nil {
		t.Fatalf("ack evt-1: %v", err)
	}

	if err := w.Append(testEvent("evt-3"), []string{"orb"}); err != nil {
		t.Fatalf("append evt-3 after freeing space: %v", err)
	}
}

const (
	crashHelperEnv  = "TALLYD_WAL_CRASH_HELPER"
	crashDirEnv     = "TALLYD_WAL_CRASH_DIR"
	crashEventCount = 5
)

// TestCrashRecovery proves the WAL's core invariant survives a real process
// kill, not just a graceful shutdown: a helper subprocess durably appends
// several events (each Append only returns after fsync) and then SIGKILLs
// itself immediately, with no clean Close(). Reopening the WAL afterward
// must recover every event that was ever acked back to a caller.
func TestCrashRecovery(t *testing.T) {
	if os.Getenv(crashHelperEnv) == "1" {
		runCrashHelperProcess()
		return
	}

	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
	cmd.Env = append(os.Environ(), crashHelperEnv+"=1", crashDirEnv+"="+dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err == nil {
		t.Fatalf("expected crash helper subprocess to be killed, but it exited cleanly")
	}

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("reopen wal after simulated crash: %v", err)
	}
	defer func() { _ = w.Close() }()

	pending := w.Pending()
	if len(pending) != crashEventCount {
		t.Fatalf("after crash recovery: got %d pending entries, want %d", len(pending), crashEventCount)
	}

	seen := make(map[string]bool, len(pending))
	for _, e := range pending {
		seen[e.Event.ID] = true
	}
	for i := 0; i < crashEventCount; i++ {
		id := fmt.Sprintf("crash-evt-%d", i)
		if !seen[id] {
			t.Errorf("missing event %s after crash recovery", id)
		}
	}
}

// runCrashHelperProcess runs inside the killed subprocess.
func runCrashHelperProcess() {
	dir := os.Getenv(crashDirEnv)

	w, err := wal.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: open wal: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < crashEventCount; i++ {
		id := fmt.Sprintf("crash-evt-%d", i)
		if err := w.Append(testEvent(id), []string{"orb"}); err != nil {
			fmt.Fprintf(os.Stderr, "helper: append %s: %v\n", id, err)
			os.Exit(1)
		}
	}

	// Every Append above only returned once its record was fsync'd, so all
	// crashEventCount events are already durable. Kill this process right
	// now, with no graceful Close, to simulate a crash immediately after
	// the last durable write.
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)

	// Should never reach here.
	time.Sleep(time.Second)
}
