package wal_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/wal"
)

// fixedTestTimestamp is used instead of time.Now() so every testEvent has
// an identical on-disk frame size. time.Time's JSON encoding trims
// trailing zero fractional digits (RFC3339Nano's `.999999999`), so
// real timestamps vary in encoded length by several bytes depending on
// the nanosecond value at creation — exactly the kind of variance that
// breaks byte-size arithmetic in tests like
// TestBufferSpaceFreesAfterSegmentGC, which assume every event frame is
// the same size.
var fixedTestTimestamp = time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)

func testEvent(id string) adapter.Event {
	return adapter.Event{
		ID:         id,
		CustomerID: "cust_1",
		EventName:  "api_call",
		Timestamp:  fixedTestTimestamp,
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

// countSegmentFiles returns how many *.wal segment files exist on disk in
// dir — the observable that segment GC acts on.
func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".wal" {
			n++
		}
	}
	return n
}

// probeEventAckSizes opens a throwaway WAL to measure the on-disk frame
// size of one event and one ack, so size-sensitive tests can tune
// WithMaxSegmentBytes precisely (see TestBufferSpaceFreesAfterSegmentGC
// for the same technique).
func probeEventAckSizes(t *testing.T) (eventSize, ackSize int64) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("probe open: %v", err)
	}
	if err := w.Append(testEvent("probe"), []string{"orb"}); err != nil {
		t.Fatalf("probe append: %v", err)
	}
	eventSize = w.TotalBytes()
	if err := w.Ack("probe", "orb", adapter.Ok); err != nil {
		t.Fatalf("probe ack: %v", err)
	}
	ackSize = w.TotalBytes() - eventSize
	if err := w.Close(); err != nil {
		t.Fatalf("probe close: %v", err)
	}
	if ackSize >= eventSize {
		t.Fatalf("test assumption violated: ack (%d) not smaller than event (%d)", ackSize, eventSize)
	}
	return eventSize, ackSize
}

// TestResolvedSegmentsDoNotLeak covers the defect where a segment whose
// refcount reached zero while it was still the active segment was never
// deleted once it rotated out of active position (the old delete-on-1->0
// logic only fired for the segment being decremented, and skipped the
// active one). Appending and fully acking one event per segment, forcing a
// rotation each time, must not accumulate resolved segment files on disk.
func TestResolvedSegmentsDoNotLeak(t *testing.T) {
	eventSize, ackSize := probeEventAckSizes(t)
	dir := t.TempDir()
	// A segment holds exactly one event plus one ack, so the next event
	// always rotates — driving each just-resolved segment out of active
	// position immediately after it hits refcount zero.
	w, err := wal.Open(dir, wal.WithMaxSegmentBytes(eventSize+ackSize))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w.Close() }()

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("evt-%d", i)
		if err := w.Append(testEvent(id), []string{"orb"}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
		if err := w.Ack(id, "orb", adapter.Ok); err != nil {
			t.Fatalf("ack %s: %v", id, err)
		}
	}

	if got := w.UnackedCount(); got != 0 {
		t.Fatalf("UnackedCount() = %d, want 0 (everything acked)", got)
	}
	// Only the active segment should remain; every resolved one is GC'd.
	if n := countSegmentFiles(t, dir); n != 1 {
		t.Fatalf("segment files on disk = %d, want 1 (resolved segments leaked)", n)
	}
}

// TestNoResurrectionWhenAckOutlivesEventSegment covers the defect where
// out-of-order (arbitrary zero-ref) segment deletion could drop an ack
// whose event record survived in an older segment, resurrecting that event
// on a later replay. The fix deletes segments strictly oldest-first, so an
// ack and the record it resolves are only ever freed together.
func TestNoResurrectionWhenAckOutlivesEventSegment(t *testing.T) {
	eventSize, _ := probeEventAckSizes(t)
	dir := t.TempDir()
	// Two events per segment before rotating.
	w, err := wal.Open(dir, wal.WithMaxSegmentBytes(eventSize*2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Layout the defect needs: A's record shares seg0 with a still-pending
	// B (keeping seg0 alive), while A's ack lands in a newer segment that
	// itself becomes zero-ref and non-active.
	//   seg0: [A, B]          seg1: [C, ackA]      seg2: [D, ackC] (active)
	for _, id := range []string{"evt-A", "evt-B"} { // seg0
		if err := w.Append(testEvent(id), []string{"orb"}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	if err := w.Append(testEvent("evt-C"), []string{"orb"}); err != nil { // rotates -> seg1
		t.Fatalf("append C: %v", err)
	}
	if err := w.Ack("evt-A", "orb", adapter.Ok); err != nil { // ackA -> seg1
		t.Fatalf("ack A: %v", err)
	}
	if err := w.Append(testEvent("evt-D"), []string{"orb"}); err != nil { // rotates -> seg2
		t.Fatalf("append D: %v", err)
	}
	if err := w.Ack("evt-C", "orb", adapter.Ok); err != nil { // ackC -> seg2; seg1 now zero-ref, non-active
		t.Fatalf("ack C: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Two restarts: the resurrection only manifests on the replay *after*
	// the one that (wrongly) deletes the ack-bearing segment.
	for restart := 1; restart <= 2; restart++ {
		w, err = wal.Open(dir)
		if err != nil {
			t.Fatalf("reopen %d: %v", restart, err)
		}
		pendingIDs := map[string]bool{}
		for _, e := range w.Pending() {
			pendingIDs[e.Event.ID] = true
		}
		if pendingIDs["evt-A"] {
			t.Fatalf("restart %d: evt-A resurrected (fully acked before, now pending again)", restart)
		}
		if !pendingIDs["evt-B"] {
			t.Fatalf("restart %d: evt-B should still be pending, got %v", restart, pendingIDs)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close %d: %v", restart, err)
		}
	}
}

// TestDuplicateAppendReleasesOldSegmentRef covers the defect where
// re-appending an event that's already in the index (the whole-request
// retry the receiver documents as the correct recovery from a partial
// append) overwrote the index entry without releasing the prior entry's
// segment refcount, pinning that segment from GC until restart.
func TestDuplicateAppendReleasesOldSegmentRef(t *testing.T) {
	eventSize, _ := probeEventAckSizes(t)
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.WithMaxSegmentBytes(eventSize*2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w.Close() }()

	// A and its duplicate both land in seg0; B then rotates seg0 out.
	if err := w.Append(testEvent("evt-A"), []string{"orb"}); err != nil {
		t.Fatalf("append A: %v", err)
	}
	if err := w.Append(testEvent("evt-A"), []string{"orb"}); err != nil { // duplicate ID (retry)
		t.Fatalf("re-append A: %v", err)
	}
	if err := w.Append(testEvent("evt-B"), []string{"orb"}); err != nil { // rotates -> seg1
		t.Fatalf("append B: %v", err)
	}
	if n := countSegmentFiles(t, dir); n != 2 {
		t.Fatalf("before ack: segment files = %d, want 2", n)
	}

	// Resolving A must drop seg0 to refcount 0 and GC it. With the leaked
	// duplicate refcount, seg0 would stay pinned at refcount 1 and survive.
	if err := w.Ack("evt-A", "orb", adapter.Ok); err != nil {
		t.Fatalf("ack A: %v", err)
	}
	if n := countSegmentFiles(t, dir); n != 1 {
		t.Fatalf("after acking A: segment files = %d, want 1 (seg0 pinned by leaked duplicate refcount)", n)
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
