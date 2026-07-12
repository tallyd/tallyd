package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// syncBuffer is a concurrency-safe io.Writer: the subprocess writes to it
// from its own OS-level stdout/stderr pipes (copied by goroutines exec.Cmd
// owns) while the test concurrently reads its contents.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

var listenAddrRE = regexp.MustCompile(`tallyd HTTP listening on (\S+)`)

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestBinaryEndToEnd builds the actual tallyd binary and drives it as a
// real OS process over real HTTP — not just the in-process pipeline
// package — to automatically catch regressions in main.go's own wiring
// (flag parsing, signal handling, graceful shutdown) that
// internal/pipeline's tests can't see, since they never touch main.go.
// This is the automated version of the manual verification (POST an
// event, check /metrics, send SIGTERM, confirm clean shutdown) done by
// hand throughout this project's development.
func TestBinaryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary integration test in -short mode")
	}

	dir := t.TempDir()
	binPath := filepath.Join(dir, "tallyd-under-test")

	build := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build tallyd: %v\n%s", err, out)
	}

	walDir := filepath.Join(dir, "wal")
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := fmt.Sprintf(`
listen:
  http: "127.0.0.1:0"
buffer:
  dir: %q
providers:
  stdout:
    type: stdout
    batch:
      linger: 50ms
routing:
  default: [stdout]
`, walDir)
	if err := os.WriteFile(configPath, []byte(configYAML), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var output syncBuffer
	cmd := exec.Command(binPath, "-config", configPath)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start tallyd: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	var addr string
	waitForCondition(t, 5*time.Second, func() bool {
		m := listenAddrRE.FindStringSubmatch(output.String())
		if m == nil {
			return false
		}
		addr = m[1]
		return true
	})
	baseURL := "http://" + addr

	evtBody, err := json.Marshal(map[string]any{
		"id":          "integration-evt-1",
		"customer_id": "cust_1",
		"event_name":  "api_call",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	resp, err := http.Post(baseURL+"/v1/events", "application/json", bytes.NewReader(evtBody))
	if err != nil {
		t.Fatalf("POST /v1/events: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v1/events status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return strings.Contains(output.String(), "integration-evt-1")
	})

	// Poll rather than a single-shot check: the ack (and its metric) is
	// recorded microseconds after the stdout print above, and
	// wal_unacked_entries is a periodically-refreshed gauge (updated
	// every couple seconds by RunGauges), not computed live at scrape
	// time, so a single request right after the print can race either.
	const wantAckMetric = `events_acked_total{disposition="Ok",provider="stdout"} 1`
	var lastMetricsBody string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/metrics")
		if err == nil {
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(resp.Body)
			_ = resp.Body.Close()
			lastMetricsBody = buf.String()
			if strings.Contains(lastMetricsBody, wantAckMetric) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(lastMetricsBody, wantAckMetric) {
		t.Fatalf("metrics never showed %q within 3s; last scrape:\n%s", wantAckMetric, lastMetricsBody)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("tallyd exited with error after SIGTERM: %v; output:\n%s", err, output.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("tallyd did not exit within 5s of SIGTERM; output:\n%s", output.String())
	}

	if !strings.Contains(output.String(), "shutting down") {
		t.Errorf("expected a graceful-shutdown log line; output:\n%s", output.String())
	}
}
