package pipeline_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/internal/pipeline"
)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// captureStdout temporarily redirects os.Stdout to a pipe so the stdout
// adapter's printed batches can be inspected by tests.
func captureStdout(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r)
		done <- buf.String()
	}()

	return func() string {
		os.Stdout = orig
		_ = w.Close()
		return <-done
	}
}

func TestPipelineEndToEnd(t *testing.T) {
	dir := t.TempDir()

	cfg := &pipeline.Config{
		Buffer: pipeline.BufferConfig{Dir: filepath.Join(dir, "wal")},
		Providers: map[string]pipeline.ProviderConfig{
			"stdouttest": {
				Type:  "stdout",
				Batch: pipeline.BatchConfig{Linger: pipeline.Duration{Duration: 10 * time.Millisecond}},
			},
		},
		Routing: pipeline.RoutingConfig{Default: []string{"stdouttest"}},
	}

	p, err := pipeline.Build(cfg)
	if err != nil {
		t.Fatalf("build pipeline: %v", err)
	}
	defer func() { _ = p.Close() }()

	restoreStdout := captureStdout(t)

	body, err := json.Marshal(map[string]any{
		"id":          "e2e-evt-1",
		"customer_id": "cust_1",
		"event_name":  "api_call",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /v1/events status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	// The event must be durable (in the WAL) immediately after the 2xx ack,
	// and then delivered to the stdout adapter and fully resolved shortly
	// after via the batcher's linger flush.
	waitFor(t, 2*time.Second, func() bool { return p.WAL.UnackedCount() == 0 })

	stdoutOutput := restoreStdout()
	if !strings.Contains(stdoutOutput, "e2e-evt-1") {
		t.Errorf("stdout adapter output missing event id; got:\n%s", stdoutOutput)
	}

	// /metrics should reflect the round trip.
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	p.Handler().ServeHTTP(metricsRec, metricsReq)

	metricsBody := metricsRec.Body.String()
	if !strings.Contains(metricsBody, "events_received_total 1") {
		t.Errorf("metrics missing events_received_total 1; got:\n%s", metricsBody)
	}
	if !strings.Contains(metricsBody, `events_acked_total{disposition="Ok",provider="stdouttest"} 1`) {
		t.Errorf("metrics missing events_acked_total Ok; got:\n%s", metricsBody)
	}
}
