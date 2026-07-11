package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/metrics"
)

func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func TestMetricsRecordAndScrape(t *testing.T) {
	m := metrics.New()

	m.RecordEventsReceived(3)
	m.RecordAck("orb", adapter.Ok)
	m.RecordAck("orb", adapter.DeadLetter)
	m.ObserveFlushLatency("orb", 250*time.Millisecond)
	m.RecordSendError("metronome", adapter.Retry)
	m.SetWALUnacked(7)
	m.SetDLQDepth("orb", 2)

	body := scrape(t, m)

	wantSubstrings := []string{
		"events_received_total 3",
		`events_acked_total{disposition="Ok",provider="orb"} 1`,
		`events_acked_total{disposition="DeadLetter",provider="orb"} 1`,
		`provider_send_errors_total{disposition="Retry",provider="metronome"} 1`,
		"wal_unacked_entries 7",
		`dlq_depth{provider="orb"} 2`,
		"provider_flush_latency_seconds",
	}

	for _, want := range wantSubstrings {
		if !strings.Contains(body, want) {
			t.Errorf("scraped output missing %q\nfull output:\n%s", want, body)
		}
	}
}
