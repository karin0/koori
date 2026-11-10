package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

type stubSession struct{}

func (stubSession) StreamJournal(context.Context, *Journal, *ServerState) error { return nil }
func (stubSession) FetchMetrics(context.Context) (string, error)                { return "", nil }
func (stubSession) RunCommand(context.Context, string) (string, error)          { return "", nil }
func (stubSession) Close()                                                      {}

// liveServer has a session and metrics with rates, as after its second scrape.
func liveServer(name string) *ServerState {
	s := NewServerState(name, nil)
	s.session = stubSession{}
	s.metrics = parseMetrics(name, parseMetrics(name, nil, scrape1), scrape2)
	return s
}

func decodePanel(t *testing.T, servers []*ServerState) []map[string]any {
	t.Helper()
	data, err := panelData(servers)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > widgetDataLimit {
		t.Fatalf("panel data is %d bytes, over %d", len(data), widgetDataLimit)
	}
	var out []map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("panel data %s: %v", data, err)
	}
	return out
}

func TestPanelDataFitsThreeServers(t *testing.T) {
	var servers []*ServerState
	for _, name := range []string{"server-one", "server-two", "server-three"} {
		s := NewServerState(name, nil)
		s.session = stubSession{}
		// Every field at a large but real magnitude, with an alert.
		s.metrics = &MetricsSnapshot{
			Timestamp: 1762850000.123, ScrapeTime: 0.012345, FetchTime: 0.045678,
			CPUPct: 12.345, CPUMaxPct: 98.765, CPUCount: 16,
			MemUsed: 12345678901, MemTotal: 34359738368,
			SwapUsed: 1234567890, SwapTotal: 8589934592,
			FSUsed: 123456789012, FSTotal: 512110190592,
			Rx: 1234567890123, Tx: 987654321098, RxBps: 12345678.912, TxBps: 1234567.891,
			Temp: 67.891, Load1: 12.345, BootTime: 1760000000,
			Alerts: []string{"Conntrack: 52429/65536"},
		}
		for range historyLen {
			s.cpuHistory = append(s.cpuHistory, 55.5)
		}
		servers = append(servers, s)
	}
	if got := decodePanel(t, servers); len(got) != len(servers) {
		t.Fatalf("panel data has %d servers, want %d", len(got), len(servers))
	}
}

func TestPanelDataDropsTrailingServers(t *testing.T) {
	var servers []*ServerState
	for i := range 20 {
		servers = append(servers, liveServer(fmt.Sprintf("server-%d", i)))
	}
	got := decodePanel(t, servers)
	if len(got) == 0 || len(got) == len(servers) || got[0]["n"] != "server-0" {
		t.Fatalf("panel data kept %d of %d servers, first %v", len(got), len(servers), got[0]["n"])
	}
}

func TestPanelDataStates(t *testing.T) {
	first := NewServerState("first", nil)
	first.session = stubSession{}
	first.metrics = parseMetrics("first", nil, scrape1) // no rates yet

	down := liveServer("down")
	down.session = nil
	down.err = "dial tcp: connection refused"

	failing := liveServer("failing")
	failing.fetchErr = "scrape timed out"

	got := decodePanel(t, []*ServerState{first, down, failing})

	if c := got[0]["c"].([]any); c[0] != nil || c[1] != nil {
		t.Errorf("CPU without a previous scrape should be null: %v", got[0])
	}
	if got[0]["t"] == nil || got[0]["d"] != nil {
		t.Errorf("live server should carry metrics and not be down: %v", got[0])
	}
	if got[1]["d"] != true || got[1]["e"] != down.err || got[1]["s"] == nil || got[1]["t"] != nil {
		t.Errorf("down server should carry its error and last scrape time only: %v", got[1])
	}
	if got[2]["e"] != failing.fetchErr || got[2]["t"] == nil {
		t.Errorf("failing server should carry its scrape error and last metrics: %v", got[2])
	}
}

func TestPanelHTMLSize(t *testing.T) {
	if len(panelHTML) > 32768 {
		t.Errorf("panel.html is %d bytes, over 32768", len(panelHTML))
	}
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(panelHTML)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() > 4096 {
		t.Errorf("panel.html compresses to %d bytes, over 4096", buf.Len())
	}
}

func TestRenderJournal(t *testing.T) {
	j := NewJournal(20)
	if got := renderJournal(j, false); !strings.Contains(got, "no journal lines") {
		t.Errorf("empty journal = %q", got)
	}
	for i := range 10 {
		j.Append(JournalEntry{Message: fmt.Sprintf("line %d", i)})
	}
	collapsed := renderJournal(j, false)
	if !strings.HasPrefix(collapsed, "<pre>line 9\n") || strings.Contains(collapsed, "line 2") {
		t.Errorf("collapsed journal should show the newest %d lines newest first: %q", journalLines, collapsed)
	}
	expanded := renderJournal(j, true)
	if !strings.HasPrefix(expanded, "<pre>line 0\n") || !strings.HasSuffix(expanded, "line 9</pre>") {
		t.Errorf("expanded journal should show every line oldest first: %q", expanded)
	}
}

func TestEffLen(t *testing.T) {
	// Tags drop out; &lt; decodes to a single char.
	if got := effLen("<b>hi</b> &lt;x&gt;"); got != 6 { // "hi <x>"
		t.Errorf("effLen = %d, want 6", got)
	}
}

func TestFitStream(t *testing.T) {
	lines := []string{"aaa", "bbb", "ccc"}
	out, eff := fitStream(lines, 0)
	if len(out) != 3 || eff != 3+3+3+2 { // 3 lines + 2 joining newlines
		t.Errorf("out=%v eff=%d", out, eff)
	}

	// Starting at the limit yields nothing.
	if out, _ := fitStream(lines, tgMessageLimit); out != nil {
		t.Errorf("expected nothing past limit, got %v", out)
	}
}

func TestNumMarshal(t *testing.T) {
	cases := map[float64]string{
		12.345:        "12.3",
		0.5234:        "0.523",
		100:           "100",
		1234:          "1.23e3",
		34359738368:   "3.44e10",
		1234567890123: "1.23e12",
		0:             "0",
	}
	for v, want := range cases {
		got, err := json.Marshal(num(v))
		if err != nil || string(got) != want {
			t.Errorf("num(%v) = %s %v, want %s", v, got, err, want)
		}
		var back float64
		if err := json.Unmarshal(got, &back); err != nil {
			t.Errorf("num(%v) = %s is not a JSON number: %v", v, got, err)
		}
	}
}

func TestEncodeHistory(t *testing.T) {
	if got := encodeHistory([]float64{0, 50, 100, 120, -1}); got != "0izz0" {
		t.Errorf("encodeHistory = %q, want 0izz0", got)
	}
}

func TestRecordCPU(t *testing.T) {
	s := NewServerState("web-1", nil)
	record := func(ts, cpu float64) { s.recordCPU(&MetricsSnapshot{Timestamp: ts, CPUPct: cpu}) }

	record(1000, math.NaN()) // the first scrape has no rate
	for ts := 1000.0; ts <= 1030; ts += 5 {
		record(ts, 40)
	}
	record(1035, 60) // opens the next step
	if len(s.cpuHistory) != 1 || s.cpuHistory[0] != 40 {
		t.Fatalf("history after one step = %v, want [40]", s.cpuHistory)
	}

	for ts := 1040.0; ts < 1040+30*historyLen*2; ts += 5 {
		record(ts, 10)
	}
	if len(s.cpuHistory) != historyLen {
		t.Errorf("history keeps %d points, want %d", len(s.cpuHistory), historyLen)
	}
}
