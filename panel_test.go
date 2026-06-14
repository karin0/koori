package main

import (
	"context"
	"math"
	"strings"
	"testing"
)

// stubSession satisfies Session for render tests; it is never asked to fetch
// because the render uses the skip-update (cached metrics) path.
type stubSession struct{}

func (stubSession) StreamJournal(context.Context, *Journal, *ServerState) error { return nil }
func (stubSession) FetchMetrics(context.Context) (string, error)                { return "", nil }
func (stubSession) RunCommand(context.Context, string) (string, error)          { return "", nil }
func (stubSession) Close()                                                      {}

func TestRenderServer(t *testing.T) {
	s := NewServerState("web-1", nil)
	s.session = stubSession{}
	s.metrics = parseMetrics("web-1", nil, scrape1)

	block := renderServer(context.Background(), s, true)
	out := strings.Join(block, "\n")

	for _, want := range []string{"🟢 <b>web-1</b>", "MEM", "<pre>", "Load: 1.50"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered block missing %q:\n%s", want, out)
		}
	}
	// scrape1 has SwapTotal==0, so the swap gauge must be omitted.
	if strings.Contains(out, "SWP") {
		t.Errorf("swap gauge should be omitted when SwapTotal==0:\n%s", out)
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

func TestBar(t *testing.T) {
	if got := bar(math.NaN()); got != strings.Repeat("─", barWidth) {
		t.Errorf("NaN bar = %q", got)
	}
	if got := []rune(bar(50.0)); len(got) != barWidth {
		t.Errorf("bar width = %d, want %d", len(got), barWidth)
	}
	if got := bar(100.0); got != strings.Repeat("█", barWidth) {
		t.Errorf("full bar = %q", got)
	}
}

func TestUptime(t *testing.T) {
	cases := map[float64]string{
		90000: "1d 1h", // days set => minutes suppressed
		3661:  "1h 1m", // days==0 => sub-hour minutes shown
		120:   "2m",
		45:    "45s",
	}
	for up, want := range cases {
		if got := uptime(up, 0); got != want {
			t.Errorf("uptime(%v) = %q, want %q", up, got, want)
		}
	}
}

func TestFmtSize(t *testing.T) {
	if got := fmtSize(1536); got != "1.5K" {
		t.Errorf("fmtSize(1536) = %q, want 1.5K", got)
	}
	if got := fmtSize(512); got != "512.0B" {
		t.Errorf("fmtSize(512) = %q, want 512.0B", got)
	}
}
