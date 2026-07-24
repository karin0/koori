package main

import (
	"strings"
	"testing"
	"time"
)

func newTestBuilder(prev map[string]bool) *alertBuilder {
	return &alertBuilder{
		server:   NewServerState("web-1", nil),
		prevKeys: prev,
		newKeys:  map[string]bool{},
	}
}

func TestAlertBuilderChanged(t *testing.T) {
	a := newTestBuilder(map[string]bool{"mem": true})
	a.alert("still low", "mem")
	if a.changed {
		t.Error("re-emitting an active key must not mark changed")
	}
	a.alert("OOM: 1 (+1)", "")
	if !a.changed {
		t.Error("keyless alert must mark changed")
	}

	r := newTestBuilder(map[string]bool{"mem": true})
	r.recover("recovered")
	if !r.changed || r.newKeys["mem"] {
		t.Errorf("recover: changed=%v newKeys=%v", r.changed, r.newKeys)
	}
}

func TestAlertBuilderHold(t *testing.T) {
	a := newTestBuilder(map[string]bool{"mem": true})
	a.hold("mem")
	a.hold("fs") // not previously active: no-op
	if !a.newKeys["mem"] || a.newKeys["fs"] || a.changed || len(a.lines) != 0 {
		t.Errorf("hold: newKeys=%v changed=%v lines=%v", a.newKeys, a.changed, a.lines)
	}
}

// The hand-accumulated eff must match what Telegram counts after parsing,
// including the newlines joining the lines.
func TestAlertBuilderEff(t *testing.T) {
	a := newTestBuilder(nil)
	a.alert("down: boom", "down")
	a.pushTime(time.Unix(0, 0))
	a.recover("back online")

	text := strings.Join(a.lines, "\n")
	if got := effLen(text); got != a.eff {
		t.Errorf("eff = %d, want %d (%q)", a.eff, got, text)
	}
}
