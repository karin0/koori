package main

import (
	"context"
	"fmt"
	"html"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	alertResend  = 10 * time.Minute
	memAvailLow  = 200 << 20 // 200 MiB
	fsAvailLow   = 2 << 30   // 2 GiB
	tempHighC    = 80.0
	tempCritC    = 90.0
	tempLowC     = 75.0
	tempSustain  = 30 * time.Second
	psCommand    = "ps -eo pcpu,pmem,args --sort=-pcpu"
	maxPsLines   = 10
	recoverEmoji = "✅"
	alertEmoji   = "⚠️"
)

// alertBuilder accumulates the lines of one server's alert message while
// tracking which keyed conditions are active and whether anything changed
// versus the previous cycle.
type alertBuilder struct {
	server   *ServerState
	prevKeys map[string]bool
	newKeys  map[string]bool
	lines    []string
	eff      int
	changed  bool
}

// push appends a line whose effective (post-parse) length is eff, accounting
// for the newline that joins it to the previous line.
func (a *alertBuilder) push(line string, eff int) {
	if len(a.lines) > 0 {
		eff++
	}
	a.lines = append(a.lines, line)
	a.eff += eff
}

func (a *alertBuilder) pushTime(t time.Time) {
	date := t.Format("2006-01-02 15:04:05")
	a.push(fmt.Sprintf(`⏱️ <tg-time unix="%d" format="wdT">%s</tg-time>`, t.Unix(), date),
		utf8.RuneCountInString(date)+3)
}

// emit records a line. A non-empty key marks a recoverable condition: it stays
// "unchanged" while the same key was already active last cycle.
func (a *alertBuilder) emit(symbol, msg, key string) {
	if key == "" {
		a.changed = true
	} else {
		a.newKeys[key] = true
		if !a.changed {
			a.changed = !a.prevKeys[key]
		}
	}
	a.push(fmt.Sprintf("%s <b>%s</b>: %s", symbol, html.EscapeString(a.server.Name), html.EscapeString(msg)),
		utf8.RuneCountInString(a.server.Name)+utf8.RuneCountInString(msg)+utf8.RuneCountInString(symbol)+3)
}

func (a *alertBuilder) alert(msg, key string) { a.emit(alertEmoji, msg, key) }
func (a *alertBuilder) recover(msg string)    { a.emit(recoverEmoji, msg, "") }

// hold carries a keyed condition forward without alerting or recovering when
// the data needed to evaluate it is absent from the current snapshot.
func (a *alertBuilder) hold(key string) {
	if a.prevKeys[key] {
		a.newKeys[key] = true
	}
}

func checkServerAlert(ctx context.Context, b *bot.Bot, chatID int64, s *ServerState, journal *Journal, now time.Time) {
	sess, errMsg, m := s.snapshot()
	pending := s.pendingAlerts()

	a := &alertBuilder{
		server:   s,
		prevKeys: s.alertKeys,
		newKeys:  map[string]bool{},
	}

	if sess == nil {
		a.alert("down: "+orDefault(errMsg, "<unknown error>"), "down")
		a.pushTime(now)
	} else if a.prevKeys["down"] {
		a.recover("back online")
		a.pushTime(now)
	}

	if m != nil {
		before := len(a.lines)

		memAvail := m.MemTotal - m.MemUsed
		switch {
		case m.MemTotal == 0:
			a.hold("mem")
		case memAvail <= memAvailLow:
			a.alert(fmt.Sprintf("available memory low (%d MiB)", memAvail>>20), "mem")
		case a.prevKeys["mem"]:
			a.recover(fmt.Sprintf("available memory recovered (%d MiB)", memAvail>>20))
		}

		fsAvail := m.FSTotal - m.FSUsed
		switch {
		case m.FSTotal == 0:
			a.hold("fs")
		case fsAvail <= fsAvailLow:
			a.alert(fmt.Sprintf("available disk space low (%.1f GiB)", float64(fsAvail)/(1<<30)), "fs")
		case a.prevKeys["fs"]:
			a.recover(fmt.Sprintf("available disk space recovered (%.1f GiB)", float64(fsAvail)/(1<<30)))
		}

		tempMsg := fmt.Sprintf("%s: %.1f°C", orDefault(m.TempChip, "<unknown sensor>"), m.Temp)

		// Warn only after tempSustain of continuous heat above tempHighC, then
		// recover through the tempLowC dead band.
		switch {
		case math.IsNaN(m.Temp):
			a.hold("temp")
		case m.Temp >= tempHighC:
			if s.tempHotSince.IsZero() {
				s.tempHotSince = now
			}
			if now.Sub(s.tempHotSince) >= tempSustain {
				a.alert(tempMsg, "temp")
			} else {
				a.hold("temp")
			}
		case m.Temp >= tempLowC:
			s.tempHotSince = time.Time{}
			a.hold("temp")
		default:
			s.tempHotSince = time.Time{}
			if a.prevKeys["temp"] {
				a.recover(tempMsg)
			}
		}

		// Critical temperature alerts at once, independent of the sustain delay,
		// and latches down to tempLowC.
		switch {
		case math.IsNaN(m.Temp):
			a.hold("tempcrit")
		case m.Temp >= tempCritC:
			a.alert("critical "+tempMsg, "tempcrit")
		case m.Temp >= tempLowC:
			a.hold("tempcrit")
		default:
			if a.prevKeys["tempcrit"] {
				a.recover("below critical, " + tempMsg)
			}
		}

		if len(m.Alerts) > 0 {
			a.alert(strings.Join(m.Alerts, " | "), "sys")
		} else if a.prevKeys["sys"] {
			a.recover("alerts cleared")
		}

		if len(pending) > 0 {
			a.alert(strings.Join(pending, " | "), "")
		}

		if len(a.lines) != before {
			a.pushTime(time.Unix(int64(m.Timestamp), 0))
		}
	}

	// held keys can leave newKeys non-empty with nothing to say; never send then
	resend := len(a.newKeys) > 0 && now.Sub(s.alertAt) >= alertResend
	if len(a.lines) == 0 || (!a.changed && !resend) {
		return
	}

	if a.eff < tgMessageLimit {
		if sess != nil {
			if out, err := sess.RunCommand(ctx, psCommand); err != nil {
				slog.Warn("fetch busiest processes failed", "server", s.Name, "err", err)
			} else if psLines, eff := fitStream(headLines(out, maxPsLines), a.eff+1); len(psLines) > 0 {
				a.lines = append(a.lines, quoteLines(psLines))
				a.eff = eff
			}
		}
		if jrnl, eff := fitStream(journal.messagesNewest(s, math.MaxInt), a.eff+1); len(jrnl) > 0 {
			a.lines = append(a.lines, quoteLines(jrnl))
			a.eff = eff
		}
	}

	text := strings.Join(a.lines, "\n")
	parseMode := models.ParseModeHTML
	if a.eff > tgMessageLimit {
		slog.Error("alert too long", "server", s.Name, "eff", a.eff, "limit", tgMessageLimit)
		text = truncateRunes(text, tgMessageLimit, "…")
		parseMode = ""
	}

	if _, err := b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:             chatID,
		Text:               text,
		ParseMode:          parseMode,
		LinkPreviewOptions: disabledPreview,
	}); err != nil {
		slog.Error("alert send failed", "server", s.Name, "err", err)
	} else {
		// Commit only after delivery so a failed send retries next cycle.
		s.alertAt = now
		s.alertKeys = a.newKeys
		s.dropPendingAlerts(len(pending))
	}
}

func checkAlerts(ctx context.Context, b *bot.Bot, chatID int64, servers []*ServerState, journal *Journal) {
	now := time.Now()
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() {
			checkServerAlert(ctx, b, chatID, s, journal, now)
		})
	}
	wg.Wait()
}

func headLines(s string, n int) []string {
	var lines []string
	for line := range strings.Lines(s) {
		if len(lines) == n {
			break
		}
		lines = append(lines, line)
	}
	return lines
}
