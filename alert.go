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

func (a *alertBuilder) pushTime(t time.Time) {
	date := t.Format("2006-01-02 15:04:05")
	a.lines = append(a.lines, fmt.Sprintf(`⏱️ <tg-time unix="%d" format="wdT">%s</tg-time>`, t.Unix(), date))
	a.eff += utf8.RuneCountInString(date) + 2
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
	a.lines = append(a.lines, fmt.Sprintf("%s <b>%s</b>: %s", symbol, html.EscapeString(a.server.Name), html.EscapeString(msg)))
	a.eff += utf8.RuneCountInString(a.server.Name) + utf8.RuneCountInString(msg) + utf8.RuneCountInString(symbol) + 3
}

func (a *alertBuilder) alert(msg, key string) { a.emit(alertEmoji, msg, key) }
func (a *alertBuilder) recover(msg string)    { a.emit(recoverEmoji, msg, "") }

func checkServerAlert(ctx context.Context, b *bot.Bot, chatID int64, s *ServerState, journal *Journal, now time.Time) {
	sess, errMsg, m := s.snapshot()

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

		if memAvail := m.MemTotal - m.MemUsed; m.MemTotal > 0 && memAvail <= memAvailLow {
			a.alert(fmt.Sprintf("available memory low (%d MiB)", memAvail>>20), "mem")
		} else if a.prevKeys["mem"] {
			a.recover(fmt.Sprintf("available memory recovered (%d MiB)", memAvail>>20))
		}

		if fsAvail := m.FSTotal - m.FSUsed; m.FSTotal > 0 && fsAvail <= fsAvailLow {
			a.alert(fmt.Sprintf("available disk space low (%.1f GiB)", float64(fsAvail)/(1<<30)), "fs")
		} else if a.prevKeys["fs"] {
			a.recover(fmt.Sprintf("available disk space recovered (%.1f GiB)", float64(fsAvail)/(1<<30)))
		}

		if !math.IsNaN(m.Temp) {
			if m.Temp >= tempHighC {
				a.alert(fmt.Sprintf("%s: %.1f°C", orDefault(m.TempChip, "<unknown sensor>"), m.Temp), "temp")
			} else if a.prevKeys["temp"] {
				a.recover(fmt.Sprintf("%s: %.1f°C", orDefault(m.TempChip, "<unknown sensor>"), m.Temp))
			}
		}

		if len(m.Alerts) > 0 {
			a.alert(strings.Join(m.Alerts, " | "), "sys")
		} else if a.prevKeys["sys"] {
			a.recover("alerts cleared")
		}

		if len(m.CntAlerts) > 0 {
			a.alert(strings.Join(m.CntAlerts, " | "), "")
		}

		if len(a.lines) != before {
			a.pushTime(time.Unix(int64(m.Timestamp), 0))
		}
	}

	s.alertKeys = a.newKeys

	resend := len(a.newKeys) > 0 && now.Sub(s.alertAt) >= alertResend
	if !a.changed && !resend {
		return
	}

	if a.eff < tgMessageLimit {
		if sess != nil {
			if out, err := sess.RunCommand(ctx, psCommand); err != nil {
				slog.Warn("fetch busiest processes failed", "server", s.Name, "err", err)
			} else {
				psLines, eff := fitStream(headLines(out, maxPsLines), a.eff)
				a.eff = eff
				if len(psLines) > 0 {
					a.lines = append(a.lines, quoteLines(psLines))
				}
			}
		}
		if jrnl, _ := fitStream(journal.messagesNewest(s), a.eff); len(jrnl) > 0 {
			a.lines = append(a.lines, quoteLines(jrnl))
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
		s.alertAt = now
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
