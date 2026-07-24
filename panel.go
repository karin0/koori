package main

import (
	"context"
	"fmt"
	"html"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	tgMessageLimit = 4096 // Telegram MAX_TEXT_LENGTH, post-parse
	lineTrim       = 180
	barWidth       = 10
)

var partials = []rune("▏▎▍▌▋▊▉")

var tagRE = regexp.MustCompile(`<[^>]+>`)

// effLen counts characters as Telegram does after parsing: tags are dropped and
// HTML entities decode to a single character.
func effLen(s string) int {
	return utf8.RuneCountInString(html.UnescapeString(tagRE.ReplaceAllString(s, "")))
}

func fmtSize(x float64) string {
	for _, unit := range []string{"B", "K", "M", "G", "T"} {
		if x < 1024 {
			return fmt.Sprintf("%.1f%s", x, unit)
		}
		x /= 1024
	}
	return fmt.Sprintf("%.1fP", x)
}

func fmtBps(x float64) string {
	if math.IsNaN(x) {
		return "  —  "
	}
	for _, unit := range []string{"B/s", "K/s", "M/s", "G/s", "T/s"} {
		if x < 1024 {
			return fmt.Sprintf("%5.1f%s", x, unit)
		}
		x /= 1024
	}
	return fmt.Sprintf("%5.1fP/s", x)
}

func fmtDuration(t float64) string {
	switch {
	case t < 1e-3:
		return fmt.Sprintf("%.1fµs", t*1e6)
	case t < 1:
		return fmt.Sprintf("%.1fms", t*1e3)
	default:
		return fmt.Sprintf("%.1fs", t)
	}
}

func clampPct(v float64) float64 { return max(0, min(100, v)) }

func bar(pct float64) string {
	if math.IsNaN(pct) {
		return strings.Repeat("─", barWidth)
	}
	eighths := int(clampPct(pct)/100*barWidth*8 + 0.5)
	full, rem := eighths/8, eighths%8
	var b strings.Builder
	b.WriteString(strings.Repeat("█", full))
	if rem > 0 && full < barWidth {
		b.WriteRune(partials[rem-1])
		full++
	}
	b.WriteString(strings.Repeat("░", barWidth-full))
	return b.String()
}

func barDual(avg, peak float64) string {
	if math.IsNaN(avg) {
		return strings.Repeat("─", barWidth)
	}
	avgC := clampPct(avg)
	peakC := avgC
	if !math.IsNaN(peak) {
		peakC = max(avgC, min(100, peak))
	}

	eighths := int(avgC/100*barWidth*8 + 0.5)
	full, rem := eighths/8, eighths%8
	var b strings.Builder
	b.WriteString(strings.Repeat("█", full))
	used := full
	if rem > 0 && full < barWidth {
		b.WriteRune(partials[rem-1])
		used++
	}
	if peakChars := int(peakC/100*barWidth + 0.5); peakChars > used {
		b.WriteString(strings.Repeat("▒", peakChars-used))
		used = peakChars
	}
	b.WriteString(strings.Repeat("░", barWidth-used))
	return b.String()
}

func pctStr(pct float64) string {
	if math.IsNaN(pct) {
		return "  —  "
	}
	return fmt.Sprintf("%4.1f%%", pct)
}

func gauge(label string, pct float64, used, total int64) string {
	row := fmt.Sprintf("%-3s %s %s", label, bar(pct), pctStr(pct))
	if total > 0 {
		row += fmt.Sprintf(" %6s / %6s", fmtSize(float64(used)), fmtSize(float64(total)))
	}
	return row
}

func cpuRow(avg, peak float64, count int) string {
	cores := ""
	if count > 0 {
		cores = fmt.Sprintf(" / %6s", fmt.Sprintf("%dx", count))
	}
	peakS := ""
	if !math.IsNaN(peak) {
		peakS = fmt.Sprintf(" %6s", strings.TrimSpace(pctStr(peak)))
	}
	return fmt.Sprintf("CPU %s %s%s%s", barDual(avg, peak), pctStr(avg), peakS, cores)
}

func trimLine(line string) string {
	return truncateRunes(strings.TrimRight(line, " \t\r\n"), lineTrim, "…")
}

// truncateRunes returns a string truncated to at most limit runes (including the suffix length).
// It avoids heap allocations by checking the rune count first and scanning bytes on the stack.
func truncateRunes(s string, limit int, suffix string) string {
	runeLen := utf8.RuneCountInString(s)
	if runeLen <= limit {
		return s
	}
	n := limit - utf8.RuneCountInString(suffix)
	if n <= 0 {
		return suffix
	}
	if len(s) == runeLen {
		return s[:n] + suffix
	}
	count := 0
	for idx := range s {
		if count == n {
			return s[:idx] + suffix
		}
		count++
	}
	return s + suffix
}

// hm formats a time as H:MM:SS with the leading zero stripped.
func hm(t time.Time) string {
	return strings.TrimLeft(t.Format("15:04:05"), "0")
}

func renderServer(ctx context.Context, s *ServerState, skipUpdate bool) []string {
	name := "<b>" + html.EscapeString(s.Name) + "</b>"
	sess, errMsg, m := s.snapshot()
	if sess == nil {
		return []string{"🔴 " + name, "offline: " + html.EscapeString(orDefault(errMsg, "<unknown>"))}
	}

	header := ""
	dt := ""
	if skipUpdate && m != nil {
		// keep last snapshot, no fetch
	} else {
		t0 := time.Now()
		fresh, err := s.refreshMetrics(ctx, sess)
		elapsed := time.Since(t0).Seconds()
		if err != nil {
			slog.Error("fetch metrics failed", "server", s.Name, "err", err)
			errLine := "error: " + html.EscapeString(err.Error())
			if m == nil {
				return []string{"🟡 " + name, errLine}
			}
			header = "🟡 " + name + "\n" + errLine
		} else {
			m = fresh
		}
		dt = fmtDuration(elapsed) + ", "
	}

	if header == "" {
		date := hm(time.Unix(int64(m.Timestamp), 0))
		header = fmt.Sprintf("🟢 %s  (%s%s, <tg-time unix=\"%d\" format=\"r\">%s</tg-time>)",
			name, dt, fmtDuration(m.ScrapeTime), int64(m.Timestamp), date)
	}

	var rows []string
	rows = append(rows, cpuRow(m.CPUPct, m.CPUMaxPct, m.CPUCount))
	rows = append(rows, gauge("MEM", pctOf(m.MemUsed, m.MemTotal), m.MemUsed, m.MemTotal))
	if m.SwapTotal > 0 {
		rows = append(rows, gauge("SWP", pctOf(m.SwapUsed, m.SwapTotal), m.SwapUsed, m.SwapTotal))
	}
	if m.FSMount != "" {
		rows = append(rows, gauge(m.FSMount, pctOf(m.FSUsed, m.FSTotal), m.FSUsed, m.FSTotal))
	}
	rows = append(rows, fmt.Sprintf("NET ↓%s (%s) ↑%s (%s)",
		fmtBps(m.RxBps), fmtSize(m.Rx), fmtBps(m.TxBps), fmtSize(m.Tx)))

	rows = append(rows, fmt.Sprintf("Load: %.2f | Up: %s", m.Load1, uptime(m.Timestamp, m.BootTime)))
	if !math.IsNaN(m.Temp) {
		rows[len(rows)-1] += fmt.Sprintf(" | %.1f°C", m.Temp)
	}
	if len(m.Alerts) > 0 {
		rows = append(rows, "⚠️ "+strings.Join(m.Alerts, " | "))
	}

	return []string{header, "<pre>" + html.EscapeString(strings.Join(rows, "\n")) + "</pre>"}
}

func uptime(ts, boot float64) string {
	total := int64(ts - boot)
	days, rem := total/86400, total%86400
	hours, secs := rem/3600, rem%3600

	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if days == 0 && secs >= 60 {
		parts = append(parts, fmt.Sprintf("%dm", secs/60))
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}
	return strings.Join(parts, " ")
}

func pctOf(used, total int64) float64 {
	if total <= 0 {
		return math.NaN()
	}
	return 100 * float64(used) / float64(total)
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// fitStream greedily packs trimmed lines until the effective Telegram limit
// would be exceeded, returning the accepted lines and the new running length.
func fitStream(lines []string, eff int) ([]string, int) {
	if eff >= tgMessageLimit {
		return nil, eff
	}
	var out []string
	for _, line := range lines {
		s := trimLine(line)
		newEff := eff + utf8.RuneCountInString(s)
		if len(out) > 0 {
			newEff++
		}
		if newEff > tgMessageLimit {
			break
		}
		eff = newEff
		out = append(out, s)
	}
	return out, eff
}

func quoteLines(lines []string) string {
	var b strings.Builder
	b.WriteString("<blockquote expandable>")
	for i, line := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("<code>")
		b.WriteString(html.EscapeString(line))
		b.WriteString("</code>")
	}
	b.WriteString("</blockquote>")
	return b.String()
}

func renderPanel(ctx context.Context, servers []*ServerState, journal *Journal, ctrl *PanelControl) string {
	skip := ctrl.takeSkipUpdate()

	blocks := make([][]string, len(servers))
	var wg sync.WaitGroup
	for i, s := range servers {
		wg.Go(func() {
			blocks[i] = renderServer(ctx, s, skip)
		})
	}
	wg.Wait()

	join := func(bs [][]string) string {
		parts := make([]string, len(bs))
		for i, b := range bs {
			parts[i] = strings.Join(b, "\n")
		}
		return strings.Join(parts, "\n")
	}

	text := join(blocks)
	eff := effLen(text)
	for eff > tgMessageLimit && len(blocks) > 1 {
		blocks = blocks[:len(blocks)-1]
		text = join(blocks)
		eff = effLen(text)
	}
	if eff > tgMessageLimit {
		return fmt.Sprintf("<i>panel too large to render (%d chars)</i>", eff)
	}

	expanded := ctrl.Expanded()
	limit := 7
	if expanded {
		limit = math.MaxInt
	}
	all := journal.messagesNewest(nil, limit)
	rows, eff := fitStream(all, eff)
	if len(rows) > 0 {
		if expanded {
			reverse(rows)
		}
		text += "<pre>" + html.EscapeString(strings.Join(rows, "\n")) + "</pre>"
	}
	slog.Debug("render_panel", "journal", len(rows), "total", len(all), "len", len(text), "eff", eff)
	return text
}

func reverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
