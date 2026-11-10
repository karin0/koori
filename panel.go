package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"html"
	"log/slog"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	tgMessageLimit  = 4096 // Telegram MAX_TEXT_LENGTH, post-parse
	widgetDataLimit = 1024 // bytes of a widget answer
	lineTrim        = 180
	errTrim         = 80
	journalLines    = 7
)

// panelHTML draws the JSON array that panelData encodes.
//
//go:embed panel.html
var panelHTML string

// num marshals with three significant digits, the precision panel.html shows.
// NaN marshals as null, and a NaN field is omitted.
type num float64

func (n num) IsZero() bool { return math.IsNaN(float64(n)) }

func (n num) MarshalJSON() ([]byte, error) {
	if n.IsZero() {
		return []byte("null"), nil
	}
	b := strconv.AppendFloat(nil, float64(n), 'g', 3, 64)
	b = bytes.Replace(b, []byte("e+0"), []byte("e"), 1)
	return bytes.Replace(b, []byte("e+"), []byte("e"), 1), nil
}

// serverData is one server's entry in the panel data. Keys are short and pairs
// are arrays to fit several servers in a widget answer.
type serverData struct {
	Name string `json:"n"`
	Down bool   `json:"d,omitzero"`
	Err  string `json:"e,omitzero"`
	Seen int64  `json:"s,omitzero"` // time of the last scrape of a down server
	*metricsData
}

type metricsData struct {
	Time     int64    `json:"t"`
	FetchMs  num      `json:"q"`
	ScrapeMs num      `json:"x"`
	CPU      [2]num   `json:"c"` // average and busiest core, percent
	Cores    int      `json:"k"`
	Mem      [2]num   `json:"m"`          // used and total bytes
	Swap     []num    `json:"w,omitzero"` // used and total bytes
	FS       []num    `json:"f,omitzero"` // used and total bytes of /
	Rx       [2]num   `json:"rx"`         // total bytes and bytes per second
	Tx       [2]num   `json:"tx"`         // total bytes and bytes per second
	Load     [3]num   `json:"l"`          // 1, 5 and 15 minute averages
	Boot     int64    `json:"b"`
	Temp     num      `json:"T,omitzero"`
	Alerts   []string `json:"a,omitzero"`
	History  string   `json:"h,omitzero"` // CPU history, a level character per point
}

// historyLevels encodes a CPU percent of 0 to 100 as one character.
const historyLevels = "0123456789abcdefghijklmnopqrstuvwxyz"

func encodeHistory(points []float64) string {
	b := make([]byte, len(points))
	for i, p := range points {
		b[i] = historyLevels[int(math.Round(max(0, min(100, p))/100*float64(len(historyLevels)-1)))]
	}
	return string(b)
}

// usage pairs used and total bytes, or returns nil when there is no total.
func usage(used, total int64) []num {
	if total == 0 {
		return nil
	}
	return []num{num(used), num(total)}
}

func newServerData(s *ServerState) serverData {
	sess, errMsg, m := s.snapshot()
	d := serverData{
		Name: s.Name,
		Down: sess == nil,
		Err:  truncateRunes(errMsg, errTrim, "…"),
	}
	switch {
	case m == nil:
	case d.Down:
		d.Seen = int64(m.Timestamp)
	default:
		d.metricsData = &metricsData{
			Time:     int64(m.Timestamp),
			FetchMs:  num(m.FetchTime * 1e3),
			ScrapeMs: num(m.ScrapeTime * 1e3),
			CPU:      [2]num{num(m.CPUPct), num(m.CPUMaxPct)},
			Cores:    m.CPUCount,
			Mem:      [2]num{num(m.MemUsed), num(m.MemTotal)},
			Swap:     usage(m.SwapUsed, m.SwapTotal),
			FS:       usage(m.FSUsed, m.FSTotal),
			Rx:       [2]num{num(m.Rx), num(m.RxBps)},
			Tx:       [2]num{num(m.Tx), num(m.TxBps)},
			Load:     [3]num{num(m.Load[0]), num(m.Load[1]), num(m.Load[2])},
			Boot:     int64(m.BootTime),
			Temp:     num(m.Temp),
			Alerts:   m.Alerts,
			History:  encodeHistory(s.history()),
		}
	}
	return d
}

// panelData encodes the servers for panel.html, dropping trailing servers that
// do not fit a widget answer.
func panelData(servers []*ServerState) ([]byte, error) {
	entries := make([]serverData, len(servers))
	for i, s := range servers {
		entries[i] = newServerData(s)
	}
	for n := len(entries); ; n-- {
		data, err := json.Marshal(entries[:n])
		if err != nil {
			return nil, err
		}
		if len(data) <= widgetDataLimit {
			if n < len(entries) {
				slog.Error("panel data too large", "shown", n, "servers", len(entries))
			}
			return data, nil
		}
	}
}

// renderJournal renders the newest journal lines, or every buffered line
// oldest first when expanded.
func renderJournal(journal *Journal, expanded bool) string {
	limit := journalLines
	if expanded {
		limit = math.MaxInt
	}
	rows, _ := fitStream(journal.messagesNewest(nil, limit), 0)
	if len(rows) == 0 {
		return "<i>no journal lines</i>"
	}
	if expanded {
		slices.Reverse(rows)
	}
	return "<pre>" + html.EscapeString(strings.Join(rows, "\n")) + "</pre>"
}

var tagRE = regexp.MustCompile(`<[^>]+>`)

// effLen counts characters as Telegram does after parsing: tags are dropped and
// HTML entities decode to a single character.
func effLen(s string) int {
	return utf8.RuneCountInString(html.UnescapeString(tagRE.ReplaceAllString(s, "")))
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
