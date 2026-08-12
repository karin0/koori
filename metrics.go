package main

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
)

// collectors are requested from node_exporter via repeated collect[] query
// parameters, limiting the scrape to the families we actually parse.
var collectors = []string{
	"cpu", "meminfo", "netdev", "filesystem", "hwmon", "time", "loadavg",
	"conntrack", "vmstat", "stat", "kernel_hung", "softnet",
}

// alertRule maps a monotonically-increasing counter to a short alert label;
// order is significant for stable output.
type alertRule struct{ metric, label string }

var alertRules = []alertRule{
	{"node_vmstat_oom_kill", "OOM"},
	{"node_kernel_hung_tasks_total", "Hung"},
	{"node_cpu_package_throttles_total", "Throttle"},
	{"node_softnet_dropped_total", "NetDrop"},
}

// scalarKeys are summed across all samples sharing the name; everything else is
// filtered out in O(1).
var scalarKeys = func() map[string]struct{} {
	keys := []string{
		"node_time_seconds",
		"node_scrape_collector_duration_seconds",
		"node_load1",
		"node_boot_time_seconds",
		"node_memory_MemTotal_bytes",
		"node_memory_MemAvailable_bytes",
		"node_memory_SwapTotal_bytes",
		"node_memory_SwapFree_bytes",
		"node_nf_conntrack_entries",
		"node_nf_conntrack_entries_limit",
		"node_memory_HardwareCorrupted_bytes",
	}
	m := make(map[string]struct{}, len(keys)+len(alertRules))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	for _, r := range alertRules {
		m[r.metric] = struct{}{}
	}
	return m
}()

// MetricsSnapshot is an immutable parsed view of one scrape. NaN denotes an
// unavailable rate/gauge (no prior sample to diff against, or no sensor present).
type MetricsSnapshot struct {
	Timestamp  float64 // server unix time of the scrape
	ScrapeTime float64 // total collector duration, seconds
	CPUPct     float64
	CPUMaxPct  float64
	CPUCount   int
	CPUSeconds map[string][2]float64 // cpu id -> (idle, total) seconds
	MemUsed    int64
	MemTotal   int64
	SwapUsed   int64
	SwapTotal  int64
	FSUsed     int64
	FSTotal    int64
	FSMount    string // "" when the root filesystem was not reported
	Rx         float64
	Tx         float64
	RxBps      float64
	TxBps      float64
	Temp       float64
	TempChip   string
	Load1      float64
	BootTime   float64
	Alerts     []string
	CntAlerts  []string
	Counters   map[string]int64
}

const metricsPort = 9100

var metricsAddr = fmt.Sprintf("127.0.0.1:%d", metricsPort)

func parseMetrics(name string, prev *MetricsSnapshot, text string) *MetricsSnapshot {
	scalars := make(map[string]float64, len(scalarKeys))
	cpuCap := 8
	if prev != nil {
		cpuCap = prev.CPUCount
	}
	perCPUIdle := make(map[string]float64, cpuCap)
	perCPUTotal := make(map[string]float64, cpuCap)
	var fsTotal, fsAvailBytes int64
	tempMax := math.Inf(-1)

	debug := debugMode
	var seen map[string]bool
	if debug {
		seen = make(map[string]bool, len(scalarKeys))
	}

	var rxTotal, txTotal float64
	var tempChip string

	for line := range strings.Lines(text) {
		line = strings.TrimRight(line, "\r\n")
		if line == "" || line[0] == '#' {
			continue
		}
		sample, ok := parseSample(line)
		if !ok {
			continue
		}
		if debug {
			seen[sample.name] = true
		}

		switch sample.name {
		case "node_cpu_seconds_total":
			cpu := sample.label("cpu")
			perCPUTotal[cpu] += sample.value
			if sample.label("mode") == "idle" {
				perCPUIdle[cpu] += sample.value
			}
		case "node_filesystem_size_bytes":
			if sample.label("mountpoint") == "/" {
				fsTotal = int64(sample.value)
			}
		case "node_filesystem_avail_bytes":
			if sample.label("mountpoint") == "/" {
				fsAvailBytes = int64(sample.value)
			}
		case "node_network_receive_bytes_total":
			if sample.label("device") != "lo" {
				rxTotal += sample.value
			}
		case "node_network_transmit_bytes_total":
			if sample.label("device") != "lo" {
				txTotal += sample.value
			}
		case "node_hwmon_temp_celsius":
			if sample.value > tempMax {
				tempMax = sample.value
				tempChip = sample.label("chip")
			}
		case "node_scrape_collector_success":
			if sample.value == 0 {
				slog.Warn("collector failed", "server", name, "collector", sample.label("collector"))
			}
		default:
			if _, ok := scalarKeys[sample.name]; ok {
				scalars[sample.name] += sample.value // accumulate repeated samples
			}
		}
	}

	if debug {
		var missing []string
		for k := range scalarKeys {
			if !seen[k] {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			slog.Debug("missing metrics", "server", name, "keys", missing)
		}
	}

	memTotal := int64(scalars["node_memory_MemTotal_bytes"])
	memAvail := int64(scalars["node_memory_MemAvailable_bytes"])
	swapTotal := int64(scalars["node_memory_SwapTotal_bytes"])
	swapFree := int64(scalars["node_memory_SwapFree_bytes"])

	ts := scalars["node_time_seconds"]
	if ts == 0 {
		ts = float64(time.Now().Unix())
		slog.Warn("missing node_time_seconds", "server", name)
	}

	var alerts []string
	if limit := int64(scalars["node_nf_conntrack_entries_limit"]); limit > 0 {
		if entries := int64(scalars["node_nf_conntrack_entries"]); entries*5 >= limit*4 {
			alerts = append(alerts, fmt.Sprintf("Conntrack: %d/%d", entries, limit))
		}
	}
	if corrupt := scalars["node_memory_HardwareCorrupted_bytes"]; corrupt != 0 {
		alerts = append(alerts, fmt.Sprintf("RAM Corrupted: %g", corrupt))
	}

	cpuPct := math.NaN()
	cpuMaxPct := math.NaN()
	rxBps := math.NaN()
	txBps := math.NaN()
	var cntAlerts []string
	counters := make(map[string]int64, len(alertRules))

	if prev == nil {
		for _, r := range alertRules {
			counters[r.metric] = int64(scalars[r.metric])
		}
	} else {
		for _, r := range alertRules {
			val := int64(scalars[r.metric])
			counters[r.metric] = val
			if val != 0 {
				if delta := val - prev.Counters[r.metric]; delta != 0 {
					cntAlerts = append(cntAlerts, fmt.Sprintf("%s: %d (%+d)", r.label, val, delta))
				}
			}
		}

		var sum, peak float64
		var nPct int
		for cid, total := range perCPUTotal {
			prevInfo, ok := prev.CPUSeconds[cid]
			if !ok {
				continue
			}
			prevIdle, prevTotal := prevInfo[0], prevInfo[1]
			if td := total - prevTotal; td > 0 {
				pct := max(0, 100*(1-(perCPUIdle[cid]-prevIdle)/td))
				sum += pct
				nPct++
				if pct > peak {
					peak = pct
				}
			}
		}
		if nPct > 0 {
			cpuPct = sum / float64(nPct)
			cpuMaxPct = peak
		}

		if dt := ts - prev.Timestamp; dt > 0 {
			rxBps = max(0, (rxTotal-prev.Rx)/dt)
			txBps = max(0, (txTotal-prev.Tx)/dt)
		}
	}

	if tempMax == math.Inf(-1) {
		tempMax = math.NaN()
	}

	cpuSeconds := make(map[string][2]float64, len(perCPUTotal))
	for cid, total := range perCPUTotal {
		cpuSeconds[cid] = [2]float64{perCPUIdle[cid], total}
	}

	fsMount := ""
	var fsUsed int64
	if fsTotal != 0 {
		fsMount = "/"
		fsUsed = max(0, fsTotal-fsAvailBytes)
	}

	return &MetricsSnapshot{
		Timestamp:  ts,
		ScrapeTime: scalars["node_scrape_collector_duration_seconds"],
		CPUPct:     cpuPct,
		CPUMaxPct:  cpuMaxPct,
		CPUCount:   len(perCPUTotal),
		CPUSeconds: cpuSeconds,
		MemUsed:    max(0, memTotal-memAvail),
		MemTotal:   memTotal,
		SwapUsed:   max(0, swapTotal-swapFree),
		SwapTotal:  swapTotal,
		FSUsed:     fsUsed,
		FSTotal:    fsTotal,
		FSMount:    fsMount,
		Rx:         rxTotal,
		Tx:         txTotal,
		RxBps:      rxBps,
		TxBps:      txBps,
		Temp:       tempMax,
		TempChip:   tempChip,
		Load1:      scalars["node_load1"],
		BootTime:   scalars["node_boot_time_seconds"],
		Alerts:     alerts,
		CntAlerts:  cntAlerts,
		Counters:   counters,
	}
}

// sample is one parsed Prometheus line. Labels are parsed lazily and only for
// the families that need them.
type sample struct {
	name   string
	value  float64
	labels string // raw text between { and }, parsed on demand
}

func (s sample) label(key string) string {
	rest := s.labels
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq < 0 {
			return ""
		}
		k := strings.TrimSpace(rest[:eq])
		rest = rest[eq+1:]
		if len(rest) == 0 || rest[0] != '"' {
			return ""
		}
		val, tail, ok := scanQuoted(rest)
		if !ok {
			return ""
		}
		if k == key {
			return val
		}
		rest = strings.TrimPrefix(strings.TrimSpace(tail), ",")
	}
	return ""
}

// scanQuoted reads a Prometheus-escaped quoted string starting at s[0]=='"' and
// returns the unescaped value plus the unconsumed remainder.
func scanQuoted(s string) (value, rest string, ok bool) {
	if len(s) < 2 || s[0] != '"' {
		return "", "", false
	}
	idx := strings.IndexAny(s[1:], `"\`)
	if idx < 0 {
		return "", "", false
	}
	idx++ // adjust for starting at s[1:]
	if s[idx] == '"' {
		return s[1:idx], s[idx+1:], true
	}

	var b strings.Builder
	b.Grow(idx - 1)
	b.WriteString(s[1:idx])
	for i := idx; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			if i++; i >= len(s) {
				return "", "", false
			}
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case '\\', '"':
				b.WriteByte(s[i])
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i])
			}
		case '"':
			return b.String(), s[i+1:], true
		default:
			b.WriteByte(c)
		}
	}
	return "", "", false
}

func parseSample(line string) (sample, bool) {
	var s sample
	if brace := strings.IndexByte(line, '{'); brace >= 0 {
		end := strings.LastIndexByte(line, '}')
		if end < brace {
			return s, false
		}
		s.name = line[:brace]
		s.labels = line[brace+1 : end]
		line = line[end+1:]
	} else {
		sp := strings.IndexByte(line, ' ')
		if sp < 0 {
			return s, false
		}
		s.name = line[:sp]
		line = line[sp:]
	}

	fields := strings.Fields(line)
	if len(fields) == 0 {
		return s, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return s, false
	}
	s.value = v
	return s, true
}
