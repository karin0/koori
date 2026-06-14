package main

import (
	"math"
	"testing"
)

const scrape1 = `# HELP node_time_seconds System time.
# TYPE node_time_seconds gauge
node_time_seconds 1000
node_cpu_seconds_total{cpu="0",mode="idle"} 100
node_cpu_seconds_total{cpu="0",mode="user"} 50
node_network_receive_bytes_total{device="eth0"} 1000
node_network_receive_bytes_total{device="lo"} 99999
node_network_transmit_bytes_total{device="eth0"} 500
node_memory_MemTotal_bytes 1000
node_memory_MemAvailable_bytes 400
node_memory_SwapTotal_bytes 0
node_memory_SwapFree_bytes 0
node_filesystem_size_bytes{mountpoint="/"} 1000
node_filesystem_avail_bytes{mountpoint="/"} 250
node_vmstat_oom_kill 5
node_hwmon_temp_celsius{chip="platform_x"} 40
node_hwmon_temp_celsius{chip="platform_y"} 55
node_load1 1.5
node_boot_time_seconds 0
node_procs_blocked 0
node_scrape_collector_duration_seconds{collector="cpu"} 0.01
node_scrape_collector_success{collector="cpu"} 1
`

const scrape2 = `node_time_seconds 1010
node_cpu_seconds_total{cpu="0",mode="idle"} 105
node_cpu_seconds_total{cpu="0",mode="user"} 55
node_network_receive_bytes_total{device="eth0"} 2000
node_network_receive_bytes_total{device="lo"} 99999
node_network_transmit_bytes_total{device="eth0"} 500
node_memory_MemTotal_bytes 1000
node_memory_MemAvailable_bytes 400
node_vmstat_oom_kill 7
node_boot_time_seconds 0
`

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestParseMetricsBaseline(t *testing.T) {
	m := parseMetrics("test", nil, scrape1)

	if !math.IsNaN(m.CPUPct) || !math.IsNaN(m.RxBps) {
		t.Errorf("first snapshot must have NaN rates, got cpu=%v rx=%v", m.CPUPct, m.RxBps)
	}
	if m.MemUsed != 600 || m.MemTotal != 1000 {
		t.Errorf("mem = %d/%d, want 600/1000", m.MemUsed, m.MemTotal)
	}
	if m.FSUsed != 750 || m.FSTotal != 1000 || m.FSMount != "/" {
		t.Errorf("fs = %d/%d %q, want 750/1000 /", m.FSUsed, m.FSTotal, m.FSMount)
	}
	if math.IsNaN(m.Temp) || m.Temp != 55 || m.TempChip != "platform_y" {
		t.Errorf("temp = %v %q, want 55 platform_y", m.Temp, m.TempChip)
	}
	if m.Rx != 1000 { // loopback excluded
		t.Errorf("rx = %v, want 1000 (lo excluded)", m.Rx)
	}
	if m.Counters["node_vmstat_oom_kill"] != 5 {
		t.Errorf("oom counter = %d, want 5", m.Counters["node_vmstat_oom_kill"])
	}
	if m.CPUCount != 1 {
		t.Errorf("cpu count = %d, want 1", m.CPUCount)
	}
	if !almost(m.Load1, 1.5) {
		t.Errorf("load1 = %v, want 1.5", m.Load1)
	}
}

func TestParseMetricsDiff(t *testing.T) {
	prev := parseMetrics("test", nil, scrape1)
	m := parseMetrics("test", prev, scrape2)

	// idle +5 over total +10 => 50% busy.
	if math.IsNaN(m.CPUPct) || !almost(m.CPUPct, 50) {
		t.Errorf("cpu pct = %v, want 50", m.CPUPct)
	}
	if math.IsNaN(m.CPUMaxPct) || !almost(m.CPUMaxPct, 50) {
		t.Errorf("cpu max pct = %v, want 50", m.CPUMaxPct)
	}
	// +1000 bytes over 10s => 100 B/s.
	if math.IsNaN(m.RxBps) || !almost(m.RxBps, 100) {
		t.Errorf("rx bps = %v, want 100", m.RxBps)
	}
	if len(m.CntAlerts) != 1 || m.CntAlerts[0] != "OOM: 7 (+2)" {
		t.Errorf("cnt alerts = %v, want [OOM: 7 (+2)]", m.CntAlerts)
	}
}

func TestLabelEscaping(t *testing.T) {
	s, ok := parseSample(`node_filesystem_size_bytes{device="/dev/sda",mountpoint="/mnt/a\"b",fstype="ext4"} 4096`)
	if !ok {
		t.Fatal("parse failed")
	}
	if got := s.label("mountpoint"); got != `/mnt/a"b` {
		t.Errorf("mountpoint = %q, want /mnt/a\"b", got)
	}
	if got := s.label("fstype"); got != "ext4" {
		t.Errorf("fstype = %q, want ext4", got)
	}
	if s.value != 4096 {
		t.Errorf("value = %v, want 4096", s.value)
	}
}

func TestParseSampleNoLabels(t *testing.T) {
	s, ok := parseSample("node_load1 1.5")
	if !ok || s.name != "node_load1" || s.value != 1.5 {
		t.Errorf("got %+v ok=%v", s, ok)
	}
}
