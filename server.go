package main

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"
)

const (
	reconnectBackoffMax = 60 * time.Second
	historyStep         = 30 // seconds of CPU usage averaged into one history point
	historyLen          = 20
)

// ServerState holds the live, mutable state for one monitored server. Its
// fields are read by the render/alert paths while the run loop mutates them, so
// access is guarded by mu. The metrics snapshot is immutable once published, so
// callers may use the returned pointer without further locking.
type ServerState struct {
	Name   string
	dialer Dialer

	mu           sync.Mutex
	session      Session
	err          string // why the session is down
	fetchErr     string // why the last scrape of the live session failed
	metrics      *MetricsSnapshot
	pendingCnt   []string // counter alerts queued until successfully delivered
	alertAt      time.Time
	alertKeys    map[string]bool
	tempHotSince time.Time // start of the current spell above tempHighC, zero when cool

	cpuHistory []float64 // average CPU percent of each historyStep, oldest first
	histStart  float64   // scrape time that opened the current step
	histSum    float64
	histN      int
}

func NewServerState(name string, dialer Dialer) *ServerState {
	return &ServerState{Name: name, dialer: dialer, alertKeys: map[string]bool{}}
}

// snapshot returns the session, the error of the session when it is down or of
// the last scrape otherwise, and the last metrics.
func (s *ServerState) snapshot() (Session, string, *MetricsSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == nil {
		return nil, s.err, s.metrics
	}
	return s.session, s.fetchErr, s.metrics
}

func (s *ServerState) setSession(sess Session, err string) {
	s.mu.Lock()
	s.session, s.err, s.fetchErr = sess, err, ""
	s.mu.Unlock()
}

// refreshMetrics scrapes and parses a fresh snapshot, publishing it as the new
// baseline for the next rate computation.
func (s *ServerState) refreshMetrics(ctx context.Context, sess Session) error {
	t0 := time.Now()
	text, err := sess.FetchMetrics(ctx)
	if err != nil {
		s.mu.Lock()
		s.fetchErr = err.Error()
		s.mu.Unlock()
		return err
	}
	fetchTime := time.Since(t0).Seconds()

	s.mu.Lock()
	prev := s.metrics
	s.mu.Unlock()

	m := parseMetrics(s.Name, prev, text)
	m.FetchTime = fetchTime

	s.mu.Lock()
	s.metrics = m
	s.fetchErr = ""
	s.pendingCnt = append(s.pendingCnt, m.CntAlerts...)
	s.recordCPU(m)
	s.mu.Unlock()
	return nil
}

// recordCPU folds a scrape into the CPU history. The caller holds mu.
func (s *ServerState) recordCPU(m *MetricsSnapshot) {
	if math.IsNaN(m.CPUPct) {
		return
	}
	if s.histN == 0 && s.histStart == 0 {
		s.histStart = m.Timestamp
	}
	s.histSum += m.CPUPct
	s.histN++
	if m.Timestamp-s.histStart >= historyStep {
		s.cpuHistory = append(s.cpuHistory, s.histSum/float64(s.histN))
		if len(s.cpuHistory) > historyLen {
			s.cpuHistory = s.cpuHistory[1:]
		}
		s.histStart, s.histSum, s.histN = m.Timestamp, 0, 0
	}
}

func (s *ServerState) history() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.cpuHistory)
}

// scrape refreshes the metrics of every connected server concurrently.
func scrape(ctx context.Context, servers []*ServerState) {
	var wg sync.WaitGroup
	for _, s := range servers {
		sess, _, _ := s.snapshot()
		if sess == nil {
			continue
		}
		wg.Go(func() {
			if err := s.refreshMetrics(ctx, sess); err != nil {
				slog.Error("fetch metrics failed", "server", s.Name, "err", err)
			}
		})
	}
	wg.Wait()
}

// pendingAlerts returns the queued counter alerts without consuming them;
// dropPendingAlerts removes the first n once they have been delivered.
func (s *ServerState) pendingAlerts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.pendingCnt)
}

func (s *ServerState) dropPendingAlerts(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingCnt = s.pendingCnt[n:]
}

// Run maintains a persistent connection, streaming journal lines into the
// shared buffer and reconnecting with exponential backoff. ready is closed-over
// to signal the first successful connection exactly once.
func (s *ServerState) Run(ctx context.Context, journal *Journal, ready chan<- struct{}) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}

		sess, err := s.dialer.Open(ctx, s.Name)
		if err == nil {
			s.setSession(sess, "")
			backoff = time.Second
			if ready != nil {
				ready <- struct{}{}
				ready = nil
				slog.Debug("connected", "server", s.Name)
			} else {
				slog.Info("reconnected", "server", s.Name)
			}

			err = sess.StreamJournal(ctx, journal, s)
			sess.Close()

			switch {
			case ctx.Err() != nil:
				s.setSession(nil, "cancelled")
				return
			case err != nil:
				s.setSession(nil, err.Error())
				slog.Error("stream error", "server", s.Name, "err", err)
			default:
				s.setSession(nil, "journal stream ended")
				slog.Warn("journal stream ended", "server", s.Name)
			}
		} else {
			s.setSession(nil, err.Error())
			slog.Error("connect failed", "server", s.Name, "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, reconnectBackoffMax)
	}
}
