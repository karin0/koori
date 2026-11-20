package main

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

const reconnectBackoffMax = 60 * time.Second

// ServerState holds the live, mutable state for one monitored server. Its
// fields are read by the render/alert paths while the run loop mutates them, so
// access is guarded by mu. The metrics snapshot is immutable once published, so
// callers may use the returned pointer without further locking.
type ServerState struct {
	Name   string
	dialer Dialer

	mu           sync.Mutex
	session      Session
	err          string
	metrics      *MetricsSnapshot
	pendingCnt   []string // counter alerts queued until successfully delivered
	alertAt      time.Time
	alertKeys    map[string]bool
	tempHotSince time.Time // start of the current spell above tempHighC, zero when cool
}

func NewServerState(name string, dialer Dialer) *ServerState {
	return &ServerState{Name: name, dialer: dialer, alertKeys: map[string]bool{}}
}

func (s *ServerState) snapshot() (Session, string, *MetricsSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session, s.err, s.metrics
}

func (s *ServerState) setSession(sess Session, err string) {
	s.mu.Lock()
	s.session, s.err = sess, err
	s.mu.Unlock()
}

// refreshMetrics scrapes and parses a fresh snapshot, publishing it as the new
// baseline for the next rate computation.
func (s *ServerState) refreshMetrics(ctx context.Context, sess Session) (*MetricsSnapshot, error) {
	text, err := sess.FetchMetrics(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	prev := s.metrics
	s.mu.Unlock()

	m := parseMetrics(s.Name, prev, text)

	s.mu.Lock()
	s.metrics = m
	s.pendingCnt = append(s.pendingCnt, m.CntAlerts...)
	s.mu.Unlock()
	return m, nil
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
