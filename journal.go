package main

import "sync"

// JournalEntry pairs a streamed log line with its originating server. The
// pointer identity is used to filter a server's own lines during alerting.
type JournalEntry struct {
	Server  *ServerState
	Message string
}

// Journal is a fixed-capacity ring buffer shared by every server's stream
// goroutine and the render loop. It is safe for concurrent use.
type Journal struct {
	mu   sync.Mutex
	buf  []JournalEntry
	head int // index of the oldest entry
	size int
}

func NewJournal(capacity int) *Journal {
	return &Journal{buf: make([]JournalEntry, capacity)}
}

func (j *Journal) Append(e JournalEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()

	n := len(j.buf)
	if j.size < n {
		j.buf[(j.head+j.size)%n] = e
		j.size++
		return
	}
	j.buf[j.head] = e
	j.head = (j.head + 1) % n
}

// messagesNewest returns trimmed messages newest first, optionally filtered to
// a single server.
func (j *Journal) messagesNewest(server *ServerState, limit int) []string {
	j.mu.Lock()
	defer j.mu.Unlock()

	n := len(j.buf)
	idx := j.head + j.size - 1 // newest entry
	if idx >= n {
		idx -= n
	}
	var out []string
	for range j.size {
		if e := j.buf[idx]; server == nil || e.Server == server {
			out = append(out, e.Message)
			if len(out) >= limit {
				break
			}
		}
		if idx--; idx < 0 {
			idx = n - 1
		}
	}
	return out
}
