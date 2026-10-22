package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-telegram/bot"
)

// fakeTelegram accepts getMe and getUpdates and rejects every other method as
// a bad request.
func fakeTelegram(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"koori"}}`))
		case strings.HasSuffix(r.URL.Path, "/getUpdates"):
			select {
			case <-r.Context().Done():
			case <-time.After(100 * time.Millisecond):
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRunBotReturnsPanelError(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("TELEGRAM_BOT_TOKEN", "1:test")
	t.Setenv("LOCAL_SERVER", fakeTelegram(t).URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := runBot(ctx, &Config{ChatID: 1, AlertChatID: 1}, nil, NewJournal(1), NewPanelControl())
	if !errors.Is(err, bot.ErrorBadRequest) {
		t.Fatalf("runBot = %v, want bad request", err)
	}
	if ctx.Err() != nil {
		t.Fatal("runBot returned only after the test deadline")
	}
}
