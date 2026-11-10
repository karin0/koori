package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
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

	err := runBot(ctx, &Config{ChatID: 1, AlertChatID: 1}, nil, NewJournal(1))
	if !errors.Is(err, bot.ErrorBadRequest) {
		t.Fatalf("runBot = %v, want bad request", err)
	}
	if ctx.Err() != nil {
		t.Fatal("runBot returned only after the test deadline")
	}
}

func TestLoadStateRejectsUnknownFormat(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile(stateFile, []byte(`{"-100123":212}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(); err == nil {
		t.Fatal("loadState accepted a state file of another format")
	}
}

func TestAnswerPanelQuery(t *testing.T) {
	answers := make(chan url.Values, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"koori"}}`))
		case strings.HasSuffix(r.URL.Path, "/answerWidgetQuery"):
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			answers <- r.MultipartForm.Value
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		default:
			t.Errorf("unexpected call %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	a := &app{chatID: 7, servers: []*ServerState{liveServer("web-1")}}
	b, err := bot.New("1:test", bot.WithServerURL(srv.URL), bot.WithSkipGetMe())
	if err != nil {
		t.Fatal(err)
	}
	a.bot = b

	ctx := context.Background()
	a.onUpdate(ctx, b, &models.Update{WidgetQuery: &models.WidgetQuery{ID: "other", Chat: &models.Chat{ID: 8}, Data: "p"}})
	a.onUpdate(ctx, b, &models.Update{WidgetQuery: &models.WidgetQuery{ID: "panel", Chat: &models.Chat{ID: 7}, Data: "p"}})

	got := <-answers
	if id := got.Get("widget_query_id"); id != "panel" {
		t.Fatalf("answered query %q, want the one from the panel chat", id)
	}
	var data []map[string]any
	if err := json.Unmarshal([]byte(got.Get("data")), &data); err != nil || len(data) != 1 || data[0]["n"] != "web-1" {
		t.Errorf("answer data %q: %v", got.Get("data"), err)
	}
	select {
	case extra := <-answers:
		t.Errorf("answered a query from another chat: %v", extra)
	default:
	}
}
