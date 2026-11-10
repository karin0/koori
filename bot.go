package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	stateFile      = "state.json"
	scrapeInterval = 5 * time.Second
	httpTimeout    = 30 * time.Second
	pollTimeout    = 30 * time.Second
)

func ptr[T any](v T) *T { return &v }

var disabledPreview = &models.LinkPreviewOptions{IsDisabled: ptr(true)}

type app struct {
	bot         *bot.Bot
	chatID      int64
	alertChatID int64
	servers     []*ServerState
	journal     *Journal

	// expanded is flipped by the journal button and read by the render loop,
	// which toggled wakes.
	expanded atomic.Bool
	toggled  chan struct{}
}

func buildBot(a *app) (*bot.Bot, error) {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("TELEGRAM_BOT_TOKEN not set")
	}

	opts := []bot.Option{
		bot.WithDefaultHandler(a.onUpdate),
		bot.WithAllowedUpdates(bot.AllowedUpdates{
			models.AllowedUpdateCallbackQuery,
			models.AllowedUpdateWidgetQuery,
		}),
		bot.WithHTTPClient(pollTimeout, &http.Client{Timeout: httpTimeout + pollTimeout}),
		// Handlers run serially, so onUpdate never races itself and expanded
		// needs only atomic load and store.
		bot.WithNotAsyncHandlers(),
	}
	if local := os.Getenv("LOCAL_SERVER"); local != "" {
		opts = append(opts, bot.WithServerURL(strings.TrimRight(local, "/")))
	}
	return bot.New(token, opts...)
}

func journalKeyboard(expanded bool) *models.InlineKeyboardMarkup {
	icon := "📜"
	if expanded {
		icon = "📃"
	}
	return &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{{{Text: icon, CallbackData: "log"}}},
	}
}

func (a *app) onUpdate(ctx context.Context, b *bot.Bot, update *models.Update) {
	switch {
	case update.WidgetQuery != nil:
		a.answerPanel(ctx, update.WidgetQuery)
	case update.CallbackQuery != nil:
		a.toggleJournal(ctx, update.CallbackQuery)
	}
}

// answerPanel answers every widget query from the panel chat with the latest
// metrics, whatever its data.
func (a *app) answerPanel(ctx context.Context, q *models.WidgetQuery) {
	if q.Chat == nil || q.Chat.ID != a.chatID {
		return
	}
	data, err := panelData(a.servers)
	if err != nil {
		slog.Error("encode panel data", "err", err)
	}
	if _, err := a.bot.AnswerWidgetQuery(ctx, &bot.AnswerWidgetQueryParams{
		WidgetQueryID: q.ID,
		Data:          string(data),
	}); err != nil {
		slog.Warn("answer widget query failed", "err", err)
	}
}

func (a *app) toggleJournal(ctx context.Context, cb *models.CallbackQuery) {
	if cb.Message.Message == nil || cb.Message.Message.Chat.ID != a.chatID || cb.Data != "log" {
		return
	}
	a.expanded.Store(!a.expanded.Load())
	select {
	case a.toggled <- struct{}{}:
	default:
	}
	if _, err := a.bot.AnswerCallbackQuery(ctx, &bot.AnswerCallbackQueryParams{CallbackQueryID: cb.ID}); err != nil {
		slog.Warn("answer callback failed", "err", err)
	}
}

// panelState records the messages of the running panel, so that the next start
// can delete them.
type panelState struct {
	ChatID  int64 `json:"chat_id"`
	Panel   int   `json:"panel"`
	Journal int   `json:"journal"`
}

func loadState() (panelState, error) {
	var st panelState
	data, err := os.ReadFile(stateFile)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return st, fmt.Errorf("%s: %w", stateFile, err)
	}
	return st, nil
}

func saveState(st panelState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return os.WriteFile(stateFile, data, 0o644)
}

func (a *app) sendJournal(ctx context.Context, text string, expanded bool) (*models.Message, error) {
	return a.bot.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:              a.chatID,
		Text:                text,
		ParseMode:           models.ParseModeHTML,
		LinkPreviewOptions:  disabledPreview,
		DisableNotification: true,
		ReplyMarkup:         journalKeyboard(expanded),
	})
}

// startPanel sends the journal message and then the panel widget under it,
// pins the widget and deletes the messages of the previous run. Every start
// sends a new widget, since a sent widget keeps its HTML forever.
func (a *app) startPanel(ctx context.Context) (panelState, error) {
	old, err := loadState()
	if err != nil {
		return panelState{}, err
	}

	journal, err := a.sendJournal(ctx, renderJournal(a.journal, false), false)
	if err != nil {
		return panelState{}, err
	}
	panel, err := a.bot.SendWidget(ctx, &bot.SendWidgetParams{
		ChatID:              a.chatID,
		HTML:                panelHTML,
		AllowQueries:        true,
		DisableNotification: true,
	})
	if err != nil {
		return panelState{}, err
	}
	if _, err := a.bot.PinChatMessage(ctx, &bot.PinChatMessageParams{
		ChatID:              a.chatID,
		MessageID:           panel.ID,
		DisableNotification: true,
	}); err != nil {
		slog.Warn("pin failed (continuing)", "err", err)
	} else if _, err := a.bot.DeleteMessage(ctx, &bot.DeleteMessageParams{
		// The pin posts a service message whose ID the API does not return. It
		// takes the ID after the widget unless the user sent a message between
		// the two calls.
		ChatID:    a.chatID,
		MessageID: panel.ID + 1,
	}); err != nil {
		slog.Warn("delete pin notice failed", "err", err)
	}

	st := panelState{ChatID: a.chatID, Panel: panel.ID, Journal: journal.ID}
	if err := saveState(st); err != nil {
		return panelState{}, err
	}
	if old != (panelState{}) {
		if _, err := a.bot.DeleteMessages(ctx, &bot.DeleteMessagesParams{
			ChatID:     old.ChatID,
			MessageIDs: []int{old.Panel, old.Journal},
		}); err != nil {
			slog.Warn("delete previous panel failed", "err", err)
		}
	}
	return st, nil
}

// shownJournal is what the journal message last showed.
type shownJournal struct {
	text     string
	expanded bool
	retryAt  time.Time // set while rate-limited
}

// syncJournal edits the journal message when its content changed, and sends a
// new one when the message is gone.
func (a *app) syncJournal(ctx context.Context, st *panelState, shown *shownJournal) error {
	expanded := a.expanded.Load()
	text := renderJournal(a.journal, expanded)
	if text == shown.text && expanded == shown.expanded || time.Now().Before(shown.retryAt) {
		return nil
	}

	_, err := a.bot.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:             a.chatID,
		MessageID:          st.Journal,
		Text:               text,
		ParseMode:          models.ParseModeHTML,
		LinkPreviewOptions: disabledPreview,
		ReplyMarkup:        journalKeyboard(expanded),
	})
	var tooMany *bot.TooManyRequestsError
	switch {
	case err == nil, strings.Contains(strings.ToLower(err.Error()), "not modified"):
	case errors.As(err, &tooMany):
		slog.Warn("rate-limited", "retry_after", tooMany.RetryAfter)
		shown.retryAt = time.Now().Add(time.Duration(tooMany.RetryAfter) * time.Second)
		return nil
	case errors.Is(err, bot.ErrorBadRequest):
		slog.Error("edit journal failed, recreating", "err", err)
		msg, err := a.sendJournal(ctx, text, expanded)
		if err != nil {
			slog.Error("recreate journal failed", "err", err)
			return nil
		}
		st.Journal = msg.ID
		if err := saveState(*st); err != nil {
			return err
		}
	default:
		slog.Error("edit journal failed", "err", err)
		return nil
	}
	shown.text, shown.expanded = text, expanded
	return nil
}

// awaitServers blocks until every server has reported up or the first interval
// elapses, then announces the outcome to the alert chat.
func (a *app) awaitServers(ctx context.Context, ready <-chan struct{}) {
	timer := time.NewTimer(scrapeInterval)
	defer timer.Stop()

	got := 0
	for got < len(a.servers) {
		select {
		case <-ready:
			got++
		case <-timer.C:
			got = -1
		case <-ctx.Done():
			return
		}
		if got < 0 {
			break
		}
	}

	var info string
	if got < 0 {
		var offline []string
		for _, s := range a.servers {
			if sess, _, _ := s.snapshot(); sess == nil {
				offline = append(offline, s.Name)
			}
		}
		info = "timed out waiting for servers up; offline: " + strings.Join(offline, ", ")
		slog.Warn(info)
	} else {
		info = "all servers up: " + strings.Join(serverNames(a.servers), ", ")
		slog.Info(info)
	}

	if _, err := a.bot.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:             a.alertChatID,
		Text:               info,
		LinkPreviewOptions: disabledPreview,
	}); err != nil {
		slog.Warn("startup notice failed", "err", err)
	}
}

// renderLoop scrapes every scrapeInterval, checks alerts and keeps the journal
// message in sync, while the panel widget pulls the metrics through widget
// queries. It returns nil once ctx is cancelled and an error when it cannot set
// up the panel or save its state.
func (a *app) renderLoop(ctx context.Context, ready <-chan struct{}) error {
	st, err := a.startPanel(ctx)
	if err != nil {
		return fmt.Errorf("start panel: %w", err)
	}

	a.awaitServers(ctx, ready)

	ticker := time.NewTicker(scrapeInterval)
	defer ticker.Stop()
	var shown shownJournal
	for due := true; ; {
		if ctx.Err() != nil {
			return nil
		}
		if due {
			scrape(ctx, a.servers)
			checkAlerts(ctx, a.bot, a.alertChatID, a.servers, a.journal)
		}
		if err := a.syncJournal(ctx, &st, &shown); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			due = true
		case <-a.toggled:
			due = false
		}
	}
}

func serverNames(servers []*ServerState) []string {
	names := make([]string, len(servers))
	for i, s := range servers {
		names[i] = s.Name
	}
	return names
}
