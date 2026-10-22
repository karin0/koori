package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	stateFile       = "state.json"
	defaultInterval = 5 * time.Second
	minInterval     = 1 * time.Second
	maxInterval     = 60 * time.Second
	httpTimeout     = 30 * time.Second
	pollTimeout     = 30 * time.Second
)

func ptr[T any](v T) *T { return &v }

var disabledPreview = &models.LinkPreviewOptions{IsDisabled: ptr(true)}

// PanelControl is the shared control surface mutated by inline-button
// callbacks and read by the render loop.
type PanelControl struct {
	interval   atomic.Int64
	expanded   atomic.Bool
	skipUpdate atomic.Bool
	refresh    chan struct{}
}

func NewPanelControl() *PanelControl {
	c := &PanelControl{
		refresh: make(chan struct{}, 1),
	}
	c.interval.Store(int64(defaultInterval))
	return c
}

func (c *PanelControl) Interval() time.Duration {
	return time.Duration(c.interval.Load())
}

func (c *PanelControl) Expanded() bool {
	return c.expanded.Load()
}

// takeSkipUpdate reports and clears the skip flag; the render path consumes it
// so a single button press skips exactly one metrics fetch.
func (c *PanelControl) takeSkipUpdate() bool {
	return c.skipUpdate.Swap(false)
}

func (c *PanelControl) peekSkipUpdate() bool {
	return c.skipUpdate.Load()
}

// slower, faster and toggleLog are only called from the serialized onUpdate
// handler.
func (c *PanelControl) slower() {
	c.interval.Store(int64(min(maxInterval, c.Interval()*2)))
}

func (c *PanelControl) faster() {
	c.interval.Store(int64(max(minInterval, c.Interval()/2)))
}

func (c *PanelControl) toggleLog() {
	c.expanded.Store(!c.expanded.Load())
	c.skipUpdate.Store(true)
}

func (c *PanelControl) signalRefresh() {
	select {
	case c.refresh <- struct{}{}:
	default:
	}
}

func (c *PanelControl) waitRefresh(ctx context.Context, timeout time.Duration) bool {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-c.refresh:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

type app struct {
	bot         *bot.Bot
	ctrl        *PanelControl
	chatID      int64
	alertChatID int64
	servers     []*ServerState
	journal     *Journal
}

func buildBot(a *app) (*bot.Bot, error) {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("TELEGRAM_BOT_TOKEN not set")
	}

	opts := []bot.Option{
		bot.WithDefaultHandler(a.onUpdate),
		bot.WithAllowedUpdates(bot.AllowedUpdates{"callback_query"}),
		bot.WithHTTPClient(pollTimeout, &http.Client{Timeout: httpTimeout + pollTimeout}),
		// Handlers run serially, so onUpdate never races itself: the control
		// fields need only atomic load/store, not compare-and-swap.
		bot.WithNotAsyncHandlers(),
	}
	if local := os.Getenv("LOCAL_SERVER"); local != "" {
		opts = append(opts, bot.WithServerURL(strings.TrimRight(local, "/")))
	}
	return bot.New(token, opts...)
}

func keyboard(ctrl *PanelControl) *models.InlineKeyboardMarkup {
	logIcon := "📜"
	if ctrl.Expanded() {
		logIcon = "📃"
	}
	return &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{{
			{Text: "🐢", CallbackData: "slower"},
			{Text: "🔄" + hm(time.Now()), CallbackData: "refresh"},
			{Text: "🐇", CallbackData: "faster"},
			{Text: fmt.Sprintf("%s%gs", logIcon, ctrl.Interval().Seconds()), CallbackData: "log"},
		}},
	}
}

func (a *app) onUpdate(ctx context.Context, b *bot.Bot, update *models.Update) {
	cb := update.CallbackQuery
	if cb == nil || cb.Message.Message == nil || cb.Message.Message.Chat.ID != a.chatID {
		return
	}
	switch cb.Data {
	case "slower":
		a.ctrl.slower()
	case "faster":
		a.ctrl.faster()
	case "log":
		a.ctrl.toggleLog()
	}
	slog.Debug("ctrl", "interval", a.ctrl.Interval(), "expanded", a.ctrl.Expanded())
	a.ctrl.signalRefresh()
	if _, err := b.AnswerCallbackQuery(ctx, &bot.AnswerCallbackQueryParams{CallbackQueryID: cb.ID}); err != nil {
		slog.Warn("answer callback failed", "err", err)
	}
}

func loadState() (map[string]int, error) {
	data, err := os.ReadFile(stateFile)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]int{}, nil
	}
	if err != nil {
		return nil, err
	}
	state := map[string]int{}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return state, nil
}

func saveState(state map[string]int) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(stateFile, data, 0o644)
}

// ensurePanelMessage returns the pinned panel message id, reusing the persisted
// one when text is empty (startup) and otherwise sending and pinning a fresh
// message (recovery after a failed edit).
func (a *app) ensurePanelMessage(ctx context.Context, text string) (int, error) {
	state, err := loadState()
	if err != nil {
		return 0, err
	}
	key := strconv.FormatInt(a.chatID, 10)
	if text == "" {
		if id, ok := state[key]; ok {
			return id, nil
		}
		text = "starting…"
	}

	msg, err := a.bot.SendRichMessage(ctx, &bot.SendRichMessageParams{
		ChatID:      a.chatID,
		RichMessage: models.InputRichMessage{HTML: text},
		ReplyMarkup: keyboard(a.ctrl),
	})
	if err != nil {
		return 0, err
	}
	if _, err := a.bot.PinChatMessage(ctx, &bot.PinChatMessageParams{ChatID: a.chatID, MessageID: msg.ID}); err != nil {
		slog.Warn("pin failed (continuing)", "err", err)
	}

	state[key] = msg.ID
	if err := saveState(state); err != nil {
		return 0, err
	}
	return msg.ID, nil
}

// awaitServers blocks until every server has reported up or the first interval
// elapses, then announces the outcome to the alert chat.
func (a *app) awaitServers(ctx context.Context, ready <-chan struct{}) {
	timer := time.NewTimer(a.ctrl.Interval())
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

// renderLoop returns nil once ctx is cancelled and an error when it cannot
// obtain the panel message.
func (a *app) renderLoop(ctx context.Context, ready <-chan struct{}) error {
	msgID, err := a.ensurePanelMessage(ctx, "")
	if err != nil {
		return fmt.Errorf("ensure panel message: %w", err)
	}

	a.awaitServers(ctx, ready)
	if ctx.Err() != nil {
		return nil
	}

	var scheduled, t0 time.Time
	for {
		if ctx.Err() != nil {
			return nil
		}
		text := renderPanel(ctx, a.servers, a.journal, a.ctrl)
		checkAlerts(ctx, a.bot, a.alertChatID, a.servers, a.journal)

		_, err := a.bot.EditMessageText(ctx, &bot.EditMessageTextParams{
			ChatID:    a.chatID,
			MessageID: msgID,
			RichMessage: &models.InputRichMessage{
				HTML: text,
			},
			ReplyMarkup:        keyboard(a.ctrl),
			LinkPreviewOptions: disabledPreview,
		})
		if err != nil {
			var tooMany *bot.TooManyRequestsError
			switch {
			case errors.As(err, &tooMany):
				slog.Warn("rate-limited", "retry_after", tooMany.RetryAfter)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(tooMany.RetryAfter) * time.Second):
				}
				continue
			case strings.Contains(strings.ToLower(err.Error()), "not modified"):
			case errors.Is(err, bot.ErrorBadRequest):
				slog.Error("edit failed, recreating", "err", err)
				if id, e := a.ensurePanelMessage(ctx, text); e == nil {
					msgID = id
				} else {
					slog.Error("recreate failed", "err", e)
				}
			default:
				slog.Error("edit failed", "err", err)
			}
		}

		var timeout time.Duration
		if !scheduled.IsZero() {
			timeout = max(time.Until(scheduled), time.Second)
			scheduled = time.Time{}
		} else {
			t0 = time.Now()
			timeout = a.ctrl.Interval()
		}

		refreshed := a.ctrl.waitRefresh(ctx, timeout)
		if ctx.Err() != nil {
			return nil
		}
		if refreshed && a.ctrl.peekSkipUpdate() {
			scheduled = t0.Add(a.ctrl.Interval())
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
