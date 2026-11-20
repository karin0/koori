package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"
)

const journalBufferSize = 50

func main() {
	dryRun := flag.Bool("dry-run", false, "render the panel to stdout instead of Telegram")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	configPath := flag.Arg(0)
	if configPath == "" {
		configPath = "config.yaml"
	}

	setupLogging(*debug)

	cfg, err := loadConfig(configPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	servers, err := buildServers(cfg)
	if err != nil {
		slog.Error("build servers", "err", err)
		os.Exit(1)
	}

	journal := NewJournal(journalBufferSize)
	ctrl := NewPanelControl()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *dryRun {
		runDryRun(ctx, servers, journal, ctrl)
	} else if err := runBot(ctx, cfg, servers, journal, ctrl); err != nil {
		slog.Error("bot", "err", err)
		os.Exit(1)
	}
	slog.Info("interrupted")
}

var debugMode bool

func setupLogging(debug bool) {
	debugMode = debug
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	if _, ok := os.LookupEnv("JOURNAL_STREAM"); ok {
		// systemd already stamps each line; drop the redundant timestamp.
		opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			return a
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, opts)))
}

func buildServers(cfg *Config) ([]*ServerState, error) {
	var hostKey ssh.HostKeyCallback
	for _, s := range cfg.Servers {
		if s.Kind == "ssh" {
			cb, err := loadHostKeyCallback(cfg.KnownHosts)
			if err != nil {
				return nil, err
			}
			hostKey = cb
			break
		}
	}

	servers := make([]*ServerState, len(cfg.Servers))
	for i, spec := range cfg.Servers {
		var dialer Dialer
		switch spec.Kind {
		case "local":
			dialer = localDialer{exclude: spec.Exclude}
		case "ssh":
			dialer = newSSHDialer(spec, hostKey)
		}
		servers[i] = NewServerState(spec.Name, dialer)
	}
	return servers, nil
}

func runDryRun(ctx context.Context, servers []*ServerState, journal *Journal, ctrl *PanelControl) {
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() {
			s.Run(ctx, journal, nil)
		})
	}
	defer wg.Wait()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	render := func() {
		fmt.Print("\x1b[2J\x1b[H")
		fmt.Println(renderPanel(ctx, servers, journal, ctrl))
	}
	render()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			render()
		}
	}
}

func runBot(ctx context.Context, cfg *Config, servers []*ServerState, journal *Journal, ctrl *PanelControl) error {
	if cfg.ChatID == 0 {
		return errors.New("chat_id required")
	}

	a := &app{
		ctrl:        ctrl,
		chatID:      cfg.ChatID,
		alertChatID: cfg.AlertChatID,
		servers:     servers,
		journal:     journal,
	}
	b, err := buildBot(a)
	if err != nil {
		return fmt.Errorf("build bot: %w", err)
	}
	a.bot = b

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ready := make(chan struct{}, len(servers))
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() {
			s.Run(ctx, journal, ready)
		})
	}
	var renderErr error
	wg.Go(func() {
		renderErr = a.renderLoop(ctx, ready)
		cancel()
	})

	b.Start(ctx) // blocks until ctx is cancelled (signal or render loop exit)
	cancel()
	wg.Wait()
	return renderErr
}
