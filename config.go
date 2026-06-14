package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

// Config is the on-disk YAML configuration. Secrets (the bot token) stay in the
// environment; this file holds only routing and per-server topology.
type Config struct {
	ChatID         int64        `yaml:"chat_id"`
	AlertChatID    int64        `yaml:"alert_chat_id"`
	KnownHosts     string       `yaml:"known_hosts"`
	IdentityFile   string       `yaml:"identity_file"`    // default for servers that omit one
	ResolveWithSSH bool         `yaml:"resolve_with_ssh"` // opt in to `ssh -G` parameter resolution
	Servers        []ServerSpec `yaml:"servers"`
}

type ServerSpec struct {
	Name         string   `yaml:"name"`
	Kind         string   `yaml:"kind"` // "ssh" (default) or "local"
	Host         string   `yaml:"host"` // defaults to Name
	Port         int      `yaml:"port"` // defaults to 22
	User         string   `yaml:"user"` // defaults to $USER
	IdentityFile string   `yaml:"identity_file"`
	Exclude      []string `yaml:"exclude"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.UnmarshalWithOptions(data, &cfg, yaml.Strict()); err != nil {
		return nil, err
	}

	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("no servers configured")
	}

	topIdentity := expandUser(cfg.IdentityFile)

	for i := range cfg.Servers {
		s := &cfg.Servers[i]
		if s.Name == "" {
			return nil, fmt.Errorf("server #%d: missing name", i)
		}
		switch s.Kind {
		case "":
			s.Kind = "ssh"
		case "ssh", "local":
		default:
			return nil, fmt.Errorf("%s: unknown kind %q", s.Name, s.Kind)
		}
		if s.Kind == "ssh" {
			if err := resolveSSHParams(s, topIdentity, cfg.ResolveWithSSH); err != nil {
				return nil, err
			}
		}
	}

	if cfg.AlertChatID == 0 {
		cfg.AlertChatID = cfg.ChatID
	}
	if cfg.KnownHosts == "" {
		cfg.KnownHosts = filepath.Join(homeDir(), ".ssh", "known_hosts")
	} else {
		cfg.KnownHosts = expandUser(cfg.KnownHosts)
	}

	return &cfg, nil
}

// resolveSSHParams fills a server's unset connection parameters. Precedence is
// explicit YAML > `ssh -G` (only when resolve_with_ssh is set) > built-in
// default. The server name is the lookup alias.
func resolveSSHParams(s *ServerSpec, topIdentity string, resolveWithSSH bool) error {
	var g sshParams
	if resolveWithSSH {
		resolved, err := resolveSSHConfig(s.Name)
		if err != nil {
			slog.Warn("ssh -G failed, using defaults", "server", s.Name, "err", err)
		} else {
			g = resolved
		}
	}

	if s.Host == "" {
		s.Host = orDefault(g.hostName, s.Name)
	}
	if s.Port == 0 {
		if g.port > 0 {
			s.Port = g.port
		} else {
			s.Port = 22
		}
	}
	if s.User == "" {
		if s.User = g.user; s.User == "" {
			if s.User = os.Getenv("USER"); s.User == "" {
				return fmt.Errorf("%s: no user configured and $USER unset", s.Name)
			}
		}
	}

	switch {
	case s.IdentityFile != "":
		s.IdentityFile = expandUser(s.IdentityFile)
	case topIdentity != "":
		s.IdentityFile = topIdentity
	default:
		s.IdentityFile = firstExisting(g.identityFiles)
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

func expandUser(path string) string {
	if path == "~" {
		return homeDir()
	}
	if len(path) >= 2 && path[0] == '~' && (path[1] == '/' || path[1] == filepath.Separator) {
		return filepath.Join(homeDir(), path[2:])
	}
	return path
}
