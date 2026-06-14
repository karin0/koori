package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestParseSSHConfigG(t *testing.T) {
	out := []byte("hostname real.example.com\nport 2222\nuser deploy\n" +
		"identityfile /nope/a\nidentityfile /nope/b\nproxycommand none\n")
	p := parseSSHConfigG(out)
	if p.hostName != "real.example.com" || p.port != 2222 || p.user != "deploy" {
		t.Errorf("parsed %+v", p)
	}
	if len(p.identityFiles) != 2 {
		t.Errorf("identityFiles = %v, want 2", p.identityFiles)
	}
}

func TestFirstExisting(t *testing.T) {
	dir := t.TempDir()
	real := writeFile(t, dir, "key", "x")
	if got := firstExisting([]string{"/nope", real, "/also-nope"}); got != real {
		t.Errorf("firstExisting = %q, want %q", got, real)
	}
	if got := firstExisting([]string{"/nope"}); got != "" {
		t.Errorf("firstExisting = %q, want empty", got)
	}
}

// With resolve_with_ssh off (default), SSH_CONFIG is ignored and parameters fall
// back to built-in defaults — no ssh subprocess is run.
func TestSSHConfigDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	sshCfg := writeFile(t, dir, "ssh_config", "Host alias\n  HostName real.example.com\n  Port 2222\n")
	cfgPath := writeFile(t, dir, "koori.yaml", "chat_id: 1\nservers:\n  - name: alias\n")
	t.Setenv("SSH_CONFIG", sshCfg)
	t.Setenv("USER", "tester")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if s := cfg.Servers[0]; s.Host != "alias" || s.Port != 22 || s.User != "tester" {
		t.Errorf("got %q:%d user=%q, want alias:22 tester (no resolution)", s.Host, s.Port, s.User)
	}
}

// Explicit YAML wins over ssh -G regardless of whether resolution runs.
func TestExplicitOverridesSSHConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "koori.yaml", `
chat_id: 1
resolve_with_ssh: true
servers:
  - name: alias
    host: override.example.com
    port: 22
    user: root
`)
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if s := cfg.Servers[0]; s.Host != "override.example.com" || s.Port != 22 || s.User != "root" {
		t.Errorf("explicit YAML did not win: %q:%d user=%q", s.Host, s.Port, s.User)
	}
}

// End-to-end through the real `ssh -G`, made deterministic by pointing -F at a
// throwaway config. Skipped where ssh is unavailable.
func TestResolveViaSSHConfigG(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("ssh not installed")
	}
	dir := t.TempDir()
	sshCfg := writeFile(t, dir, "ssh_config", "Host alias\n  HostName real.example.com\n  Port 2222\n  User deploy\n")
	cfgPath := writeFile(t, dir, "koori.yaml", "chat_id: 1\nresolve_with_ssh: true\nservers:\n  - name: alias\n")
	t.Setenv("SSH_CONFIG", sshCfg)

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if s := cfg.Servers[0]; s.Host != "real.example.com" || s.Port != 2222 || s.User != "deploy" {
		t.Errorf("ssh -G resolution: %q:%d user=%q, want real.example.com:2222 deploy", s.Host, s.Port, s.User)
	}
}

func TestLocalServerSkipsSSHResolution(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "koori.yaml", `
chat_id: 1
servers:
  - name: box
    kind: local
`)
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if s := cfg.Servers[0]; s.Host != "" || s.Port != 0 || s.User != "" {
		t.Errorf("local server should not be resolved: %+v", s)
	}
}
