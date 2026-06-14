package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

var journalCmd = []string{"journalctl", "-f", "-n", "15", "-o", "short", "--no-pager"}

const (
	metricsTimeout    = 10 * time.Second
	commandTimeout    = 10 * time.Second
	sshLoginTimeout   = 15 * time.Second
	keepaliveInterval = 30 * time.Second
	keepaliveTimeout  = 10 * time.Second
	keepaliveMaxMiss  = 3
)

// metricsURL is resolved once: the path/query never changes between scrapes.
var metricsURL = func() string {
	q := url.Values{}
	for _, c := range collectors {
		q.Add("collect[]", c)
	}
	return "http://" + metricsAddr + "/metrics?" + q.Encode()
}()

type Session interface {
	// StreamJournal reads journal lines until the stream ends or ctx is
	// cancelled, appending matching lines to the journal.
	StreamJournal(ctx context.Context, journal *Journal, s *ServerState) error
	FetchMetrics(ctx context.Context) (string, error)
	RunCommand(ctx context.Context, cmd string) (string, error)
	Close()
}

type Dialer interface {
	Open(ctx context.Context, name string) (Session, error)
}

// metricsClient is a persistent, pooled HTTP client bound to one connection
// (local TCP or an SSH-tunnelled channel). It is embedded by both session
// kinds; Go's transport transparently re-dials a dropped idle connection.
type metricsClient struct {
	client *http.Client
}

func newMetricsClient(dial func(ctx context.Context, network, addr string) (net.Conn, error)) metricsClient {
	return metricsClient{client: &http.Client{
		Timeout: metricsTimeout,
		Transport: &http.Transport{
			DialContext:           dial,
			MaxIdleConns:          1,
			MaxIdleConnsPerHost:   1,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: metricsTimeout,
			DisableCompression:    true,
		},
	}}
}

func (m metricsClient) FetchMetrics(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, metricsTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return "", fmt.Errorf("HTTP status %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	slog.Debug("metrics", "bytes", len(body))
	return string(body), nil
}

func (m metricsClient) Close() { m.client.CloseIdleConnections() }

// scanJournal streams lines from r, trimming whitespace and dropping any line
// containing an excluded keyword. Valid lines are appended directly to the journal.
func scanJournal(ctx context.Context, r io.Reader, exclude []string, journal *Journal, s *ServerState) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" || containsAny(line, exclude) {
			continue
		}
		journal.Append(JournalEntry{Server: s, Message: line})
	}
	return scanner.Err()
}

func containsAny(s string, kws []string) bool {
	for _, kw := range kws {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// --- local ---

type localDialer struct{ exclude []string }

func (d localDialer) Open(ctx context.Context, name string) (Session, error) {
	proc := exec.CommandContext(ctx, journalCmd[0], journalCmd[1:]...)
	proc.Stderr = nil
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := proc.Start(); err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: metricsTimeout}
	return &localSession{
		metricsClient: newMetricsClient(dialer.DialContext),
		exclude:       d.exclude,
		proc:          proc,
		stdout:        stdout,
	}, nil
}

type localSession struct {
	metricsClient
	exclude []string
	proc    *exec.Cmd
	stdout  io.ReadCloser
}

func (s *localSession) StreamJournal(ctx context.Context, journal *Journal, state *ServerState) error {
	return scanJournal(ctx, s.stdout, s.exclude, journal, state)
}

func (s *localSession) RunCommand(ctx context.Context, cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	c := exec.CommandContext(ctx, "sh", "-c", cmd)
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil && stdout.Len() == 0 && stderr.Len() == 0 {
		return "", err
	}
	if stdout.Len() > 0 {
		return stdout.String(), nil
	}
	return stderr.String(), nil
}

func (s *localSession) Close() {
	if s.proc.Process != nil {
		s.proc.Process.Kill()
		s.proc.Wait()
	}
	s.metricsClient.Close()
}

// --- ssh ---

type sshDialer struct {
	addr         string
	user         string
	identityFile string
	exclude      []string
	hostKey      ssh.HostKeyCallback
}

func newSSHDialer(spec ServerSpec, hostKey ssh.HostKeyCallback) sshDialer {
	return sshDialer{
		addr:         net.JoinHostPort(spec.Host, strconv.Itoa(spec.Port)),
		user:         spec.User,
		identityFile: spec.IdentityFile,
		exclude:      spec.Exclude,
		hostKey:      hostKey,
	}
}

func (d sshDialer) Open(ctx context.Context, name string) (Session, error) {
	auth, err := sshAuthMethods(d.identityFile)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User:            d.user,
		Auth:            auth,
		HostKeyCallback: d.hostKey,
		Timeout:         sshLoginTimeout,
		// Align priorities with OpenSSH. Go defaults to ECDSA over Ed25519,
		HostKeyAlgorithms: []string{
			ssh.KeyAlgoED25519,
			ssh.KeyAlgoECDSA256,
			ssh.KeyAlgoECDSA384,
			ssh.KeyAlgoECDSA521,
			ssh.KeyAlgoRSASHA256,
			ssh.KeyAlgoRSASHA512,
		},
	}

	dialer := &net.Dialer{Timeout: sshLoginTimeout, KeepAlive: keepaliveInterval}
	conn, err := dialer.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return nil, err
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, d.addr, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	client := ssh.NewClient(sshConn, chans, reqs)

	sess, err := client.NewSession()
	if err != nil {
		client.Close()
		return nil, err
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		client.Close()
		return nil, err
	}
	if err := sess.Start(strings.Join(journalCmd, " ")); err != nil {
		client.Close()
		return nil, err
	}

	s := &sshSession{
		metricsClient: newMetricsClient(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return client.Dial("tcp", metricsAddr)
		}),
		exclude: d.exclude,
		client:  client,
		journal: sess,
		stdout:  stdout,
		done:    make(chan struct{}),
	}
	go s.keepalive(name)
	return s, nil
}

type sshSession struct {
	metricsClient
	exclude []string
	client  *ssh.Client
	journal *ssh.Session
	stdout  io.Reader
	done    chan struct{}
}

// keepalive probes the peer like OpenSSH's ServerAliveInterval/CountMax. The SSH
// read loop has no deadline (golang/go#19338), so a silently dead peer would
// otherwise hang the journal reader until slow TCP timeouts fire.
func (s *sshSession) keepalive(name string) {
	t := time.NewTicker(keepaliveInterval)
	defer t.Stop()
	misses := 0
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			// SendRequest blocks until a reply or transport error, so bound it in
			// its own goroutine. errc is buffered so a late reply, or the error
			// from a concurrent Close, is always delivered and the goroutine exits.
			errc := make(chan error, 1)
			go func() {
				_, _, err := s.client.SendRequest("keepalive@openssh.com", true, nil)
				errc <- err
			}()
			select {
			case <-s.done:
				return
			case err := <-errc:
				if err != nil {
					slog.Error("keepalive failed, closing connection", "server", name, "err", err)
					s.client.Close()
					return
				}
				misses = 0
			case <-time.After(keepaliveTimeout):
				if misses++; misses >= keepaliveMaxMiss {
					slog.Error("keepalive timeout, closing connection", "server", name, "misses", misses)
					s.client.Close()
					return
				}
			}
		}
	}
}

func (s *sshSession) StreamJournal(ctx context.Context, journal *Journal, state *ServerState) error {
	// Closing the client unblocks the blocking Read when ctx is cancelled.
	stop := context.AfterFunc(ctx, func() { s.client.Close() })
	defer stop()
	return scanJournal(ctx, s.stdout, s.exclude, journal, state)
}

func (s *sshSession) RunCommand(ctx context.Context, cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	sess, err := s.client.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	stop := context.AfterFunc(ctx, func() { sess.Close() })
	defer stop()

	if err := sess.Run(cmd); err != nil && stdout.Len() == 0 && stderr.Len() == 0 {
		return "", err
	}
	if stdout.Len() > 0 {
		return stdout.String(), nil
	}
	return stderr.String(), nil
}

func (s *sshSession) Close() {
	close(s.done)
	s.client.Close()
	s.metricsClient.Close()
}

func sshAuthMethods(identityFile string) ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			slog.Warn("ssh agent dial failed", "err", err)
		} else {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		}
	}

	if identityFile != "" {
		key, err := os.ReadFile(identityFile)
		if err != nil {
			return nil, err
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, err
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}

	if len(methods) == 0 {
		return nil, fmt.Errorf("no ssh auth: set SSH_AUTH_SOCK or configure identity_file")
	}
	return methods, nil
}

const sshConfigGTimeout = 10 * time.Second

// sshParams holds the connection parameters OpenSSH resolves for a host alias.
type sshParams struct {
	hostName      string
	port          int
	user          string
	identityFiles []string
}

// resolveSSHConfig shells out to `ssh -G` so the real OpenSSH client resolves
// the effective configuration — Match (including exec), Include, and token
// expansion all handled exactly as a normal connection would. $SSH_CONFIG
// selects an alternate config file.
func resolveSSHConfig(alias string) (sshParams, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sshConfigGTimeout)
	defer cancel()

	args := []string{"-G"}
	if f := os.Getenv("SSH_CONFIG"); f != "" {
		args = append(args, "-F", expandUser(f))
	}
	args = append(args, alias)

	out, err := exec.CommandContext(ctx, "ssh", args...).Output()
	if err != nil {
		return sshParams{}, err
	}
	return parseSSHConfigG(out), nil
}

func parseSSHConfigG(out []byte) sshParams {
	var p sshParams
	for line := range strings.Lines(string(out)) {
		key, val, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		switch key { // ssh -G emits lowercased keywords
		case "hostname":
			p.hostName = val
		case "port":
			p.port, _ = strconv.Atoi(val)
		case "user":
			p.user = val
		case "identityfile":
			p.identityFiles = append(p.identityFiles, expandUser(val))
		}
	}
	return p
}

func firstExisting(paths []string) string {
	for _, p := range paths {
		if fileExists(p) {
			return p
		}
	}
	return ""
}

func loadHostKeyCallback(path string) (ssh.HostKeyCallback, error) {
	cb, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("known_hosts %s: %w", path, err)
	}
	return cb, nil
}
