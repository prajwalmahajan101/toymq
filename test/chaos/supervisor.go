//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	supervisorBootPollInterval = 50 * time.Millisecond
	supervisorBootTimeout      = 5 * time.Second
	supervisorKillGrace        = 2 * time.Second
)

// supervisor owns the broker subprocess lifecycle: spawn, wait for
// it to bind, kill, wait for exit, spawn again. The kill cadence
// is driven by run(); spawn/kill outside the loop is the test's
// concern (setup before producer/consumer start, final teardown).
type supervisor struct {
	binaryPath string
	dataDir    string
	addr       string
	stderr     io.Writer

	mu  sync.Mutex
	cmd *exec.Cmd

	restarts atomic.Int64
}

func newSupervisor(binaryPath, dataDir, addr string, stderr io.Writer) *supervisor {
	return &supervisor{
		binaryPath: binaryPath,
		dataDir:    dataDir,
		addr:       addr,
		stderr:     stderr,
	}
}

// start launches the broker and blocks until it accepts TCP. Returns
// an error if the bind never completes within supervisorBootTimeout.
func (s *supervisor) start() error {
	cmd := exec.Command(s.binaryPath,
		"-addr", s.addr,
		"-data-dir", s.dataDir,
		"-shutdown-timeout", "1s",
	)
	cmd.Stderr = s.stderr
	cmd.Stdout = s.stderr // merge both into the chaos test's stderr capture
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start broker: %w", err)
	}

	s.mu.Lock()
	s.cmd = cmd
	s.mu.Unlock()

	deadline := time.Now().Add(supervisorBootTimeout)
	for {
		conn, err := net.DialTimeout("tcp", s.addr, supervisorBootPollInterval)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			_ = s.kill()
			return fmt.Errorf("broker did not bind %s within %s: %w", s.addr, supervisorBootTimeout, err)
		}
		time.Sleep(supervisorBootPollInterval)
	}
}

// kill sends SIGKILL and waits for the process to exit. Idempotent.
func (s *supervisor) kill() error {
	s.mu.Lock()
	cmd := s.cmd
	s.cmd = nil
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("sigkill: %w", err)
	}

	// Wait may return a non-nil err (we asked for SIGKILL); swallow.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(supervisorKillGrace):
		return errors.New("broker did not exit after SIGKILL within grace")
	}
}

// run alternates between "broker up for interval" and "kill + restart"
// until ctx is cancelled. The broker must already be running when run
// is called; cancellation leaves the broker running (the test will
// kill it during final teardown).
func (s *supervisor) run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.kill(); err != nil {
				return fmt.Errorf("supervisor kill: %w", err)
			}
			if err := s.start(); err != nil {
				return fmt.Errorf("supervisor restart: %w", err)
			}
			s.restarts.Add(1)
		}
	}
}

func (s *supervisor) restartCount() int64 {
	return s.restarts.Load()
}
