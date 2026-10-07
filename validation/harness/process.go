// Package harness runs the components under validation as real processes: the
// Temporal dev server, the SentryGate proxy and worker, and the test MCP
// server. It provides each test case with a disposable stack and with the
// observations its evidence is built from.
package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// process is a child process whose output goes to a log file.
type process struct {
	name string
	cmd  *exec.Cmd
	log  *os.File
	done chan struct{}
	once sync.Once
}

func startProcess(name, logPath string, env []string, bin string, args ...string) (*process, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &process{name: name, cmd: cmd, log: logFile, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

// Exited reports whether the process has exited.
func (p *process) Exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// Stop kills the process and waits for it to exit. On Windows there is no
// graceful signal, so every stop is abrupt; the harness only stops processes
// when that is what the test requires or when the test is over.
func (p *process) Stop() {
	p.once.Do(func() {
		if !p.Exited() {
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
		}
		_ = p.log.Close()
	})
}

// freeAddr returns a loopback address with a port that was free when checked.
func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// waitHTTP polls url until it answers 200, the process exits or ctx is done.
func waitHTTP(ctx context.Context, p *process, url string) error {
	client := &http.Client{Timeout: time.Second}
	for {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if p != nil && p.Exited() {
			return fmt.Errorf("%s exited before it was ready (see its log)", p.name)
		}
		select {
		case <-ctx.Done():
			return errors.Join(fmt.Errorf("%s not ready at %s", nameOf(p), url), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func nameOf(p *process) string {
	if p == nil {
		return "service"
	}
	return p.name
}
