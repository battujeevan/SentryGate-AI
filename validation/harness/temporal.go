package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// TemporalServer is a local Temporal dev server (the Temporal CLI's
// "server start-dev", in-memory, no Docker).
type TemporalServer struct {
	HostPort string
	Binary   string
	Version  string
	proc     *process
}

// temporalCacheDir is where the Temporal Go SDK test suite downloads the CLI.
func temporalCacheDir() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "sentrygate-validation", "temporal-cli")
}

// temporalBinary returns the Temporal CLI to run: TEMPORAL_CLI_PATH if set,
// otherwise the CLI cached by an earlier run, otherwise one downloaded now by
// the Temporal Go SDK.
func temporalBinary(ctx context.Context) (string, error) {
	if p := os.Getenv("TEMPORAL_CLI_PATH"); p != "" {
		return p, nil
	}
	dir := temporalCacheDir()
	if p := findCachedCLI(dir); p != "" {
		return p, nil
	}
	// The SDK downloads the CLI into dir and starts it; stop it again and run
	// the downloaded binary directly, so its output goes to a log file.
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{DestDir: dir},
		LogLevel:       "error",
	})
	if err != nil {
		return "", fmt.Errorf("download Temporal CLI: %w", err)
	}
	_ = srv.Stop()
	if p := findCachedCLI(dir); p != "" {
		return p, nil
	}
	return "", fmt.Errorf("Temporal CLI not found in %s after download", dir)
}

func findCachedCLI(dir string) string {
	matches, _ := filepath.Glob(filepath.Join(dir, "temporal-cli-go-sdk-*"))
	for _, m := range matches {
		if st, err := os.Stat(m); err == nil && !st.IsDir() && !strings.HasSuffix(m, ".zip") && !strings.HasSuffix(m, ".tar.gz") {
			return m
		}
	}
	return ""
}

// StartTemporal starts a dev server on a free loopback port.
func StartTemporal(ctx context.Context, logPath string) (*TemporalServer, error) {
	bin, err := temporalBinary(ctx)
	if err != nil {
		return nil, err
	}
	version := "unknown"
	if out, err := exec.CommandContext(ctx, bin, "--version").Output(); err == nil {
		version = strings.TrimSpace(string(out))
	}
	addr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	host, port, _ := net.SplitHostPort(addr)
	uiAddr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	_, uiPort, _ := net.SplitHostPort(uiAddr)
	proc, err := startProcess("temporal", logPath, nil, bin,
		"server", "start-dev", "--headless", "--ip", host, "--port", port, "--ui-port", uiPort, "--log-level", "error")
	if err != nil {
		return nil, err
	}
	ts := &TemporalServer{HostPort: addr, Binary: bin, Version: version, proc: proc}

	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := client.Dial(client.Options{HostPort: addr, Logger: quietLogger()})
		if err == nil {
			_, err = c.CheckHealth(ctx, &client.CheckHealthRequest{})
			c.Close()
			if err == nil {
				return ts, nil
			}
		}
		if proc.Exited() || time.Now().After(deadline) {
			proc.Stop()
			return nil, fmt.Errorf("Temporal dev server did not become ready on %s (log: %s): %v", addr, logPath, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Stop stops the dev server. Its state is in memory and is discarded.
func (t *TemporalServer) Stop() {
	if t != nil && t.proc != nil {
		t.proc.Stop()
	}
}
