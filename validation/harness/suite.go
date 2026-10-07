package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
)

// Options configures where the suite runs and how a stack is wired.
type Options struct {
	// RepoRoot is the module root; binaries are built from it.
	RepoRoot string
	// RunDir holds binaries, databases and process logs. It is never part
	// of the evidence.
	RunDir string
	// PolicyV1 and PolicyV2 are the validation policies.
	PolicyV1, PolicyV2 string

	// MCPListen fixes the test MCP server's address (default: a free port).
	// Set it when an external gateway must be configured to forward to it.
	MCPListen string
	// WorkerMCPURL overrides the URL the worker's MCP adapter calls, for
	// example a gateway route in front of the test MCP server. Default: the
	// test MCP server directly.
	WorkerMCPURL string
	// IngressURL overrides the base URL the test agent sends ingress
	// requests to, for example a gateway in front of SentryGate. Default:
	// the SentryGate proxy directly.
	IngressURL string
}

// Environment describes what the suite ran against. It is written to the
// evidence of every test.
type Environment struct {
	GoVersion       string `json:"go_version"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	TemporalVersion string `json:"temporal_version"`
	TemporalMode    string `json:"temporal_mode"`
	Adapter         string `json:"sentrygate_worker_adapter"`
	MCPServer       string `json:"mcp_server"`
	IngressPath     string `json:"ingress_path"`
	WorkerMCPPath   string `json:"worker_mcp_path"`
	SourceRevision  string `json:"source_revision"`
}

// Suite owns the binaries and the Temporal dev server shared by all tests.
type Suite struct {
	Opts     Options
	Env      Environment
	Temporal *TemporalServer
	bins     map[string]string
	runID    string
}

// NewSuite builds the binaries and starts the Temporal dev server.
func NewSuite(ctx context.Context, opts Options) (*Suite, error) {
	if err := os.MkdirAll(opts.RunDir, 0o755); err != nil {
		return nil, err
	}
	s := &Suite{Opts: opts, bins: map[string]string{}, runID: time.Now().UTC().Format("20060102T150405")}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	for name, pkg := range map[string]string{
		"sentrygate":      "./cmd/sentrygate",
		"worker":          "./cmd/worker",
		"mcp-test-server": "./validation/cmd/mcp-test-server",
	} {
		out := filepath.Join(opts.RunDir, "bin", name+ext)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", out, pkg)
		cmd.Dir = opts.RepoRoot
		if b, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("build %s: %v\n%s", pkg, err, b)
		}
		s.bins[name] = out
	}
	t, err := StartTemporal(ctx, filepath.Join(opts.RunDir, "logs", "temporal.log"))
	if err != nil {
		return nil, err
	}
	s.Temporal = t
	s.Env = Environment{
		GoVersion:       runtime.Version(),
		OS:              runtime.GOOS,
		Arch:            runtime.GOARCH,
		TemporalVersion: t.Version,
		TemporalMode:    "Temporal CLI dev server (server start-dev, in-memory), started by the harness",
		Adapter:         "mcp (workflows.MCPAdapter, SENTRYGATE_ADAPTER=mcp)",
		MCPServer:       "validation/cmd/mcp-test-server (disposable SQLite database per test)",
		IngressPath:     "test agent -> SentryGate proxy (direct, no gateway)",
		WorkerMCPPath:   "SentryGate worker -> test MCP server (direct, no gateway)",
		SourceRevision:  sourceRevision(ctx, opts.RepoRoot),
	}
	if opts.IngressURL != "" {
		s.Env.IngressPath = "test agent -> " + opts.IngressURL + " (operator-supplied override)"
	}
	if opts.WorkerMCPURL != "" {
		s.Env.WorkerMCPPath = "SentryGate worker -> " + opts.WorkerMCPURL + " (operator-supplied override)"
	}
	return s, nil
}

// Close stops the Temporal dev server.
func (s *Suite) Close() { s.Temporal.Stop() }

// sourceRevision returns the git commit and whether the tree had local
// changes, or "unknown".
func sourceRevision(ctx context.Context, root string) string {
	rev, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	r := strings.TrimSpace(string(rev))
	if st, err := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(strings.TrimSpace(string(st))) > 0 {
		r += " (with uncommitted changes)"
	}
	return r
}

// Dial returns a Temporal client for the suite's dev server.
func (s *Suite) Dial() (client.Client, error) {
	return client.Dial(client.Options{HostPort: s.Temporal.HostPort, Logger: quietLogger()})
}

func quietLogger() tlog.Logger {
	return tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}
