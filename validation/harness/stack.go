package harness

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/validation/agent"
	"github.com/battujeevan/SentryGate-AI/validation/mcpserver"
)

// Validation agents. Their API keys are generated per stack and exist only in
// memory and in the proxy's environment.
const (
	AgentA = "agent-a"
	AgentB = "agent-b"
)

// Stack is one disposable deployment: its own task queue, audit database,
// policy file, MCP database and processes. Only the Temporal dev server is
// shared between stacks.
type Stack struct {
	TestID     string
	Dir        string
	TaskQueue  string
	PolicyPath string
	MCPURL     string
	IngressURL string
	Agents     map[string]agent.Identity

	Temporal client.Client
	Audit    *audit.Store

	suite      *Suite
	proxyAddr  string
	healthAddr string
	mcpAddr    string
	auditDB    string
	keySpec    string
	proxy      *process
	worker     *process
	mcp        *process
}

// NewStack starts the test MCP server, the proxy and the worker for testID,
// with policy V1.
func (s *Suite) NewStack(ctx context.Context, testID string) (*Stack, error) {
	dir := filepath.Join(s.Opts.RunDir, s.runID, testID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st := &Stack{
		TestID:     testID,
		Dir:        dir,
		TaskQueue:  "validation-" + strings.ToLower(testID) + "-" + s.runID,
		PolicyPath: filepath.Join(dir, "policy.yaml"),
		auditDB:    filepath.Join(dir, "audit.db"),
		suite:      s,
		Agents:     map[string]agent.Identity{},
	}
	var specs []string
	for _, id := range []string{AgentA, AgentB} {
		key := "vk_" + rand.Text()
		st.Agents[id] = agent.Identity{AgentID: id, Key: key}
		specs = append(specs, id+":"+key)
	}
	st.keySpec = strings.Join(specs, ",")
	if err := st.SetPolicy(s.Opts.PolicyV1); err != nil {
		return nil, err
	}

	ok := false
	defer func() {
		if !ok {
			st.Close()
		}
	}()
	var err error
	if st.mcpAddr = s.Opts.MCPListen; st.mcpAddr == "" {
		if st.mcpAddr, err = freeAddr(); err != nil {
			return nil, err
		}
	}
	if st.proxyAddr, err = freeAddr(); err != nil {
		return nil, err
	}
	if st.healthAddr, err = freeAddr(); err != nil {
		return nil, err
	}
	st.MCPURL = "http://" + st.mcpAddr + "/mcp"
	st.IngressURL = "http://" + st.proxyAddr
	if s.Opts.IngressURL != "" {
		st.IngressURL = strings.TrimRight(s.Opts.IngressURL, "/")
	}

	st.mcp, err = startProcess("mcp-test-server", filepath.Join(dir, "mcp-test-server.log"), nil, s.bins["mcp-test-server"],
		"-addr", st.mcpAddr, "-db", filepath.Join(dir, "mcp.db"))
	if err != nil {
		return nil, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := waitHTTP(readyCtx, st.mcp, "http://"+st.mcpAddr+"/healthz"); err != nil {
		return nil, err
	}

	st.proxy, err = startProcess("sentrygate", filepath.Join(dir, "sentrygate.log"), append(st.commonEnv(),
		"SENTRYGATE_ADDR="+st.proxyAddr,
		"SENTRYGATE_AGENT_KEYS="+st.keySpec,
	), s.bins["sentrygate"])
	if err != nil {
		return nil, err
	}
	if err := waitHTTP(readyCtx, st.proxy, "http://"+st.proxyAddr+"/readyz"); err != nil {
		return nil, err
	}
	if err := st.StartWorker(ctx); err != nil {
		return nil, err
	}
	if st.Audit, err = audit.Open(st.auditDB); err != nil {
		return nil, err
	}
	if st.Temporal, err = s.Dial(); err != nil {
		return nil, err
	}
	ok = true
	return st, nil
}

func (st *Stack) commonEnv() []string {
	return []string{
		"TEMPORAL_HOST_PORT=" + st.suite.Temporal.HostPort,
		"TEMPORAL_TASK_QUEUE=" + st.TaskQueue,
		"SENTRYGATE_POLICY_PATH=" + st.PolicyPath,
		"SENTRYGATE_AUDIT_DB=" + st.auditDB,
		"SENTRYGATE_POLICY_RELOAD=1s",
		"OTEL_EXPORTER=none",
	}
}

// StartWorker starts the SentryGate worker with the MCP adapter.
func (st *Stack) StartWorker(ctx context.Context) error {
	if st.worker != nil && !st.worker.Exited() {
		return errors.New("worker already running")
	}
	mcpURL := st.MCPURL
	if st.suite.Opts.WorkerMCPURL != "" {
		mcpURL = st.suite.Opts.WorkerMCPURL
	}
	w, err := startProcess("worker", filepath.Join(st.Dir, "worker.log"), append(st.commonEnv(),
		"SENTRYGATE_ADAPTER=mcp",
		"SENTRYGATE_MCP_URL="+mcpURL,
		"SENTRYGATE_MCP_TARGET_ARGUMENT=customer_id",
		"SENTRYGATE_WORKER_HEALTH_ADDR="+st.healthAddr,
	), st.suite.bins["worker"])
	if err != nil {
		return err
	}
	st.worker = w
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return waitHTTP(readyCtx, w, "http://"+st.healthAddr+"/readyz")
}

// StopWorker kills the worker. Workflows started while it is stopped wait in
// the task queue.
func (st *Stack) StopWorker() {
	if st.worker != nil {
		st.worker.Stop()
	}
}

// SetPolicy copies the policy file at src into the stack's policy path. The
// proxy reloads it within a second; the worker reads it at start and reloads
// it within a second while running.
func (st *Stack) SetPolicy(src string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	tmp := st.PolicyPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, st.PolicyPath)
}

// PolicyV2 returns the path of validation policy V2.
func (st *Stack) PolicyV2() string { return st.suite.Opts.PolicyV2 }

// PolicyStatus is the proxy's view of its active policy (GET /v1/policy).
type PolicyStatus struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// ProxyPolicy returns the proxy's active policy.
func (st *Stack) ProxyPolicy(ctx context.Context) (PolicyStatus, error) {
	var ps PolicyStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+st.proxyAddr+"/v1/policy", nil)
	if err != nil {
		return ps, err
	}
	agent.Authorize(req, st.Agents[AgentA])
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ps, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ps, fmt.Errorf("GET /v1/policy: %s", resp.Status)
	}
	return ps, json.NewDecoder(resp.Body).Decode(&ps)
}

// WaitProxyPolicy waits until the proxy reports the given policy version.
func (st *Stack) WaitProxyPolicy(ctx context.Context, version string) (PolicyStatus, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		ps, err := st.ProxyPolicy(ctx)
		if err == nil && ps.Version == version {
			return ps, nil
		}
		if time.Now().After(deadline) {
			return ps, fmt.Errorf("proxy did not load policy %s (last: %+v, %v)", version, ps, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Infrastructure returns the test MCP server's database state.
func (st *Stack) Infrastructure(ctx context.Context) (mcpserver.State, error) {
	var state mcpserver.State
	err := st.getJSON(ctx, "http://"+st.mcpAddr+"/state", &state)
	return state, err
}

// GateWaiting returns how many calls are held at the gated target.
func (st *Stack) GateWaiting(ctx context.Context) (int, error) {
	var g struct {
		Waiting int `json:"waiting"`
	}
	err := st.getJSON(ctx, "http://"+st.mcpAddr+"/gate", &g)
	return g.Waiting, err
}

// ReleaseGate lets calls held at the gated target proceed.
func (st *Stack) ReleaseGate(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+st.mcpAddr+"/gate/release", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("release gate: %s", resp.Status)
	}
	return nil
}

func (st *Stack) getJSON(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: %s %s", url, resp.Status, b)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// Gateway returns the test agent's ingress client for the stack.
func (st *Stack) Gateway() *agent.Gateway {
	return &agent.Gateway{BaseURL: st.IngressURL, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// Boundary returns the test agent's execution-boundary client: direct access
// to Temporal, bypassing the proxy.
func (st *Stack) Boundary() *agent.Boundary {
	return &agent.Boundary{Temporal: st.Temporal, TaskQueue: st.TaskQueue}
}

// Close stops the stack's processes and closes its clients.
func (st *Stack) Close() {
	if st.Temporal != nil {
		st.Temporal.Close()
	}
	if st.Audit != nil {
		_ = st.Audit.Close()
	}
	for _, p := range []*process{st.worker, st.proxy, st.mcp} {
		if p != nil {
			p.Stop()
		}
	}
}
