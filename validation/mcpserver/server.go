// Package mcpserver is a deterministic MCP test server backed by a disposable
// SQLite database. It is the system the validation suite measures: every
// tools/call that reaches it is recorded, so the evidence can show whether an
// operation happened zero, one or more times.
//
// It deliberately does not deduplicate by idempotency key. A second delivery
// of the same key is applied and recorded again, so a duplicate dispatch by
// the caller is visible instead of being masked.
package mcpserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// Tool names.
const (
	ToolRead   = "read_customer"
	ToolUpdate = "update_customer"
	ToolExport = "export_customer"
	ToolDelete = "delete_customer"
	// ToolStatus answers reconciliation queries. It is not an infrastructure
	// action and is not recorded as an execution.
	ToolStatus = "sentrygate_execution_status"
)

// _meta keys read from tools/call. They match the keys SentryGate's MCP
// adapter sends (workflows.MCPMeta*); a caller that sends none is recorded
// with empty values.
const (
	metaIdempotencyKey = "io.sentrygate/idempotency_key"
	metaRequestHash    = "io.sentrygate/request_hash"
	metaAgentID        = "io.sentrygate/agent_id"
	metaProposalID     = "io.sentrygate/proposal_id"
)

// Execution results recorded per tools/call.
const (
	ResultApplied  = "APPLIED"
	ResultRejected = "REJECTED"
	ResultFenced   = "REJECTED_FENCED"
	ResultAborted  = "ABORTED_BEFORE_APPLY"
)

// Config configures a Server.
type Config struct {
	DBPath string
	// TargetArgument is the tool argument that names the customer.
	TargetArgument string
	// LostResponseTargets: calls on these targets are applied and recorded,
	// then the connection is closed without a response.
	LostResponseTargets []string
	// GatedTargets: calls on these targets wait until Release is called (or
	// the call is cancelled) before they are applied.
	GatedTargets []string
}

// Server is the MCP test server.
type Server struct {
	cfg  Config
	db   *sql.DB
	gate gate
	http http.Handler
}

// Seed customers present in every fresh database.
var seed = []Customer{
	{ID: "customer-123", Name: "Ada Example", Email: "ada@example.test", Tier: "standard"},
	{ID: "customer-456", Name: "Grace Example", Email: "grace@example.test", Tier: "gold"},
	{ID: "customer-lost-response", Name: "Lost Response", Email: "lost@example.test", Tier: "standard"},
	{ID: "customer-gated", Name: "Gated Target", Email: "gated@example.test", Tier: "standard"},
}

// New opens (or creates) the database and returns a ready server.
func New(cfg Config) (*Server, error) {
	if cfg.DBPath == "" || cfg.TargetArgument == "" {
		return nil, errors.New("mcpserver: DBPath and TargetArgument are required")
	}
	db, err := sql.Open("sqlite", "file:"+cfg.DBPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Server{cfg: cfg, db: db, gate: gate{released: make(chan struct{})}}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}

	srv := mcp.NewServer(&mcp.Implementation{Name: "sentrygate-validation-mcp", Version: "1"}, nil)
	schema := json.RawMessage(`{"type":"object"}`)
	for _, t := range []struct{ name, desc string }{
		{ToolRead, "Read a customer record. Arguments: " + cfg.TargetArgument + "."},
		{ToolUpdate, "Update a customer's name, email or tier. Arguments: " + cfg.TargetArgument + ", name, email, tier."},
		{ToolExport, "Export a customer record to a test sink. Arguments: " + cfg.TargetArgument + ", destination."},
		{ToolDelete, "Delete a customer record. Arguments: " + cfg.TargetArgument + "."},
	} {
		name := t.name
		srv.AddTool(&mcp.Tool{Name: name, Description: t.desc, InputSchema: schema},
			func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return s.execute(ctx, name, req)
			})
	}
	srv.AddTool(&mcp.Tool{Name: ToolStatus, Description: "Report what happened to an idempotency key; fence=true refuses it from now on if it never arrived.", InputSchema: schema}, s.status)

	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	mux := http.NewServeMux()
	mux.Handle("/mcp", s.dropLostResponses(mcpHandler))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /state", s.serveState)
	mux.HandleFunc("GET /gate", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]int{"waiting": s.gate.Waiting()})
	})
	mux.HandleFunc("POST /gate/release", func(w http.ResponseWriter, _ *http.Request) {
		s.gate.Release()
		writeJSON(w, map[string]bool{"released": true})
	})
	s.http = mux
	return s, nil
}

// Handler serves /mcp (MCP Streamable HTTP) and the harness endpoints
// /healthz, /state, /gate and /gate/release.
func (s *Server) Handler() http.Handler { return s.http }

// Close closes the database.
func (s *Server) Close() error { return s.db.Close() }

func (s *Server) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS customers (
			customer_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			tier TEXT NOT NULL,
			deleted INTEGER NOT NULL DEFAULT 0,
			version INTEGER NOT NULL DEFAULT 1)`,
		`CREATE TABLE IF NOT EXISTS executions (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_id TEXT NOT NULL DEFAULT '',
			timestamp TEXT NOT NULL,
			agent_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			target TEXT NOT NULL,
			request TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			sentrygate_request_hash TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			proposal_id TEXT NOT NULL,
			result TEXT NOT NULL,
			detail TEXT NOT NULL,
			mutation_count INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS exports (
			export_id INTEGER PRIMARY KEY AUTOINCREMENT,
			customer_id TEXT NOT NULL,
			destination TEXT NOT NULL,
			execution_id TEXT NOT NULL,
			exported_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS fenced_keys (
			idempotency_key TEXT PRIMARY KEY,
			fenced_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS transport_events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL,
			event TEXT NOT NULL,
			detail TEXT NOT NULL)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("mcpserver: migrate: %w", err)
		}
	}
	for _, c := range seed {
		if _, err := s.db.Exec(`INSERT OR IGNORE INTO customers (customer_id, name, email, tier) VALUES (?, ?, ?, ?)`,
			c.ID, c.Name, c.Email, c.Tier); err != nil {
			return fmt.Errorf("mcpserver: seed: %w", err)
		}
	}
	return nil
}

// call is one decoded tools/call.
type call struct {
	tool, target                     string
	args                             map[string]any
	request, requestHash             string
	agentID, key, sgHash, proposalID string
}

func decodeCall(tool, targetArg string, req *mcp.CallToolRequest) (call, error) {
	c := call{tool: tool, args: map[string]any{}}
	meta := req.Params.GetMeta()
	c.agentID, _ = meta[metaAgentID].(string)
	c.key, _ = meta[metaIdempotencyKey].(string)
	c.sgHash, _ = meta[metaRequestHash].(string)
	c.proposalID, _ = meta[metaProposalID].(string)
	if len(req.Params.Arguments) > 0 {
		if err := json.Unmarshal(req.Params.Arguments, &c.args); err != nil || c.args == nil {
			c.args = map[string]any{}
			c.request = string(req.Params.Arguments)
			c.requestHash = hashRequest(tool, c.request)
			return c, errors.New("arguments are not a JSON object")
		}
	}
	canonical, _ := json.Marshal(c.args) // map keys are sorted
	c.request = string(canonical)
	c.requestHash = hashRequest(tool, c.request)
	c.target, _ = c.args[targetArg].(string)
	if c.target == "" {
		return c, fmt.Errorf("argument %q is required", targetArg)
	}
	return c, nil
}

// hashRequest is the SHA-256 of the tool name and the canonical arguments the
// server actually received.
func hashRequest(tool, args string) string {
	sum := sha256.Sum256([]byte(tool + "\n" + args))
	return hex.EncodeToString(sum[:])
}

func (s *Server) execute(ctx context.Context, tool string, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	c, err := decodeCall(tool, s.cfg.TargetArgument, req)
	if err != nil {
		return s.finish(ctx, c, ResultRejected, err.Error(), 0, nil)
	}
	if slices.Contains(s.cfg.GatedTargets, c.target) {
		if err := s.gate.Wait(ctx); err != nil {
			return s.finish(context.WithoutCancel(ctx), c, ResultAborted, "call ended while waiting at the gate: "+err.Error(), 0, nil)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if c.key != "" {
		var fenced int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fenced_keys WHERE idempotency_key = ?`, c.key).Scan(&fenced); err != nil {
			return nil, err
		}
		if fenced > 0 {
			return s.record(ctx, tx, c, ResultFenced, "idempotency key was fenced by a reconciliation query", 0, nil)
		}
	}

	var cust Customer
	err = tx.QueryRowContext(ctx, `SELECT customer_id, name, email, tier, deleted, version FROM customers WHERE customer_id = ?`, c.target).
		Scan(&cust.ID, &cust.Name, &cust.Email, &cust.Tier, &cust.Deleted, &cust.Version)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && cust.Deleted) {
		return s.record(ctx, tx, c, ResultRejected, "customer not found", 0, nil)
	}
	if err != nil {
		return nil, err
	}

	switch tool {
	case ToolRead:
		return s.record(ctx, tx, c, ResultApplied, "customer read", 0, &cust)
	case ToolUpdate:
		sets, vals, err := updateFields(c.args, s.cfg.TargetArgument)
		if err != nil {
			return s.record(ctx, tx, c, ResultRejected, err.Error(), 0, nil)
		}
		res, err := tx.ExecContext(ctx, `UPDATE customers SET `+strings.Join(sets, ", ")+`, version = version + 1 WHERE customer_id = ? AND deleted = 0`,
			append(vals, c.target)...)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		return s.record(ctx, tx, c, ResultApplied, "customer updated", int(n), nil)
	case ToolExport:
		dest, _ := c.args["destination"].(string)
		if dest == "" {
			dest = "local-test-sink"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO exports (customer_id, destination, execution_id, exported_at) VALUES (?, ?, '', ?)`,
			c.target, dest, now()); err != nil {
			return nil, err
		}
		return s.record(ctx, tx, c, ResultApplied, "customer exported to "+dest, 1, &cust)
	case ToolDelete:
		res, err := tx.ExecContext(ctx, `UPDATE customers SET deleted = 1, version = version + 1 WHERE customer_id = ? AND deleted = 0`, c.target)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		return s.record(ctx, tx, c, ResultApplied, "customer deleted", int(n), nil)
	}
	return s.record(ctx, tx, c, ResultRejected, "unknown tool", 0, nil)
}

func updateFields(args map[string]any, targetArg string) ([]string, []any, error) {
	var keys []string
	for k := range args {
		if k != targetArg {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var sets []string
	var vals []any
	for _, k := range keys {
		switch k {
		case "name", "email", "tier":
			v, ok := args[k].(string)
			if !ok || v == "" {
				return nil, nil, fmt.Errorf("field %q must be a non-empty string", k)
			}
			sets = append(sets, k+" = ?")
			vals = append(vals, v)
		default:
			return nil, nil, fmt.Errorf("field %q cannot be updated", k)
		}
	}
	if len(sets) == 0 {
		return nil, nil, errors.New("no fields to update")
	}
	return sets, vals, nil
}

// finish records an execution outside a tool transaction.
func (s *Server) finish(ctx context.Context, c call, result, detail string, mutations int, cust *Customer) (*mcp.CallToolResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return s.record(ctx, tx, c, result, detail, mutations, cust)
}

// record writes the execution row in tx, commits, and builds the tool result.
func (s *Server) record(ctx context.Context, tx *sql.Tx, c call, result, detail string, mutations int, cust *Customer) (*mcp.CallToolResult, error) {
	res, err := tx.ExecContext(ctx, `INSERT INTO executions
		(timestamp, agent_id, tool, target, request, request_hash, sentrygate_request_hash, idempotency_key, proposal_id, result, detail, mutation_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		now(), c.agentID, c.tool, c.target, c.request, c.requestHash, c.sgHash, c.key, c.proposalID, result, detail, mutations)
	if err != nil {
		return nil, err
	}
	seq, _ := res.LastInsertId()
	execID := fmt.Sprintf("x%06d", seq)
	if _, err := tx.ExecContext(ctx, `UPDATE executions SET execution_id = ? WHERE seq = ?`, execID, seq); err != nil {
		return nil, err
	}
	if c.tool == ToolExport && result == ResultApplied {
		if _, err := tx.ExecContext(ctx, `UPDATE exports SET execution_id = ? WHERE execution_id = ''`, execID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	status := "SUCCESS"
	if result != ResultApplied {
		status = "FAILURE"
	}
	out := map[string]any{
		"status": status, "result": result, "execution_id": execID, "tool": c.tool,
		"target": c.target, "mutation_count": mutations, "detail": detail,
	}
	if cust != nil {
		out["customer"] = cust
	}
	text, _ := json.Marshal(out)
	return &mcp.CallToolResult{
		IsError:           status != "SUCCESS",
		StructuredContent: out,
		Content:           []mcp.Content{&mcp.TextContent{Text: string(text)}},
	}, nil
}

func (s *Server) status(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var in struct {
		IdempotencyKey string `json:"idempotency_key"`
		Fence          bool   `json:"fence"`
	}
	if err := json.Unmarshal(req.Params.Arguments, &in); err != nil || in.IdempotencyKey == "" {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "idempotency_key is required"}}}, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var applied, invocations, mutations int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(result = ?), 0), COALESCE(SUM(mutation_count), 0)
		FROM executions WHERE idempotency_key = ?`, ResultApplied, in.IdempotencyKey).Scan(&invocations, &applied, &mutations); err != nil {
		return nil, err
	}
	state := "NOT_FOUND"
	switch {
	case applied > 0:
		state = "APPLIED"
	case invocations > 0:
		state = "REJECTED"
	case in.Fence:
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO fenced_keys (idempotency_key, fenced_at) VALUES (?, ?)`, in.IdempotencyKey, now()); err != nil {
			return nil, err
		}
		state = "FENCED"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO transport_events (timestamp, event, detail) VALUES (?, 'STATUS_QUERY', ?)`,
		now(), fmt.Sprintf("key=%s fence=%t state=%s", in.IdempotencyKey, in.Fence, state)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	out := map[string]any{"state": state, "invocation_count": invocations, "mutation_count": mutations}
	text, _ := json.Marshal(out)
	return &mcp.CallToolResult{StructuredContent: out, Content: []mcp.Content{&mcp.TextContent{Text: string(text)}}}, nil
}

// dropLostResponses runs tools/call requests on a lost-response target to
// completion, then closes the connection instead of sending the response.
func (s *Server) dropLostResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.cfg.LostResponseTargets) == 0 || r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var msg struct {
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		target := ""
		if json.Unmarshal(body, &msg) == nil && msg.Method == "tools/call" {
			target, _ = msg.Params.Arguments[s.cfg.TargetArgument].(string)
		}
		if !slices.Contains(s.cfg.LostResponseTargets, target) {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(httptest.NewRecorder(), r)
		_, _ = s.db.Exec(`INSERT INTO transport_events (timestamp, event, detail) VALUES (?, 'RESPONSE_DROPPED', ?)`,
			now(), fmt.Sprintf("tool=%s target=%s: call processed, connection closed without a response", msg.Params.Name, target))
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
	})
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type gate struct {
	mu       sync.Mutex
	waiting  int
	released chan struct{}
	once     sync.Once
}

func (g *gate) Wait(ctx context.Context) error {
	g.mu.Lock()
	g.waiting++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.waiting--
		g.mu.Unlock()
	}()
	select {
	case <-g.released:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) Waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting
}

func (g *gate) Release() { g.once.Do(func() { close(g.released) }) }
