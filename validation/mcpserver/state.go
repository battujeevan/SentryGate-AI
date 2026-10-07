package mcpserver

import (
	"context"
	"net/http"
)

// Customer is one row of the test database.
type Customer struct {
	ID      string `json:"customer_id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Tier    string `json:"tier"`
	Deleted bool   `json:"deleted"`
	Version int    `json:"version"`
}

// Execution is the record of one tools/call that reached the server.
type Execution struct {
	ExecutionID           string `json:"execution_id"`
	Timestamp             string `json:"timestamp"`
	AgentID               string `json:"agent_id"`
	Tool                  string `json:"tool"`
	Target                string `json:"target"`
	Request               string `json:"request"`
	RequestHash           string `json:"request_hash"`
	SentryGateRequestHash string `json:"sentrygate_request_hash"`
	IdempotencyKey        string `json:"idempotency_key"`
	ProposalID            string `json:"proposal_id"`
	Result                string `json:"result"`
	Detail                string `json:"detail"`
	MutationCount         int    `json:"mutation_count"`
}

// Export is one row written by export_customer.
type Export struct {
	CustomerID  string `json:"customer_id"`
	Destination string `json:"destination"`
	ExecutionID string `json:"execution_id"`
	ExportedAt  string `json:"exported_at"`
}

// TransportEvent records a fault injected at the transport (a dropped
// response) or a reconciliation query.
type TransportEvent struct {
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
	Detail    string `json:"detail"`
}

// State is a full snapshot of the test database.
type State struct {
	Customers       []Customer       `json:"customers"`
	Executions      []Execution      `json:"executions"`
	Exports         []Export         `json:"exports"`
	FencedKeys      []string         `json:"fenced_keys"`
	TransportEvents []TransportEvent `json:"transport_events"`
}

// Snapshot reads the whole database.
func (s *Server) Snapshot(ctx context.Context) (State, error) {
	st := State{Customers: []Customer{}, Executions: []Execution{}, Exports: []Export{}, FencedKeys: []string{}, TransportEvents: []TransportEvent{}}
	rows, err := s.db.QueryContext(ctx, `SELECT customer_id, name, email, tier, deleted, version FROM customers ORDER BY customer_id`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var c Customer
		if err := rows.Scan(&c.ID, &c.Name, &c.Email, &c.Tier, &c.Deleted, &c.Version); err != nil {
			rows.Close()
			return st, err
		}
		st.Customers = append(st.Customers, c)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT execution_id, timestamp, agent_id, tool, target, request, request_hash,
		sentrygate_request_hash, idempotency_key, proposal_id, result, detail, mutation_count FROM executions ORDER BY seq`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var e Execution
		if err := rows.Scan(&e.ExecutionID, &e.Timestamp, &e.AgentID, &e.Tool, &e.Target, &e.Request, &e.RequestHash,
			&e.SentryGateRequestHash, &e.IdempotencyKey, &e.ProposalID, &e.Result, &e.Detail, &e.MutationCount); err != nil {
			rows.Close()
			return st, err
		}
		st.Executions = append(st.Executions, e)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT customer_id, destination, execution_id, exported_at FROM exports ORDER BY export_id`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var x Export
		if err := rows.Scan(&x.CustomerID, &x.Destination, &x.ExecutionID, &x.ExportedAt); err != nil {
			rows.Close()
			return st, err
		}
		st.Exports = append(st.Exports, x)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT idempotency_key FROM fenced_keys ORDER BY idempotency_key`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return st, err
		}
		st.FencedKeys = append(st.FencedKeys, k)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT timestamp, event, detail FROM transport_events ORDER BY seq`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var ev TransportEvent
		if err := rows.Scan(&ev.Timestamp, &ev.Event, &ev.Detail); err != nil {
			return st, err
		}
		st.TransportEvents = append(st.TransportEvents, ev)
	}
	return st, rows.Err()
}

func (s *Server) serveState(w http.ResponseWriter, r *http.Request) {
	st, err := s.Snapshot(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, st)
}

// Counts summarises executions.
type Counts struct {
	Invocations int            `json:"invocations"`
	Applied     int            `json:"applied"`
	Mutations   int            `json:"mutations"`
	ByTool      map[string]int `json:"invocations_by_tool"`
	ByTarget    map[string]int `json:"invocations_by_target"`
	ByKey       map[string]int `json:"invocations_by_idempotency_key"`
	MutByTarget map[string]int `json:"mutations_by_target"`
}

// Count summarises the executions in st.
func (st State) Count() Counts {
	c := Counts{ByTool: map[string]int{}, ByTarget: map[string]int{}, ByKey: map[string]int{}, MutByTarget: map[string]int{}}
	for _, e := range st.Executions {
		c.Invocations++
		if e.Result == ResultApplied {
			c.Applied++
		}
		c.Mutations += e.MutationCount
		c.ByTool[e.Tool]++
		c.ByTarget[e.Target]++
		key := e.IdempotencyKey
		if key == "" {
			key = "(none)"
		}
		c.ByKey[key]++
		c.MutByTarget[e.Target] += e.MutationCount
	}
	return c
}
