// Command agent-sim plays the role of an AI agent calling SentryGate. Each
// scenario submits one proposal and prints the response. With -check it exits
// non-zero if any response, decision record or workflow outcome is unexpected.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

type scenario struct {
	name       string
	title      string
	cmd        contracts.CommandType
	target     string
	payload    string
	wantStatus int
	wantVerd   contracts.Verdict
	// wantPhase/wantPhaseVerdict: audit phase that must appear when the workflow finishes.
	wantPhase        contracts.AuditPhase
	wantPhaseVerdict contracts.AuditVerdict
}

var scenarios = []scenario{
	{name: "protected-delete", title: "DENY: delete on protected ROOT_CORE_EDGE",
		cmd: contracts.CmdDeletePolicy, target: contracts.RootCoreEdgeID, payload: `{"reason":"cleanup"}`,
		wantStatus: http.StatusForbidden, wantVerd: contracts.VerdictDeny},
	{name: "protected-modify", title: "DENY: routing change on protected ROOT_CORE_EDGE",
		cmd: contracts.CmdModifyRouting, target: contracts.RootCoreEdgeID, payload: `{"route":"bypass"}`,
		wantStatus: http.StatusForbidden, wantVerd: contracts.VerdictDeny},
	{name: "unknown-command", title: "DENY: command not in the catalogue",
		cmd: "REBOOT_ALL", target: "edge-node-west-1", payload: `{}`,
		wantStatus: http.StatusForbidden, wantVerd: contracts.VerdictDeny},
	{name: "unregistered-target", title: "DENY: target not in the registry",
		cmd: contracts.CmdModifyRouting, target: "shadow-edge-9", payload: `{"route":"stable"}`,
		wantStatus: http.StatusForbidden, wantVerd: contracts.VerdictDeny},
	{name: "approval-required", title: "REQUIRE_APPROVAL: production change is not executed",
		cmd: contracts.CmdUpdateCert, target: "prod-payments-edge", payload: `{"cert":"mock"}`,
		wantStatus: http.StatusForbidden, wantVerd: contracts.VerdictRequireApproval},
	{name: "allowed", title: "ALLOW: staging routing change runs the workflow",
		cmd: contracts.CmdModifyRouting, target: "edge-node-west-1", payload: `{"route":"stable"}`,
		wantStatus: http.StatusOK, wantVerd: contracts.VerdictAllow,
		wantPhase: contracts.AuditPhaseWorkflowComplete, wantPhaseVerdict: contracts.AuditVerdictPass},
	{name: "rollback", title: "ALLOW then simulated partial dispatch failure and compensation",
		cmd: contracts.CmdUpdateCert, target: contracts.FailingNodeID, payload: `{"cert":"mock"}`,
		wantStatus: http.StatusOK, wantVerd: contracts.VerdictAllow,
		wantPhase: contracts.AuditPhaseCompensation, wantPhaseVerdict: contracts.AuditVerdictPass},
	{name: "unknown-outcome", title: "ALLOW, simulated lost response (UNKNOWN), reconciliation confirms success",
		cmd: contracts.CmdModifyRouting, target: contracts.LostResponseNodeID, payload: `{"route":"stable"}`,
		wantStatus: http.StatusOK, wantVerd: contracts.VerdictAllow,
		wantPhase: contracts.AuditPhaseReconciliation, wantPhaseVerdict: contracts.AuditVerdictPass},
}

type sim struct {
	client *http.Client
	base   string
	apiKey string
	check  bool
	wait   time.Duration
}

func main() {
	check := flag.Bool("check", false, "exit non-zero if any scenario produces an unexpected result")
	wait := flag.Duration("wait", 30*time.Second, "how long to wait for a workflow to finish when checking")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: agent-sim [-check] [-wait 30s] [all|scenario]\nscenarios:\n")
		for _, sc := range scenarios {
			fmt.Fprintf(os.Stderr, "  %-20s %s\n", sc.name, sc.title)
		}
	}
	flag.Parse()

	name := "all"
	if flag.NArg() > 0 {
		name = flag.Arg(0)
	}
	var selected []scenario
	for _, sc := range scenarios {
		if name == "all" || sc.name == name {
			selected = append(selected, sc)
		}
	}
	if len(selected) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	s := &sim{
		client: &http.Client{Timeout: 15 * time.Second},
		base:   envOr("SENTRYGATE_URL", "http://localhost:8080"),
		apiKey: envOr("SENTRYGATE_API_KEY", "dev-secret-change-me"),
		check:  *check,
		wait:   *wait,
	}

	failures := 0
	for i, sc := range selected {
		fmt.Printf("=== %d. %s (%s) ===\n", i+1, sc.title, sc.name)
		if err := s.run(sc); err != nil {
			failures++
			fmt.Printf("CHECK FAILED: %v\n", err)
		}
		fmt.Println()
	}
	if s.check {
		if failures > 0 {
			fmt.Printf("%d of %d scenarios failed\n", failures, len(selected))
			os.Exit(1)
		}
		fmt.Printf("all %d scenarios behaved as expected\n", len(selected))
	}
}

type interceptResponse struct {
	Status     string            `json:"status"`
	Verdict    contracts.Verdict `json:"verdict"`
	Reasons    []string          `json:"reasons"`
	DecisionID string            `json:"decision_id"`
}

func (s *sim) run(sc scenario) error {
	prop := contracts.AgentProposal{
		ID:       fmt.Sprintf("%s-%d", sc.name, time.Now().UnixNano()),
		Type:     sc.cmd,
		TargetID: sc.target,
		Payload:  sc.payload,
	}
	raw, _ := json.Marshal(prop)
	status, body, err := s.do(http.MethodPost, "/v1/intercept", raw)
	if err != nil {
		return err
	}
	fmt.Printf("HTTP %d\n%s\n", status, prettyJSON(body))

	var resp interceptResponse
	_ = json.Unmarshal(body, &resp)
	if !s.check {
		if sc.wantPhase != "" && status == http.StatusOK {
			time.Sleep(3 * time.Second)
			s.printAudit(prop.ID)
		}
		return nil
	}

	if status != sc.wantStatus || resp.Verdict != sc.wantVerd {
		return fmt.Errorf("got HTTP %d verdict %q, want HTTP %d verdict %q", status, resp.Verdict, sc.wantStatus, sc.wantVerd)
	}
	if resp.DecisionID == "" {
		return fmt.Errorf("response has no decision_id")
	}
	if err := s.checkDecision(prop.ID, resp.DecisionID, sc.wantVerd); err != nil {
		return err
	}
	if sc.wantPhase != "" {
		return s.waitForPhase(prop.ID, sc.wantPhase, sc.wantPhaseVerdict)
	}
	return nil
}

func (s *sim) checkDecision(proposalID, decisionID string, want contracts.Verdict) error {
	status, body, err := s.do(http.MethodGet, "/v1/decisions/"+proposalID, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET /v1/decisions returned HTTP %d", status)
	}
	var recs []contracts.DecisionRecord
	if err := json.Unmarshal(body, &recs); err != nil {
		return fmt.Errorf("decode decisions: %w", err)
	}
	for _, r := range recs {
		if r.DecisionID == decisionID && r.Stage == contracts.StageIngress && r.Verdict == want {
			fmt.Printf("decision record %s: %s %v (policy %s)\n", r.DecisionID, r.Verdict, r.Reasons, r.PolicyVersion)
			return nil
		}
	}
	return fmt.Errorf("no INGRESS %s decision record %s for proposal %s", want, decisionID, proposalID)
}

func (s *sim) waitForPhase(proposalID string, phase contracts.AuditPhase, verdict contracts.AuditVerdict) error {
	deadline := time.Now().Add(s.wait)
	for time.Now().Before(deadline) {
		status, body, err := s.do(http.MethodGet, "/v1/audit/"+proposalID, nil)
		if err == nil && status == http.StatusOK {
			var recs []contracts.AuditRecord
			if json.Unmarshal(body, &recs) == nil {
				for _, r := range recs {
					if r.Phase == phase && r.Verdict == verdict {
						fmt.Println("--- audit trail ---")
						fmt.Println(prettyJSON(body))
						return nil
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("audit phase %s %s not recorded for %s within %s", phase, verdict, proposalID, s.wait)
}

func (s *sim) printAudit(proposalID string) {
	_, body, err := s.do(http.MethodGet, "/v1/audit/"+proposalID, nil)
	if err != nil {
		fmt.Println("audit error:", err)
		return
	}
	fmt.Println("--- audit trail ---")
	fmt.Println(prettyJSON(body))
}

func (s *sim) do(method, path string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, s.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-API-Key", s.apiKey)
	res, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	return res.StatusCode, b, err
}

func prettyJSON(b []byte) string {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return string(b)
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return string(b)
	}
	return string(out)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
