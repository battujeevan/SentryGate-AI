package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func main() {
	base := envOr("SENTRYGATE_URL", "http://localhost:8080")
	apiKey := envOr("SENTRYGATE_API_KEY", "dev-secret-change-me")
	scenario := "all"
	if len(os.Args) > 1 {
		scenario = os.Args[1]
	}

	client := &http.Client{Timeout: 15 * time.Second}

	switch scenario {
	case "deny":
		runDeny(client, base, apiKey)
	case "allow":
		runAllow(client, base, apiKey)
	case "rollback":
		runRollback(client, base, apiKey)
	case "all":
		fmt.Println("=== Scenario 1: DENY (ROOT_CORE_EDGE delete) ===")
		runDeny(client, base, apiKey)
		fmt.Println()
		fmt.Println("=== Scenario 2: ALLOW (safe routing update) ===")
		runAllow(client, base, apiKey)
		fmt.Println()
		fmt.Println("=== Scenario 3: ROLLBACK (FAILING_NODE) ===")
		runRollback(client, base, apiKey)
	default:
		fmt.Fprintf(os.Stderr, "usage: agent-sim [all|deny|allow|rollback]\n")
		os.Exit(2)
	}
}

func runDeny(client *http.Client, base, apiKey string) {
	prop := contracts.AgentProposal{
		ID:        fmt.Sprintf("deny-%d", time.Now().UnixNano()),
		Type:      contracts.CmdDeletePolicy,
		TargetID:  contracts.RootCoreEdgeID,
		Payload:   `{"reason":"agent hallucination"}`,
		RiskScore: 0.2,
	}
	status, body := postIntercept(client, base, apiKey, prop)
	fmt.Printf("HTTP %d\n%s\n", status, body)
}

func runAllow(client *http.Client, base, apiKey string) {
	prop := contracts.AgentProposal{
		ID:        fmt.Sprintf("allow-%d", time.Now().UnixNano()),
		Type:      contracts.CmdModifyRouting,
		TargetID:  "edge-node-west-1",
		Payload:   `{"route":"stable"}`,
		RiskScore: 0.42,
	}
	status, body := postIntercept(client, base, apiKey, prop)
	fmt.Printf("HTTP %d\n%s\n", status, body)
	if status == http.StatusOK {
		var resp map[string]string
		_ = json.Unmarshal([]byte(body), &resp)
		if id := resp["proposal_id"]; id != "" {
			time.Sleep(2 * time.Second)
			printAudit(client, base, apiKey, id)
		}
	}
}

func runRollback(client *http.Client, base, apiKey string) {
	prop := contracts.AgentProposal{
		ID:        fmt.Sprintf("rollback-%d", time.Now().UnixNano()),
		Type:      contracts.CmdUpdateCert,
		TargetID:  contracts.FailingNodeID,
		Payload:   `{"cert":"mock"}`,
		RiskScore: 0.3,
	}
	status, body := postIntercept(client, base, apiKey, prop)
	fmt.Printf("HTTP %d\n%s\n", status, body)
	if status == http.StatusOK {
		var resp map[string]string
		_ = json.Unmarshal([]byte(body), &resp)
		if id := resp["proposal_id"]; id != "" {
			time.Sleep(3 * time.Second)
			printAudit(client, base, apiKey, id)
		}
	}
}

func postIntercept(client *http.Client, base, apiKey string, prop contracts.AgentProposal) (int, string) {
	raw, _ := json.Marshal(prop)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/intercept", bytes.NewReader(raw))
	if err != nil {
		return 0, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	res, err := client.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

func printAudit(client *http.Client, base, apiKey, proposalID string) {
	req, err := http.NewRequest(http.MethodGet, base+"/v1/audit/"+proposalID, nil)
	if err != nil {
		fmt.Println("audit error:", err)
		return
	}
	req.Header.Set("X-API-Key", apiKey)
	res, err := client.Do(req)
	if err != nil {
		fmt.Println("audit error:", err)
		return
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	fmt.Println("--- audit trail ---")
	fmt.Println(prettyJSON(b))
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
