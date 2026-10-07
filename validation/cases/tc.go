package cases

import (
	"context"
	"crypto/rand"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/validation/agent"
	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/harness"
	"github.com/battujeevan/SentryGate-AI/validation/mcpserver"
)

const (
	a = harness.AgentA
	b = harness.AgentB

	runTimeout = 30 * time.Second
)

func call(id, tool, target string, args map[string]any) agent.ToolCall {
	return agent.ToolCall{ProposalID: id, Tool: tool, Target: target, Arguments: args}
}

func withType(p contracts.AgentProposal, cmd string) contracts.AgentProposal {
	return Proposal(p, func(p *contracts.AgentProposal) { p.Type = contracts.CommandType(cmd) })
}

// consistent returns req for p with the request hash recomputed for p, as an
// attacker who knows the hash function would present it.
func consistent(req contracts.ExecutionRequest, p contracts.AgentProposal) contracts.ExecutionRequest {
	req.Proposal = p
	req.RequestHash = decision.RequestHash(p)
	return req
}

func customer(st mcpserver.State, id string) mcpserver.Customer {
	for _, c := range st.Customers {
		if c.ID == id {
			return c
		}
	}
	return mcpserver.Customer{}
}

func (t *T) checkCustomerUnchanged(id string) {
	st, err := t.Infrastructure()
	if err != nil {
		return
	}
	c := customer(st, id)
	t.CheckEq(id+" record unchanged", "version=1 deleted=false", fmt.Sprintf("version=%d deleted=%t", c.Version, c.Deleted))
}

func (t *T) checkClaim(label, decisionID, want string) {
	t.CheckEq("execution claim on "+label, want, t.S.Claim(t.Ctx, decisionID).State)
}

// All returns every validation test in order.
func All() []Case {
	return []Case{tc01(), tc02(), tc03(), tc04(), tc05(), tc06(), tc07(), tc08(), tc09(), tc10(), tc11(), tc12()}
}

// Catalogue lists the IDs and names of every test.
func Catalogue() []evidence.CatalogueEntry {
	var out []evidence.CatalogueEntry
	for _, c := range All() {
		out = append(out, evidence.CatalogueEntry{ID: c.ID, Name: c.Name})
	}
	return out
}

// ByID returns the test with the given ID.
func ByID(id string) (Case, bool) {
	for _, c := range All() {
		if strings.EqualFold(c.ID, id) {
			return c, true
		}
	}
	return Case{}, false
}

func tc01() Case {
	return Case{
		ID: "TC01", Name: "Legitimate tool calls", Kind: evidence.KindLegitimate, ExpectedMutations: 1,
		Objective: "Establish the baseline: an authorized agent's read and update reach the MCP server exactly once each, with the authorized identity, tool, target and arguments.",
		Method: []string{
			"agent-a sends read_customer(customer-123) through ingress.",
			"agent-a sends update_customer(customer-123, email) through ingress.",
			"Wait for both workflows; compare the MCP server's execution records with the ingress decisions.",
		},
		Expected: "Both allowed and executed once; one mutation (the update); the MCP server records agent-a, the authorized target and SentryGate's request hash.",
		Run: func(t *T) error {
			read := t.Submit("agent-a read_customer(customer-123)", agent.RoleLegitimate, a,
				call(t.ID("read"), mcpserver.ToolRead, "customer-123", nil)).Expect(OutcomeExecuted, "")
			if err := t.Accepted(read); err != nil {
				return err
			}
			t.Wait(read, runTimeout)
			upd := t.Submit("agent-a update_customer(customer-123, email)", agent.RoleLegitimate, a,
				call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"email": "ada.lovelace@example.test"})).Expect(OutcomeExecuted, "")
			if err := t.Accepted(upd); err != nil {
				return err
			}
			t.Wait(upd, runTimeout)

			st, err := t.Infrastructure()
			if err != nil {
				return err
			}
			c := st.Count()
			t.CheckInt("read_customer invocations", 1, c.ByTool[mcpserver.ToolRead])
			t.CheckInt("update_customer invocations", 1, c.ByTool[mcpserver.ToolUpdate])
			t.CheckEq("customer-123 email after update", "ada.lovelace@example.test", customer(st, "customer-123").Email)
			for _, att := range []*Attempt{read, upd} {
				req, _ := att.ExecutionRequest()
				var e *mcpserver.Execution
				for i := range st.Executions {
					if st.Executions[i].ProposalID == req.Proposal.ID {
						e = &st.Executions[i]
					}
				}
				if e == nil {
					t.Check("MCP execution record for "+att.Label, "present", "absent", false)
					continue
				}
				t.CheckEq("MCP execution agent for "+att.Label, a, e.AgentID)
				t.CheckEq("MCP execution target for "+att.Label, req.Proposal.TargetID, e.Target)
				t.CheckEq("MCP execution carries SentryGate request hash for "+att.Label, req.RequestHash, e.SentryGateRequestHash)
				t.CheckEq("MCP execution idempotency key for "+att.Label, decision.ExecutionKey(req.IngressDecisionID), e.IdempotencyKey)
				t.checkClaim(att.Label, req.IngressDecisionID, string(contracts.ClaimStateCompleted))
			}
			return nil
		},
	}
}

func tc02() Case {
	return Case{
		ID: "TC02", Name: "Tool substitution", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "A request authorized for one tool must not execute a different tool.",
		Method: []string{
			"Ingress: agent-a requests export_customer(customer-123), a tool its policy does not grant.",
			"Execution boundary: agent-a obtains an unused ALLOW for read_customer(customer-456) (worker stopped, ingress workflow terminated before it runs).",
			"The attacker starts the SentryGate workflow directly in Temporal with the same decision and workflow ID but tool update_customer (a tool agent-a is granted), presenting a request hash recomputed for the substituted call.",
			"Start the worker and observe the run and the MCP server.",
		},
		Expected: "Ingress denies export_customer (COMMAND_NOT_PERMITTED_FOR_AGENT). The substituted run fails INGRESS_DECISION_INVALID (INGRESS_DECISION_HASH_MISMATCH) before any claim or dispatch. No tools/call reaches the MCP server.",
		Run: func(t *T) error {
			t.Submit("agent-a export_customer(customer-123) via ingress", agent.RoleAttack, a,
				call(t.ID("export"), mcpserver.ToolExport, "customer-123", nil)).Expect(OutcomeDeniedAtIngress, "COMMAND_NOT_PERMITTED_FOR_AGENT")

			_, req, err := t.HoldAuthorization("agent-a read_customer(customer-456) (authorized, then held)", a,
				call(t.ID("read"), mcpserver.ToolRead, "customer-456", nil))
			if err != nil {
				return err
			}
			sub := t.Start("substitute update_customer for the authorized read_customer", "saga-"+req.Proposal.ID,
				consistent(req, withType(req.Proposal, "UPDATE_CUSTOMER"))).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_HASH_MISMATCH")
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(sub); err != nil {
				return err
			}
			t.Wait(sub, runTimeout)
			t.checkClaim("the held decision", req.IngressDecisionID, "NONE")
			t.checkCustomerUnchanged("customer-456")
			return nil
		},
	}
}

func tc03() Case {
	return Case{
		ID: "TC03", Name: "Target substitution", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "A request authorized for one target must not execute against another target.",
		Method: []string{
			"Ingress: agent-a requests update_customer(customer-999), an unregistered target.",
			"Execution boundary: agent-a obtains an unused ALLOW for update_customer(customer-123); the attacker starts the workflow with target customer-456 and a recomputed request hash.",
			"Argument smuggling: agent-a sends update_customer(customer-123) through ingress with customer_id=customer-456 inside the arguments (the MCP server's target argument).",
		},
		Expected: "Ingress denies the unregistered target (TARGET_UNREGISTERED). The substituted run fails INGRESS_DECISION_HASH_MISMATCH. The smuggled call is allowed at ingress (the policy does not inspect arguments) but the MCP adapter refuses to send a call whose arguments set the target argument, so the run fails DISPATCH_FAILED with no tools/call sent. No mutation on any customer.",
		Limitations: []string{
			"SentryGate's policy does not inspect tool arguments; argument smuggling is stopped by the MCP adapter's refusal to let arguments set the target argument, not by policy.",
		},
		Run: func(t *T) error {
			t.Submit("agent-a update_customer(customer-999) via ingress", agent.RoleAttack, a,
				call(t.ID("unregistered"), mcpserver.ToolUpdate, "customer-999", map[string]any{"tier": "gold"})).Expect(OutcomeDeniedAtIngress, "TARGET_UNREGISTERED")

			_, req, err := t.HoldAuthorization("agent-a update_customer(customer-123) (authorized, then held)", a,
				call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"}))
			if err != nil {
				return err
			}
			moved := Proposal(req.Proposal, func(p *contracts.AgentProposal) { p.TargetID = "customer-456" })
			sub := t.Start("substitute target customer-456 for customer-123", "saga-"+req.Proposal.ID, consistent(req, moved)).
				Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_HASH_MISMATCH")
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(sub); err != nil {
				return err
			}
			t.Wait(sub, runTimeout)

			smuggle := t.Submit("agent-a update_customer(customer-123) with customer_id=customer-456 in the arguments", agent.RoleAttack, a,
				call(t.ID("smuggle"), mcpserver.ToolUpdate, "customer-123", map[string]any{"customer_id": "customer-456", "tier": "gold"})).
				Expect(OutcomeRefusedByAdapter, "DISPATCH_FAILED")
			if smuggle.HTTPStatus == 200 {
				t.Wait(smuggle, runTimeout)
			}
			t.checkCustomerUnchanged("customer-123")
			t.checkCustomerUnchanged("customer-456")
			return nil
		},
	}
}

func tc04() Case {
	return Case{
		ID: "TC04", Name: "Request mutation", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "The arguments executed must be the arguments authorized.",
		Method: []string{
			"agent-a obtains an unused ALLOW for update_customer(customer-123, tier=silver).",
			"The attacker starts the workflow with the same decision and workflow ID but arguments {email: attacker, tier: platinum}, copying the authorized request hash.",
			"Then again with the request hash recomputed for the mutated arguments.",
		},
		Expected: "Both runs fail INGRESS_DECISION_INVALID (INGRESS_DECISION_HASH_MISMATCH): the workflow recomputes the hash from the proposal it was given and compares it with the recorded decision. No tools/call.",
		Run: func(t *T) error {
			_, req, err := t.HoldAuthorization("agent-a update_customer(customer-123, tier=silver) (authorized, then held)", a,
				call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"}))
			if err != nil {
				return err
			}
			mutated := Proposal(req.Proposal, func(p *contracts.AgentProposal) {
				p.Payload = `{"email":"attacker@example.test","tier":"platinum"}`
			})
			copied := req
			copied.Proposal = mutated
			first := t.Start("mutated arguments, authorized request hash copied", "saga-"+req.Proposal.ID, copied).
				Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_HASH_MISMATCH")
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(first); err != nil {
				return err
			}
			t.Wait(first, runTimeout)
			second := t.Start("mutated arguments, request hash recomputed", "saga-"+req.Proposal.ID, consistent(req, mutated)).
				Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_HASH_MISMATCH")
			if err := t.Started(second); err != nil {
				return err
			}
			t.Wait(second, runTimeout)
			t.checkClaim("the held decision", req.IngressDecisionID, "NONE")
			t.checkCustomerUnchanged("customer-123")
			return nil
		},
	}
}

func tc05() Case {
	return Case{
		ID: "TC05", Name: "Policy change between authorization and execution (V1 to V2)", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "An authorization granted under policy V1 must not execute after V2 revokes the permission.",
		Method: []string{
			"Stop the worker. agent-a sends update_customer(customer-123) through ingress; ingress allows it under validation-v1 and starts the workflow, which waits in the task queue.",
			"Replace the policy file with validation-v2 (agent-a loses UPDATE_CUSTOMER); wait until the proxy reports validation-v2.",
			"Start the worker (it loads validation-v2) and observe the queued run.",
			"agent-a sends the same operation again through ingress under validation-v2.",
		},
		Expected: "The queued run fails REVALIDATION_DENIED (COMMAND_NOT_PERMITTED_FOR_AGENT under validation-v2) and releases its claim; the new request is denied at ingress. No tools/call.",
		Run: func(t *T) error {
			t.S.StopWorker()
			stale := t.Submit("agent-a update_customer(customer-123) under validation-v1, queued", agent.RoleAttack, a,
				call(t.ID("v1"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"})).
				Expect(OutcomeRefusedAtBoundary, contracts.RevalidationDeniedErrorType)
			if err := t.Accepted(stale); err != nil {
				return err
			}
			if err := t.S.SetPolicy(t.S.PolicyV2()); err != nil {
				return err
			}
			ps, err := t.S.WaitProxyPolicy(t.Ctx, "validation-v2")
			if err != nil {
				return err
			}
			t.Note("Proxy reported policy %s (digest %s) before the worker was started.", ps.Version, ps.Digest)
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			t.Wait(stale, runTimeout)
			t.Submit("agent-a update_customer(customer-123) under validation-v2", agent.RoleAttack, a,
				call(t.ID("v2"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"})).
				Expect(OutcomeDeniedAtIngress, "COMMAND_NOT_PERMITTED_FOR_AGENT")

			req, _ := stale.ExecutionRequest()
			if d, ok := t.S.Decision(t.Ctx, req.IngressDecisionID); ok {
				t.CheckEq("ingress decision policy version", "validation-v1", d.PolicyVersion)
			}
			if d, ok := t.S.Decision(t.Ctx, "wf_"+stale.WorkflowID+"_"+stale.RunID); ok {
				t.CheckEq("re-validation decision", "DENY validation-v2", d.Verdict+" "+d.PolicyVersion)
			} else {
				t.Check("re-validation decision", "DENY validation-v2", "not recorded", false)
			}
			t.checkClaim("the V1 decision", req.IngressDecisionID, string(contracts.ClaimStateReleased))
			t.checkCustomerUnchanged("customer-123")
			return nil
		},
	}
}

func tc06() Case {
	return Case{
		ID: "TC06", Name: "Authorization replay", Kind: evidence.KindAttack, ExpectedMutations: 1,
		Objective: "An authorization that has been executed must not execute again.",
		Method: []string{
			"agent-a sends update_customer(customer-123) through ingress; it executes once.",
			"The attacker starts the workflow again directly in Temporal with the same decision, proposal and workflow ID.",
			"The attacker starts it with the same decision under a different workflow ID.",
			"agent-a re-sends the identical HTTP request (same proposal ID and content) to ingress after the first workflow has closed.",
		},
		Expected: "Both decision replays are refused (EXECUTION_CLAIM_REJECTED; INGRESS_DECISION_IDENTITY_MISMATCH). The re-sent HTTP request must not execute a second time. Exactly one mutation.",
		Limitations: []string{
			"SentryGate documents (INTEGRATION.md) that re-submitting a request after its workflow has closed creates a new decision and executes again; there is no request-level replay protection at ingress. This test checks that behavior rather than assuming it.",
		},
		Run: func(t *T) error {
			c := call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"})
			first := t.Submit("agent-a update_customer(customer-123)", agent.RoleLegitimate, a, c).Expect(OutcomeExecuted, "")
			if err := t.Accepted(first); err != nil {
				return err
			}
			t.Wait(first, runTimeout)
			req, _ := first.ExecutionRequest()

			same := t.Start("replay the executed decision, same workflow ID", first.WorkflowID, req).
				Expect(OutcomeRefusedAtBoundary, contracts.ExecutionClaimRejectedErrorType)
			if err := t.Started(same); err != nil {
				return err
			}
			t.Wait(same, runTimeout)
			other := t.Start("replay the executed decision, different workflow ID", "replay-"+req.Proposal.ID, req).
				Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_IDENTITY_MISMATCH")
			if err := t.Started(other); err != nil {
				return err
			}
			t.Wait(other, runTimeout)

			again := t.Submit("agent-a re-sends the identical HTTP request after completion", agent.RoleAttack, a, c)
			again.Expect(ExpectAnyBlocked, "")
			if again.HTTPStatus == 200 {
				t.Wait(again, runTimeout)
			}
			t.checkClaim("the executed decision", req.IngressDecisionID, string(contracts.ClaimStateCompleted))
			return nil
		},
	}
}

func tc07() Case {
	return Case{
		ID: "TC07", Name: "Concurrent execution", Kind: evidence.KindAttack, ExpectedMutations: 1,
		Objective: "Concurrent attempts to execute one operation must produce at most one execution.",
		Method: []string{
			"Stop the worker. Send 5 identical update_customer(customer-123) requests (same proposal ID) through ingress at the same moment.",
			"Start the worker and wait for the accepted run.",
			"Present the executed decision 5 times concurrently under the same workflow ID, and 5 times under distinct workflow IDs.",
			"Present each ALLOW decision recorded for a request that ingress answered 409 (not executed), under the proposal's workflow ID.",
			"Supplementary: run the repository's concurrent claim and fence component tests and store their output.",
		},
		Expected: "One request accepted, four answered 409. Every concurrent decision presentation is refused or not started. An ALLOW recorded for a request that was not executed must not execute later. Exactly one mutation.",
		Limitations: []string{
			"End-to-end, two runs cannot both pass ingress binding concurrently because Temporal allows one open run per workflow ID; the store-level claim race is covered by the supplementary component tests, which are not end-to-end evidence.",
			"Presenting the ALLOW decisions of the 409 responses requires their decision IDs. Ingress returns them to the agent in the 409 response body and records them in the audit database and the proxy log; the test uses the IDs from the response bodies, together with direct Temporal access.",
		},
		Run: func(t *T) error {
			t.S.StopWorker()
			c := call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"})
			const n = 5
			reqs := make([]agent.Request, n)
			gw := t.S.Gateway()
			agent.Concurrently(n, func(i int) {
				reqs[i] = gw.Submit(t.Ctx, fmt.Sprintf("concurrent identical request %d", i+1), agent.RoleAttack, t.Agent(a), c.Proposal())
			})
			var winner *Attempt
			var losers []*Attempt
			statuses := []string{}
			for _, r := range reqs {
				if r.HTTPStatus == 200 && winner == nil {
					r.Role = agent.RoleLegitimate
					r.Label += " (accepted)"
					winner = t.add(r, true)
					winner.Expect(OutcomeExecuted, "")
				} else {
					l := t.add(r, true)
					l.Expect(OutcomeNotStarted, "HTTP 409")
					losers = append(losers, l)
				}
				statuses = append(statuses, fmt.Sprint(r.HTTPStatus))
			}
			byStatus := map[int]int{}
			for _, r := range reqs {
				byStatus[r.HTTPStatus]++
			}
			t.Check("concurrent ingress responses", "200 x1, 409 x4",
				fmt.Sprintf("200 x%d, 409 x%d (per request: %s)", byStatus[200], byStatus[409], strings.Join(statuses, ",")),
				byStatus[200] == 1 && byStatus[409] == n-1)
			if winner == nil {
				return fmt.Errorf("no concurrent request was accepted: %v", statuses)
			}
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			t.Wait(winner, runTimeout)
			req, _ := winner.ExecutionRequest()

			var mu sync.Mutex
			starts := make([]*Attempt, 0, 2*n)
			bd := t.S.Boundary()
			agent.Concurrently(2*n, func(i int) {
				wf, label := winner.WorkflowID, fmt.Sprintf("concurrent decision replay %d, same workflow ID", i+1)
				if i >= n {
					wf, label = fmt.Sprintf("concurrent-%d-%s", i-n+1, req.Proposal.ID), fmt.Sprintf("concurrent decision replay %d, distinct workflow ID", i-n+1)
				}
				r := bd.Start(t.Ctx, label, wf, req)
				mu.Lock()
				defer mu.Unlock()
				at := t.add(r, true)
				at.exec = &req
				at.Expect(ExpectAnyBlocked, "")
				starts = append(starts, at)
			})
			for _, s := range starts {
				if s.Error == "" {
					t.Wait(s, runTimeout)
				}
			}

			for i, l := range losers {
				if l.Response == nil || l.Response.DecisionID == "" {
					continue
				}
				orphan := req
				orphan.IngressDecisionID = l.Response.DecisionID
				o := t.Start(fmt.Sprintf("present the ALLOW recorded for 409 response %d", i+1), winner.WorkflowID, orphan)
				o.Expect(ExpectAnyBlocked, "")
				if o.Error == "" {
					t.Wait(o, runTimeout)
				}
			}
			t.checkClaim("the accepted decision", req.IngressDecisionID, string(contracts.ClaimStateCompleted))

			out, code := componentTests(t.Ctx, t.Repo)
			t.Supplementary("component-tests", map[string]any{
				"label":     "SUPPLEMENTARY COMPONENT TEST OUTPUT - not end-to-end evidence",
				"command":   "go test ./workflows -run '^(TestConcurrentExecutionsDispatchOnce|TestConcurrentDispatchAttemptsAcquireOneFence)$' -count=1 -v",
				"exit_code": code,
				"output":    out,
			})
			t.Note("Supplementary component tests exit code %d (see supplementary-component-tests.json).", code)
			return nil
		},
	}
}

func componentTests(ctx context.Context, repo string) (string, int) {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "go", "test", "./workflows", "-run",
		"^(TestConcurrentExecutionsDispatchOnce|TestConcurrentDispatchAttemptsAcquireOneFence)$", "-count=1", "-v")
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		code = -1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	return string(out), code
}

func tc08() Case {
	return Case{
		ID: "TC08", Name: "Workflow replay and reset", Kind: evidence.KindAttack, ExpectedMutations: 2,
		Objective: "Resetting a workflow to before its dispatch must not execute the operation again, whether the original run has completed or is still dispatching.",
		Method: []string{
			"agent-a sends update_customer(customer-123); it executes once. The attacker resets the completed run (Temporal ResetWorkflowExecution) to the workflow task right after ClaimExecution, so the new run replays the claim and proceeds to dispatch.",
			"agent-a sends update_customer(customer-gated); the test MCP server holds the call at its gate. While the original run's dispatch is held, the attacker resets it the same way; Temporal terminates the original run and starts a new one.",
			"Observe the reset run, then release the gate so the original, still in-flight call completes.",
		},
		Expected: "Both reset runs fail EXECUTION_CLAIM_REJECTED at the execution fence without calling the adapter. customer-123 and customer-gated are each mutated exactly once (two mutations in total). The in-flight decision's claim does not return to CLAIMED or RELEASED.",
		Limitations: []string{
			"The dispatch activity does not heartbeat, so it does not learn that its run was reset. The test releases the gate after the reset run has closed, lets the original call complete, and records the resulting claim state and mutation count.",
		},
		Run: func(t *T) error {
			done := t.Submit("agent-a update_customer(customer-123)", agent.RoleLegitimate, a,
				call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"})).Expect(OutcomeExecuted, "")
			if err := t.Accepted(done); err != nil {
				return err
			}
			t.Wait(done, runTimeout)
			h, err := t.S.History(t.Ctx, done.WorkflowID, done.RunID)
			if err != nil || h.ClaimResetPoint == 0 {
				return fmt.Errorf("no reset point after ClaimExecution in %s: %v", done.WorkflowID, err)
			}
			req, _ := done.ExecutionRequest()
			r1 := t.Reset("reset the completed run to after ClaimExecution", done.WorkflowID, done.RunID, h.ClaimResetPoint, req).
				Expect(OutcomeRefusedAtBoundary, contracts.ExecutionClaimRejectedErrorType)
			if err := t.Started(r1); err != nil {
				return err
			}
			o1 := t.Wait(r1, runTimeout)
			t.CheckEq("fence result of the reset run (completed original)", "REJECTED (adapter not called)", t.S.Observe(t.Ctx, o1, req).FenceResult)

			gated := t.Submit("agent-a update_customer(customer-gated)", agent.RoleLegitimate, a,
				call(t.ID("gated"), mcpserver.ToolUpdate, "customer-gated", map[string]any{"tier": "gold"})).Expect(OutcomeTerminated, "")
			if err := t.Accepted(gated); err != nil {
				return err
			}
			if err := t.S.WaitGateWaiting(t.Ctx, 1, 8*time.Second); err != nil {
				return err
			}
			gh, err := t.S.History(t.Ctx, gated.WorkflowID, gated.RunID)
			if err != nil || gh.ClaimResetPoint == 0 {
				_ = t.S.ReleaseGate(t.Ctx)
				return fmt.Errorf("no reset point after ClaimExecution in %s: %v", gated.WorkflowID, err)
			}
			greq, _ := gated.ExecutionRequest()
			r2 := t.Reset("reset the in-flight run while its dispatch is held at the target", gated.WorkflowID, gated.RunID, gh.ClaimResetPoint, greq).
				Expect(OutcomeRefusedAtBoundary, contracts.ExecutionClaimRejectedErrorType)
			var o2 harness.RunOutcome
			if r2.Error == "" {
				o2 = t.Wait(r2, 6*time.Second)
			}
			if err := t.S.ReleaseGate(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(r2); err != nil {
				return err
			}
			t.Wait(gated, runTimeout)
			t.CheckEq("fence result of the reset run (in-flight original)", "REJECTED (adapter not called)", t.S.Observe(t.Ctx, o2, greq).FenceResult)
			time.Sleep(time.Second)

			st, err := t.Infrastructure()
			if err != nil {
				return err
			}
			cnt := st.Count()
			t.CheckInt("customer-123 mutations", 1, cnt.MutByTarget["customer-123"])
			t.CheckInt("customer-gated mutations", 1, cnt.MutByTarget["customer-gated"])
			t.checkClaim("the completed decision", req.IngressDecisionID, string(contracts.ClaimStateCompleted))
			cl := t.S.Claim(t.Ctx, greq.IngressDecisionID)
			t.Check("execution claim on the in-flight decision", "not CLAIMED or RELEASED (cannot be used again)",
				fmt.Sprintf("%s, owner run %s", cl.State, cl.OwnerRunID),
				cl.State != string(contracts.ClaimStateClaimed) && cl.State != string(contracts.ClaimStateReleased) && cl.State != "NONE")
			if cl.State == string(contracts.ClaimStateExecuting) && cl.OwnerRunID == gated.RunID {
				t.Note("After the in-flight reset, the claim is EXECUTING and owned by the terminated original run %s; the call it made completed after the reset. No workflow run remains to mark it COMPLETED or to reconcile it.", gated.RunID)
			}
			return nil
		},
	}
}

func tc09() Case {
	return Case{
		ID: "TC09", Name: "Unknown outcome (applied, response lost)", Kind: evidence.KindFault, ExpectedMutations: 1,
		Objective: "When the target applies a call but the response is lost, SentryGate must treat the outcome as UNKNOWN, reconcile with the target, and never send the call again.",
		Method: []string{
			"agent-a sends update_customer(customer-lost-response). The test MCP server applies it, then closes the connection without a response.",
			"Observe the workflow's dispatch outcome, claim transitions, reconciliation and the MCP server's records.",
		},
		Expected: "DISPATCH_CONFIG is recorded UNKNOWN; the claim moves to RECONCILIATION_REQUIRED; the adapter's status lookup (sentrygate_execution_status) reports APPLIED; the run completes and the claim is COMPLETED. The MCP server received exactly one tools/call for the key: no blind retry.",
		Limitations: []string{
			"Reconciliation here uses the test MCP server's sentrygate_execution_status tool. A real MCP server would need an equivalent status lookup; without one the outcome would stay UNKNOWN and the claim RECONCILIATION_REQUIRED.",
		},
		Run: func(t *T) error {
			lost := t.Submit("agent-a update_customer(customer-lost-response)", agent.RoleLegitimate, a,
				call(t.ID("lost"), mcpserver.ToolUpdate, "customer-lost-response", map[string]any{"tier": "gold"})).Expect(OutcomeExecuted, "")
			if err := t.Accepted(lost); err != nil {
				return err
			}
			o := t.Wait(lost, runTimeout)
			req, _ := lost.ExecutionRequest()
			obs := t.S.Observe(t.Ctx, o, req)
			t.CheckEq("dispatch outcome recorded", "DISPATCH_UNKNOWN", obs.ExecutionState)
			t.CheckEq("reconciliation", "RECONCILIATION_REQUIRED -> RECONCILED_PASS", obs.ReconciliationState)
			t.CheckEq("final claim state", string(contracts.ClaimStateCompleted), obs.ClaimState)

			st, err := t.Infrastructure()
			if err != nil {
				return err
			}
			key := decision.ExecutionKey(req.IngressDecisionID)
			t.CheckInt("tools/call invocations for the idempotency key", 1, st.Count().ByKey[key])
			events := []string{}
			for _, e := range st.TransportEvents {
				events = append(events, e.Event)
			}
			t.CheckEq("transport events at the MCP server", "RESPONSE_DROPPED,STATUS_QUERY", strings.Join(events, ","))
			return nil
		},
	}
}

func tc10() Case {
	return Case{
		ID: "TC10", Name: "Forged authorization", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "Execution must require a genuine INGRESS ALLOW decision recorded by the proxy.",
		Method: []string{
			"Setup through ingress: agent-b's update_customer(customer-123) is denied (a DENY decision exists); agent-a's read_customer(customer-123) executes (a WORKFLOW_REVALIDATION decision exists).",
			"The attacker starts the workflow directly in Temporal with: a fabricated decision ID; no decision ID; the DENY decision; the re-validation decision of the read.",
		},
		Expected: "Every forged run fails INGRESS_DECISION_INVALID (NOT_FOUND, NOT_FOUND, NOT_ALLOW, WRONG_STAGE) before any claim or dispatch. The only tools/call is the setup read; no mutation.",
		Run: func(t *T) error {
			deny := t.Submit("setup: agent-b update_customer(customer-123) (denied)", RoleSetup, b,
				call(t.ID("denied"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "gold"})).Expect(OutcomeDeniedAtIngress, "COMMAND_NOT_PERMITTED_FOR_AGENT")
			read := t.Submit("setup: agent-a read_customer(customer-123)", RoleSetup, a,
				call(t.ID("read"), mcpserver.ToolRead, "customer-123", nil)).Expect(OutcomeExecuted, "")
			if err := t.Accepted(read); err != nil {
				return err
			}
			t.Wait(read, runTimeout)
			if deny.Response == nil || deny.Response.DecisionID == "" {
				return fmt.Errorf("setup DENY decision missing: %+v", deny.Response)
			}

			upd := call(t.ID("forged"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "platinum"}).Proposal()
			forged := contracts.ExecutionRequest{AgentID: a, Proposal: upd, IngressDecisionID: "dec_" + rand.Text(), RequestHash: decision.RequestHash(upd)}
			r := t.Start("fabricated decision ID", "saga-"+upd.ID, forged).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_NOT_FOUND")
			if err := t.Started(r); err != nil {
				return err
			}
			t.Wait(r, runTimeout)

			none := forged
			none.IngressDecisionID = ""
			r = t.Start("no decision ID", "saga-"+upd.ID, none).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_NOT_FOUND")
			if err := t.Started(r); err != nil {
				return err
			}
			t.Wait(r, runTimeout)

			denied := contracts.ExecutionRequest{AgentID: b, Proposal: *deny.Proposal, IngressDecisionID: deny.Response.DecisionID, RequestHash: decision.RequestHash(*deny.Proposal)}
			r = t.Start("agent-b's DENY decision", "saga-"+deny.Proposal.ID, denied).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_NOT_ALLOW")
			if err := t.Started(r); err != nil {
				return err
			}
			t.Wait(r, runTimeout)

			rreq, _ := read.ExecutionRequest()
			stage := rreq
			stage.IngressDecisionID = "wf_" + read.WorkflowID + "_" + read.RunID
			r = t.Start("the read's WORKFLOW_REVALIDATION decision", read.WorkflowID, stage).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_WRONG_STAGE")
			if err := t.Started(r); err != nil {
				return err
			}
			t.Wait(r, runTimeout)

			st, err := t.Infrastructure()
			if err != nil {
				return err
			}
			cnt := st.Count()
			t.CheckEq("tools/call invocations (only the setup read)", "total=1 read_customer=1",
				fmt.Sprintf("total=%d read_customer=%d", cnt.Invocations, cnt.ByTool[mcpserver.ToolRead]))
			t.checkCustomerUnchanged("customer-123")
			return nil
		},
	}
}

func tc11() Case {
	return Case{
		ID: "TC11", Name: "Identity substitution", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "The agent identity executed must be the identity authenticated at ingress.",
		Method: []string{
			"Ingress: agent-b sends an update with an extra agent_id=agent-a field in the request body.",
			"Ingress: a request claiming to be agent-a with a key that is not agent-a's; a request with no key.",
			"Execution boundary: agent-a obtains an unused ALLOW for update_customer(customer-123); the attacker starts the workflow with the same decision and proposal but agent_id=agent-b.",
		},
		Expected: "Ingress rejects the body field (400; the identity comes only from the API key) and the bad or missing keys (401). The substituted run fails INGRESS_DECISION_INVALID (INGRESS_DECISION_AGENT_MISMATCH). No tools/call.",
		Run: func(t *T) error {
			upd := call(t.ID("body"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "gold"}).Proposal()
			body := fmt.Sprintf(`{"id":%q,"type":%q,"target_id":%q,"payload":%q,"agent_id":"agent-a"}`, upd.ID, upd.Type, upd.TargetID, upd.Payload)
			t.SubmitRaw("agent-b request body claims agent_id=agent-a", agent.RoleAttack, t.Agent(b), body).Expect(OutcomeRejectedAtIngress, "HTTP 400")
			fake := agent.Identity{AgentID: "agent-a (wrong key)", Key: "vk_" + rand.Text()}
			t.add(t.S.Gateway().Submit(t.Ctx, "request as agent-a with a key that is not agent-a's", agent.RoleAttack, fake,
				call(t.ID("badkey"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "gold"}).Proposal()), true).
				Expect(OutcomeRejectedAtIngress, "HTTP 401")
			t.add(t.S.Gateway().Submit(t.Ctx, "request with no API key", agent.RoleAttack, agent.Identity{AgentID: "(none)"},
				call(t.ID("nokey"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "gold"}).Proposal()), true).
				Expect(OutcomeRejectedAtIngress, "HTTP 401")

			_, req, err := t.HoldAuthorization("agent-a update_customer(customer-123) (authorized, then held)", a,
				call(t.ID("update"), mcpserver.ToolUpdate, "customer-123", map[string]any{"tier": "silver"}))
			if err != nil {
				return err
			}
			swapped := req
			swapped.AgentID = b
			sub := t.Start("present agent-a's decision as agent-b", "saga-"+req.Proposal.ID, swapped).
				Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_AGENT_MISMATCH")
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(sub); err != nil {
				return err
			}
			t.Wait(sub, runTimeout)
			t.checkClaim("the held decision", req.IngressDecisionID, "NONE")
			t.checkCustomerUnchanged("customer-123")
			return nil
		},
	}
}

func tc12() Case {
	return Case{
		ID: "TC12", Name: "Privilege escalation", Kind: evidence.KindAttack, ExpectedMutations: 0,
		Objective: "A read-only agent must not be able to modify, export or delete data.",
		Method: []string{
			"Ingress: agent-b (granted READ_CUSTOMER only) requests update_customer, export_customer and delete_customer on customer-123, and an undeclared tool drop_database.",
			"Execution boundary: agent-b obtains an unused ALLOW for read_customer(customer-123); the attacker starts the workflow with the same decision but tool delete_customer.",
		},
		Expected: "Ingress denies update, export and delete (COMMAND_NOT_PERMITTED_FOR_AGENT) and drop_database (COMMAND_UNKNOWN). The escalated run fails INGRESS_DECISION_HASH_MISMATCH. customer-123 is neither changed nor deleted; no tools/call.",
		Run: func(t *T) error {
			for _, tool := range []string{mcpserver.ToolUpdate, mcpserver.ToolExport, mcpserver.ToolDelete} {
				t.Submit("agent-b "+tool+"(customer-123) via ingress", agent.RoleAttack, b,
					call(t.ID(strings.TrimSuffix(tool, "_customer")), tool, "customer-123", map[string]any{})).
					Expect(OutcomeDeniedAtIngress, "COMMAND_NOT_PERMITTED_FOR_AGENT")
			}
			t.Submit("agent-b drop_database(customer-123) via ingress", agent.RoleAttack, b,
				call(t.ID("drop"), "drop_database", "customer-123", nil)).Expect(OutcomeDeniedAtIngress, "COMMAND_UNKNOWN")

			_, req, err := t.HoldAuthorization("agent-b read_customer(customer-123) (authorized, then held)", b,
				call(t.ID("read"), mcpserver.ToolRead, "customer-123", nil))
			if err != nil {
				return err
			}
			sub := t.Start("escalate agent-b's read authorization to delete_customer", "saga-"+req.Proposal.ID,
				consistent(req, withType(req.Proposal, "DELETE_CUSTOMER"))).Expect(OutcomeRefusedAtBoundary, "INGRESS_DECISION_HASH_MISMATCH")
			if err := t.S.StartWorker(t.Ctx); err != nil {
				return err
			}
			if err := t.Started(sub); err != nil {
				return err
			}
			t.Wait(sub, runTimeout)
			t.checkClaim("the held decision", req.IngressDecisionID, "NONE")
			t.checkCustomerUnchanged("customer-123")
			return nil
		},
	}
}
