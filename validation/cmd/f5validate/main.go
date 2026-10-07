// Command f5validate runs the validation tests TC01-TC12 against a local
// stack (Temporal dev server, SentryGate proxy and worker with the MCP
// adapter, test MCP server) and writes evidence and the validation matrix.
//
// No F5 component is started or simulated. F5 observations come from the
// selected provider; the only provider is "mock", which observes nothing, so
// every F5 result is NOT_TESTED (or NOT_APPLICABLE for tests with no gateway
// traffic).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/battujeevan/SentryGate-AI/validation/cases"
	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/f5"
	"github.com/battujeevan/SentryGate-AI/validation/harness"
	"github.com/battujeevan/SentryGate-AI/validation/report"
)

func main() {
	tc := flag.String("tc", "all", "tests to run: all, or a comma-separated list such as TC01,TC05")
	provider := flag.String("f5-provider", "mock", "F5 observation provider (only \"mock\" exists)")
	mcpListen := flag.String("mcp-listen", "", "fixed address for the test MCP server (default: a free port)")
	workerMCP := flag.String("worker-mcp-url", "", "URL the worker's MCP adapter calls instead of the test MCP server (e.g. a gateway route)")
	ingressURL := flag.String("ingress-url", "", "base URL the test agent sends ingress requests to instead of the proxy (e.g. a gateway)")
	flag.Parse()

	root, err := repoRoot()
	if err != nil {
		fail(err)
	}
	var p f5.F5ObservationProvider
	switch *provider {
	case "mock":
		p = f5.MockProvider{}
	default:
		fail(fmt.Errorf("unknown -f5-provider %q: only \"mock\" is implemented; a real provider must be written against F5's documented audit interface (see validation/F5/README.md)", *provider))
	}

	var selected []cases.Case
	if strings.EqualFold(*tc, "all") {
		selected = cases.All()
	} else {
		for _, id := range strings.Split(*tc, ",") {
			c, ok := cases.ByID(strings.TrimSpace(id))
			if !ok {
				fail(fmt.Errorf("unknown test %q", id))
			}
			selected = append(selected, c)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	evidenceDir := filepath.Join(root, "validation", "evidence")
	fmt.Println("building binaries and starting the Temporal dev server...")
	suite, err := harness.NewSuite(ctx, harness.Options{
		RepoRoot:     root,
		RunDir:       filepath.Join(root, "validation", "run"),
		PolicyV1:     filepath.Join(root, "validation", "policies", "validation-v1.yaml"),
		PolicyV2:     filepath.Join(root, "validation", "policies", "validation-v2.yaml"),
		MCPListen:    *mcpListen,
		WorkerMCPURL: *workerMCP,
		IngressURL:   *ingressURL,
	})
	if err != nil {
		// Without a stack no test can run: record each selected test as BLOCKED.
		fmt.Fprintln(os.Stderr, "suite setup failed:", err)
		for _, c := range selected {
			res := evidence.Result{TestID: c.ID, TestName: c.Name, Kind: c.Kind, Objective: c.Objective, Method: c.Method,
				Expected: c.Expected, ExpectedMutations: c.ExpectedMutations, Status: evidence.StatusBlocked,
				BlockedReason: "suite setup failed: " + err.Error(), F5Result: f5.ResultNotTested, F5Provider: p.Name(),
				SentryGateResult: evidence.SGUnknown, InfrastructureResult: evidence.InfraUnknown,
				EvidencePath: "validation/evidence/" + c.ID, Checks: []evidence.Check{}, Attempts: []evidence.AttemptSummary{},
				Notes: []string{}, Limitations: c.Limitations, Files: []string{"test-result.json"}}
			_ = os.RemoveAll(filepath.Join(evidenceDir, c.ID))
			if werr := evidence.WriteJSON(filepath.Join(evidenceDir, c.ID, "test-result.json"), res); werr != nil {
				fail(werr)
			}
		}
		writeMatrix(root, evidenceDir)
		os.Exit(1)
	}
	defer suite.Close()
	fmt.Printf("Temporal: %s at %s\n", suite.Env.TemporalVersion, suite.Temporal.HostPort)

	runner := &cases.Runner{Suite: suite, Provider: p, EvidenceDir: evidenceDir, RepoRoot: root}
	for _, c := range selected {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("%s %s ... ", c.ID, c.Name)
		res, err := runner.Run(ctx, c)
		if err != nil {
			fmt.Println("evidence write failed:", err)
			continue
		}
		fmt.Printf("%s (f5=%s sentrygate=%s infrastructure=%s mutations=%d/%d)\n",
			res.Status, res.F5Result, res.SentryGateResult, res.InfrastructureResult, res.MutationCount, res.ExpectedMutations)
		if res.BlockedReason != "" {
			fmt.Println("   blocked:", res.BlockedReason)
		}
		for _, ch := range res.Checks {
			if ch.Result != evidence.CheckPass {
				fmt.Printf("   %s: %s\n      expected: %s\n      observed: %s\n", ch.Result, ch.Name, ch.Expected, ch.Observed)
			}
		}
	}
	writeMatrix(root, evidenceDir)
}

func writeMatrix(root, evidenceDir string) {
	results, err := evidence.Load(evidenceDir)
	if err != nil {
		fail(err)
	}
	path := filepath.Join(root, "validation", "results", "validation-matrix.csv")
	if err := report.WriteMatrix(path, report.Matrix(cases.Catalogue(), results, evidenceDir)); err != nil {
		fail(err)
	}
	fmt.Println("wrote", evidence.Slash(path))
}

// repoRoot returns the nearest directory at or above the working directory
// that contains go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found; run from inside the repository")
		}
		dir = parent
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "f5validate:", err)
	os.Exit(2)
}
