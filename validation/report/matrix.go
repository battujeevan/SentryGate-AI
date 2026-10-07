// Package report builds the validation matrix and the validation report from
// stored evidence only. Neither depends on the time it is run or on anything
// outside the evidence directory, so the same evidence always produces the
// same output. A test without evidence is reported as NOT_TESTED.
package report

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/f5"
)

// MatrixHeader is the validation matrix's column order.
var MatrixHeader = []string{
	"test_id", "test_name", "f5_result", "sentrygate_result", "infrastructure_result",
	"mutation_count", "evidence_path", "status", "notes",
}

// Row is one validation matrix row. Result is nil for a test without evidence.
type Row struct {
	TestID, TestName, F5Result, SentryGateResult, InfrastructureResult string
	MutationCount, EvidencePath, Status, Notes                         string
	Result                                                             *evidence.Result
	MissingFiles                                                       []string
}

// Matrix returns one row per catalogue entry, in catalogue order, followed by
// rows for any evidence of tests the catalogue does not list.
func Matrix(catalogue []evidence.CatalogueEntry, results []evidence.Result, evidenceDir string) []Row {
	byID := map[string]*evidence.Result{}
	for i := range results {
		byID[results[i].TestID] = &results[i]
	}
	var rows []Row
	listed := map[string]bool{}
	for _, c := range catalogue {
		listed[c.ID] = true
		rows = append(rows, row(c.ID, c.Name, byID[c.ID], evidenceDir))
	}
	for i := range results {
		if !listed[results[i].TestID] {
			rows = append(rows, row(results[i].TestID, results[i].TestName, &results[i], evidenceDir))
		}
	}
	return rows
}

func row(id, name string, r *evidence.Result, evidenceDir string) Row {
	if r == nil {
		return Row{
			TestID: id, TestName: name, F5Result: f5.ResultNotTested, SentryGateResult: evidence.StatusNotTested,
			InfrastructureResult: evidence.StatusNotTested, Status: evidence.StatusNotTested,
			Notes: "No evidence: the test has not been run.",
		}
	}
	out := Row{
		TestID: id, TestName: name, F5Result: r.F5Result, SentryGateResult: r.SentryGateResult,
		InfrastructureResult: r.InfrastructureResult, MutationCount: fmt.Sprint(r.MutationCount),
		EvidencePath: r.EvidencePath, Status: r.Status, Result: r,
	}
	var notes []string
	out.MissingFiles = evidence.MissingFiles(filepath.Join(evidenceDir, id))
	if r.Status == evidence.StatusBlocked {
		out.MutationCount = ""
		notes = append(notes, "Blocked: "+r.BlockedReason)
	}
	if len(out.MissingFiles) > 0 && r.Status != evidence.StatusBlocked {
		out.Status = evidence.StatusUnknown
		notes = append(notes, "Evidence incomplete, missing "+strings.Join(out.MissingFiles, ", ")+".")
	}
	if consistent, why := statusConsistent(r); !consistent {
		out.Status = evidence.StatusUnknown
		notes = append(notes, why)
	}
	notes = append(notes, fmt.Sprintf("Mutations observed %d, expected %d.", r.MutationCount, r.ExpectedMutations))
	for _, c := range r.Checks {
		if c.Result != evidence.CheckPass {
			notes = append(notes, fmt.Sprintf("%s check: %s (expected %s; observed %s).", c.Result, c.Name, c.Expected, c.Observed))
		}
	}
	switch r.F5Result {
	case f5.ResultNotTested:
		notes = append(notes, "F5 not in the request path.")
	case f5.ResultNotApplicable:
		notes = append(notes, "No attack request in this test passes through a gateway in front of ingress.")
	}
	out.Notes = strings.Join(notes, " ")
	return out
}

// statusConsistent checks that a stored status agrees with the stored checks.
func statusConsistent(r *evidence.Result) (bool, string) {
	fails, unknowns := 0, 0
	for _, c := range r.Checks {
		switch c.Result {
		case evidence.CheckFail:
			fails++
		case evidence.CheckUnknown:
			unknowns++
		}
	}
	switch r.Status {
	case evidence.StatusVerified:
		if fails > 0 || unknowns > 0 || len(r.Checks) == 0 {
			return false, "Stored status VERIFIED does not agree with the stored checks; treated as UNKNOWN."
		}
	case evidence.StatusFailed:
		if fails == 0 {
			return false, "Stored status FAILED has no failed check; treated as UNKNOWN."
		}
	case evidence.StatusBlocked, evidence.StatusUnknown, evidence.StatusNotApplicable:
	default:
		return false, fmt.Sprintf("Stored status %q is not a recognised status; treated as UNKNOWN.", r.Status)
	}
	return true, ""
}

// WriteMatrix writes rows as CSV.
func WriteMatrix(path string, rows []Row) error {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(MatrixHeader)
	for _, r := range rows {
		_ = w.Write([]string{r.TestID, r.TestName, r.F5Result, r.SentryGateResult, r.InfrastructureResult,
			r.MutationCount, r.EvidencePath, r.Status, r.Notes})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
