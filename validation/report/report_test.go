package report

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/f5"
)

var catalogue = []evidence.CatalogueEntry{{ID: "TC01", Name: "Legitimate"}, {ID: "TC02", Name: "Tool substitution"}}

func passing(id, kind, f5Result, sg string) evidence.Result {
	return evidence.Result{
		TestID: id, TestName: id, Kind: kind, Status: evidence.StatusVerified, F5Result: f5Result,
		SentryGateResult: sg, InfrastructureResult: evidence.InfraNotExecuted, EvidencePath: "validation/evidence/" + id,
		Checks: []evidence.Check{{Name: "c", Expected: "x", Observed: "x", Result: evidence.CheckPass}},
	}
}

// writeEvidence creates the required files for id so the row is complete.
func writeEvidence(t *testing.T, dir, id string) {
	t.Helper()
	for _, f := range evidence.RequiredFiles {
		if err := evidence.WriteJSON(filepath.Join(dir, id, f), map[string]string{"test_id": id}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNoEvidenceIsNotTested(t *testing.T) {
	rows := Matrix(catalogue, nil, t.TempDir())
	for _, r := range rows {
		if r.Status != evidence.StatusNotTested || r.F5Result != f5.ResultNotTested || r.Result != nil {
			t.Fatalf("row = %+v, want NOT_TESTED without a result", r)
		}
	}
	md := Generate(rows)
	if !strings.Contains(md, "Tests with evidence: 0.") {
		t.Fatal("summary does not state that no test has evidence")
	}
	// Matrix rows end with the status column.
	if strings.Contains(md, "| VERIFIED |\n") || strings.Contains(md, "| FAILED |\n") {
		t.Error("report without evidence has a VERIFIED or FAILED matrix row")
	}
}

func TestRequiredPhrasesAlwaysPresent(t *testing.T) {
	for _, rows := range [][]Row{
		Matrix(catalogue, nil, t.TempDir()),
		Matrix(catalogue, []evidence.Result{passing("TC01", evidence.KindLegitimate, f5.ResultNotTested, evidence.SGExecuted)}, t.TempDir()),
	} {
		md := Generate(rows)
		for _, p := range []string{PhraseNotTested, PhraseUnknown, PhraseF5Prevented, PhraseF5AllowedSGBlk} {
			if !strings.Contains(md, p) {
				t.Errorf("report does not contain %q", p)
			}
		}
		if strings.Contains(strings.ToLower(md), "vulnerable") {
			t.Error("report uses the word vulnerable")
		}
		for i := 1; i <= 15; i++ {
			if !strings.Contains(md, "\n## "+strconv.Itoa(i)+". ") {
				t.Errorf("report has no section %d", i)
			}
		}
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	writeEvidence(t, dir, "TC01")
	results := []evidence.Result{passing("TC01", evidence.KindLegitimate, f5.ResultNotTested, evidence.SGExecuted)}
	a := Generate(Matrix(catalogue, results, dir))
	b := Generate(Matrix(catalogue, results, dir))
	if a != b {
		t.Fatal("same evidence produced different reports")
	}
}

func TestF5Wording(t *testing.T) {
	row := func(kind, f5r, sg string) Row {
		r := passing("TC02", kind, f5r, sg)
		return Row{TestID: "TC02", F5Result: f5r, SentryGateResult: sg, Result: &r}
	}
	cases := []struct {
		r    Row
		want string
		not  []string
	}{
		{row(evidence.KindAttack, f5.ResultNotTested, evidence.SGBlocked), PhraseNotTested, []string{"allowed", "prevented"}},
		{row(evidence.KindAttack, f5.ResultUnknown, evidence.SGBlocked), PhraseUnknown, nil},
		{row(evidence.KindAttack, f5.ResultPrevented, evidence.SGBlocked), PhraseF5Prevented, nil},
		{row(evidence.KindAttack, f5.ResultAllowed, evidence.SGBlocked), PhraseF5AllowedSGBlk, nil},
		{row(evidence.KindAttack, f5.ResultAllowed, evidence.SGDidNotBlock), "not a vulnerability finding", []string{PhraseF5AllowedSGBlk}},
		{row(evidence.KindLegitimate, f5.ResultAllowed, evidence.SGExecuted), "as expected", []string{PhraseF5AllowedSGBlk}},
		{Row{TestID: "TC09", F5Result: f5.ResultNotTested}, PhraseNotTested, nil},
	}
	for i, tc := range cases {
		got := f5Wording(tc.r)
		if !strings.Contains(got, tc.want) {
			t.Errorf("case %d: wording %q does not contain %q", i, got, tc.want)
		}
		for _, n := range tc.not {
			if strings.Contains(got, n) {
				t.Errorf("case %d: wording %q contains %q", i, got, n)
			}
		}
	}
}

func TestInconsistentOrIncompleteEvidenceIsUnknown(t *testing.T) {
	dir := t.TempDir()
	writeEvidence(t, dir, "TC01")
	bad := passing("TC01", evidence.KindLegitimate, f5.ResultNotTested, evidence.SGExecuted)
	bad.Checks[0].Result = evidence.CheckFail
	incomplete := passing("TC02", evidence.KindAttack, f5.ResultNotTested, evidence.SGBlocked)
	rows := Matrix(catalogue, []evidence.Result{bad, incomplete}, dir)
	if rows[0].Status != evidence.StatusUnknown {
		t.Errorf("VERIFIED with a failed check: status %s, want UNKNOWN", rows[0].Status)
	}
	if rows[1].Status != evidence.StatusUnknown || len(rows[1].MissingFiles) != len(evidence.RequiredFiles) {
		t.Errorf("missing files: status %s missing %v, want UNKNOWN with all files missing", rows[1].Status, rows[1].MissingFiles)
	}
}

func TestMatrixCSV(t *testing.T) {
	dir := t.TempDir()
	writeEvidence(t, dir, "TC01")
	path := filepath.Join(dir, "m.csv")
	rows := Matrix(catalogue, []evidence.Result{passing("TC01", evidence.KindLegitimate, f5.ResultNotTested, evidence.SGExecuted)}, dir)
	if err := WriteMatrix(path, rows); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(recs[0], ",") != "test_id,test_name,f5_result,sentrygate_result,infrastructure_result,mutation_count,evidence_path,status,notes" {
		t.Fatalf("header = %v", recs[0])
	}
	if len(recs) != 3 || recs[1][7] != evidence.StatusVerified || recs[2][7] != evidence.StatusNotTested || recs[2][2] != f5.ResultNotTested {
		t.Fatalf("rows = %v", recs[1:])
	}
	allowed := map[string]bool{evidence.StatusVerified: true, evidence.StatusFailed: true, evidence.StatusBlocked: true,
		evidence.StatusNotTested: true, evidence.StatusUnknown: true, evidence.StatusNotApplicable: true}
	for _, r := range recs[1:] {
		if !allowed[r[7]] {
			t.Errorf("status %q is not an allowed status", r[7])
		}
	}
}
