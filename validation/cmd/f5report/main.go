// Command f5report writes the validation matrix and the validation report
// from the evidence stored in validation/evidence. It runs nothing and adds
// nothing that is not in the evidence.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/battujeevan/SentryGate-AI/validation/cases"
	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/report"
)

func main() {
	root, err := repoRoot()
	if err != nil {
		fail(err)
	}
	evidenceDir := filepath.Join(root, "validation", "evidence")
	results, err := evidence.Load(evidenceDir)
	if err != nil {
		fail(err)
	}
	rows := report.Matrix(cases.Catalogue(), results, evidenceDir)
	matrix := filepath.Join(root, "validation", "results", "validation-matrix.csv")
	if err := report.WriteMatrix(matrix, rows); err != nil {
		fail(err)
	}
	out := filepath.Join(root, "validation", "report", "F5-AI-Gateway-Independent-Security-Validation.md")
	if err := os.WriteFile(out, []byte(report.Generate(rows)), 0o644); err != nil {
		fail(err)
	}
	fmt.Printf("evidence for %d of %d tests\nwrote %s\nwrote %s\n", len(results), len(rows), evidence.Slash(matrix), evidence.Slash(out))
}

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
	fmt.Fprintln(os.Stderr, "f5report:", err)
	os.Exit(2)
}
