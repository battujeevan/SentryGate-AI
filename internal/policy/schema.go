package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// EnvironmentMode is the rule applied to every mutation in an environment.
type EnvironmentMode string

const (
	ModeAllow           EnvironmentMode = "allow"
	ModeRequireApproval EnvironmentMode = "require_approval"
	ModeDeny            EnvironmentMode = "deny"
)

const maxParallelTasksLimit = 10000

var (
	versionPattern     = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
	environmentPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	commandPattern     = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
	agentIDPattern     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	targetIDPattern    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

// document is the on-disk YAML schema. Every field is required; there are no defaults.
type document struct {
	Version          string            `yaml:"version"`
	MaxParallelTasks int               `yaml:"max_parallel_tasks"`
	Environments     map[string]string `yaml:"environments"`
	Commands         []string          `yaml:"commands"`
	Agents           []agentDocument   `yaml:"agents"`
	Targets          []targetDocument  `yaml:"targets"`
}

type agentDocument struct {
	ID       string   `yaml:"id"`
	Commands []string `yaml:"commands"`
}

type targetDocument struct {
	ID          string `yaml:"id"`
	Environment string `yaml:"environment"`
	Protected   *bool  `yaml:"protected"`
}

// Target is a registered infrastructure target.
type Target struct {
	ID          string
	Environment string
	Protected   bool
}

// Snapshot is an immutable, validated policy. It is safe for concurrent use.
type Snapshot struct {
	version          string
	digest           string
	maxParallelTasks int
	environments     map[string]EnvironmentMode
	commands         map[contracts.CommandType]struct{}
	agents           map[string]map[contracts.CommandType]struct{}
	targets          map[string]Target
}

// Version is the operator-supplied policy version string.
func (s *Snapshot) Version() string { return s.version }

// Digest is the hex SHA-256 of the raw policy bytes.
func (s *Snapshot) Digest() string { return s.digest }

// MaxParallelTasks is the ingress concurrency bound declared by the policy.
func (s *Snapshot) MaxParallelTasks() int { return s.maxParallelTasks }

// HasCommand reports whether the command is in the catalogue.
func (s *Snapshot) HasCommand(c contracts.CommandType) bool {
	_, ok := s.commands[c]
	return ok
}

// HasAgent reports whether the agent is declared.
func (s *Snapshot) HasAgent(agentID string) bool {
	_, ok := s.agents[agentID]
	return ok
}

// AgentMayUse reports whether a declared agent is permitted to issue the command.
func (s *Snapshot) AgentMayUse(agentID string, c contracts.CommandType) bool {
	cmds, ok := s.agents[agentID]
	if !ok {
		return false
	}
	_, ok = cmds[c]
	return ok
}

// Target returns the registered target with the given ID.
func (s *Snapshot) Target(id string) (Target, bool) {
	t, ok := s.targets[id]
	return t, ok
}

// EnvironmentMode returns the rule for a declared environment.
func (s *Snapshot) EnvironmentMode(name string) (EnvironmentMode, bool) {
	m, ok := s.environments[name]
	return m, ok
}

// AgentIDs returns the declared agent IDs in sorted order.
func (s *Snapshot) AgentIDs() []string {
	ids := make([]string, 0, len(s.agents))
	for id := range s.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Parse strictly decodes and validates a policy document.
func Parse(raw []byte) (*Snapshot, error) {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)

	var doc document
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("policy: document is empty")
		}
		return nil, fmt.Errorf("policy: decode: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("policy: file must contain exactly one YAML document")
	}

	s, err := build(doc)
	if err != nil {
		return nil, fmt.Errorf("policy: %w", err)
	}
	s.digest = rawDigest(raw)
	return s, nil
}

func rawDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func build(doc document) (*Snapshot, error) {
	if doc.Version == "" {
		return nil, errors.New("version is required")
	}
	if !versionPattern.MatchString(doc.Version) {
		return nil, fmt.Errorf("version %q is invalid", doc.Version)
	}
	if doc.MaxParallelTasks < 1 || doc.MaxParallelTasks > maxParallelTasksLimit {
		return nil, fmt.Errorf("max_parallel_tasks must be between 1 and %d", maxParallelTasksLimit)
	}

	s := &Snapshot{
		version:          doc.Version,
		maxParallelTasks: doc.MaxParallelTasks,
		environments:     make(map[string]EnvironmentMode, len(doc.Environments)),
		commands:         make(map[contracts.CommandType]struct{}, len(doc.Commands)),
		agents:           make(map[string]map[contracts.CommandType]struct{}, len(doc.Agents)),
		targets:          make(map[string]Target, len(doc.Targets)),
	}

	if len(doc.Environments) == 0 {
		return nil, errors.New("environments must declare at least one environment")
	}
	for name, mode := range doc.Environments {
		if !environmentPattern.MatchString(name) {
			return nil, fmt.Errorf("environment name %q is invalid", name)
		}
		switch m := EnvironmentMode(mode); m {
		case ModeAllow, ModeRequireApproval, ModeDeny:
			s.environments[name] = m
		default:
			return nil, fmt.Errorf("environment %q: mode %q must be allow, require_approval or deny", name, mode)
		}
	}

	if len(doc.Commands) == 0 {
		return nil, errors.New("commands must declare at least one command")
	}
	for _, c := range doc.Commands {
		if !commandPattern.MatchString(c) {
			return nil, fmt.Errorf("command %q is invalid", c)
		}
		ct := contracts.CommandType(c)
		if _, dup := s.commands[ct]; dup {
			return nil, fmt.Errorf("command %q is declared more than once", c)
		}
		s.commands[ct] = struct{}{}
	}

	if len(doc.Agents) == 0 {
		return nil, errors.New("agents must declare at least one agent")
	}
	for i, a := range doc.Agents {
		if !agentIDPattern.MatchString(a.ID) {
			return nil, fmt.Errorf("agents[%d]: id %q is invalid", i, a.ID)
		}
		if _, dup := s.agents[a.ID]; dup {
			return nil, fmt.Errorf("agent %q is declared more than once", a.ID)
		}
		if len(a.Commands) == 0 {
			return nil, fmt.Errorf("agent %q: commands must not be empty", a.ID)
		}
		allowed := make(map[contracts.CommandType]struct{}, len(a.Commands))
		for _, c := range a.Commands {
			ct := contracts.CommandType(c)
			if _, ok := s.commands[ct]; !ok {
				return nil, fmt.Errorf("agent %q: command %q is not in the command catalogue", a.ID, c)
			}
			if _, dup := allowed[ct]; dup {
				return nil, fmt.Errorf("agent %q: command %q is listed more than once", a.ID, c)
			}
			allowed[ct] = struct{}{}
		}
		s.agents[a.ID] = allowed
	}

	if len(doc.Targets) == 0 {
		return nil, errors.New("targets must declare at least one target")
	}
	for i, t := range doc.Targets {
		if !targetIDPattern.MatchString(t.ID) {
			return nil, fmt.Errorf("targets[%d]: id %q is invalid", i, t.ID)
		}
		if _, dup := s.targets[t.ID]; dup {
			return nil, fmt.Errorf("target %q is declared more than once", t.ID)
		}
		if _, ok := s.environments[t.Environment]; !ok {
			return nil, fmt.Errorf("target %q: environment %q is not declared", t.ID, t.Environment)
		}
		if t.Protected == nil {
			return nil, fmt.Errorf("target %q: protected must be set explicitly", t.ID)
		}
		s.targets[t.ID] = Target{ID: t.ID, Environment: t.Environment, Protected: *t.Protected}
	}

	return s, nil
}

// Source supplies the currently active policy snapshot. Current may return nil
// when no valid policy is available; callers must fail closed.
type Source interface {
	Current() *Snapshot
}

// StaticSource returns a Source that always yields s.
func StaticSource(s *Snapshot) Source { return staticSource{s: s} }

type staticSource struct{ s *Snapshot }

func (s staticSource) Current() *Snapshot { return s.s }
