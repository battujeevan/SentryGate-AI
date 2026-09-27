package auth_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/auth"
)

const (
	keyA = "key-a-0123456789abcdef"
	keyB = "key-b-0123456789abcdef"
)

func TestParseAgentKeysAndResolve(t *testing.T) {
	k, err := auth.ParseAgentKeys("agent-a:" + keyA + ",agent-b:" + keyB)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{keyA: "agent-a", keyB: "agent-b"} {
		got, ok := k.Resolve(key)
		if !ok || got != want {
			t.Fatalf("Resolve = %q,%v; want %q", got, ok, want)
		}
	}
	for _, bad := range []string{"", "wrong-key-0123456789", keyA + "x", keyA[:len(keyA)-1]} {
		if id, ok := k.Resolve(bad); ok {
			t.Fatalf("Resolve(%q) unexpectedly matched %q", bad, id)
		}
	}
}

func TestParseAgentKeysRejectsInvalidSpecs(t *testing.T) {
	cases := map[string]string{
		"empty":          "",
		"missing colon":  "agent-a" + keyA,
		"empty id":       ":" + keyA,
		"invalid id":     "agent a:" + keyA,
		"short key":      "agent-a:Tiny9Key",
		"whitespace key": "agent-a:key with spaces 0123456789",
		"duplicate id":   "agent-a:" + keyA + ",agent-a:" + keyB,
		"duplicate key":  "agent-a:" + keyA + ",agent-b:" + keyA,
		"empty entry":    "agent-a:" + keyA + ",",
		"overlong key":   "agent-a:" + strings.Repeat("k", 257),
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := auth.ParseAgentKeys(spec)
			if err == nil {
				t.Fatalf("expected %q to be rejected", spec)
			}
			for _, secret := range []string{keyA, keyB, "Tiny9Key", "key with spaces"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error message leaks key material: %v", err)
				}
			}
		})
	}
}

func TestMiddlewareSetsAgentIdentity(t *testing.T) {
	k, err := auth.ParseAgentKeys("agent-a:" + keyA + ",agent-b:" + keyB)
	if err != nil {
		t.Fatal(err)
	}
	var seen string
	h := auth.Middleware(k, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, _ = auth.AgentIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name    string
		key     string
		status  int
		agentID string
	}{
		{"missing key", "", http.StatusUnauthorized, ""},
		{"wrong key", "not-a-valid-key-000000", http.StatusUnauthorized, ""},
		{"agent a", keyA, http.StatusOK, "agent-a"},
		{"agent b", keyB, http.StatusOK, "agent-b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen = ""
			req := httptest.NewRequest(http.MethodGet, "/v1/policy", nil)
			if tc.key != "" {
				req.Header.Set(auth.HeaderAPIKey, tc.key)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("status = %d, want %d", rr.Code, tc.status)
			}
			if seen != tc.agentID {
				t.Fatalf("agent id = %q, want %q", seen, tc.agentID)
			}
			if strings.Contains(rr.Body.String(), keyA) || strings.Contains(rr.Body.String(), keyB) {
				t.Fatal("response leaks key material")
			}
		})
	}
}
