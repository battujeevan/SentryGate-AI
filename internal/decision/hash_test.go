package decision_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// referenceHash re-implements the documented encoding byte by byte so the
// production implementation is checked against the specification, not itself.
func referenceHash(id, typ, target, payload string) string {
	buf := []byte("sentrygate.request.v1")
	for _, f := range []string{id, typ, target, payload} {
		n := uint64(len(f))
		buf = append(buf,
			byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
		buf = append(buf, f...)
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

func TestRequestHashMatchesReferenceEncoding(t *testing.T) {
	p := contracts.AgentProposal{ID: "p-1", Type: contracts.CmdModifyRouting, TargetID: "edge-1", Payload: `{"route":"stable"}`}
	got := decision.RequestHash(p)
	want := referenceHash("p-1", "MODIFY_ROUTING", "edge-1", `{"route":"stable"}`)
	if got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}

func TestRequestHashIndependentOfJSONFormatting(t *testing.T) {
	compact := `{"id":"p-1","type":"MODIFY_ROUTING","target_id":"edge-1","payload":"{\"route\":\"stable\"}"}`
	reordered := "{\n  \"payload\" : \"{\\\"route\\\":\\\"stable\\\"}\",\n\t\"target_id\": \"edge-1\",\n  \"type\":   \"MODIFY_ROUTING\",\n  \"id\": \"p-1\"\n}\n"

	var a, b contracts.AgentProposal
	if err := json.Unmarshal([]byte(compact), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(reordered), &b); err != nil {
		t.Fatal(err)
	}
	if decision.RequestHash(a) != decision.RequestHash(b) {
		t.Fatal("hash changed with JSON whitespace or key order")
	}
}

func TestRequestHashChangesWithEveryField(t *testing.T) {
	base := contracts.AgentProposal{ID: "p-1", Type: contracts.CmdModifyRouting, TargetID: "edge-1", Payload: `{"route":"stable"}`}
	h := decision.RequestHash(base)

	variants := map[string]contracts.AgentProposal{
		"payload": {ID: base.ID, Type: base.Type, TargetID: base.TargetID, Payload: `{"route":"canary"}`},
		"id":      {ID: "p-2", Type: base.Type, TargetID: base.TargetID, Payload: base.Payload},
		"type":    {ID: base.ID, Type: contracts.CmdUpdateCert, TargetID: base.TargetID, Payload: base.Payload},
		"target":  {ID: base.ID, Type: base.Type, TargetID: "edge-2", Payload: base.Payload},
	}
	for name, v := range variants {
		if decision.RequestHash(v) == h {
			t.Fatalf("hash did not change when %s changed", name)
		}
	}
}

func TestRequestHashFieldBoundariesAreUnambiguous(t *testing.T) {
	a := contracts.AgentProposal{ID: "ab", Type: "C", TargetID: "t"}
	b := contracts.AgentProposal{ID: "a", Type: "bC", TargetID: "t"}
	if decision.RequestHash(a) == decision.RequestHash(b) {
		t.Fatal("shifting bytes between fields must change the hash")
	}
}
