package decision

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// requestHashDomain separates request hashes from any other SHA-256 use.
const requestHashDomain = "sentrygate.request.v1"

// RequestHash returns the canonical SHA-256 of a proposal as lowercase hex.
//
// Encoding: the domain string, then ID, type, target ID and payload, each
// preceded by its byte length as a big-endian uint64. The hash is computed over
// decoded field values, so JSON whitespace and key order do not affect it, and
// the length prefixes make field boundaries unambiguous.
func RequestHash(p contracts.AgentProposal) string {
	h := sha256.New()
	h.Write([]byte(requestHashDomain))
	for _, field := range []string{p.ID, string(p.Type), p.TargetID, p.Payload} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(field)))
		h.Write(n[:])
		h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}
