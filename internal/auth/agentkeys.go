package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode"
)

// HeaderAPIKey is the request header carrying the agent API key.
const HeaderAPIKey = "X-API-Key"

// MinKeyLength is the shortest accepted agent API key.
const MinKeyLength = 16

const maxKeyLength = 256

var agentIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type keyEntry struct {
	agentID string
	digest  [sha256.Size]byte
}

// Keyring maps agent API keys to agent IDs. Keys are held only as SHA-256
// digests and are never exposed by any method.
type Keyring struct {
	entries []keyEntry
}

// ParseAgentKeys parses "agent-id:key,agent-id:key". It rejects malformed
// entries, duplicate agent IDs, duplicate keys, short keys and keys containing
// whitespace. Error messages identify entries by position and never include key material.
func ParseAgentKeys(spec string) (*Keyring, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, errors.New("agent keys: no entries configured")
	}
	parts := strings.Split(spec, ",")
	k := &Keyring{entries: make([]keyEntry, 0, len(parts))}
	seenIDs := make(map[string]struct{}, len(parts))
	seenKeys := make(map[[sha256.Size]byte]struct{}, len(parts))
	for i, part := range parts {
		pos := i + 1
		id, key, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("agent keys: entry %d is not in agent-id:key form", pos)
		}
		if !agentIDPattern.MatchString(id) {
			return nil, fmt.Errorf("agent keys: entry %d has an invalid agent id", pos)
		}
		if len(key) < MinKeyLength {
			return nil, fmt.Errorf("agent keys: entry %d (%s) key is shorter than %d bytes", pos, id, MinKeyLength)
		}
		if len(key) > maxKeyLength {
			return nil, fmt.Errorf("agent keys: entry %d (%s) key is longer than %d bytes", pos, id, maxKeyLength)
		}
		if strings.IndexFunc(key, unicode.IsSpace) >= 0 {
			return nil, fmt.Errorf("agent keys: entry %d (%s) key contains whitespace", pos, id)
		}
		if _, dup := seenIDs[id]; dup {
			return nil, fmt.Errorf("agent keys: entry %d duplicates agent id %s", pos, id)
		}
		digest := sha256.Sum256([]byte(key))
		if _, dup := seenKeys[digest]; dup {
			return nil, fmt.Errorf("agent keys: entry %d (%s) reuses a key assigned to another agent", pos, id)
		}
		seenIDs[id] = struct{}{}
		seenKeys[digest] = struct{}{}
		k.entries = append(k.entries, keyEntry{agentID: id, digest: digest})
	}
	return k, nil
}

// Resolve returns the agent ID bound to presented. Every entry is compared in
// constant time and the loop never exits early, so timing does not reveal
// which entry, if any, matched.
func (k *Keyring) Resolve(presented string) (string, bool) {
	digest := sha256.Sum256([]byte(presented))
	match := -1
	found := 0
	for i := range k.entries {
		eq := subtle.ConstantTimeCompare(digest[:], k.entries[i].digest[:])
		match = subtle.ConstantTimeSelect(eq, i, match)
		found |= eq
	}
	if found != 1 {
		return "", false
	}
	return k.entries[match].agentID, true
}

// AgentIDs returns the configured agent IDs in configuration order.
func (k *Keyring) AgentIDs() []string {
	ids := make([]string, len(k.entries))
	for i, e := range k.entries {
		ids[i] = e.agentID
	}
	return ids
}

type agentIDKey struct{}

// AgentIDFromContext returns the authenticated agent ID set by Middleware.
func AgentIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(agentIDKey{}).(string)
	return id, ok && id != ""
}

// WithAgentID returns a context carrying an authenticated agent ID.
func WithAgentID(ctx context.Context, agentID string) context.Context {
	return context.WithValue(ctx, agentIDKey{}, agentID)
}

// Middleware authenticates the X-API-Key header against the keyring and
// stores the resolved agent ID in the request context. Unauthenticated
// requests receive 401 and never reach next.
func Middleware(k *Keyring, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agentID, ok := k.Resolve(r.Header.Get(HeaderAPIKey))
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"status":"unauthorized","error":"missing or invalid API key"}`))
			return
		}
		next.ServeHTTP(w, r.WithContext(WithAgentID(r.Context(), agentID)))
	})
}
