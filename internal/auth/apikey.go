package auth

import (
	"crypto/subtle"
	"net/http"
)

const HeaderAPIKey = "X-API-Key"

// APIKeyMiddleware rejects requests that do not present a matching API key.
// Health endpoints should be registered outside this middleware.
func APIKeyMiddleware(apiKey string, next http.Handler) http.Handler {
	expected := []byte(apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get(HeaderAPIKey))
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"status":"unauthorized","error":"missing or invalid API key"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
