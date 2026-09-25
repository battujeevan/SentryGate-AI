package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sentrygate-ai/sentrygate/internal/auth"
)

func TestAPIKeyMiddleware_AcceptsValidKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := auth.APIKeyMiddleware("secret", next)

	req := httptest.NewRequest(http.MethodGet, "/v1/policy", nil)
	req.Header.Set(auth.HeaderAPIKey, "secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAPIKeyMiddleware_RejectsMissingKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := auth.APIKeyMiddleware("secret", next)

	req := httptest.NewRequest(http.MethodGet, "/v1/policy", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}
