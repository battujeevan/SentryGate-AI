#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export SENTRYGATE_API_KEY="${SENTRYGATE_API_KEY:-dev-secret-change-me}"
export SENTRYGATE_URL="${SENTRYGATE_URL:-http://localhost:8080}"

echo "==> Starting SentryGate stack (Temporal + worker + proxy)"
docker compose up -d --build

echo "==> Waiting for proxy on :8080"
for i in $(seq 1 60); do
  if curl -sf "$SENTRYGATE_URL/healthz" >/dev/null; then
    echo "proxy is up"
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "proxy did not become healthy in time" >&2
    docker compose logs --tail=80 proxy worker temporal
    exit 1
  fi
done

echo "==> Running agent-sim scenarios (deny / allow / rollback)"
go run ./cmd/agent-sim all

echo ""
echo "Demo complete."
echo "  Proxy:       $SENTRYGATE_URL"
echo "  Temporal UI: http://localhost:8088"
echo "  API key:     $SENTRYGATE_API_KEY"
