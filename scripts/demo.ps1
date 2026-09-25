$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$env:SENTRYGATE_API_KEY = if ($env:SENTRYGATE_API_KEY) { $env:SENTRYGATE_API_KEY } else { "dev-secret-change-me" }
$env:SENTRYGATE_URL = if ($env:SENTRYGATE_URL) { $env:SENTRYGATE_URL } else { "http://localhost:8080" }

Write-Host "==> Starting SentryGate stack (Temporal + worker + proxy)"
docker compose up -d --build

Write-Host "==> Waiting for proxy on :8080"
$ready = $false
for ($i = 1; $i -le 60; $i++) {
  try {
    $r = Invoke-WebRequest -Uri "$($env:SENTRYGATE_URL)/healthz" -UseBasicParsing -TimeoutSec 2
    if ($r.StatusCode -eq 200) {
      Write-Host "proxy is up"
      $ready = $true
      break
    }
  } catch {
    Start-Sleep -Seconds 2
  }
}
if (-not $ready) {
  Write-Host "proxy did not become healthy in time" -ForegroundColor Red
  docker compose logs --tail=80 proxy worker temporal
  exit 1
}

Write-Host "==> Running agent-sim scenarios (deny / allow / rollback)"
go run ./cmd/agent-sim all

Write-Host ""
Write-Host "Demo complete."
Write-Host "  Proxy:       $($env:SENTRYGATE_URL)"
Write-Host "  Temporal UI: http://localhost:8088"
Write-Host "  API key:     $($env:SENTRYGATE_API_KEY)"
