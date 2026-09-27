.PHONY: demo demo-ps up down test race bench check build tidy

up:
	docker compose up -d --build

down:
	docker compose down -v

demo:
	bash scripts/demo.sh

demo-ps:
	powershell -ExecutionPolicy Bypass -File scripts/demo.ps1

test:
	go test ./...

# Requires cgo (a C toolchain).
race:
	go test -race -count=1 ./...

bench:
	go test ./proxy -run '^$$' -bench=BenchmarkProxy -benchmem -count=1

# Run every agent-sim scenario against a running stack and fail on any unexpected result.
check:
	go run ./cmd/agent-sim -check all

build:
	go build -o bin/sentrygate ./cmd/sentrygate
	go build -o bin/worker ./cmd/worker
	go build -o bin/agent-sim ./cmd/agent-sim

tidy:
	go mod tidy
