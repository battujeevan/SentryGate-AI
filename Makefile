.PHONY: demo demo-ps up down test race bench check build tidy validation-f5 validation-report

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

# F5 validation harness (validation/F5/README.md). Runs locally without F5:
# every F5 result is NOT_TESTED until a real F5 observation provider exists.
validation-f5:
	go run ./validation/cmd/f5validate -tc all
	go run ./validation/cmd/f5report

# One test, e.g. make validation-f5-tc05.
validation-f5-tc%:
	go run ./validation/cmd/f5validate -tc TC$*

# Matrix and report from stored evidence only; runs no test.
validation-report:
	go run ./validation/cmd/f5report
