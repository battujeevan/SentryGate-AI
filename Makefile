.PHONY: demo demo-ps up down test bench build tidy

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

bench:
	go test ./proxy -bench=BenchmarkProxy -benchmem -count=1

build:
	go build -o bin/sentrygate ./cmd/sentrygate
	go build -o bin/worker ./cmd/worker
	go build -o bin/agent-sim ./cmd/agent-sim

tidy:
	go mod tidy
