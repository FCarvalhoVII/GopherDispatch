.PHONY: run run-api run-worker test test-race benchmark lint docker-up docker-down

run: docker-up
run-api:
	go run ./cmd/api
run-worker:
	go run ./cmd/worker
test:
	go test ./...
test-race:
	go test -race ./...
benchmark:
	go test -bench=. ./...
lint:
	go fmt ./...
	go vet ./...
docker-up:
	docker compose up --build
docker-down:
	docker compose down

