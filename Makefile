.PHONY: fmt test test-integration vet build frontend-dev frontend-test frontend-build compose-up compose-down e2e failure-drills

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

test-integration:
	test -n "$$TEST_DATABASE_URL"
	go test -count=1 ./internal/postgres

vet:
	go vet ./...

build:
	go build ./cmd/...

frontend-dev:
	cd web && npm run dev

frontend-test:
	cd web && npm run lint && npm test

frontend-build:
	cd web && npm run build

compose-up:
	docker compose up --build

compose-down:
	docker compose down

e2e:
	sh scripts/e2e.sh

failure-drills:
	sh scripts/failure-drills.sh
