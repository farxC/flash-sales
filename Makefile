.PHONY: up down test test-integration psql

up:
	docker compose up --build

down:
	docker compose down

# Fast unit tests only -- no external dependencies required.
test:
	cd backend && go test ./...

# Runs the full suite including the Postgres concurrency tests in
# internal/product/postgres_repository_test.go. Requires Postgres to
# already be running (`make up`, or `docker compose up -d postgres`).
test-integration:
	cd backend && POSTGRES_DSN="postgres://flashsales:flashsales@localhost:5432/flashsales?sslmode=disable" go test ./... -v

# Opens a psql shell against the running dev database.
psql:
	docker exec -it flash-sales-postgres-1 psql -U flashsales -d flashsales
