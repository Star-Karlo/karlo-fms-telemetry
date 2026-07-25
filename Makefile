.PHONY: up up-app down migrate migrate-remote psql redis-cli logs seed reset swagger run-geocode run-ingest load-geocode

# Apply migrations against $DATABASE_URL via host psql (remote DBs, e.g. Tiger Cloud)
migrate-remote:
	@test -n "$$DATABASE_URL" || { echo "usage: DATABASE_URL=postgres://... make migrate-remote"; exit 1; }
	./scripts/migrate.sh

# Regenerate OpenAPI specs from swaggo annotations. Each service is scoped to
# its own dirs (-d) so its spec lists only its own endpoints (not siblings').
swagger:
	@which swag >/dev/null 2>&1 || go install github.com/swaggo/swag/cmd/swag@latest
	swag init -g main.go -d cmd/geocode,internal/geocode -o cmd/geocode/docs
	swag init -g main.go -d cmd/ingest,internal/ingest,internal/dashcam -o cmd/ingest/docs
	@echo "swagger specs regenerated"

# Run the geocode service (Swagger UI at http://localhost:8081/swagger/index.html)
run-geocode:
	go run ./cmd/geocode

# Run the ingest service (Swagger UI at http://localhost:8082/swagger/index.html)
run-ingest:
	go run ./cmd/ingest

# Start infra + app services (containerized) via the "app" compose profile
up-app:
	docker compose --profile app up -d --build

# Load Indonesia kecamatan boundary master data (see scripts/geocode/README.md)
load-geocode:
	./scripts/geocode/load.sh

# Start local infra (Timescale/PostGIS + Redis)
up:
	docker compose up -d

down:
	docker compose down

# Apply SQL migrations in order (idempotent-ish; run against a fresh DB)
migrate:
	@for f in db/migrations/*.sql; do \
		echo ">> applying $$f"; \
		docker compose exec -T db psql -U postgres -d fms -v ON_ERROR_STOP=1 -f - < $$f || exit 1; \
	done
	@echo "migrations applied."

psql:
	docker compose exec db psql -U postgres -d fms

redis-cli:
	docker compose exec redis redis-cli

logs:
	docker compose logs -f

# Wipe volumes and re-apply from scratch
reset: down
	docker compose down -v
	$(MAKE) up
	@sleep 5
	$(MAKE) migrate
