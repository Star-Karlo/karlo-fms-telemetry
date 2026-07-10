.PHONY: up up-app down migrate psql redis-cli logs seed reset swagger run-api run-geocode load-geocode

# Regenerate OpenAPI specs (api + geocode) from swaggo annotations
swagger:
	@which swag >/dev/null 2>&1 || go install github.com/swaggo/swag/cmd/swag@latest
	swag init -g cmd/api/main.go -o docs --parseInternal
	swag init -g cmd/geocode/main.go -o cmd/geocode/docs --parseInternal
	@echo "swagger specs regenerated"

# Run the HTTP API (Swagger UI at http://localhost:8080/swagger/index.html)
run-api:
	go run ./cmd/api

# Run the geocode service (Swagger UI at http://localhost:8081/swagger/index.html)
run-geocode:
	go run ./cmd/geocode

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
