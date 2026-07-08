.PHONY: up down migrate psql redis-cli logs seed reset

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
