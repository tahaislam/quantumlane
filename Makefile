# QuantumLane — the interface.
# One-liner for every common operation. Don't memorize commands; read this file.

.DEFAULT_GOAL := help
COMPOSE := docker compose -f ops/compose/docker-compose.yml --env-file .env
PYTHON := python3

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------

.PHONY: help
help:  ## Show this help
	@awk 'BEGIN {FS = ":.*?## "; printf "\nQuantumLane commands\n"} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# -----------------------------------------------------------------------------
# Local development
# -----------------------------------------------------------------------------

.PHONY: bootstrap
bootstrap: env-check build migrate  ## First-time setup: build images, run migrations
	@echo "✓ Bootstrap complete. Run 'make up' to start the stack."

.PHONY: env-check
env-check:
	@test -f .env || (echo "ERROR: .env not found. Copy .env.example to .env and fill it in." && exit 1)

.PHONY: build
build: env-check  ## Build all Docker images
	$(COMPOSE) build

.PHONY: up
up: env-check  ## Start the full stack
	$(COMPOSE) up -d
	@echo ""
	@echo "  Website:     http://localhost:8080"
	@echo "  API docs:    http://localhost:8080/api/docs"
	@echo "  Dagster UI:  http://localhost:8080/dagster"
	@echo ""

.PHONY: down
down:  ## Stop the stack (preserves volumes)
	$(COMPOSE) down

.PHONY: nuke
nuke:  ## Stop the stack AND delete all volumes (destroys data)
	$(COMPOSE) down -v

.PHONY: logs
logs:  ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: logs-ingestion
logs-ingestion:
	$(COMPOSE) logs -f --tail=100 dagster-code dagster-daemon dagster-webserver

.PHONY: ps
ps:  ## List running services
	$(COMPOSE) ps

.PHONY: psql
psql:  ## Open psql against the main database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-quantumlane} -d $${POSTGRES_DB:-quantumlane}

# -----------------------------------------------------------------------------
# Migrations
# -----------------------------------------------------------------------------

.PHONY: migrate
migrate: env-check  ## Apply all pending DB migrations
	$(COMPOSE) up -d postgres
	@echo "Waiting for Postgres to be ready..."
	@until $(COMPOSE) exec -T postgres pg_isready -U $${POSTGRES_USER:-quantumlane} > /dev/null 2>&1; do sleep 1; done
	$(COMPOSE) run --rm -v $$(pwd)/db:/db -v $$(pwd)/ops:/ops \
		--entrypoint "python /ops/scripts/migrate.py" dagster-code

.PHONY: migrate-status
migrate-status:  ## Show migration status
	$(COMPOSE) run --rm -v $$(pwd)/db:/db -v $$(pwd)/ops:/ops \
		--entrypoint "python /ops/scripts/migrate.py --status" dagster-code

# -----------------------------------------------------------------------------
# Tests & lint
# -----------------------------------------------------------------------------

.PHONY: test
test: test-ingestion test-api  ## Run all tests

.PHONY: test-ingestion
test-ingestion:
	cd ingestion && $(PYTHON) -m pytest -q

.PHONY: test-api
test-api:
	cd api && $(PYTHON) -m pytest -q

.PHONY: lint
lint:  ## Run ruff + mypy across all packages
	cd ingestion && ruff check . && ruff format --check . && mypy src
	cd api && ruff check . && ruff format --check . && mypy src

.PHONY: fmt
fmt:  ## Auto-format
	cd ingestion && ruff format . && ruff check --fix .
	cd api && ruff format . && ruff check --fix .

# -----------------------------------------------------------------------------
# Deployment
# -----------------------------------------------------------------------------

.PHONY: deploy
deploy: env-check  ## Deploy to the Hetzner box. Requires $DEPLOY_HOST in env.
	@test -n "$$DEPLOY_HOST" || (echo "ERROR: set DEPLOY_HOST (e.g. ql@quantumlane.com)" && exit 1)
	@bash ops/scripts/deploy.sh "$$DEPLOY_HOST"

.PHONY: backup
backup:  ## Run a manual database backup to R2
	@bash ops/scripts/backup.sh
