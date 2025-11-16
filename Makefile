.PHONY: help cluster-create cluster-delete cluster-start cluster-stop cluster-status cluster-deploy db-init test lint format \
        generate-all generate-ddl generate-orm docker-up docker-down docker-init docker-migrate docker-reset

# Default target
help:
	@echo "MLB Stats API Data Platform - Development Commands"
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make quickstart          Complete setup: install + generate + docker up + init"
	@echo ""
	@echo "📦 Code Generation:"
	@echo "  make generate-all        Generate DDL + ORM from YAML schemas"
	@echo "  make generate-ddl        Generate PostgreSQL migrations"
	@echo "  make generate-orm        Generate SQLModel ORM classes"
	@echo ""
	@echo "🐳 Docker Compose (Local Development):"
	@echo "  make docker-up           Start PostgreSQL, Redis, MinIO"
	@echo "  make docker-down         Stop all services"
	@echo "  make docker-init         Initialize database schemas"
	@echo "  make docker-migrate      Run migrations"
	@echo "  make docker-reset        Reset database (destroys data)"
	@echo "  make docker-shell        Open PostgreSQL shell"
	@echo "  make docker-logs         Show database logs"
	@echo ""
	@echo "☸️  Local K3d Cluster:"
	@echo "  make cluster-create      Create local K3d cluster"
	@echo "  make cluster-deploy      Deploy infrastructure"
	@echo "  make cluster-start       Start the cluster"
	@echo "  make cluster-stop        Stop the cluster"
	@echo "  make cluster-status      Show cluster status"
	@echo "  make cluster-delete      Delete the cluster"
	@echo "  make cluster-full        Create + deploy + init"
	@echo ""
	@echo "🗄️  Database:"
	@echo "  make db-init             Initialize PostgreSQL schemas"
	@echo "  make db-connect          Connect to PostgreSQL CLI"
	@echo ""
	@echo "🧪 Development:"
	@echo "  make install             Install dependencies (uv sync)"
	@echo "  make test                Run unit tests"
	@echo "  make test-integration    Run integration tests"
	@echo "  make lint                Run linters (ruff check)"
	@echo "  make format              Format code (ruff format)"
	@echo "  make clean               Clean build artifacts"
	@echo ""
	@echo "🔧 CLI:"
	@echo "  make cli-version         Show version"
	@echo "  make cli-schema-list     List available schemas"
	@echo "  make cli-ingest-test     Test ingestion with stubs"

# Cluster management
cluster-create:
	@./scripts/k3d-cluster.sh create

cluster-deploy:
	@./scripts/k3d-cluster.sh deploy

cluster-start:
	@./scripts/k3d-cluster.sh start

cluster-stop:
	@./scripts/k3d-cluster.sh stop

cluster-status:
	@./scripts/k3d-cluster.sh status

cluster-delete:
	@./scripts/k3d-cluster.sh delete

cluster-full: cluster-create cluster-deploy
	@echo "Waiting for pods to be ready..."
	@sleep 10
	@$(MAKE) db-init
	@echo ""
	@echo "✓ Local cluster fully configured!"
	@echo ""
	@./scripts/k3d-cluster.sh status

# Database operations
db-init:
	@echo "Initializing database..."
	@./scripts/init_database.sh local

db-connect:
	@kubectl exec -it -n mlb-data-platform deployment/postgres -- psql -U mlb_admin -d mlb_games

# Development
install:
	uv sync

test:
	uv run pytest
	uv run behave

lint:
	uv run ruff check .

format:
	uv run ruff format .

clean:
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# CLI shortcuts
cli-version:
	uv run mlb-etl version

cli-schema-list:
	uv run mlb-etl schema list

cli-ingest-test:
	uv run mlb-etl ingest \
		--job config/jobs/season_daily.yaml \
		--stub-mode replay \
		--dry-run

# Code generation
generate-all: generate-ddl generate-orm
	@echo "✓ All code generation complete"

generate-ddl:
	@echo "Generating DDL migrations..."
	@mkdir -p sql/migrations
	uv run python -m mlb_data_platform.schema.generator \
		config/schemas/mappings/game/live_game_v1.yaml \
		> sql/migrations/V2__game_live_tables.sql
	@echo "✓ DDL → sql/migrations/V2__game_live_tables.sql"

generate-orm:
	@echo "Generating ORM models..."
	@mkdir -p src/mlb_data_platform/models
	uv run python -m mlb_data_platform.schema.orm_generator \
		config/schemas/mappings/game/live_game_v1.yaml \
		> src/mlb_data_platform/models/game_live.py
	@echo "✓ ORM → src/mlb_data_platform/models/game_live.py"

# Docker Compose targets
docker-up:
	@echo "Starting Docker services..."
	docker compose up -d
	@echo "Waiting for PostgreSQL..."
	@sleep 5
	@docker compose exec -T postgres pg_isready -U mlb_admin || (echo "PostgreSQL not ready" && exit 1)
	@echo ""
	@echo "✓ Services running:"
	@echo "  PostgreSQL: postgresql://mlb_admin:mlb_dev_password@localhost:5432/mlb_games"
	@echo "  Redis:      redis://localhost:6379"
	@echo "  MinIO API:  http://localhost:9000 (mlb_admin/mlb_dev_password)"
	@echo "  MinIO UI:   http://localhost:9001"

docker-down:
	@echo "Stopping Docker services..."
	docker compose down
	@echo "✓ Services stopped"

docker-logs:
	docker compose logs -f postgres

docker-init: docker-up
	@echo "Initializing database..."
	docker compose exec -T postgres psql -U mlb_admin -d mlb_games -f /docker-entrypoint-initdb.d/V1__initial_schema.sql || true
	@echo "✓ Database initialized"

docker-migrate: generate-ddl
	@echo "Running migrations..."
	docker compose exec -T postgres psql -U mlb_admin -d mlb_games < sql/migrations/V2__game_live_tables.sql
	@echo "✓ Migrations complete"

docker-reset:
	@echo "⚠️  WARNING: This will destroy all data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [ "$$REPLY" = "y" ] || [ "$$REPLY" = "Y" ]; then \
		echo "Resetting database..."; \
		docker compose down -v; \
		$(MAKE) docker-up; \
		sleep 5; \
		$(MAKE) docker-init; \
		$(MAKE) docker-migrate; \
		echo "✓ Database reset complete"; \
	else \
		echo "Aborted"; \
	fi

docker-shell:
	docker compose exec postgres psql -U mlb_admin -d mlb_games

# Testing
test-integration:
	@echo "Running integration tests..."
	SPARK_VERSION=3.5 uv run pytest tests/integration/ -v --no-cov
	@echo "✓ Integration tests complete"

# Quick start for new developers
quickstart: install generate-all docker-up docker-init docker-migrate
	@echo ""
	@echo "========================================="
	@echo "✓ Quick Start Complete!"
	@echo "========================================="
	@echo ""
	@echo "Next steps:"
	@echo "  1. Check database: make docker-shell"
	@echo "  2. Run tests:      make test"
	@echo "  3. View help:      make help"
	@echo ""
