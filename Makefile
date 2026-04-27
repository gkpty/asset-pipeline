.PHONY: help up down logs psql install migrate migration test lint format clean

help:
	@echo "Targets:"
	@echo "  up         Start Postgres"
	@echo "  down       Stop Postgres"
	@echo "  logs       Tail Postgres logs"
	@echo "  psql       Open psql shell on the dev DB"
	@echo "  install    Install all Python + JS deps"
	@echo "  migrate    Apply Alembic migrations"
	@echo "  migration  Create a new Alembic migration (NAME=description)"
	@echo "  test       Run pytest"
	@echo "  lint       Run ruff + mypy"
	@echo "  format     Format with ruff"
	@echo "  clean      Stop containers and clear caches"

up:
	docker compose up -d
	@echo "Waiting for Postgres..."
	@until docker compose exec -T postgres pg_isready -U asset_pipeline > /dev/null 2>&1; do sleep 1; done
	@echo "Postgres ready on localhost:5433"

down:
	docker compose down

logs:
	docker compose logs -f postgres

psql:
	docker compose exec postgres psql -U asset_pipeline -d asset_pipeline

install:
	uv sync
	pnpm install

migrate:
	cd packages/db && uv run alembic upgrade head

migration:
	@if [ -z "$(NAME)" ]; then echo "Usage: make migration NAME=add_foo_table"; exit 1; fi
	cd packages/db && uv run alembic revision --autogenerate -m "$(NAME)"

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run mypy packages apps

format:
	uv run ruff format .
	uv run ruff check --fix .

clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
