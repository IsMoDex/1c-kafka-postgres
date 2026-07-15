set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

default:
    @just --list

up:
    docker compose up -d --build

down:
    docker compose down

reset:
    docker compose down -v --remove-orphans

ps:
    docker compose ps

logs:
    docker compose logs -f

sync-full:
    docker compose exec integration-service python -m integration sync full

sync-incremental:
    docker compose exec integration-service python -m integration sync incremental

verify:
    ./make.ps1 verify

health:
    ./make.ps1 health

test:
    uv run --directory integration-service pytest -q -m "not integration"
    uv run --directory consumer-service pytest -q

test-integration:
    docker compose exec integration-service python -m pytest -q -m integration

format:
    uv run --project integration-service ruff format integration-service
    uv run --project consumer-service ruff format consumer-service

format-check:
    uv run --project integration-service ruff format --check integration-service
    uv run --project consumer-service ruff format --check consumer-service

lint:
    uv run --project integration-service ruff check integration-service
    uv run --project consumer-service ruff check consumer-service

typecheck:
    uv run --project integration-service ty check --project integration-service
    uv run --project consumer-service ty check --project consumer-service

lock-check:
    uv lock --project integration-service --check
    uv lock --project consumer-service --check

quality: format-check lint typecheck lock-check test

hooks:
    prek run --all-files
