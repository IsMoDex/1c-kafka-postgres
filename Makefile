# ============================================================================
# Makefile — управление интеграционным контуром 1С → Kafka → PostgreSQL
# Требуется Docker + Docker Compose. На Windows используйте Git Bash / WSL,
# либо эквивалентные команды из README (раздел «Запуск на Windows»).
# ============================================================================

COMPOSE ?= docker compose

.DEFAULT_GOAL := help

.PHONY: help up down restart build logs ps topics psql \
        sync-full sync-incremental demo-touch demo-delete \
        verify clean reset health onec-check test test-integration

help: ## Показать список команд
	@echo Available commands:
	@echo   up, down, restart, build, logs, ps, topics, psql
	@echo   sync-full, sync-incremental, demo-touch, demo-delete
	@echo   verify, health, onec-check, test, test-integration
	@echo   clean, reset

up: ## Поднять инфраструктуру (postgres, kafka, consumer, kafka-ui) + миграции + топики
	$(COMPOSE) up -d --build
	@echo "Готово. Kafka UI: http://localhost:8080"

down: ## Остановить сервисы (данные сохраняются в volume)
	$(COMPOSE) down

restart: down up ## Перезапустить

build: ## Пересобрать образы сервисов
	$(COMPOSE) build

logs: ## Логи всех сервисов (follow)
	$(COMPOSE) logs -f

ps: ## Статус сервисов
	$(COMPOSE) ps

topics: ## Список топиков Kafka
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list

# ── Синхронизация (producer) ─────────────────────────────────────────────────
sync-full: ## Полная синхронизация 1С → Kafka
	$(COMPOSE) exec integration-service python -m integration sync full

sync-incremental: ## Инкрементальная синхронизация 1С → Kafka
	$(COMPOSE) exec integration-service python -m integration sync incremental

# ── Демо-помощники (только для SOURCE_TYPE=mock) ─────────────────────────────
demo-touch: ## Изменить контрагента в mock. Пример: make demo-touch ID=<guid> NAME="Новое имя"
	$(COMPOSE) exec integration-service python -m integration demo touch $(ID) $(if $(NAME),--name "$(NAME)",)

demo-delete: ## Пометить контрагента удалённым в mock. Пример: make demo-delete ID=<guid>
	$(COMPOSE) exec integration-service python -m integration demo delete $(ID)

# ── Проверки ─────────────────────────────────────────────────────────────────
psql: ## Открыть psql в контейнере postgres
	$(COMPOSE) exec postgres sh -c "psql -U \"$$POSTGRES_USER\" -d \"$$POSTGRES_DB\""

verify: ## Показать содержимое таблиц (проверочные запросы)
	$(COMPOSE) exec -T postgres sh -c "psql -U \"$$POSTGRES_USER\" -d \"$$POSTGRES_DB\" -f -" < sql/verify.sql

health: ## Проверить /health consumer-service
	@curl -s http://localhost:8081/health || echo "consumer недоступен"

onec-check: ## Проверить доступность HTTP-сервиса 1С из контейнера (ONEC_BASE_URL)
	$(COMPOSE) exec integration-service python -c "import os,httpx; u=os.environ['ONEC_BASE_URL']; r=httpx.get(u+'/ownership-forms',timeout=30); print('URL:',u); print('HTTP',r.status_code); print(r.text[:200])"

test: ## Запустить unit/component тесты сервисов (без живого контура)
	$(COMPOSE) exec integration-service python -m pytest -q -m "not integration"
	$(COMPOSE) exec consumer-service python -m pytest -q

test-integration: ## Проверить живой контур 1С → Kafka → consumer → PostgreSQL
	$(COMPOSE) exec integration-service python -m pytest -q -m integration

# ── Очистка ──────────────────────────────────────────────────────────────────
clean: ## Остановить и удалить контейнеры + сети
	$(COMPOSE) down --remove-orphans

reset: ## ПОЛНЫЙ сброс: удалить контейнеры И данные (volume PostgreSQL)
	$(COMPOSE) down -v --remove-orphans
