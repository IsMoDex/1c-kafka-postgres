# GitHub Actions

## Обычный CI

Workflow `.github/workflows/ci.yml` запускается для каждого push в `main`, pull
request и вручную через `workflow_dispatch`.

Он содержит три обязательные проверки:

1. `Unit tests (integration-service)` — 14 unit/component тестов producer-а.
2. `Unit tests (consumer-service)` — 18 unit/component тестов consumer-а.
3. `Compose integration` — настоящий Docker Compose контур с Kafka,
   PostgreSQL, integration-service и consumer-service. В GitHub-hosted runner
   используется воспроизводимый `SOURCE_TYPE=mock`, потому что 1С нельзя
   установить на стандартный Ubuntu runner.

Compose integration выполняет полный и повторный full sync, incremental update,
soft delete, проверяет отсутствие дублей и FK-сирот, health и пустые DLQ.
Сценарий находится в `scripts/ci-integration.sh` и запускается локально так:

```bash
SOURCE_TYPE=mock docker compose up -d --build
bash scripts/ci-integration.sh
docker compose down -v --remove-orphans
```

Workflow имеет минимальные права `contents: read`, отменяет устаревшие запуски
той же ветки через `concurrency` и всегда удаляет compose volumes после теста.

## Интеграция с реальной 1С

Workflow `.github/workflows/live-onec.yml` запускается только вручную. Он требует
выделенный self-hosted Windows runner с метками:

```text
self-hosted, Windows, X64, roshim-1c
```

На runner должны быть:

- Docker Desktop с Linux containers;
- опубликованная в IIS реальная 1С;
- доступ к HTTP-сервису 1С из Docker;
- PowerShell 5.1+.

В настройках GitHub-репозитория (`Settings → Secrets and variables → Actions`)
нужно создать:

- `ONEC_BASE_URL` — обязательно, например
  `http://host.docker.internal/roshim/hs/integration`;
- `ONEC_USERNAME` — опционально;
- `ONEC_PASSWORD` — опционально.

Ручной workflow выполняет:

1. запуск compose-контура;
2. `onec-check`;
3. четыре live pytest integration tests;
4. проверку PostgreSQL;
5. health consumer-а;
6. вывод логов при ошибке и гарантированную остановку compose.

Live tests временно меняют имя контрагента `000001`, но восстанавливают его в
`finally`. Контрагент `000005` используется для проверки soft delete.

## Branch protection

После первого успешного CI-запуска рекомендуется включить для `main` правило
защиты ветки и потребовать проверки:

- `Unit tests (integration-service)`;
- `Unit tests (consumer-service)`;
- `Compose integration`.

Live 1C workflow не следует делать обязательным для каждого pull request: он
зависит от локальной Windows/1С инфраструктуры и запускается перед демонстрацией
или релизом.
