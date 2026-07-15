# Разработка и качество кода

## Инструменты

Проект использует Python 3.12 и отдельное окружение для каждого сервиса. Нужен
[`uv`](https://docs.astral.sh/uv/). Удобные команды и hooks устанавливаются так:

```bash
uv tool install rust-just
uv tool install prek
prek install
```

Корневой Python-проект намеренно не создаётся: `integration-service` и
`consumer-service` собираются разными Dockerfile и имеют независимые зависимости
и lock-файлы.

## Воспроизводимое окружение

```bash
uv sync --project integration-service --locked
uv sync --project consumer-service --locked
```

Изменение зависимостей выполняется в каталоге соответствующего сервиса. После
этого `uv lock` обновляет только его `uv.lock`.

## Проверки

Полный набор:

```bash
just quality
```

Отдельные проверки:

```bash
just format-check
just lint
just typecheck
just lock-check
just test
```

Автоформатирование:

```bash
just format
```

Все pre-commit hooks:

```bash
just hooks
```

Ruff включает `select = ["ALL"]`. Исключены только конфликтующие правила,
русские комментарии/docstrings и узкие правила, неприменимые к pytest или
интеграционным boundary-функциям. Все публичные и тестовые функции имеют
аннотации; `ty check` обязателен для обоих сервисов.

## Docker targets

Оба Dockerfile являются multi-stage:

```text
build -> locked production dependencies
test  -> dev dependencies and tests (used by compose.yaml)
prod  -> non-root runtime without build and test tools
```

Проверить production-образы локально:

```bash
docker build --target prod -t integration-service:prod ./integration-service
docker build --target prod -t consumer-service:prod ./consumer-service
```

## Ignore policy

Корневой `.gitignore` и сервисные `.dockerignore` используют whitelist-подход:
новый файл должен быть явно частью структуры проекта, а в Docker build context
попадают только `pyproject.toml`, `uv.lock`, `src/` и `tests/`. Это снижает риск
случайно отправить секреты, локальные окружения или временные файлы.
