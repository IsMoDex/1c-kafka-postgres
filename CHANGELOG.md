# Changelog

Формат основан на Keep a Changelog; версия проекта следует Semantic Versioning.

## [Unreleased]

### Added

- Реальные PostgreSQL integration tests для транзакционного слоя consumer.
- Раздельные `/livez`, `/readyz` и `/metrics`.
- Flyway schema history и checksum validation.
- Строгая валидация ENV обоих сервисов.
- Dependency audit, Trivy image scan, Gitleaks, CodeQL и Dependabot.
- Комплект GUI-скриншотов реальной 1С, IIS, Kafka UI и PostgreSQL для приёмки.

### Changed

- Основной Compose использует только non-root production images.
- `updated_at` стал обязательным timezone-aware полем event payload.
- Временные ошибки PostgreSQL больше не отправляют валидные события в DLQ.
- `POST /seed` восстанавливает эталонные demo-записи и снимает их пометки удаления.
- Live soft-delete test проверяет переход `false → true` и очищает состояние в `finally`.
- Incremental sync сохраняет секундный overlap чтения, но не публикует в Kafka
  неизменённые или устаревшие записи из этого окна.
- Документация явно фиксирует 1С 8.5.1.1302 и современное имя `compose.yaml`.

### Security

- PowerShell wrapper больше не использует `Invoke-Expression`.
- Ошибки зависимостей исключены из публичного health payload.
