# Changelog

Формат основан на Keep a Changelog; версия проекта следует Semantic Versioning.

## [Unreleased]

### Added

- Реальные PostgreSQL integration tests для транзакционного слоя consumer.
- Раздельные `/livez`, `/readyz` и `/metrics`.
- Flyway schema history и checksum validation.
- Строгая валидация ENV обоих сервисов.
- Dependency audit, Trivy image scan, Gitleaks, CodeQL и Dependabot.

### Changed

- Основной Compose использует только non-root production images.
- `updated_at` стал обязательным timezone-aware полем event payload.
- Временные ошибки PostgreSQL больше не отправляют валидные события в DLQ.

### Security

- PowerShell wrapper больше не использует `Invoke-Expression`.
- Ошибки зависимостей исключены из публичного health payload.
