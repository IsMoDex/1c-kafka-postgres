-- ============================================================================
-- verify.sql — проверочные запросы для демо-сценария (раздел 11 ТЗ)
-- Запуск: make verify   ИЛИ   psql ... -f sql/verify.sql
-- ============================================================================

\echo '=== Формы собственности ==='
SELECT id, code, name, deleted, source_updated_at, imported_at
FROM ownership_forms
ORDER BY code;

\echo ''
\echo '=== Контрагенты ==='
SELECT c.id, c.code, c.name, c.inn, c.kpp,
       c.ownership_form_id, o.name AS ownership_form,
       c.deleted, c.source_updated_at, c.imported_at
FROM counterparties c
LEFT JOIN ownership_forms o ON o.id = c.ownership_form_id
ORDER BY c.code;

\echo ''
\echo '=== Итоги (всего / активных / удалённых) ==='
SELECT 'ownership_forms' AS table, count(*) AS total,
       count(*) FILTER (WHERE NOT deleted) AS active,
       count(*) FILTER (WHERE deleted) AS deleted
FROM ownership_forms
UNION ALL
SELECT 'counterparties', count(*),
       count(*) FILTER (WHERE NOT deleted),
       count(*) FILTER (WHERE deleted)
FROM counterparties;

\echo ''
\echo '=== Watermark синхронизации ==='
SELECT entity, last_synced_at, updated_at FROM sync_state ORDER BY entity;

\echo ''
\echo '=== Проверка отсутствия дублей (по PK дубли невозможны, контроль count) ==='
SELECT 'duplicate ids in counterparties' AS check, count(*) - count(DISTINCT id) AS extra
FROM counterparties;
