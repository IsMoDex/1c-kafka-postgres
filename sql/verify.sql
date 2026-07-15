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
\echo '=== Проверка инвариантов ==='
SELECT 'duplicate ownership form ids' AS check, count(*) - count(DISTINCT id) AS violations
FROM ownership_forms
UNION ALL
SELECT 'duplicate counterparty ids', count(*) - count(DISTINCT id)
FROM counterparties
UNION ALL
SELECT 'orphan ownership form references', count(*)
FROM counterparties c
LEFT JOIN ownership_forms o ON o.id = c.ownership_form_id
WHERE c.ownership_form_id IS NOT NULL AND o.id IS NULL
UNION ALL
SELECT 'null ownership form timestamps', count(*)
FROM ownership_forms
WHERE source_updated_at IS NULL
UNION ALL
SELECT 'null counterparty timestamps', count(*)
FROM counterparties
WHERE source_updated_at IS NULL;

DO $$
BEGIN
    IF (SELECT count(*) FROM ownership_forms) < 3
       OR (SELECT count(*) FROM counterparties) < 5 THEN
        RAISE EXCEPTION 'Verification failed: expected at least 3 ownership forms and 5 counterparties';
    END IF;
    IF (SELECT count(*) FROM sync_state WHERE last_synced_at IS NOT NULL) <> 2 THEN
        RAISE EXCEPTION 'Verification failed: both sync watermarks must be initialized';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM counterparties c
        LEFT JOIN ownership_forms o ON o.id = c.ownership_form_id
        WHERE c.ownership_form_id IS NOT NULL AND o.id IS NULL
    ) THEN
        RAISE EXCEPTION 'Verification failed: orphan ownership form reference';
    END IF;
    IF EXISTS (SELECT 1 FROM ownership_forms WHERE source_updated_at IS NULL)
       OR EXISTS (SELECT 1 FROM counterparties WHERE source_updated_at IS NULL) THEN
        RAISE EXCEPTION 'Verification failed: source_updated_at must not be null';
    END IF;
END
$$;
