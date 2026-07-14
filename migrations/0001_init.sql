-- ============================================================================
-- 0001_init.sql — начальная схема интеграции 1С → Kafka → PostgreSQL
-- Идемпотентна: повторный запуск не ломает существующую схему.
-- ============================================================================

-- Справочник «Формы собственности».
-- id = строковый код (ooo/ip/ao/pao), т.к. в ТЗ ownership_forms.id TEXT.
CREATE TABLE IF NOT EXISTS ownership_forms (
    id                TEXT PRIMARY KEY,
    code              TEXT,
    name              TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ,
    imported_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted           BOOLEAN NOT NULL DEFAULT false
);

COMMENT ON TABLE  ownership_forms IS 'Справочник «Формы собственности» из 1С';
COMMENT ON COLUMN ownership_forms.id                IS 'Стабильный идентификатор объекта 1С (строковый код)';
COMMENT ON COLUMN ownership_forms.source_updated_at IS 'Момент изменения записи в 1С (для идемпотентности по времени)';
COMMENT ON COLUMN ownership_forms.imported_at       IS 'Технический момент записи в интеграционную БД';
COMMENT ON COLUMN ownership_forms.deleted           IS 'Мягкое удаление (пометка удаления в 1С)';

-- Справочник «Контрагенты».
-- id = GUID из Ссылка.УникальныйИдентификатор() → UUID.
CREATE TABLE IF NOT EXISTS counterparties (
    id                UUID PRIMARY KEY,
    code              TEXT,
    name              TEXT NOT NULL,
    inn               TEXT,
    kpp               TEXT,
    ownership_form_id TEXT REFERENCES ownership_forms(id),
    source_updated_at TIMESTAMPTZ,
    imported_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted           BOOLEAN NOT NULL DEFAULT false
);

COMMENT ON TABLE  counterparties IS 'Справочник «Контрагенты» из 1С';
COMMENT ON COLUMN counterparties.id                IS 'GUID объекта 1С (Ссылка.УникальныйИдентификатор())';
COMMENT ON COLUMN counterparties.ownership_form_id IS 'FK на форму собственности (ownership_forms.id)';
COMMENT ON COLUMN counterparties.source_updated_at IS 'Момент изменения записи в 1С';
COMMENT ON COLUMN counterparties.deleted           IS 'Мягкое удаление (пометка удаления в 1С)';

-- Индекс для выборок активных контрагентов по форме собственности.
CREATE INDEX IF NOT EXISTS idx_counterparties_ownership_form
    ON counterparties (ownership_form_id) WHERE deleted = false;

CREATE INDEX IF NOT EXISTS idx_counterparties_inn
    ON counterparties (inn) WHERE inn IS NOT NULL;

-- ----------------------------------------------------------------------------
-- Служебная таблица watermark инкрементальной синхронизации.
-- integration-service читает last_synced_at, запрашивает у 1С changed_since,
-- после успешной публикации двигает watermark вперёд.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_state (
    entity         TEXT PRIMARY KEY,       -- 'ownership_forms' | 'counterparties'
    last_synced_at TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE sync_state IS 'Watermark инкрементальной синхронизации по каждому справочнику';

INSERT INTO sync_state (entity, last_synced_at)
VALUES ('ownership_forms', NULL), ('counterparties', NULL)
ON CONFLICT (entity) DO NOTHING;
