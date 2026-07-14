# AGENTS.md — Проект «Интеграция 1С → Kafka → PostgreSQL»

> Рабочий журнал и архитектурный справочник проекта. Ведётся для любой модели/агента,
> который продолжит работу. Читать **до** внесения изменений, обновлять **по мере** работы.

---

## 1. Что это за проект

Тестовое задание АО «Росхим». Нужно построить рабочий end-to-end поток данных:

```
1С → integration-service (producer) → Kafka topic → consumer-service → PostgreSQL
```

Выгружаем два справочника из 1С («Формы собственности», «Контрагенты»), публикуем
события в Kafka, сохраняем в нормализованные таблицы PostgreSQL с идемпотентным
upsert и мягким удалением.

Первоисточник требований: `../ts/Тестовое задание_интеграция.docx`
(извлечённый текст — см. раздел 11 «Соответствие ТЗ» ниже).

---

## 2. Ключевые решения (принято с пользователем)

| Решение | Выбор | Причина |
|---|---|---|
| Источник 1С | **Реальная 1С:Предприятие 8.3** (приоритет ТЗ). Пользователь ставит её сам. | Приоритетный вариант по ТЗ. |
| Абстракция источника | **Да.** Интерфейс `Source` → `OneCHttpSource` + `MockSource`. | Контур Kafka→PG воспроизводим даже без запущенной 1С (демо/CI). |
| Язык сервисов | **Python 3.12** | Быстро, читаемо, зрелые библиотеки. |
| Запись в PG | **Собственный consumer-service** (Вариант А ТЗ) | Больше контроля: consumer group, upsert, retry, DLQ, транзакции. |
| Метод доступа к 1С | **HTTP-сервис 1С** (рекомендован), OData — как альтернатива в доках | Отдаёт чистый JSON как в ТЗ, простой `changed_since`. |
| Kafka | **KRaft** (без ZooKeeper) | Проще, современнее, меньше контейнеров. |
| Инкремент | Реквизит `ДатаИзменения` в 1С + watermark в таблице `sync_state` | Справочники 1С не хранят updated_at из коробки. |

---

## 3. Архитектура

```
┌──────────┐  HTTP/OData   ┌─────────────────────┐  produce  ┌─────────┐  consume  ┌──────────────────┐  upsert  ┌────────────┐
│  1С 8.3  │ ─────────────▶│ integration-service │ ─────────▶│  Kafka  │ ─────────▶│ consumer-service │ ────────▶│ PostgreSQL │
│ (Windows)│  справочники  │     (producer)      │  события  │ (KRaft) │  события  │   (consumer)     │ ON CONFLICT│           │
└──────────┘               └─────────────────────┘           └─────────┘           └──────────────────┘          └────────────┘
                                    │                              │                        │
                              sync_state                       *.dlq                    /health
                             (watermark)                   (poison msgs)             (JSON logs)
```

- **1С** запускается **вне Docker** (нативно на Windows) — разрешено ТЗ. Сервисы
  обращаются к ней по **реальному IPv4-адресу хоста** (адаптер vEthernet WSL,
  напр. `172.23.128.1`), а НЕ через `host.docker.internal` — Docker Desktop не
  проксирует ISAPI-ответ 1С и отдаёт 502 (см. раздел «Реальная 1С» ниже).
- **integration-service** — CLI-приложение (не демон). Запускается разово в режиме
  `full` или `incremental`, читает источник, публикует события, завершается.
- **consumer-service** — долгоживущий демон. Слушает топики, пишет в PostgreSQL.

---

## 4. Структура репозитория

```
1c-kafka-postgres/
├── AGENTS.md                 # этот файл
├── README.md                 # инструкция (11 пунктов ТЗ + демо-сценарий)
├── docker-compose.yml
├── Makefile                  # sync-full, sync-incremental, up, down, logs, psql
├── .env.example
├── migrations/               # SQL-миграции (нумерованные)
│   ├── 0001_init.sql
│   └── ...
├── sql/                      # проверочные запросы для демо
├── docs/                     # архитектура, формат событий, решения, ограничения
├── integration-service/
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── src/integration/
│       ├── __main__.py       # CLI (typer)
│       ├── config.py         # настройки из ENV
│       ├── models.py         # pydantic-модели событий/записей
│       ├── sources/          # абстракция источника
│       │   ├── base.py       # интерфейс Source
│       │   ├── onec_http.py  # реальная 1С
│       │   └── mock.py       # воспроизводимый mock
│       ├── producer.py       # Kafka producer
│       └── sync.py           # оркестрация full/incremental
└── consumer-service/
    ├── Dockerfile
    ├── pyproject.toml
    └── src/consumer/
        ├── __main__.py
        ├── config.py
        ├── models.py
        ├── db.py             # psycopg, upsert ON CONFLICT
        ├── health.py         # /health endpoint
        └── worker.py         # consumer loop, retry, DLQ
```

---

## 5. Контракт данных

### 5.1. Топики Kafka
- `1c.ownership_forms.v1` — формы собственности
- `1c.counterparties.v1` — контрагенты
- `1c.ownership_forms.dlq` / `1c.counterparties.dlq` — dead-letter

**Ключ сообщения** = `id` объекта 1С (стабильный) → идемпотентность + порядок по ключу.

### 5.2. Формат события (envelope)
```json
{
  "event_id": "<uuid>",              // уникален на каждое событие
  "event_type": "counterparty.upsert | counterparty.delete | ownership_form.upsert | ownership_form.delete",
  "source": "1c",
  "occurred_at": "<RFC3339>",
  "payload": { ... }                  // см. ниже
}
```

### 5.3. payload — ФормаСобственности
```json
{ "id": "ooo", "code": "...", "name": "ООО", "deleted": false, "updated_at": "<RFC3339>" }
```
> `id` формы = строковый код (`ooo`, `ip`, `ao`, `pao`), т.к. в таблице `ownership_forms.id TEXT`.

### 5.4. payload — Контрагент
```json
{
  "id": "<uuid>", "code": "000001", "name": "ООО Ромашка",
  "inn": "7701234567", "kpp": "770101001",
  "ownership_form_id": "ooo", "deleted": false, "updated_at": "<RFC3339>"
}
```
> `id` контрагента = GUID из `Ссылка.УникальныйИдентификатор()` → таблица `counterparties.id UUID`.

---

## 6. Схема PostgreSQL (из ТЗ + служебная)

```sql
CREATE TABLE ownership_forms (
    id TEXT PRIMARY KEY, code TEXT, name TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE counterparties (
    id UUID PRIMARY KEY, code TEXT, name TEXT NOT NULL,
    inn TEXT, kpp TEXT,
    ownership_form_id TEXT REFERENCES ownership_forms(id),
    source_updated_at TIMESTAMPTZ,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted BOOLEAN NOT NULL DEFAULT false
);
-- служебная: watermark инкрементальной синхронизации
CREATE TABLE sync_state (
    entity TEXT PRIMARY KEY,          -- 'ownership_forms' | 'counterparties'
    last_synced_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 7. Инварианты и важные правила (НЕ нарушать)

1. **Идемпотентность.** Запись в PG только через `INSERT ... ON CONFLICT (id) DO UPDATE`.
   Повторная синхронизация НЕ создаёт дублей (демо-сценарий ТЗ, п.6).
2. **Порядок применения.** `ownership_forms` синхронизируем/применяем ДО `counterparties`
   (FK `counterparties.ownership_form_id → ownership_forms.id`).
3. **Защита от гонки FK в consumer.** Если контрагент ссылается на ещё не пришедшую
   форму собственности → retry, при исчерпании → DLQ. Не ронять весь пакет.
4. **Мягкое удаление.** Удаление = событие с `deleted=true`, UPDATE флага, строку не DELETE.
5. **Идемпотентность по времени.** При upsert не затирать более свежую запись более старой:
   обновляем, только если `source_updated_at` входящего >= сохранённого (или если NULL).
6. **Секреты только из ENV.** Ни паролей, ни строк подключения в коде. `.env` в `.gitignore`,
   в репозитории — `.env.example`.
7. **Никаких немых падений.** Ошибки 1С/Kafka/PG логируются структурированно (JSON).
8. **Транзакции.** Пакет сообщений в consumer пишется в одной транзакции PG.
9. **at-least-once.** Offset коммитится ТОЛЬКО после успешной записи в PG (или отправки в DLQ).

---

## 8. Как запускать (кратко; подробно — README)

```bash
make up                 # поднять инфраструктуру (postgres, kafka, consumer, kafka-ui)
make sync-full          # полная синхронизация (по умолчанию источник = mock)
make sync-incremental   # инкрементальная синхронизация
make psql               # консоль PostgreSQL
make logs               # логи сервисов
make down               # остановить
```

Переключение источника — переменной `SOURCE_TYPE=mock|onec` в `.env`.

---

## 9. Статус работы (обновлять!)

- [x] Изучено ТЗ, согласована архитектура и решения с пользователем.
- [x] Создана структура проекта, начат AGENTS.md.
- [x] docker-compose.yml
- [x] migrations/
- [x] integration-service (source abstraction, producer, full/incremental)
- [x] consumer-service (upsert, retry, DLQ, /health)
- [x] .env.example, Makefile, make.ps1, sql/
- [x] docs/, 1c/setup.md
- [x] README.md
- [x] Прогон демо end-to-end на mock — **успешно** (см. ниже)
- [x] Подключение реальной 1С — **успешно** (см. «Реальная 1С» ниже)

### Результат прогона демо (mock, 2026-07-13)
Весь сценарий раздела 11 ТЗ пройден:
- full sync: 4 формы + 5 контрагентов → Kafka → PostgreSQL;
- повторный full: без дублей (count = distinct);
- изменение контрагента + incremental: запись обновлена;
- мягкое удаление + incremental: `deleted=true`, инкремент забрал **только 1**
  изменённую запись (watermark работает);
- consumer: restarts=0, `/health` = ready, DLQ пуст (`messages_dlq=0`).

### Известные правки в ходе прогона
- `integration-service` в compose: `command:` → `entrypoint: ["sleep","infinity"]`
  (иначе ENTRYPOINT `python -m integration` + `sleep infinity` = ошибка аргументов).
- `consumer/worker.py`: транзиентные ошибки Kafka (`UNKNOWN_TOPIC_OR_PART`,
  `retriable()`) на старте больше не роняют сервис — логируем и продолжаем poll.
- **КРИТИЧНО (найдено при чистом ретесте с нуля):** consumer/integration не
  зависели от `kafka-init`. На чистом старте (без ранее созданных топиков)
  consumer подписывался ДО создания топиков и не получал назначение партиций
  топика контрагентов → контрагенты не записывались (forms=4, cps=0).
  Исправлено: добавлен `depends_on: kafka-init (service_completed_successfully)`
  для обоих сервисов. Подтверждено чистым прогоном: forms=4, cps=5.
- `make.ps1` не парсился в PowerShell 5.1 из-за кириллицы в UTF-8-без-BOM.
  Переписан в ASCII-safe виде (комментарии/сообщения на английском).

### Проверено чистым ретестом с нуля (down -v → up --build)
- Стартовое состояние: таблицы есть, 0 записей.
- Группа consumer: назначены ОБА топика (ownership_forms:1 + counterparties:0,1,2).
- full → 4 формы + 5 контрагентов; повторный full → без дублей (5=5).
- изменение + incremental → обновлено; incremental забирает только изменённое.
- мягкое удаление + incremental → deleted=true (forms 0 / cps 1 в окне).
- consumer restarts=0, DLQ пуст. make.ps1 verify/health работают.

**1С:** пользователь устанавливает платформу самостоятельно. После завершения —
проверить: файловая ИБ, конфигурация «Интеграция» (справочники + реквизит
`ДатаИзменения`), публикация HTTP-сервиса, доступность эндпоинтов, формат JSON.
Затем: `SOURCE_TYPE=onec` в `.env`, `make down && make up && make sync-full`.

### Реальная 1С — реализовано (2026-07-14)
Платформа **1С:Предприятие 8.5.1.1302** (x86, community), Windows + IIS.
Выбран **Вариант Б ТЗ — собственный HTTP-сервис 1С** (OData 8.5 требовал ручной
установки состава, headless-команда `SetStandardODataInterfaceContent` нестабильна).

Сделано (всё headless, кроме включения фич IIS):
- Файловая ИБ `D:\1C\Bases\roshim-1c`.
- Конфигурация «Интеграция» (справочники + реквизит `ДатаИзменения` в
  `ПередЗаписью`) — загружена через `LoadConfigFromFiles`.
- HTTP-сервис `integration`: `/ownership-forms`, `/counterparties`,
  `?changed_since=`, плюс `/seed`, `/touch`, `/delete` для наполнения/демо.
- Публикация в IIS через `webinst.exe` (`C:\inetpub\wwwroot\roshim`).
- Исходники: `1c/src/` (XML+BSL), выгрузка `1c/configuration.cf`.

Решённые проблемы IIS/сети:
- **500.21** — в IIS выключены ISAPI Extensions/Filter → включены.
- **500** — пул 64-бит, а `wsisapi.dll` x86 → `enable32BitAppOnWin64:true`.
- **502 из контейнера** — `host.docker.internal` не проксирует ISAPI-ответ 1С →
  в `.env` реальный IPv4 хоста (vEthernet WSL), напр. `172.23.128.1`.
  **ВАЖНО:** IP может меняться при перезагрузке — смотреть `ipconfig`, обновлять
  `ONEC_BASE_URL`.

Проверено end-to-end на реальной 1С (сценарий раздела 11 ТЗ):
full → 4 формы + 5 контрагентов; повторный full → без дублей; `/touch`+incremental
→ обновление; `/delete`+incremental → `deleted=true` (инкремент забрал 1 запись);
consumer restarts=0, DLQ пуст.

### Аудит и доработки (2026-07-14)
Пройден внешний аудит (30 замечаний). Внесены правки:
- **producer.flush()**: `remaining>0` теперь считается ошибкой доставки —
  incremental не двигает watermark при недоставке в Kafka (защита от потери данных).
- **ONEC_BASE_URL**: убран нерабочий дефолт `host.docker.internal` из
  `.env.example`, `docker-compose.yml`, `config.py` → placeholder `<HOST_IPV4>`.
- **consumer DLQ**: `DlqProducer.send()` дожидается подтверждения доставки и
  возвращает bool; offset коммитится только для подтверждённых в DLQ сообщений.
- **health**: добавлены `kafka_ok`, `last_kafka_error`, разделены
  `rows_processed`/`messages_processed`; healthy() учитывает Kafka.
- **consumer `--help`**: argparse, не поднимает порт при `--help`.
- **Makefile/make.ps1**: `psql`/`verify` используют `POSTGRES_USER/DB` из env;
  добавлены `onec-check` и `test`.
- **BSL (1С)**: `ПараметрChangedSince` без параметра → `Неопределено` (full без
  фильтра); `ISOВДату` — устойчивый парс RFC3339 (первые 14 цифр); `inn/kpp`
  пустые строки → `null` (единообразие с mock).
- **unit-тесты**: integration (mock/event, 10) + consumer (parse_event, 5) = 15,
  все проходят (`make test`).
- **docs/README/AGENTS**: `configuration.dt`→`.cf`; версия 1С «8.3+ (реализовано
  на 8.5.1.1302)»; основной demo — на реальной 1С, mock как fallback; убрано
  устаревшее про `host.docker.internal`; в limitations добавлены seed create-only,
  fallback GUID формы, версия 8.5, сеть Docker.
- **04_postgresql-data.txt**: пересоздан в корректном UTF-8 (без mojibake).

Осознанно НЕ реализовано (аргументировано):
- **#18** (изоляция отдельных сообщений при FK-конфликте вместо всего батча) —
  в текущей модели порядок FK гарантирован (формы раньше контрагентов), конфликт
  не возникает; усложнение не оправдано для демо-контура.
- **#21** (CHECK-ограничения inn/kpp в БД) — ТЗ не требует; интеграционная БД
  должна принимать данные как есть из 1С, жёсткая валидация на приёмнике может
  ронять легитимные записи. Валидация — задача источника.

---

## 10. Договорённости по окружению

- ОС разработки: **Windows** (`win32`), оболочка PowerShell 5.1.
- Рабочая директория проекта: `D:\Projects\Codex-CLI\roshim-test\1c-kafka-postgres`.
- Корневая папка `roshim-test` содержит `ts/` (ТЗ) и tmp-файлы — **не** рабочая для кода.
- Telegram-уведомления с краткой выжимкой после каждого финального ответа
  (правило из глобального AGENTS.md пользователя).

---

## 11. Соответствие ТЗ (чек-лист приёмки)

Демо-сценарий (раздел 11 ТЗ), должен воспроизводиться командой:
1. ≥3 формы собственности, ≥5 контрагентов в источнике.
2. `sync-full` → события в Kafka → записи в PG.
3. Повторный `sync-full` → без дублей.
4. Изменение контрагента → `sync-incremental` → UPDATE записи.
5. Пометка удаления → `deleted=true` в PG.

Обязательные артефакты сдачи (раздел 12 ТЗ): `docker-compose.yml`, `README.md`,
`.env.example`, `migrations/`, `integration-service/`, `consumer-service/`, `sql/`,
`docs/`, `1c/` (configuration.cf + src/ + screenshots/ + setup.md).
