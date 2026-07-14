# Интеграция 1С → Kafka → PostgreSQL

Интеграционный контур, который выгружает справочные данные из 1С, передаёт их
через Kafka как шину данных и сохраняет в PostgreSQL в нормализованную структуру
таблиц с идемпотентным upsert и мягким удалением.

```
1С → integration-service (producer) → Kafka topic → consumer-service → PostgreSQL
```

Тестовое задание АО «Росхим».

---

## Содержание
- [Быстрый старт](#быстрый-старт)
- [Архитектура](#архитектура)
- [Источник данных: 1С](#источник-данных-1с)
- [Справочники](#справочники)
- [Запуск инфраструктуры](#запуск-инфраструктуры)
- [Полная синхронизация](#полная-синхронизация)
- [Инкрементальная синхронизация](#инкрементальная-синхронизация)
- [Проверка сообщений в Kafka](#проверка-сообщений-в-kafka)
- [Проверка данных в PostgreSQL](#проверка-данных-в-postgresql)
- [Демонстрационный сценарий](#демонстрационный-сценарий)
- [Перезапуск синхронизации](#перезапуск-синхронизации)
- [Архитектурные решения](#архитектурные-решения)
- [Ограничения](#ограничения)
- [Запуск на Windows](#запуск-на-windows)

---

## Быстрый старт

Проект поддерживает два источника данных (переключение — `SOURCE_TYPE` в `.env`):

**Вариант A — реальная 1С (приоритет ТЗ):**
```bash
cp .env.example .env
# в .env: SOURCE_TYPE=onec и ONEC_BASE_URL=http://<IP-хоста>/roshim/hs/integration
# подготовка 1С (база, конфигурация, HTTP-сервис, публикация в IIS) — см. 1c/setup.md
make up
make onec-check                # проверить доступность 1С из контейнера
make sync-full                 # 1С → Kafka → PostgreSQL
make verify
```

**Вариант B — mock-источник (fallback для демо/CI без 1С):**
```bash
cp .env.example .env           # SOURCE_TYPE=mock (значение по умолчанию)
make up
make sync-full                 # mock → Kafka → PostgreSQL
make verify
```

Kafka UI: <http://localhost:8080> · Consumer health: <http://localhost:8081/health>

> На Windows без `make` используйте `./make.ps1 <команда>` — см. [раздел ниже](#запуск-на-windows).

---

## Архитектура

| Компонент | Роль |
|---|---|
| **1С 8.3+** (реализовано на 8.5.1.1302) | Источник справочных данных (собственный HTTP-сервис). Запускается вне Docker. |
| **integration-service** | Producer: читает 1С, публикует события в Kafka. CLI (`full`/`incremental`). |
| **Kafka (KRaft)** | Шина данных. Топики `1c.ownership_forms.v1`, `1c.counterparties.v1` + `.dlq`. |
| **consumer-service** | Consumer: читает Kafka, делает upsert в PostgreSQL. Демон. |
| **PostgreSQL** | Целевая интеграционная БД. |
| **kafka-ui** | Просмотр топиков и сообщений. |

Подробности — в [`docs/architecture.md`](docs/architecture.md),
формат событий — в [`docs/event-format.md`](docs/event-format.md).

**Абстракция источника.** `integration-service` работает через интерфейс `Source`:
- `mock` — воспроизводимый встроенный источник (демо/CI без запущенной 1С);
- `onec` — реальная 1С по HTTP-сервису.

Переключение — переменной `SOURCE_TYPE` в `.env`. Это позволяет
продемонстрировать весь контур Kafka → PostgreSQL независимо от готовности 1С.

---

## Источник данных: 1С

Приоритетный источник по ТЗ — **реальная 1С:Предприятие 8.3+** — реализован и
проверен end-to-end на платформе **8.5.1.1302** (Windows + IIS). Полная пошаговая
инструкция (конфигурация, HTTP-сервис, публикация, нюансы IIS/сети) — в
[`1c/setup.md`](1c/setup.md). Исходники конфигурации 1С — в [`1c/src/`](1c/src),
выгрузка — [`1c/configuration.cf`](1c/configuration.cf).

**Способ доступа — собственный HTTP-сервис 1С (Вариант Б ТЗ)**, возвращающий JSON:
```
GET /ownership-forms
GET /counterparties
GET /counterparties?changed_since=<RFC3339>
```
Выбран вместо OData, т.к. стандартный интерфейс OData в сборке 8.5 требовал ручной
установки состава объектов (headless-команда вела себя нестабильно), тогда как
собственный HTTP-сервис грузится через `LoadConfigFromFiles`, отдаёт JSON точно
по формату ТЗ и полностью автоматизируется. OData описан как альтернатива в
[`docs/architecture.md`](docs/architecture.md).

Подключение контура к реальной 1С — переменными в `.env`:
```env
SOURCE_TYPE=onec
ONEC_BASE_URL=http://<IP-хоста>/roshim/hs/integration
```
> Из контейнера сначала используйте `host.docker.internal`. Если конкретная
> конфигурация Docker Desktop/IIS отдаёт HTTP 502, используйте **реальный IPv4
> хоста** (`ipconfig`, адаптер vEthernet WSL). Подробности — в `1c/setup.md`.

**Абстракция источника (`mock`/`onec`)** сохранена: контур Kafka → PostgreSQL
воспроизводим и без 1С (демо/CI) переключением `SOURCE_TYPE=mock`.

---

## Справочники

**Формы собственности** (`ownership_forms`): `id` (строковый код: `ooo/ip/ao/pao`),
`code`, `name`, `deleted`, `updated_at`.

**Контрагенты** (`counterparties`): `id` (GUID), `code`, `name`, `inn`, `kpp`,
`ownership_form_id` (FK → формы), `deleted`, `updated_at`.

Схема БД — в [`migrations/0001_init.sql`](migrations/0001_init.sql). Демо-данные
mock: 4 формы собственности, 5 контрагентов.

---

## Запуск инфраструктуры

```bash
make up        # сборка образов + запуск + миграции + создание топиков
make ps        # статус
make logs      # логи (follow)
make down      # остановить (данные в volume сохраняются)
make reset     # полный сброс, включая данные PostgreSQL
```

Порядок старта управляется зависимостями compose: `postgres (healthy)` →
`migrate (миграции)` → `kafka (healthy)` → `kafka-init (топики)` →
`consumer-service`. `integration-service` держится живым для запуска команд.

Опциональный pgAdmin: `docker compose --profile extras up -d pgadmin`
(<http://localhost:5050>).

---

## Полная синхронизация

Выгружает все записи справочников и публикует события в Kafka:

```bash
make sync-full
# или напрямую:
docker compose exec integration-service python -m integration sync full
```

Повторный запуск **не создаёт дублей** в PostgreSQL (upsert `ON CONFLICT`).

---

## Инкрементальная синхронизация

Выгружает только изменённые записи (по watermark `sync_state` и `changed_since`):

```bash
make sync-incremental
# или:
docker compose exec integration-service python -m integration sync incremental
```

Механизм: перед чтением фиксируется верхняя граница окна; из `sync_state`
берётся `last_synced_at`; у источника запрашиваются записи с
`changed_since=last_synced_at`; после успешной публикации watermark продвигается.

> На стороне 1С инкремент опирается на реквизит `ДатаИзменения` (заполняется в
> `ПередЗаписью`). Если он недоступен в выбранном варианте 1С — используйте
> повторную полную синхронизацию (идемпотентна). Подробности и промышленный
> вариант — в [`docs/limitations.md`](docs/limitations.md).

---

## Проверка сообщений в Kafka

**Kafka UI:** <http://localhost:8080> → кластер `rh-local` → Topics →
`1c.counterparties.v1` → Messages.

**CLI:**
```bash
# список топиков
make topics

# прочитать сообщения топика с начала
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic 1c.counterparties.v1 --from-beginning --timeout-ms 5000
```

---

## Проверка данных в PostgreSQL

```bash
make verify         # готовые проверочные запросы (sql/verify.sql)
make psql           # интерактивный psql
```

Пример вручную:
```sql
SELECT id, code, name, inn, ownership_form_id, deleted FROM counterparties ORDER BY code;
```

---

## Демонстрационный сценарий

Полный сценарий раздела 11 ТЗ. Приоритетный вариант — **на реальной 1С**.

### Основной: реальная 1С

Предполагается подготовленная 1С (см. [`1c/setup.md`](1c/setup.md)),
`SOURCE_TYPE=onec` и корректный `ONEC_BASE_URL` в `.env`. `$B` — базовый URL 1С,
например `http://172.23.128.1/roshim/hs/integration`.

```bash
make up
make onec-check                              # 1С доступна из контейнера

# 1) данные в 1С: 4 формы + 5 контрагентов (идемпотентный POST /seed)
curl -X POST $B/seed

# 2) полная синхронизация: 1С → Kafka → PostgreSQL
make sync-full
make verify                                  # в PostgreSQL 4 формы + 5 контрагентов

# 3) повторная полная синхронизация — дублей нет
make sync-full
make verify                                  # count не вырос

# 4) изменить контрагента прямо в 1С
curl -X POST "$B/touch?id=<guid>&name=ООО Ромашка (обновлено)"

# 5) инкрементальная синхронизация → обновление записи
make sync-incremental
make verify                                  # name обновился, дублей нет

# 6) пометить контрагента на удаление в 1С
curl -X POST "$B/delete?id=<guid>"
make sync-incremental
make verify                                  # deleted = true; инкремент забрал 1 запись
```

> GUID контрагента можно взять из `curl $B/counterparties`.

### Fallback: mock-источник (без 1С, для демо/CI)

При `SOURCE_TYPE=mock` те же шаги выполняются встроенными помощниками:

```bash
make up
make sync-full && make verify
make sync-full && make verify                 # без дублей
make demo-touch ID=b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001 NAME="ООО Ромашка (обновлено)"
make sync-incremental && make verify          # обновление
make demo-delete ID=b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005
make sync-incremental && make verify          # deleted = true
```

На Windows замените `make <cmd>` на `./make.ps1 <cmd>` (для demo — с `-Id`/`-Name`).

---

## Перезапуск синхронизации

- **Полная** синхронизация идемпотентна — можно запускать повторно в любой момент:
  `make sync-full`.
- **Инкрементальная** опирается на watermark. Чтобы «переиграть» инкремент с
  определённого момента, обновите `sync_state`:
  ```sql
  UPDATE sync_state SET last_synced_at = NULL;   -- следующий incremental заберёт всё
  ```
- **Переобработка из Kafka** (перечитать топики consumer'ом): сбросить offset
  группы или сменить `KAFKA_CONSUMER_GROUP` в `.env` и перезапустить consumer.
- **Полный сброс** данных: `make reset` (удаляет volume PostgreSQL), затем `make up`.

Ошибки логируются структурированно (JSON) в stdout сервисов: `make logs`.

---

## Архитектурные решения

Ключевые решения (полностью — в [`docs/decisions.md`](docs/decisions.md)):

- **Kafka KRaft** без ZooKeeper — проще локальный запуск.
- **Собственный consumer-service** (Вариант А ТЗ) — контроль над upsert, retry,
  DLQ, транзакциями, порядком применения FK.
- **Абстракция источника** (`onec`/`mock`) — воспроизводимость демо.
- **HTTP-сервис 1С** как основной доступ, OData — альтернатива.
- **Идемпотентность**: ключ = id объекта 1С; `ON CONFLICT DO UPDATE` + условие по
  `source_updated_at` (не затираем свежее старым); commit offset после записи.
- **Мягкое удаление**: событие `deleted=true`, физически строка не удаляется.

---

## Ограничения

Кратко (полностью — в [`docs/limitations.md`](docs/limitations.md)):

- Одиночный брокер Kafka, репликация = 1 (демо, не production-кластер).
- Без OAuth/JWT; секреты только через ENV/`.env`.
- Удаление — мягкое (пометка), т.к. физически удалённая в 1С запись не порождает
  событие.
- Инкремент опирается на реквизит `ДатаИзменения`.

Что улучшил бы в промышленной версии: Schema Registry, план обмена 1С, exactly-once
(outbox/транзакционный producer), кластер Kafka, TLS+SASL, Prometheus-метрики,
мониторинг DLQ, CDC (Debezium) при больших объёмах — подробно в `docs/limitations.md`.

---

## Запуск на Windows

Если GNU `make` не установлен, используйте PowerShell-обёртку `make.ps1`:

**Реальная 1С (приоритетный сценарий):**

```powershell
Copy-Item .env.example .env
# Отредактируйте .env:
#   SOURCE_TYPE=onec
#   ONEC_BASE_URL=http://<IP-хоста>/roshim/hs/integration
./make.ps1 up
./make.ps1 onec-check
./make.ps1 sync-full
./make.ps1 verify
./make.ps1 sync-incremental
./make.ps1 health
./make.ps1 down
```

**Mock-источник (fallback без 1С):**

```powershell
Copy-Item .env.example .env   # SOURCE_TYPE=mock по умолчанию
./make.ps1 up
./make.ps1 sync-full
./make.ps1 verify
./make.ps1 demo-touch -Id b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001 -Name "ООО Ромашка (обновлено)"
./make.ps1 sync-incremental
./make.ps1 demo-delete -Id b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005
./make.ps1 down
```

Либо напрямую через `docker compose exec` — см. примеры выше в каждом разделе.

---

## Структура репозитория

```
├── docker-compose.yml
├── README.md               # этот файл
├── AGENTS.md               # архитектурный справочник/журнал проекта
├── Makefile / make.ps1
├── .env.example
├── migrations/             # SQL-схема (0001_init.sql)
├── sql/                    # проверочные запросы (verify.sql)
├── docs/                   # architecture, event-format, decisions, limitations
├── integration-service/    # producer (Python)
├── consumer-service/       # consumer (Python)
└── 1c/                     # setup.md, configuration.cf, src/ (XML+BSL), screenshots/
```

---

## Требования

- Docker + Docker Compose.
- Для реального источника: 1С:Предприятие 8.3+ и веб-сервер
  (проверено на 8.5.1.1302 community; см. `1c/setup.md`).
- Для `make`: GNU make (Linux/macOS/WSL/Git Bash) либо `make.ps1` на Windows.
