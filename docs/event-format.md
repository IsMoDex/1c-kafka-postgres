# Формат событий Kafka

## Топики

| Топик | Назначение | Партиций | Ключ |
|---|---|---|---|
| `1c.ownership_forms.v1` | Формы собственности (upsert/delete) | 1 | `id` формы |
| `1c.counterparties.v1` | Контрагенты (upsert/delete) | 3 | `id` (GUID) |
| `1c.ownership_forms.dlq` | Dead-letter форм собственности | 1 | исходный |
| `1c.counterparties.dlq` | Dead-letter контрагентов | 1 | исходный |

**Ключ сообщения** — стабильный `id` объекта 1С. Гарантирует порядок по объекту
и корректную работу партиционирования.

## Конверт события (envelope)

```json
{
  "event_id": "9d3c8f6c-21a5-4f87-8b8e-8e2f3c7c1001",
  "event_type": "counterparty.upsert",
  "source": "1c",
  "occurred_at": "2026-07-10T12:30:00Z",
  "payload": { ... }
}
```

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | UUID | Уникален на каждое событие (не на объект) |
| `event_type` | enum | `ownership_form.upsert` / `ownership_form.delete` / `counterparty.upsert` / `counterparty.delete` |
| `source` | string | Источник, всегда `1c` |
| `occurred_at` | RFC3339 | Момент формирования события |
| `payload` | object | Тело записи справочника (см. ниже) |

## payload — Форма собственности

```json
{
  "id": "ooo",
  "code": "000000001",
  "name": "ООО",
  "deleted": false,
  "updated_at": "2026-07-10T12:00:00Z"
}
```

## payload — Контрагент

```json
{
  "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001",
  "code": "000001",
  "name": "ООО Ромашка",
  "inn": "7701234567",
  "kpp": "770101001",
  "ownership_form_id": "ooo",
  "deleted": false,
  "updated_at": "2026-07-10T12:00:00Z"
}
```

## Семантика

- **upsert** — вставка или обновление записи (`deleted: false`).
- **delete** — мягкое удаление: событие с `deleted: true`. Физически строка
  в PostgreSQL не удаляется, обновляется флаг `deleted`.
- **Идемпотентность** — повторная доставка того же события не меняет результат
  (upsert по PK + условие по `source_updated_at`).

## HTTP-контракт источника 1С

Реальная 1С (или mock) должна отдавать массив объектов payload:

```
GET {base}/ownership-forms                      → [ {payload формы}, ... ]
GET {base}/counterparties                        → [ {payload контрагента}, ... ]
GET {base}/counterparties?changed_since=<RFC3339> → только изменённые
```

Ответ — JSON-массив, `Content-Type: application/json`.
Отсутствующий `changed_since` означает полную выборку; невалидное значение
возвращает HTTP 400 (`invalid changed_since; expected RFC3339`).
