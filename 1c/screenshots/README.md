# Визуальные доказательства работы контура

Каталог содержит GUI-скриншоты и программные артефакты реального контура
`1С 8.5.1.1302 Community → Kafka → PostgreSQL`. Все чувствительные данные и
пароли исключены из кадров.

## GUI-скриншоты

| Файл | Что подтверждает |
|---|---|
| [`gui_01_configuration_metadata.png`](gui_01_configuration_metadata.png) | Конфигуратор 1С: оба справочника, HTTP-сервис `ИнтеграционныйСервис` и его URL-шаблоны |
| [`gui_02_ownership_forms.png`](gui_02_ownership_forms.png) | Справочник «Формы собственности»: ООО, ИП, АО, ПАО с кодами и датами изменения |
| [`gui_03_counterparties.png`](gui_03_counterparties.png) | Справочник «Контрагенты»: минимум пять записей, ИНН, КПП, форма собственности и пометки удаления |
| [`gui_04_iis_publication.png`](gui_04_iis_publication.png) | IIS: приложение `roshim`, пул `DefaultAppPool`, физический путь публикации |
| [`gui_05_http_ownership_forms.png`](gui_05_http_ownership_forms.png) | Живой HTTP 1С `GET /ownership-forms` и JSON четырёх форм |
| [`gui_06_http_counterparties.png`](gui_06_http_counterparties.png) | Живой HTTP 1С `GET /counterparties` и JSON контрагентов |
| [`gui_07_kafka_topics.png`](gui_07_kafka_topics.png) | Kafka UI: два основных и два DLQ-топика, partition и количество сообщений |
| [`gui_08_kafka_message.png`](gui_08_kafka_message.png) | Kafka UI: реальный `counterparty.upsert`, envelope, payload и стабильный key |
| [`gui_09_kafka_dlq_empty.png`](gui_09_kafka_dlq_empty.png) | Оба DLQ-топика с `Message Count = 0` |
| [`gui_10_postgresql_state.png`](gui_10_postgresql_state.png) | PostgreSQL после end-to-end: формы, контрагенты и `deleted=true` |
| [`gui_11_1c_incremental_update.png`](gui_11_1c_incremental_update.png) | Изменение контрагента в интерфейсе 1С |
| [`gui_12_postgresql_incremental_update.png`](gui_12_postgresql_incremental_update.png) | `sync-incremental` и обновлённая строка PostgreSQL без создания дубля |
| [`gui_13_1c_demo_reset.png`](gui_13_1c_demo_reset.png) | Снятие пометки удаления у demo-контрагента перед повторным сценарием |
| [`gui_14_postgresql_demo_reset.png`](gui_14_postgresql_demo_reset.png) | Повторный incremental применил reset: `deleted=false` в PostgreSQL |

В рабочей 1С на момент съёмки присутствовала дополнительная ручная запись с
кодом `000006`. ТЗ требует минимум пять контрагентов, поэтому она не нарушает
приёмочный сценарий. `POST /seed` восстанавливает эталонные записи `000001`–
`000005`, но намеренно не удаляет пользовательские записи сверх demo-набора.

## Машиночитаемые артефакты

| Файл | Что подтверждает |
|---|---|
| `01_ownership-forms.json` | Ответ `GET /ownership-forms` |
| `02_counterparties.json` | Ответ `GET /counterparties` |
| `03_changed-since-future-empty.json` | Будущий `changed_since` возвращает пустой массив |
| `04_postgresql-data.txt` | Таблицы PostgreSQL и health consumer после end-to-end |

## Соответствие демонстрационному сценарию ТЗ

1. Справочники и HTTP-сервис в реальной 1С: `gui_01`–`gui_06`.
2. События появились в Kafka: `gui_07`, `gui_08`.
3. DLQ пусты: `gui_09`.
4. Данные появились в PostgreSQL без дублей: `gui_10` и SQL-артефакт.
5. Инкрементальное изменение дошло до PostgreSQL: `gui_11`, `gui_12`.
6. Мягкое удаление видно в `gui_03`/`gui_10`; повторяемый reset подтверждён
   `gui_13`/`gui_14` и автоматизирован live integration test.
