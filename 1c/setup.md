# Настройка источника 1С (реальная реализация)

Здесь описано, как фактически поднят источник данных на **1С:Предприятие 8.5.1.1302**
(community-лицензия) под Windows + IIS. Это рабочая, проверенная end-to-end
конфигурация: `1С → HTTP-сервис → integration-service → Kafka → consumer → PostgreSQL`.

> Выбран **Вариант Б из ТЗ — собственный HTTP-сервис 1С**. Причина: штатный
> стандартный интерфейс OData в этой сборке 8.5 требовал ручной установки состава
> объектов (headless-команда `SetStandardODataInterfaceContent` вела себя
> нестабильно), тогда как собственный HTTP-сервис грузится через
> `LoadConfigFromFiles`, отдаёт JSON точно по формату ТЗ и полностью
> автоматизируется. OData оставлен как задокументированная альтернатива.

---

## 0. Что стоит на машине

- Платформа **1С:Предприятие 8.5.1.1302** (x86), путь `D:\1C\8.5.1.1302\bin\`.
- Веб-компоненты 1С: `wsisapi.dll`, `wsap24.dll`, `webinst.exe` (x86!).
- **IIS** с компонентами **ISAPI Extensions + ISAPI Filter + CGI**.
- Файловая ИБ: `D:\1C\Bases\roshim-1c`.
- Исходники конфигурации: `1c/src/` (в репозитории), выгрузка — `1c/configuration.cf`.

---

## 1. Конфигурация 1С

Конфигурация «Интеграция» (см. `1c/src/`) содержит:

### Справочник «ФормыСобственности»
- `Код` (строка 9), `Наименование` (строка 150)
- реквизит `ДатаИзменения` (дата/время) — заполняется в `ПередЗаписью` (UTC)
- штатная `ПометкаУдаления`
- id для выгрузки = стабильный строковый код: `ooo/ip/ao/pao`
  (маппинг код↔id зашит в модуле HTTP-сервиса)

### Справочник «Контрагенты»
- `Код`, `Наименование`, `ИНН`, `КПП`
- `ФормаСобственности` (СправочникСсылка.ФормыСобственности)
- реквизит `ДатаИзменения`, штатная `ПометкаУдаления`
- id для выгрузки = GUID (`Ссылка.УникальныйИдентификатор()`)

### Заполнение `ДатаИзменения`
В модуле объекта каждого справочника (`Ext/ObjectModule.bsl`):
```bsl
Процедура ПередЗаписью(Отказ)
    ЭтотОбъект.ДатаИзменения = ТекущаяУниверсальнаяДата();
КонецПроцедуры
```
Это основа инкрементальной синхронизации (`changed_since`).

---

## 2. HTTP-сервис «ИнтеграционныйСервис»

Корневой URL: `integration`. Модуль — `1c/src/HTTPServices/.../Ext/Module.bsl`.

Эндпоинты (после публикации доступны как `http://<host>/roshim/hs/integration/...`):

| Метод | Путь | Назначение |
|---|---|---|
| GET  | `/ownership-forms` | Все формы собственности (JSON) |
| GET  | `/ownership-forms?changed_since=<RFC3339>` | Только изменённые |
| GET  | `/counterparties` | Все контрагенты (JSON) |
| GET  | `/counterparties?changed_since=<RFC3339>` | Только изменённые |
| POST | `/seed` | Засеять демо-данные (идемпотентно): 4 формы + 5 контрагентов |
| POST | `/touch?id=<guid>&name=<...>` | Изменить контрагента (двигает `ДатаИзменения`) |
| POST | `/delete?id=<guid>` | Пометить контрагента на удаление |

Формат ответа — JSON точно по ТЗ:
```json
[{ "id":"...", "code":"000001", "name":"ООО Ромашка",
   "inn":"7701234567", "kpp":"770101001",
   "ownership_form_id":"ooo", "deleted":false,
   "updated_at":"2026-07-14T03:54:44Z" }]
```

---

## 3. Воспроизведение с нуля (headless, без GUI)

Все шаги автоматизируются через пакетный режим `1cv8.exe DESIGNER`.
Важно: запускать с `/DisableStartupDialogs /DisableStartupMessages`, дожидаться
завершения процесса; между шагами база должна быть свободна (остановить пул IIS).

```powershell
$EXE = "D:\1C\8.5.1.1302\bin\1cv8.exe"
$IB  = "D:\1C\Bases\roshim-1c"

# 3.1. Создать файловую ИБ
& $EXE CREATEINFOBASE "File=""$IB"";" /DisableStartupDialogs /DisableStartupMessages

# 3.2. Загрузить конфигурацию из XML-исходников (1c/src) и обновить БД
& $EXE DESIGNER /F "$IB" /DisableStartupDialogs /DisableStartupMessages `
    /LoadConfigFromFiles "<repo>\1c\src" /UpdateDBCfg
```

> Формат XML-исходников — платформенный (MDClasses 2.21). Их можно
> редактировать/версионировать в Git. Альтернатива — загрузить `1c/configuration.cf`
> через Конфигуратор (Конфигурация → Загрузить конфигурацию из файла).

---

## 4. Публикация на веб-сервере (IIS)

```powershell
$WEBINST = "D:\1C\8.5.1.1302\bin\webinst.exe"
& $WEBINST -publish -iis -wsdir "roshim" -dir "C:\inetpub\wwwroot\roshim" `
    -connstr "File=""D:\1C\Bases\roshim-1c"";"
```

### Обязательные условия для IIS (иначе HTTP 500.21 / 502)
1. **Включить компоненты IIS** (иначе ISAPI-обработчик 1С «повреждён», 500.21):
   ```powershell
   Enable-WindowsOptionalFeature -Online -FeatureName IIS-ISAPIExtensions -NoRestart
   Enable-WindowsOptionalFeature -Online -FeatureName IIS-ISAPIFilter -NoRestart
   Enable-WindowsOptionalFeature -Online -FeatureName IIS-CGI -NoRestart
   ```
2. **32-битный пул**: платформа 1С здесь x86, значит `wsisapi.dll` — x86,
   поэтому пулу приложения нужен режим 32 бит:
   ```powershell
   C:\Windows\System32\inetsrv\appcmd.exe set apppool "DefaultAppPool" /enable32BitAppOnWin64:true
   ```
3. **Права** пула на каталоги базы и bin 1С:
   ```powershell
   icacls "D:\1C\Bases" /grant "IIS AppPool\DefaultAppPool:(OI)(CI)M" /T
   icacls "D:\1C\8.5.1.1302\bin" /grant "IIS AppPool\DefaultAppPool:(OI)(CI)RX" /T
   ```
4. Перезапустить пул: `appcmd recycle apppool /apppool.name:"DefaultAppPool"`.

### Проверка
```powershell
# засеять данные
Invoke-WebRequest http://localhost/roshim/hs/integration/seed -Method POST
# прочитать
Invoke-WebRequest http://localhost/roshim/hs/integration/counterparties
```

---

## 5. Подключение integration-service к реальной 1С

В `.env` проекта:
```env
SOURCE_TYPE=onec
ONEC_BASE_URL=http://172.23.128.1/roshim/hs/integration
```

> **Важный нюанс сети.** Сначала используйте `host.docker.internal`. В одном
> состоянии Docker Desktop/IIS он давал **HTTP 502** при обработке ISAPI-ответа
> 1С; fallback — **реальный IPv4-адрес хоста** (интерфейс `vEthernet (WSL)`,
> напр. `172.23.128.1`). Актуальный IP: `ipconfig` → vEthernet (WSL).

Затем:
```bash
docker compose up -d --force-recreate integration-service
docker compose exec integration-service python -m integration sync full
docker compose exec integration-service python -m integration sync incremental
```

---

## 6. Демонстрационный сценарий на реальной 1С (проверено)

```powershell
$B = "http://172.23.128.1/roshim/hs/integration"

# 1) данные в 1С
Invoke-WebRequest $B/seed -Method POST                       # 4 формы + 5 контрагентов

# 2) полная синхронизация
docker compose exec integration-service python -m integration sync full
#   -> в PostgreSQL 4 формы + 5 контрагентов

# 3) повторный full -> без дублей (upsert ON CONFLICT)

# 4) изменить контрагента и синхронизировать инкрементально
Invoke-WebRequest "$B/touch?id=<guid>&name=ООО Ромашка (обновлено)" -Method POST
docker compose exec integration-service python -m integration sync incremental
#   -> запись обновлена

# 5) мягкое удаление
Invoke-WebRequest "$B/delete?id=<guid>" -Method POST
docker compose exec integration-service python -m integration sync incremental
#   -> deleted = true, инкремент забирает ТОЛЬКО изменённую запись
```

---

## 7. Известные сложности и как решены

| Проблема | Причина | Решение |
|---|---|---|
| HTTP 500.21, «IsapiModule повреждён» | В IIS выключены ISAPI Extensions/Filter | Включить компоненты IIS (см. п.4.1) |
| HTTP 500 на любой запрос | Пул IIS 64-бит, а `wsisapi.dll` x86 | `enable32BitAppOnWin64:true` |
| OData `$metadata` пуст (0 EntitySet) | Не задан состав стандартного OData | Перешли на собственный HTTP-сервис (Вариант Б) |
| HTTP 502 из контейнера | Особенность текущего состояния Docker Desktop/IIS | Вместо `host.docker.internal` использовать реальный IPv4 хоста |
| «Неопределена ИБ» в headless OData | Нестабильный парсинг аргументов команды 8.5 | HTTP-сервис вместо OData |

---

## 8. Что в репозитории (папка 1c/)

```
1c/
├── configuration.cf     # выгрузка конфигурации (загружается в Конфигуратор)
├── src/                 # XML-исходники конфигурации (LoadConfigFromFiles)
│   ├── Configuration.xml
│   ├── Catalogs/ФормыСобственности.xml, Контрагенты.xml (+ Ext/ObjectModule.bsl)
│   └── HTTPServices/ИнтеграционныйСервис.xml (+ Ext/Module.bsl)
└── setup.md             # этот файл
```
