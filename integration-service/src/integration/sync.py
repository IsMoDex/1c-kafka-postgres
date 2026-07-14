"""Оркестрация синхронизации: чтение источника → построение событий → Kafka.

Режимы:
  * full        — выгрузить все записи справочников;
  * incremental — выгрузить только изменённые с момента watermark (sync_state).

Порядок: формы собственности публикуются раньше контрагентов (FK-зависимость
на стороне PostgreSQL). Watermark продвигается только после успешного flush.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import structlog

from integration.config import Config
from integration.models import Counterparty, Event, OwnershipForm
from integration.producer import EventProducer
from integration.sources.base import Source
from integration.sync_state import SyncState

log = structlog.get_logger()


def _ownership_event(form: OwnershipForm) -> Event:
    event_type = "ownership_form.delete" if form.deleted else "ownership_form.upsert"
    return Event(event_type=event_type, payload=form.model_dump(mode="json"))


def _counterparty_event(cp: Counterparty) -> Event:
    event_type = "counterparty.delete" if cp.deleted else "counterparty.upsert"
    return Event(event_type=event_type, payload=cp.model_dump(mode="json"))


class Synchronizer:
    def __init__(self, config: Config, source: Source) -> None:
        self._cfg = config
        self._source = source
        self._producer = EventProducer(config.kafka_bootstrap_servers)
        self._state = SyncState(config.pg_dsn)

    def run(self, mode: str) -> dict:
        if mode not in ("full", "incremental"):
            raise ValueError(f"Неизвестный режим синхронизации: {mode!r}")

        # верхняя граница окна фиксируется ДО чтения — во избежание потери
        # изменений, произошедших во время выгрузки.
        window_end = datetime.now(timezone.utc)

        of_since = self._since("ownership_forms", mode)
        cp_since = self._since("counterparties", mode)

        log.info("sync_start", mode=mode, of_since=_iso(of_since), cp_since=_iso(cp_since))

        # 1) формы собственности — первыми (FK)
        forms = self._source.fetch_ownership_forms(of_since)
        for form in forms:
            self._producer.publish(self._cfg.topic_ownership_forms, _ownership_event(form))

        # 2) контрагенты
        counterparties = self._source.fetch_counterparties(cp_since)
        for cp in counterparties:
            self._producer.publish(self._cfg.topic_counterparties, _counterparty_event(cp))

        errors = self._producer.flush()
        if errors:
            log.error("sync_failed", delivery_errors=errors)
            raise RuntimeError(f"Синхронизация прервана: ошибок доставки в Kafka = {errors}")

        # watermark продвигаем только после успешного flush
        if mode == "incremental":
            self._state.set("ownership_forms", window_end)
            self._state.set("counterparties", window_end)

        result = {
            "mode": mode,
            "ownership_forms": len(forms),
            "counterparties": len(counterparties),
            "window_end": _iso(window_end),
        }
        log.info("sync_done", **result)
        return result

    def _since(self, entity: str, mode: str) -> Optional[datetime]:
        if mode == "full":
            return None
        return self._state.get(entity)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None
