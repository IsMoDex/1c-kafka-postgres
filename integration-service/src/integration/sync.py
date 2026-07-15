"""
Оркестрация синхронизации: чтение источника → построение событий → Kafka.

Режимы:
  * full        — выгрузить все записи справочников;
  * incremental — выгрузить только изменённые с момента watermark (sync_state).

Порядок: формы публикуются и применяются в PostgreSQL до публикации контрагентов.
Watermark берётся из updated_at источника и меняется только после успешного flush.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from integration.models import Counterparty, Event, OwnershipForm
from integration.producer import EventProducer
from integration.sync_state import SyncState

if TYPE_CHECKING:
    from integration.config import Config
    from integration.sources.base import Source

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

    def run(self, mode: str) -> dict[str, object]:
        if mode not in ("full", "incremental"):
            message = f"Неизвестный режим синхронизации: {mode!r}"
            raise ValueError(message)

        with self._state.lock():
            return self._run_locked(mode)

    def _run_locked(self, mode: str) -> dict[str, object]:
        run_started_at = datetime.now(UTC)

        of_since = self._since("ownership_forms", mode)
        cp_since = self._since("counterparties", mode)

        log.info("sync_start", mode=mode, of_since=_iso(of_since), cp_since=_iso(cp_since))

        # 1) формы собственности — первыми (FK)
        forms = self._source.fetch_ownership_forms(of_since)
        for form in forms:
            self._producer.publish(self._cfg.topic_ownership_forms, _ownership_event(form))

        errors = self._producer.flush()
        if errors:
            log.error("sync_failed", stage="ownership_forms", delivery_errors=errors)
            message = f"Синхронизация прервана: ошибок доставки форм в Kafka = {errors}"
            raise RuntimeError(message)

        # 2) контрагенты
        counterparties = self._source.fetch_counterparties(cp_since)

        # Kafka не гарантирует порядок между топиками. До публикации зависимых
        # записей ждём, пока consumer фактически применит требуемые формы в PG.
        required_form_ids = {cp.ownership_form_id for cp in counterparties if cp.ownership_form_id is not None}
        self._state.wait_for_ownership_forms(required_form_ids, self._cfg.fk_barrier_timeout)

        for cp in counterparties:
            self._producer.publish(self._cfg.topic_counterparties, _counterparty_event(cp))

        errors = self._producer.flush()
        if errors:
            log.error("sync_failed", delivery_errors=errors)
            message = f"Синхронизация прервана: ошибок доставки в Kafka = {errors}"
            raise RuntimeError(message)

        # Watermark берём из часов источника, а не integration-service. Overlap
        # в _since защищает записи с одинаковой секундой ДатаИзменения.
        self._advance_state("ownership_forms", forms, mode)
        self._advance_state("counterparties", counterparties, mode)

        result = {
            "mode": mode,
            "ownership_forms": len(forms),
            "counterparties": len(counterparties),
            "run_started_at": _iso(run_started_at),
        }
        log.info("sync_done", **result)
        return result

    def _since(self, entity: str, mode: str) -> datetime | None:
        if mode == "full":
            return None
        value = self._state.get(entity)
        return value - timedelta(seconds=1) if value else None

    def _advance_state(
        self,
        entity: str,
        records: list[OwnershipForm] | list[Counterparty],
        mode: str,
    ) -> None:
        timestamps = [record.updated_at for record in records if record.updated_at is not None]
        if timestamps:
            self._state.set(entity, max(timestamps), monotonic=mode == "incremental")


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
