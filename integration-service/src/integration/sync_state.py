"""
Доступ к watermark инкрементальной синхронизации (таблица sync_state).

integration-service — единственный писатель watermark. Значение читается
перед инкрементальной выгрузкой и продвигается только после успешной
публикации всех событий в Kafka.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Set as AbstractSet

    from integration.models import Counterparty, OwnershipForm


class SyncState:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get(self, entity: str) -> datetime | None:
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                "SELECT last_synced_at FROM sync_state WHERE entity = %s",
                (entity,),
            ).fetchone()
            return row[0] if row else None

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Сериализует sync-запуски через session-level advisory lock PostgreSQL."""
        with psycopg.connect(self._dsn, autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(hashtext('integration-service-sync'))")
            try:
                yield
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtext('integration-service-sync'))")

    def set(self, entity: str, value: datetime, *, monotonic: bool = True) -> None:
        with psycopg.connect(self._dsn) as conn:
            update_value = (
                "GREATEST(sync_state.last_synced_at, EXCLUDED.last_synced_at)"
                if monotonic
                else "EXCLUDED.last_synced_at"
            )
            query = sql.SQL(
                """
                INSERT INTO sync_state (entity, last_synced_at, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (entity) DO UPDATE
                    SET last_synced_at = {},
                        updated_at = now()
                """
            ).format(sql.SQL(update_value))
            conn.execute(
                query,
                (entity, value),
            )
            conn.commit()

    def changed_ownership_forms(self, records: list[OwnershipForm]) -> list[OwnershipForm]:
        if not records:
            return []
        with psycopg.connect(self._dsn) as conn:
            rows = conn.execute(
                """
                SELECT id, code, name, deleted, source_updated_at
                FROM ownership_forms
                WHERE id = ANY(%s)
                """,
                ([record.id for record in records],),
            ).fetchall()
        current = {row[0]: tuple(row[1:]) for row in rows}
        return [
            record
            for record in records
            if self._needs_publish(
                current.get(record.id),
                (record.code, record.name, record.deleted, record.updated_at),
            )
        ]

    def changed_counterparties(self, records: list[Counterparty]) -> list[Counterparty]:
        if not records:
            return []
        with psycopg.connect(self._dsn) as conn:
            rows = conn.execute(
                """
                SELECT id::text, code, name, inn, kpp, ownership_form_id, deleted, source_updated_at
                FROM counterparties
                WHERE id::text = ANY(%s)
                """,
                ([record.id for record in records],),
            ).fetchall()
        current = {row[0]: tuple(row[1:]) for row in rows}
        return [
            record
            for record in records
            if self._needs_publish(
                current.get(record.id),
                (
                    record.code,
                    record.name,
                    record.inn,
                    record.kpp,
                    record.ownership_form_id,
                    record.deleted,
                    record.updated_at,
                ),
            )
        ]

    @staticmethod
    def _needs_publish(current: tuple[object, ...] | None, incoming: tuple[object, ...]) -> bool:
        if current is None:
            return True
        current_timestamp = current[-1]
        incoming_timestamp = incoming[-1]
        if (
            isinstance(current_timestamp, datetime)
            and isinstance(incoming_timestamp, datetime)
            and incoming_timestamp < current_timestamp
        ):
            return False
        return current != incoming

    def wait_for_ownership_forms(self, ids: AbstractSet[str], timeout: float) -> None:
        """Ждёт применения форм consumer-ом перед публикацией зависимых записей."""
        if not ids:
            return
        deadline = time.monotonic() + timeout
        with psycopg.connect(self._dsn, autocommit=True) as conn:
            while True:
                rows = conn.execute(
                    "SELECT id FROM ownership_forms WHERE id = ANY(%s)",
                    (list(ids),),
                ).fetchall()
                missing = ids - {row[0] for row in rows}
                if not missing:
                    return
                if time.monotonic() >= deadline:
                    missing_list = ", ".join(sorted(missing))
                    message = f"Consumer не применил формы собственности за {timeout:g} с: {missing_list}"
                    raise TimeoutError(message)
                time.sleep(0.2)
