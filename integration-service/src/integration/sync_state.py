"""Доступ к watermark инкрементальной синхронизации (таблица sync_state).

integration-service — единственный писатель watermark. Значение читается
перед инкрементальной выгрузкой и продвигается только после успешной
публикации всех событий в Kafka.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from collections.abc import Iterator
from typing import Optional

import psycopg


class SyncState:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get(self, entity: str) -> Optional[datetime]:
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
            conn.execute(
                f"""
                INSERT INTO sync_state (entity, last_synced_at, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (entity) DO UPDATE
                    SET last_synced_at = {update_value},
                        updated_at = now()
                """,
                (entity, value),
            )
            conn.commit()

    def wait_for_ownership_forms(self, ids: set[str], timeout: float) -> None:
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
                    raise TimeoutError(
                        f"Consumer не применил формы собственности за {timeout:g} с: {missing_list}"
                    )
                time.sleep(0.2)
