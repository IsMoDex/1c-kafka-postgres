"""Доступ к watermark инкрементальной синхронизации (таблица sync_state).

integration-service — единственный писатель watermark. Значение читается
перед инкрементальной выгрузкой и продвигается только после успешной
публикации всех событий в Kafka.
"""
from __future__ import annotations

from datetime import datetime
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

    def set(self, entity: str, value: datetime) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                """
                INSERT INTO sync_state (entity, last_synced_at, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (entity) DO UPDATE
                    SET last_synced_at = EXCLUDED.last_synced_at,
                        updated_at = now()
                """,
                (entity, value),
            )
            conn.commit()
