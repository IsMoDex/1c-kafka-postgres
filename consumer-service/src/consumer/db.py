"""
Слой доступа к PostgreSQL: идемпотентный upsert в одной транзакции.

Ключевые правила (см. AGENTS.md, раздел 7):
  * upsert через INSERT ... ON CONFLICT (id) DO UPDATE — нет дублей;
  * идемпотентность по времени: не затираем более свежую запись более старой
    (обновляем, только если входящий source_updated_at >= сохранённого или NULL);
  * мягкое удаление — обновление флага deleted, строки не удаляются физически;
  * весь пакет пишется в одной транзакции (atomic).
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg
from psycopg.rows import tuple_row

if TYPE_CHECKING:
    from collections.abc import Iterable

# SQL upsert формы собственности.
# COALESCE-условие в WHERE даёт идемпотентность по времени.
_UPSERT_OWNERSHIP_FORM = """
INSERT INTO ownership_forms (id, code, name, source_updated_at, deleted, imported_at)
VALUES (%(id)s, %(code)s, %(name)s, %(source_updated_at)s, %(deleted)s, now())
ON CONFLICT (id) DO UPDATE SET
    code = EXCLUDED.code,
    name = EXCLUDED.name,
    source_updated_at = EXCLUDED.source_updated_at,
    deleted = EXCLUDED.deleted,
    imported_at = now()
WHERE ownership_forms.source_updated_at IS NULL
   OR (EXCLUDED.source_updated_at IS NOT NULL
       AND EXCLUDED.source_updated_at >= ownership_forms.source_updated_at)
"""

_UPSERT_COUNTERPARTY = """
INSERT INTO counterparties
    (id, code, name, inn, kpp, ownership_form_id, source_updated_at, deleted, imported_at)
VALUES
    (%(id)s, %(code)s, %(name)s, %(inn)s, %(kpp)s, %(ownership_form_id)s,
     %(source_updated_at)s, %(deleted)s, now())
ON CONFLICT (id) DO UPDATE SET
    code = EXCLUDED.code,
    name = EXCLUDED.name,
    inn = EXCLUDED.inn,
    kpp = EXCLUDED.kpp,
    ownership_form_id = EXCLUDED.ownership_form_id,
    source_updated_at = EXCLUDED.source_updated_at,
    deleted = EXCLUDED.deleted,
    imported_at = now()
WHERE counterparties.source_updated_at IS NULL
   OR (EXCLUDED.source_updated_at IS NOT NULL
       AND EXCLUDED.source_updated_at >= counterparties.source_updated_at)
"""


@dataclass(frozen=True)
class ApplyResult:
    ownership_forms: int
    counterparties: int

    @property
    def total(self) -> int:
        return self.ownership_forms + self.counterparties


class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn = self._connect()

    def _connect(self) -> psycopg.Connection[tuple[object, ...]]:
        return psycopg.connect(
            self._dsn,
            autocommit=False,
            row_factory=tuple_row,
            connect_timeout=5,
            options="-c statement_timeout=10000 -c lock_timeout=5000",
        )

    def _reconnect(self) -> None:
        with suppress(psycopg.Error):
            self._conn.close()
        self._conn = self._connect()

    def _ping_once(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        self._conn.rollback()

    def ping(self) -> bool:
        try:
            self._ping_once()
        except Exception:  # noqa: BLE001 -- health probe must survive any driver failure and reconnect.
            try:
                self._reconnect()
                self._ping_once()
            except Exception:  # noqa: BLE001 -- a failed retry makes the dependency unhealthy.
                return False
        return True

    def apply_batch(
        self,
        ownership_forms: Iterable[dict[str, object]],
        counterparties: Iterable[dict[str, object]],
    ) -> ApplyResult:
        """
        Применяет пакет upsert-ов в ЕДИНОЙ транзакции.

        Формы собственности применяются первыми (FK-зависимость).
        При исключении транзакция откатывается целиком.
        """
        try:
            ownership_count = 0
            counterparty_count = 0
            with self._conn.cursor() as cur:
                for row in ownership_forms:
                    cur.execute(_UPSERT_OWNERSHIP_FORM, row)
                    ownership_count += cur.rowcount
                for row in counterparties:
                    cur.execute(_UPSERT_COUNTERPARTY, row)
                    counterparty_count += cur.rowcount
            self._conn.commit()
            return ApplyResult(ownership_count, counterparty_count)
        # Roll back every failed transaction, then preserve the original exception.
        except Exception as exc:
            with suppress(psycopg.Error):
                self._conn.rollback()
            if isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                with suppress(psycopg.Error):
                    self._reconnect()
            raise

    def close(self) -> None:
        with suppress(psycopg.Error):
            self._conn.close()
