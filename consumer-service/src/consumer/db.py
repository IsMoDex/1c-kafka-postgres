"""Слой доступа к PostgreSQL: идемпотентный upsert в одной транзакции.

Ключевые правила (см. AGENTS.md, раздел 7):
  * upsert через INSERT ... ON CONFLICT (id) DO UPDATE — нет дублей;
  * идемпотентность по времени: не затираем более свежую запись более старой
    (обновляем, только если входящий source_updated_at >= сохранённого или NULL);
  * мягкое удаление — обновление флага deleted, строки не удаляются физически;
  * весь пакет пишется в одной транзакции (atomic).
"""
from __future__ import annotations

from typing import Iterable

import psycopg
from psycopg.rows import tuple_row

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


class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn = self._connect()

    def _connect(self):
        return psycopg.connect(self._dsn, autocommit=False, row_factory=tuple_row)

    def _reconnect(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = self._connect()

    def ping(self) -> bool:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            self._conn.rollback()
            return True
        except Exception:
            try:
                self._reconnect()
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                self._conn.rollback()
                return True
            except Exception:
                return False

    def apply_batch(
        self,
        ownership_forms: Iterable[dict],
        counterparties: Iterable[dict],
    ) -> None:
        """Применяет пакет upsert-ов в ЕДИНОЙ транзакции.

        Формы собственности применяются первыми (FK-зависимость).
        При исключении транзакция откатывается целиком.
        """
        try:
            with self._conn.cursor() as cur:
                for row in ownership_forms:
                    cur.execute(_UPSERT_OWNERSHIP_FORM, row)
                for row in counterparties:
                    cur.execute(_UPSERT_COUNTERPARTY, row)
            self._conn.commit()
        except Exception as exc:
            try:
                self._conn.rollback()
            except Exception:
                pass
            if isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                self._reconnect()
            raise

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
