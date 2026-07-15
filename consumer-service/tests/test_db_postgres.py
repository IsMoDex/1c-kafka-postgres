"""Transactional Database tests against a real PostgreSQL instance."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import psycopg
import pytest

from consumer.db import Database

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.postgres


@pytest.fixture
def dsn() -> str:
    value = os.getenv("TEST_PG_DSN")
    if not value:
        pytest.skip("TEST_PG_DSN is not configured")
    assert value is not None
    return value


@pytest.fixture(autouse=True)
def _clean_database(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("TRUNCATE counterparties, ownership_forms, sync_state")


@pytest.fixture
def database(dsn: str) -> Iterator[Database]:
    value = Database(dsn)
    yield value
    value.close()


def _form(timestamp: datetime, *, name: str = "ООО", deleted: bool = False) -> dict[str, object]:
    return {
        "id": "ooo",
        "code": "001",
        "name": name,
        "source_updated_at": timestamp,
        "deleted": deleted,
    }


def _counterparty(timestamp: datetime, *, name: str = "Ромашка", deleted: bool = False) -> dict[str, object]:
    return {
        "id": str(uuid4()),
        "code": "001",
        "name": name,
        "inn": "7701234567",
        "kpp": "770101001",
        "ownership_form_id": "ooo",
        "source_updated_at": timestamp,
        "deleted": deleted,
    }


def test_upsert_stale_equal_soft_delete_and_restore(database: Database, dsn: str) -> None:
    timestamp = datetime(2026, 7, 15, tzinfo=UTC)
    counterparty = _counterparty(timestamp)
    result = database.apply_batch([_form(timestamp)], [counterparty])
    assert result.total == 2

    stale = {**counterparty, "name": "Stale", "source_updated_at": timestamp - timedelta(seconds=1)}
    assert database.apply_batch([], [stale]).counterparties == 0

    equal = {**counterparty, "name": "Equal timestamp"}
    assert database.apply_batch([], [equal]).counterparties == 1

    deleted = {**counterparty, "deleted": True, "source_updated_at": timestamp + timedelta(seconds=1)}
    restored = {**counterparty, "name": "Restored", "source_updated_at": timestamp + timedelta(seconds=2)}
    assert database.apply_batch([], [deleted]).counterparties == 1
    assert database.apply_batch([], [restored]).counterparties == 1

    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT name, deleted, source_updated_at FROM counterparties WHERE id = %s",
            (counterparty["id"],),
        ).fetchone()
    assert row == ("Restored", False, restored["source_updated_at"])


def test_fk_failure_rolls_back_entire_batch(database: Database, dsn: str) -> None:
    timestamp = datetime(2026, 7, 15, tzinfo=UTC)
    invalid = {**_counterparty(timestamp), "ownership_form_id": "missing"}

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        database.apply_batch([_form(timestamp)], [invalid])

    with psycopg.connect(dsn) as connection:
        assert connection.execute("SELECT count(*) FROM ownership_forms").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM counterparties").fetchone() == (0,)

    assert database.apply_batch([_form(timestamp)], []).ownership_forms == 1


def test_connection_is_recreated_after_operational_error(database: Database, dsn: str) -> None:
    backend_pid = database._conn.info.backend_pid
    with psycopg.connect(dsn, autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))

    with pytest.raises(psycopg.OperationalError):
        database.apply_batch([_form(datetime.now(UTC))], [])

    assert database.apply_batch([_form(datetime.now(UTC))], []).ownership_forms == 1
