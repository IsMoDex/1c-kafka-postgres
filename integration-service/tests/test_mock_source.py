"""Тесты MockSource: seed, changed_since, touch, soft delete."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from integration.sources.mock import MockSource

if TYPE_CHECKING:
    from pathlib import Path


def _source(tmp_path: Path) -> MockSource:
    return MockSource(state_path=str(tmp_path / "state.json"))


def test_seed_counts(tmp_path: Path) -> None:
    src = _source(tmp_path)
    assert len(src.fetch_ownership_forms()) == 4
    assert len(src.fetch_counterparties()) == 5


def test_full_ignores_changed_since_none(tmp_path: Path) -> None:
    src = _source(tmp_path)
    # без changed_since — все записи
    assert len(src.fetch_counterparties(None)) == 5


def test_changed_since_future_empty(tmp_path: Path) -> None:
    src = _source(tmp_path)
    future = datetime(2999, 1, 1, tzinfo=UTC)
    assert src.fetch_counterparties(future) == []


def test_changed_since_past_all(tmp_path: Path) -> None:
    src = _source(tmp_path)
    past = datetime(2000, 1, 1, tzinfo=UTC)
    assert len(src.fetch_counterparties(past)) == 5


def test_touch_updates_and_is_incremental(tmp_path: Path) -> None:
    src = _source(tmp_path)
    before = datetime.now(UTC) - timedelta(seconds=1)
    cp_id = "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"
    src.touch_counterparty(cp_id, name="Изменено")
    # инкремент забирает только изменённую запись
    changed = src.fetch_counterparties(before)
    assert len(changed) == 1
    assert changed[0].id == cp_id
    assert changed[0].name == "Изменено"


def test_soft_delete_sets_flag(tmp_path: Path) -> None:
    src = _source(tmp_path)
    cp_id = "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005"
    src.soft_delete_counterparty(cp_id)
    cp = next(c for c in src.fetch_counterparties() if c.id == cp_id)
    assert cp.deleted is True
