"""Тесты MockSource: seed, changed_since, touch, soft delete."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from integration.sources.mock import MockSource

if TYPE_CHECKING:
    from pathlib import Path


def _source(tmp_path: Path) -> MockSource:
    return MockSource(state_path=str(tmp_path / "state.json"))


def test_seed_counts(tmp_path: Path) -> None:
    src = _source(tmp_path)
    assert len(src.fetch_ownership_forms()) == 4
    assert len(src.fetch_counterparties()) == 5


def test_seed_creates_missing_parent_directory(tmp_path: Path) -> None:
    state_path = tmp_path / "nested" / "state.json"
    src = MockSource(state_path=str(state_path))

    assert state_path.exists()
    assert len(src.fetch_ownership_forms()) == 4


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


def test_unknown_id_raises_without_changing_state(tmp_path: Path) -> None:
    src = _source(tmp_path)
    state_path = tmp_path / "state.json"
    before = state_path.read_bytes()

    with pytest.raises(LookupError, match="missing"):
        src.touch_counterparty("missing", name="No-op")

    assert state_path.read_bytes() == before


def test_atomic_save_leaves_no_temporary_file(tmp_path: Path) -> None:
    src = _source(tmp_path)
    src.touch_counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001", name="Atomic")

    assert list(tmp_path.glob("*.tmp")) == []


def test_two_sources_do_not_overwrite_sequential_updates(tmp_path: Path) -> None:
    first = _source(tmp_path)
    second = _source(tmp_path)
    first.touch_counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001", name="First")
    second.touch_counterparty("b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0002", name="Second")

    rows = {row.id: row.name for row in first.fetch_counterparties()}
    assert rows["b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"] == "First"
    assert rows["b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0002"] == "Second"
