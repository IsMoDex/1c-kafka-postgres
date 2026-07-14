"""Тесты MockSource: seed, changed_since, touch, soft delete."""
from __future__ import annotations

from datetime import datetime, timezone

from integration.sources.mock import MockSource


def _source(tmp_path):
    return MockSource(state_path=str(tmp_path / "state.json"))


def test_seed_counts(tmp_path):
    src = _source(tmp_path)
    assert len(src.fetch_ownership_forms()) == 4
    assert len(src.fetch_counterparties()) == 5


def test_full_ignores_changed_since_none(tmp_path):
    src = _source(tmp_path)
    # без changed_since — все записи
    assert len(src.fetch_counterparties(None)) == 5


def test_changed_since_future_empty(tmp_path):
    src = _source(tmp_path)
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    assert src.fetch_counterparties(future) == []


def test_changed_since_past_all(tmp_path):
    src = _source(tmp_path)
    past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    assert len(src.fetch_counterparties(past)) == 5


def test_touch_updates_and_is_incremental(tmp_path):
    src = _source(tmp_path)
    before = datetime.now(timezone.utc)
    cp_id = "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"
    src.touch_counterparty(cp_id, name="Изменено")
    # инкремент забирает только изменённую запись
    changed = src.fetch_counterparties(before)
    assert len(changed) == 1
    assert changed[0].id == cp_id
    assert changed[0].name == "Изменено"


def test_soft_delete_sets_flag(tmp_path):
    src = _source(tmp_path)
    cp_id = "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005"
    src.soft_delete_counterparty(cp_id)
    cp = next(c for c in src.fetch_counterparties() if c.id == cp_id)
    assert cp.deleted is True
