"""Тесты parse_event: маппинг событий в строки БД, обработка ошибок."""
from __future__ import annotations

import json

import pytest

from consumer.models import Entity, parse_event


def _event(event_type, payload):
    return json.dumps({
        "event_id": "e1", "event_type": event_type, "source": "1c",
        "occurred_at": "2026-07-10T12:00:00Z", "payload": payload,
    }).encode("utf-8")


def test_parse_counterparty_upsert():
    raw = _event("counterparty.upsert", {
        "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001", "code": "000001",
        "name": "ООО Ромашка", "inn": "7701234567", "kpp": "770101001",
        "ownership_form_id": "ooo", "deleted": False, "updated_at": "2026-07-10T12:00:00Z",
    })
    ev = parse_event(raw)
    assert ev.entity == Entity.COUNTERPARTY
    row = ev.counterparty_row()
    assert row["id"] == "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"
    assert row["name"] == "ООО Ромашка"
    assert row["source_updated_at"] == "2026-07-10T12:00:00Z"
    assert row["deleted"] is False


def test_parse_ownership_form():
    raw = _event("ownership_form.upsert", {
        "id": "ooo", "code": "000000001", "name": "ООО",
        "deleted": False, "updated_at": "2026-07-10T12:00:00Z",
    })
    ev = parse_event(raw)
    assert ev.entity == Entity.OWNERSHIP_FORM
    row = ev.ownership_form_row()
    assert row["id"] == "ooo"
    assert row["name"] == "ООО"


def test_parse_delete_event():
    raw = _event("counterparty.delete", {
        "id": "x", "code": "000005", "name": "ООО Вектор", "deleted": True,
        "updated_at": "2026-07-10T12:00:00Z",
    })
    ev = parse_event(raw)
    assert ev.counterparty_row()["deleted"] is True


def test_parse_invalid_json_raises():
    with pytest.raises(ValueError):
        parse_event(b"{not json")


def test_parse_missing_required_field_raises():
    # нет обязательного event_type
    raw = json.dumps({"event_id": "e", "source": "1c",
                      "occurred_at": "2026-07-10T12:00:00Z", "payload": {}}).encode()
    with pytest.raises(ValueError):
        parse_event(raw)
