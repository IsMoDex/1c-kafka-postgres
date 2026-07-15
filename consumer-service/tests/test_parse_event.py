"""Тесты parse_event: маппинг событий в строки БД, обработка ошибок."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from consumer.models import Entity, parse_event


def _event(event_type: str, payload: dict[str, object]) -> bytes:
    return json.dumps(
        {
            "event_id": "00000000-0000-0000-0000-000000000001",
            "event_type": event_type,
            "source": "1c",
            "occurred_at": "2026-07-10T12:00:00Z",
            "payload": payload,
        }
    ).encode("utf-8")


def test_parse_counterparty_upsert() -> None:
    raw = _event(
        "counterparty.upsert",
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001",
            "code": "000001",
            "name": "ООО Ромашка",
            "inn": "7701234567",
            "kpp": "770101001",
            "ownership_form_id": "ooo",
            "deleted": False,
            "updated_at": "2026-07-10T12:00:00Z",
        },
    )
    ev = parse_event(raw)
    assert ev.entity == Entity.COUNTERPARTY
    row = ev.counterparty_row()
    assert row["id"] == "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"
    assert row["name"] == "ООО Ромашка"
    source_updated_at = row["source_updated_at"]
    assert isinstance(source_updated_at, datetime)
    assert source_updated_at.isoformat() == "2026-07-10T12:00:00+00:00"
    assert row["deleted"] is False


def test_parse_ownership_form() -> None:
    raw = _event(
        "ownership_form.upsert",
        {
            "id": "ooo",
            "code": "000000001",
            "name": "ООО",
            "deleted": False,
            "updated_at": "2026-07-10T12:00:00Z",
        },
    )
    ev = parse_event(raw)
    assert ev.entity == Entity.OWNERSHIP_FORM
    row = ev.ownership_form_row()
    assert row["id"] == "ooo"
    assert row["name"] == "ООО"


def test_parse_delete_event() -> None:
    raw = _event(
        "counterparty.delete",
        {
            "id": "00000000-0000-0000-0000-000000000005",
            "code": "000005",
            "name": "ООО Вектор",
            "deleted": True,
            "updated_at": "2026-07-10T12:00:00Z",
        },
    )
    ev = parse_event(raw)
    assert ev.counterparty_row()["deleted"] is True


def test_parse_invalid_json_raises() -> None:
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(b"{not json")


def test_parse_missing_required_field_raises() -> None:
    # нет обязательного event_type
    raw = json.dumps({"event_id": "e", "source": "1c", "occurred_at": "2026-07-10T12:00:00Z", "payload": {}}).encode()
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(raw)


@pytest.mark.parametrize("event_type", ["counterparty.typo", "ownership_form.create"])
def test_unknown_event_type_raises(event_type: str) -> None:
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(
            _event(
                event_type,
                {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": "Invalid",
                    "deleted": False,
                    "updated_at": "2026-07-10T12:00:00Z",
                },
            )
        )


def test_string_false_is_not_accepted_as_boolean() -> None:
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(
            _event(
                "counterparty.upsert",
                {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": "Invalid",
                    "deleted": "false",
                    "updated_at": "2026-07-10T12:00:00Z",
                },
            )
        )


def test_invalid_uuid_and_missing_timestamp_raise() -> None:
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(
            _event(
                "counterparty.upsert",
                {
                    "id": "not-a-uuid",
                    "name": "Invalid",
                    "deleted": False,
                },
            )
        )


@pytest.mark.parametrize("timestamp", [1720958400, "2026-07-10T12:00:00"])
def test_timestamp_must_be_rfc3339_string_with_timezone(timestamp: int | str) -> None:
    raw = json.dumps(
        {
            "event_id": "00000000-0000-0000-0000-000000000001",
            "event_type": "counterparty.upsert",
            "source": "1c",
            "occurred_at": timestamp,
            "payload": {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "Invalid",
                "deleted": False,
                "updated_at": timestamp,
            },
        }
    ).encode()
    with pytest.raises(ValueError, match="Некорректное событие"):
        parse_event(raw)


@pytest.mark.parametrize(
    ("event_type", "deleted"),
    [("counterparty.delete", False), ("counterparty.upsert", True)],
)
def test_event_type_must_match_deleted_flag(event_type: str, deleted: object) -> None:
    with pytest.raises(ValueError, match="не согласован с deleted"):
        parse_event(
            _event(
                event_type,
                {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": "Mismatch",
                    "deleted": deleted,
                    "updated_at": "2026-07-10T12:00:00Z",
                },
            )
        )
