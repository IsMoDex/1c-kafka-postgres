"""Разбор событий Kafka и преобразование payload в строки для upsert.

Событие валидируется по конверту { event_id, event_type, source, occurred_at,
payload }. Тип сущности определяется по event_type.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ValidationError


class Entity(str, Enum):
    OWNERSHIP_FORM = "ownership_form"
    COUNTERPARTY = "counterparty"


class ParsedEvent(BaseModel):
    event_id: str
    event_type: str
    source: str
    occurred_at: str
    payload: dict[str, Any]

    @property
    def entity(self) -> Entity:
        if self.event_type.startswith("ownership_form"):
            return Entity.OWNERSHIP_FORM
        if self.event_type.startswith("counterparty"):
            return Entity.COUNTERPARTY
        raise ValueError(f"Неизвестный event_type: {self.event_type!r}")

    def ownership_form_row(self) -> dict[str, Any]:
        p = self.payload
        return {
            "id": p["id"],
            "code": p.get("code"),
            "name": p["name"],
            "source_updated_at": p.get("updated_at"),
            "deleted": bool(p.get("deleted", False)),
        }

    def counterparty_row(self) -> dict[str, Any]:
        p = self.payload
        return {
            "id": p["id"],
            "code": p.get("code"),
            "name": p["name"],
            "inn": p.get("inn"),
            "kpp": p.get("kpp"),
            "ownership_form_id": p.get("ownership_form_id"),
            "source_updated_at": p.get("updated_at"),
            "deleted": bool(p.get("deleted", False)),
        }


def parse_event(raw: bytes) -> ParsedEvent:
    """Разбирает JSON-сообщение Kafka. Бросает ValueError при некорректных данных."""
    try:
        return ParsedEvent.model_validate_json(raw)
    except ValidationError as exc:
        raise ValueError(f"Некорректное событие: {exc}") from exc
