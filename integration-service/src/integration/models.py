"""
Pydantic-модели: доменные записи справочников и событийный конверт Kafka.

Формат события соответствует ТЗ:
    { event_id, event_type, source, occurred_at, payload }
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


# ── Доменные записи (payload) ───────────────────────────────────────────────


class OwnershipForm(BaseModel):
    """Форма собственности (справочник 1С)."""

    id: str
    code: str | None = None
    name: str
    deleted: bool = False
    updated_at: AwareDatetime


class Counterparty(BaseModel):
    """Контрагент (справочник 1С)."""

    id: str  # GUID в строковом виде
    code: str | None = None
    name: str
    inn: str | None = None
    kpp: str | None = None
    ownership_form_id: str | None = None
    deleted: bool = False
    updated_at: AwareDatetime


# ── Событийный конверт (envelope) ────────────────────────────────────────────

EventType = Literal[
    "ownership_form.upsert",
    "ownership_form.delete",
    "counterparty.upsert",
    "counterparty.delete",
]


class Event(BaseModel):
    """Событие Kafka. Стабильный конверт вокруг payload справочника."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    source: str = "1c"
    occurred_at: AwareDatetime = Field(default_factory=_utcnow)
    payload: dict

    def key(self) -> str:
        """Ключ сообщения Kafka = стабильный id объекта 1С."""
        return str(self.payload["id"])

    def to_json(self) -> str:
        return self.model_dump_json()
