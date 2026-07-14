"""Pydantic-модели: доменные записи справочников и событийный конверт Kafka.

Формат события соответствует ТЗ:
    { event_id, event_type, source, occurred_at, payload }
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Доменные записи (payload) ───────────────────────────────────────────────

class OwnershipForm(BaseModel):
    """Форма собственности (справочник 1С)."""
    id: str
    code: Optional[str] = None
    name: str
    deleted: bool = False
    updated_at: Optional[datetime] = None


class Counterparty(BaseModel):
    """Контрагент (справочник 1С)."""
    id: str  # GUID в строковом виде
    code: Optional[str] = None
    name: str
    inn: Optional[str] = None
    kpp: Optional[str] = None
    ownership_form_id: Optional[str] = None
    deleted: bool = False
    updated_at: Optional[datetime] = None


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
    occurred_at: datetime = Field(default_factory=_utcnow)
    payload: dict

    def key(self) -> str:
        """Ключ сообщения Kafka = стабильный id объекта 1С."""
        return str(self.payload["id"])

    def to_json(self) -> str:
        return self.model_dump_json()
