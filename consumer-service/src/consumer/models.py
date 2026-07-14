"""Строгая валидация Kafka-событий и преобразование payload в строки БД."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field, StrictBool, ValidationError


_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def _require_rfc3339(value: Any) -> Any:
    if not isinstance(value, str) or _RFC3339_RE.fullmatch(value) is None:
        raise ValueError("Ожидается RFC3339 datetime с timezone")
    return value


Rfc3339DateTime = Annotated[AwareDatetime, BeforeValidator(_require_rfc3339)]


class Entity(str, Enum):
    OWNERSHIP_FORM = "ownership_form"
    COUNTERPARTY = "counterparty"


EventType = Literal[
    "ownership_form.upsert",
    "ownership_form.delete",
    "counterparty.upsert",
    "counterparty.delete",
]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: EventType
    source: Literal["1c"]
    occurred_at: Rfc3339DateTime
    payload: dict[str, Any]


class OwnershipFormPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    code: str | None = None
    name: str = Field(min_length=1)
    deleted: StrictBool
    updated_at: Rfc3339DateTime


class CounterpartyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    code: str | None = None
    name: str = Field(min_length=1)
    inn: str | None = None
    kpp: str | None = None
    ownership_form_id: str | None = None
    deleted: StrictBool
    updated_at: Rfc3339DateTime


@dataclass(frozen=True)
class ParsedEvent:
    event_id: UUID
    event_type: EventType
    source: Literal["1c"]
    occurred_at: datetime
    payload: OwnershipFormPayload | CounterpartyPayload

    @property
    def entity(self) -> Entity:
        if isinstance(self.payload, OwnershipFormPayload):
            return Entity.OWNERSHIP_FORM
        return Entity.COUNTERPARTY

    def ownership_form_row(self) -> dict[str, Any]:
        if not isinstance(self.payload, OwnershipFormPayload):
            raise ValueError("Payload контрагента нельзя преобразовать в ownership_form")
        p = self.payload
        return {
            "id": str(p.id),
            "code": p.code,
            "name": p.name,
            "source_updated_at": p.updated_at,
            "deleted": p.deleted,
        }

    def counterparty_row(self) -> dict[str, Any]:
        if not isinstance(self.payload, CounterpartyPayload):
            raise ValueError("Payload формы нельзя преобразовать в counterparty")
        p = self.payload
        return {
            "id": str(p.id),
            "code": p.code,
            "name": p.name,
            "inn": p.inn,
            "kpp": p.kpp,
            "ownership_form_id": p.ownership_form_id,
            "source_updated_at": p.updated_at,
            "deleted": p.deleted,
        }


def parse_event(raw: bytes) -> ParsedEvent:
    """Разбирает JSON-сообщение и строго проверяет envelope и entity payload."""
    try:
        envelope = EventEnvelope.model_validate_json(raw)
        payload: OwnershipFormPayload | CounterpartyPayload
        if envelope.event_type.startswith("ownership_form."):
            payload = OwnershipFormPayload.model_validate(envelope.payload)
        else:
            payload = CounterpartyPayload.model_validate(envelope.payload)
        expects_deleted = envelope.event_type.endswith(".delete")
        if payload.deleted is not expects_deleted:
            raise ValueError(
                f"event_type={envelope.event_type} не согласован с deleted={payload.deleted}"
            )
        return ParsedEvent(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            source=envelope.source,
            occurred_at=envelope.occurred_at,
            payload=payload,
        )
    except ValidationError as exc:
        raise ValueError(f"Некорректное событие: {exc}") from exc
