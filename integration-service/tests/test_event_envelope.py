"""Тесты событийного конверта: ключ и event_type."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from integration.models import Counterparty, OwnershipForm
from integration.sync import _counterparty_event, _ownership_event


def test_counterparty_upsert_event() -> None:
    cp = Counterparty(
        id="abc-1",
        code="000001",
        name="ООО Ромашка",
        inn="7701234567",
        kpp="770101001",
        ownership_form_id="ooo",
        updated_at=datetime.now(UTC),
    )
    ev = _counterparty_event(cp)
    assert ev.event_type == "counterparty.upsert"
    assert ev.source == "1c"
    assert ev.key() == "abc-1"  # ключ = id объекта 1С
    assert ev.payload["name"] == "ООО Ромашка"


def test_counterparty_delete_event() -> None:
    cp = Counterparty(id="abc-9", code="000009", name="X", deleted=True, updated_at=datetime.now(UTC))
    ev = _counterparty_event(cp)
    assert ev.event_type == "counterparty.delete"
    assert ev.payload["deleted"] is True


def test_ownership_form_event_key() -> None:
    form = OwnershipForm(id="ooo", code="000000001", name="ООО", updated_at=datetime.now(UTC))
    ev = _ownership_event(form)
    assert ev.event_type == "ownership_form.upsert"
    assert ev.key() == "ooo"


def test_event_id_unique_per_event() -> None:
    form = OwnershipForm(id="ooo", code="1", name="ООО", updated_at=datetime.now(UTC))
    e1 = _ownership_event(form)
    e2 = _ownership_event(form)
    assert e1.event_id != e2.event_id  # event_id уникален на каждое событие
    assert e1.key() == e2.key()  # но ключ (id объекта) стабилен


@pytest.mark.parametrize("model", [OwnershipForm, Counterparty])
@pytest.mark.parametrize("updated_at", [None, datetime.now(UTC).replace(tzinfo=None)])
def test_payload_rejects_missing_or_naive_updated_at(model: type, updated_at: object) -> None:
    with pytest.raises(ValidationError):
        model(id="id", name="name", updated_at=updated_at)
