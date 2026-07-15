"""Тесты событийного конверта: ключ и event_type."""

from __future__ import annotations

from integration.models import Counterparty, OwnershipForm
from integration.sync import _counterparty_event, _ownership_event


def test_counterparty_upsert_event() -> None:
    cp = Counterparty(
        id="abc-1", code="000001", name="ООО Ромашка", inn="7701234567", kpp="770101001", ownership_form_id="ooo"
    )
    ev = _counterparty_event(cp)
    assert ev.event_type == "counterparty.upsert"
    assert ev.source == "1c"
    assert ev.key() == "abc-1"  # ключ = id объекта 1С
    assert ev.payload["name"] == "ООО Ромашка"


def test_counterparty_delete_event() -> None:
    cp = Counterparty(id="abc-9", code="000009", name="X", deleted=True)
    ev = _counterparty_event(cp)
    assert ev.event_type == "counterparty.delete"
    assert ev.payload["deleted"] is True


def test_ownership_form_event_key() -> None:
    form = OwnershipForm(id="ooo", code="000000001", name="ООО")
    ev = _ownership_event(form)
    assert ev.event_type == "ownership_form.upsert"
    assert ev.key() == "ooo"


def test_event_id_unique_per_event() -> None:
    form = OwnershipForm(id="ooo", code="1", name="ООО")
    e1 = _ownership_event(form)
    e2 = _ownership_event(form)
    assert e1.event_id != e2.event_id  # event_id уникален на каждое событие
    assert e1.key() == e2.key()  # но ключ (id объекта) стабилен
