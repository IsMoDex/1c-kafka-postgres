"""Регрессии порядка публикации и source-based watermark."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from integration.models import Counterparty, OwnershipForm
from integration.sync import Synchronizer


TS_FORM = datetime(2026, 7, 14, 10, 0, 0, tzinfo=timezone.utc)
TS_CP = datetime(2026, 7, 14, 10, 0, 1, tzinfo=timezone.utc)


class FakeSource:
    def __init__(self, calls, forms=None, counterparties=None):
        self.calls = calls
        self.forms = forms or []
        self.counterparties = counterparties or []
        self.since = []

    def fetch_ownership_forms(self, since):
        self.calls.append("fetch_forms")
        self.since.append(since)
        return self.forms

    def fetch_counterparties(self, since):
        self.calls.append("fetch_counterparties")
        self.since.append(since)
        return self.counterparties


class FakeProducer:
    def __init__(self, calls):
        self.calls = calls

    def publish(self, topic, event):
        self.calls.append(f"publish:{topic}:{event.key()}")

    def flush(self):
        self.calls.append("flush")
        return 0


class FakeState:
    def __init__(self, calls, values=None):
        self.calls = calls
        self.values = values or {}
        self.set_calls = []

    @contextmanager
    def lock(self):
        self.calls.append("lock")
        yield

    def get(self, entity):
        return self.values.get(entity)

    def set(self, entity, value, *, monotonic=True):
        self.set_calls.append((entity, value, monotonic))

    def wait_for_ownership_forms(self, ids, timeout):
        self.calls.append(f"barrier:{','.join(sorted(ids))}")


def _synchronizer(source, producer, state):
    sync = object.__new__(Synchronizer)
    sync._cfg = SimpleNamespace(
        topic_ownership_forms="forms",
        topic_counterparties="counterparties",
        fk_barrier_timeout=30,
    )
    sync._source = source
    sync._producer = producer
    sync._state = state
    return sync


def test_forms_are_flushed_and_applied_before_counterparties_are_published():
    calls = []
    form = OwnershipForm(id="ooo", name="ООО", deleted=False, updated_at=TS_FORM)
    cp = Counterparty(
        id="00000000-0000-0000-0000-000000000001",
        name="ООО Ромашка",
        ownership_form_id="ooo",
        deleted=False,
        updated_at=TS_CP,
    )
    source = FakeSource(calls, [form], [cp])
    producer = FakeProducer(calls)
    state = FakeState(calls)

    _synchronizer(source, producer, state).run("full")

    assert calls.index("flush") < calls.index("fetch_counterparties")
    assert calls.index("barrier:ooo") < calls.index(
        "publish:counterparties:00000000-0000-0000-0000-000000000001"
    )
    assert state.set_calls == [
        ("ownership_forms", TS_FORM, False),
        ("counterparties", TS_CP, False),
    ]


def test_incremental_uses_one_second_overlap():
    calls = []
    state = FakeState(calls, {
        "ownership_forms": TS_FORM,
        "counterparties": TS_CP,
    })
    source = FakeSource(calls)

    _synchronizer(source, FakeProducer(calls), state).run("incremental")

    assert source.since[0].timestamp() == TS_FORM.timestamp() - 1
    assert source.since[1].timestamp() == TS_CP.timestamp() - 1
    assert state.set_calls == []
