"""Регрессии порядка публикации и source-based watermark."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from integration.models import Counterparty, OwnershipForm
from integration.sync import Synchronizer

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Set as AbstractSet

    from integration.config import Config
    from integration.models import Event
    from integration.producer import EventProducer
    from integration.sources.base import Source
    from integration.sync_state import SyncState

TS_FORM = datetime(2026, 7, 14, 10, 0, 0, tzinfo=UTC)
TS_CP = datetime(2026, 7, 14, 10, 0, 1, tzinfo=UTC)


class FakeSource:
    def __init__(
        self,
        calls: list[str],
        forms: list[OwnershipForm] | None = None,
        counterparties: list[Counterparty] | None = None,
    ) -> None:
        self.calls = calls
        self.forms = forms or []
        self.counterparties = counterparties or []
        self.since: list[datetime | None] = []

    def fetch_ownership_forms(self, since: datetime | None) -> list[OwnershipForm]:
        self.calls.append("fetch_forms")
        self.since.append(since)
        return self.forms

    def fetch_counterparties(self, since: datetime | None) -> list[Counterparty]:
        self.calls.append("fetch_counterparties")
        self.since.append(since)
        return self.counterparties


class FakeProducer:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def publish(self, topic: str, event: Event) -> None:
        self.calls.append(f"publish:{topic}:{event.key()}")

    def flush(self) -> int:
        self.calls.append("flush")
        return 0


class FakeState:
    def __init__(self, calls: list[str], values: dict[str, datetime] | None = None) -> None:
        self.calls = calls
        self.values = values or {}
        self.set_calls: list[tuple[str, datetime, bool]] = []

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.calls.append("lock")
        yield

    def get(self, entity: str) -> datetime | None:
        return self.values.get(entity)

    def set(self, entity: str, value: datetime, *, monotonic: bool = True) -> None:
        self.set_calls.append((entity, value, monotonic))

    def wait_for_ownership_forms(self, ids: AbstractSet[str], _timeout: float) -> None:
        self.calls.append(f"barrier:{','.join(sorted(ids))}")


def _synchronizer(source: FakeSource, producer: FakeProducer, state: FakeState) -> Synchronizer:
    sync = object.__new__(Synchronizer)
    sync._cfg = cast(
        "Config",
        SimpleNamespace(
            topic_ownership_forms="forms",
            topic_counterparties="counterparties",
            fk_barrier_timeout=30,
        ),
    )
    sync._source = cast("Source", source)
    sync._producer = cast("EventProducer", producer)
    sync._state = cast("SyncState", state)
    return sync


def test_forms_are_flushed_and_applied_before_counterparties_are_published() -> None:
    calls: list[str] = []
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
    assert calls.index("barrier:ooo") < calls.index("publish:counterparties:00000000-0000-0000-0000-000000000001")
    assert state.set_calls == [
        ("ownership_forms", TS_FORM, False),
        ("counterparties", TS_CP, False),
    ]


def test_incremental_uses_one_second_overlap() -> None:
    calls: list[str] = []
    state = FakeState(
        calls,
        {
            "ownership_forms": TS_FORM,
            "counterparties": TS_CP,
        },
    )
    source = FakeSource(calls)

    _synchronizer(source, FakeProducer(calls), state).run("incremental")

    form_since, cp_since = source.since
    assert form_since is not None
    assert cp_since is not None
    assert form_since.timestamp() == TS_FORM.timestamp() - 1
    assert cp_since.timestamp() == TS_CP.timestamp() - 1
    assert state.set_calls == []
