"""Регрессии порядка публикации и source-based watermark."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from integration.models import Counterparty, OwnershipForm
from integration.sync import Synchronizer
from integration.sync_state import SyncState

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Set as AbstractSet

    from integration.config import Config
    from integration.models import Event
    from integration.producer import EventProducer
    from integration.sources.base import Source

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
    def __init__(
        self,
        calls: list[str],
        values: dict[str, datetime] | None = None,
        unchanged_ids: AbstractSet[str] | None = None,
    ) -> None:
        self.calls = calls
        self.values = values or {}
        self.unchanged_ids = unchanged_ids or set()
        self.set_calls: list[tuple[str, datetime, bool]] = []

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.calls.append("lock")
        yield

    def get(self, entity: str) -> datetime | None:
        return self.values.get(entity)

    def set(self, entity: str, value: datetime, *, monotonic: bool = True) -> None:
        self.set_calls.append((entity, value, monotonic))

    def changed_ownership_forms(self, records: list[OwnershipForm]) -> list[OwnershipForm]:
        self.calls.append("filter_forms")
        return [record for record in records if record.id not in self.unchanged_ids]

    def changed_counterparties(self, records: list[Counterparty]) -> list[Counterparty]:
        self.calls.append("filter_counterparties")
        return [record for record in records if record.id not in self.unchanged_ids]

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


def test_incremental_filters_unchanged_overlap_records_before_kafka() -> None:
    calls: list[str] = []
    form = OwnershipForm(id="ooo", name="ООО", deleted=False, updated_at=TS_FORM)
    unchanged_cp = Counterparty(
        id="00000000-0000-0000-0000-000000000001",
        name="Без изменений",
        ownership_form_id="ooo",
        deleted=False,
        updated_at=TS_CP,
    )
    changed_cp = unchanged_cp.model_copy(update={"id": "00000000-0000-0000-0000-000000000002", "name": "Изменено"})
    source = FakeSource(calls, [form], [unchanged_cp, changed_cp])
    state = FakeState(
        calls,
        {"ownership_forms": TS_FORM, "counterparties": TS_CP},
        unchanged_ids={form.id, unchanged_cp.id},
    )

    result = _synchronizer(source, FakeProducer(calls), state).run("incremental")

    assert "publish:forms:ooo" not in calls
    assert "publish:counterparties:00000000-0000-0000-0000-000000000001" not in calls
    assert "publish:counterparties:00000000-0000-0000-0000-000000000002" in calls
    assert result["ownership_forms"] == 0
    assert result["counterparties"] == 1
    assert result["ownership_forms_fetched"] == 1
    assert result["counterparties_fetched"] == 2
    assert state.set_calls == [
        ("ownership_forms", TS_FORM, True),
        ("counterparties", TS_CP, True),
    ]


def test_overlap_comparison_publishes_only_new_or_changed_state() -> None:
    newer = datetime(2026, 7, 14, 10, 0, 2, tzinfo=UTC)

    assert SyncState._needs_publish(None, ("name", TS_CP)) is True
    assert SyncState._needs_publish(("name", TS_CP), ("name", TS_CP)) is False
    assert SyncState._needs_publish(("old", TS_CP), ("new", TS_CP)) is True
    assert SyncState._needs_publish(("new", newer), ("old", TS_CP)) is False
    assert SyncState._needs_publish(("same", TS_CP), ("same", newer)) is True
