"""Регрессии безопасности offset commit при сбое доставки в DLQ."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock

from consumer.health import HealthState
from consumer.worker import Worker


class FakeMessage:
    """Минимальный Kafka Message для unit-теста worker."""

    def __init__(self, offset: int, value: bytes) -> None:
        self._offset = offset
        self._value = value

    def topic(self) -> str:
        return "1c.counterparties.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return f"k-{self._offset}".encode()

    def value(self) -> bytes:
        return self._value


def _valid_event() -> bytes:
    return json.dumps(
        {
            "event_id": "e-11",
            "event_type": "counterparty.upsert",
            "source": "1c",
            "occurred_at": "2026-07-14T00:00:00Z",
            "payload": {"id": "00000000-0000-0000-0000-000000000011", "name": "Valid"},
        }
    ).encode()


def test_failed_dlq_does_not_commit_later_offset_same_partition() -> None:
    """Offset 11 не должен коммититься поверх failed poison offset 10."""
    worker: Any = object.__new__(Worker)
    worker._health = HealthState()
    worker._dlq = Mock()
    worker._dlq.send.return_value = False
    worker._write_with_retry = Mock()
    worker._commit = Mock()

    poison = FakeMessage(10, b"{not-json")
    valid = FakeMessage(11, _valid_event())

    worker._process_batch([poison, valid])

    worker._dlq.send.assert_called_once()
    worker._write_with_retry.assert_not_called()
    worker._commit.assert_not_called()
    assert worker._health.last_error == "DLQ delivery failed; batch will be retried"
