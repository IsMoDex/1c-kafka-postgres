"""Offset, DLQ, retry, and shutdown regressions."""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from confluent_kafka import TopicPartition
from psycopg import OperationalError
from psycopg.errors import ForeignKeyViolation

from consumer.db import ApplyResult
from consumer.health import HealthState
from consumer.worker import Worker


class FakeMessage:
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
            "event_id": "00000000-0000-0000-0000-000000000011",
            "event_type": "counterparty.upsert",
            "source": "1c",
            "occurred_at": "2026-07-14T00:00:00Z",
            "payload": {
                "id": "00000000-0000-0000-0000-000000000011",
                "name": "Valid",
                "deleted": False,
                "updated_at": "2026-07-14T00:00:00Z",
            },
        }
    ).encode()


def _worker() -> Any:
    worker: Any = object.__new__(Worker)
    worker._cfg = SimpleNamespace(max_retries=0)
    worker._health = HealthState()
    worker._health.set_running()
    worker._stop_event = threading.Event()
    worker._db = Mock()
    worker._dlq = Mock()
    worker._consumer = Mock()
    return worker


def test_failed_dlq_does_not_commit_later_offset_same_partition() -> None:
    worker = _worker()
    worker._db.apply_batch.return_value = ApplyResult(0, 1)
    worker._dlq.send.return_value = False

    poison = FakeMessage(10, b"{not-json")
    valid = FakeMessage(11, _valid_event())
    worker._process_batch([poison, valid])

    worker._db.apply_batch.assert_called_once()
    worker._dlq.send.assert_called_once()
    worker._consumer.commit.assert_not_called()
    rewind = worker._consumer.seek.call_args.args[0]
    assert isinstance(rewind, TopicPartition)
    assert (rewind.topic, rewind.partition, rewind.offset) == ("1c.counterparties.v1", 0, 10)
    assert worker._health.last_error == "DLQ delivery failed; batch will be retried"


def test_transient_db_exhaustion_rewinds_without_dlq_or_commit() -> None:
    worker = _worker()
    worker._db.apply_batch.side_effect = OperationalError("db unavailable")
    message = FakeMessage(20, _valid_event())

    with pytest.raises(OperationalError):
        worker._apply_with_retry([message], [], [{"id": "unused"}])

    worker._dlq.send.assert_not_called()
    worker._consumer.commit.assert_not_called()
    rewind = worker._consumer.seek.call_args.args[0]
    assert (rewind.topic, rewind.partition, rewind.offset) == ("1c.counterparties.v1", 0, 20)


def test_kafka_commit_failure_rewinds_already_written_batch() -> None:
    worker = _worker()
    worker._db.apply_batch.return_value = ApplyResult(0, 1)
    worker._consumer.commit.side_effect = RuntimeError("commit failed")
    message = FakeMessage(30, _valid_event())

    worker._process_batch([message])

    worker._db.apply_batch.assert_called_once()
    worker._consumer.seek.assert_called_once()
    assert worker._health.messages_processed == 0
    assert worker._health.kafka_ok is False


def test_stop_interrupts_db_retry_backoff() -> None:
    worker = _worker()
    worker._cfg = SimpleNamespace(max_retries=3)
    message = FakeMessage(40, _valid_event())

    def fail_and_stop(*_args: object) -> None:
        worker._stop_event.set()
        message = "db unavailable"
        raise OperationalError(message)

    worker._db.apply_batch.side_effect = fail_and_stop
    assert worker._apply_with_retry([message], [], [{"id": "unused"}]) is None
    worker._consumer.seek.assert_called_once()
    worker._dlq.send.assert_not_called()


def test_shutdown_closes_consumer_dlq_and_database() -> None:
    worker = _worker()
    worker._shutdown()

    worker._consumer.close.assert_called_once()
    worker._dlq.close.assert_called_once()
    worker._db.close.assert_called_once()


def test_constraint_poison_isolated_without_blocking_valid_message() -> None:
    worker = _worker()
    constraint = ForeignKeyViolation("constraint failed")
    worker._db.apply_batch.side_effect = [
        constraint,
        ApplyResult(0, 1),
        constraint,
    ]
    worker._dlq.send.return_value = True
    valid = FakeMessage(50, _valid_event())
    poison = FakeMessage(51, _valid_event())

    worker._process_batch([valid, poison])

    assert worker._db.apply_batch.call_count == 3
    worker._dlq.send.assert_called_once()
    worker._consumer.commit.assert_called_once()
    worker._consumer.seek.assert_not_called()
    payload, _ = worker._health.status()
    assert payload["messages_processed"] == 1
    assert payload["messages_dlq"] == 1


def test_foreign_key_race_is_retried_before_dlq() -> None:
    worker = _worker()
    worker._cfg = SimpleNamespace(max_retries=1)
    worker._stop_event.wait = Mock(return_value=False)
    constraint = ForeignKeyViolation("form has not arrived yet")
    worker._db.apply_batch.side_effect = [constraint, ApplyResult(0, 1)]
    message = FakeMessage(60, _valid_event())

    assert worker._apply_with_retry([message], [], [{"id": "unused"}]) == 1
    worker._consumer.seek.assert_not_called()
    payload, _ = worker._health.status()
    assert payload["db_retries"] == 1
