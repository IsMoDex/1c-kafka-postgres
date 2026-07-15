from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock

from consumer.health import HealthState
from consumer.worker import Worker

if TYPE_CHECKING:
    import pytest


def test_readiness_requires_fresh_db_and_kafka_heartbeats(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1000.0
    monkeypatch.setattr("consumer.health.time.time", lambda: now)
    state = HealthState()
    state.set_running()
    state.mark_db_ok()
    state.mark_kafka_ok()

    payload, ready = state.status()
    assert ready is True
    assert payload["ready"] is True

    now = 1031.0
    payload, ready = state.status()
    assert ready is False
    assert payload["kafka_ok"] is False
    assert state.liveness() is True


def test_public_status_does_not_expose_error_text() -> None:
    state = HealthState()
    state.record_error("postgresql://user:secret@db/internal")
    state.mark_kafka_failed("message payload with PII")

    payload, _ = state.status()
    assert "secret" not in str(payload)
    assert "PII" not in str(payload)


def test_kafka_probe_controls_health_instead_of_idle_poll() -> None:
    worker = object.__new__(Worker)
    worker._health = HealthState()
    worker._consumer = Mock()
    worker._probe_kafka()
    assert worker._health.kafka_ok is True

    worker._consumer.list_topics.side_effect = RuntimeError("broker unavailable")
    worker._probe_kafka()
    assert worker._health.kafka_ok is False
    assert worker._health.last_kafka_error == "broker unavailable"
