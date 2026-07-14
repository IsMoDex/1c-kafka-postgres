"""Health должен отражать свежесть реальных проверок зависимостей."""
from __future__ import annotations

import time
from unittest.mock import Mock

from consumer.health import HealthState
from consumer.worker import Worker


def test_health_requires_fresh_db_and_kafka_heartbeats():
    state = HealthState()
    state.ready = True
    state.mark_db_ok()
    state.mark_kafka_ok()

    assert state.healthy() is True

    state.last_kafka_ok_at = time.time() - 31
    assert state.healthy() is False


def test_kafka_probe_controls_health_instead_of_idle_poll():
    worker = object.__new__(Worker)
    worker._health = HealthState()
    worker._consumer = Mock()
    worker._probe_kafka()
    assert worker._health.kafka_ok is True

    worker._consumer.list_topics.side_effect = RuntimeError("broker unavailable")
    worker._probe_kafka()
    assert worker._health.kafka_ok is False
    assert worker._health.last_kafka_error == "broker unavailable"
