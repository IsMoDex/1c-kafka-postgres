"""Kafka-продюсер событий интеграции.

Гарантии:
  * ключ сообщения = стабильный id объекта 1С (порядок по ключу, идемпотентность);
  * acks=all + enable.idempotence — надёжная доставка без дублей на брокере;
  * ошибки доставки логируются, flush() дожидается подтверждений.
"""
from __future__ import annotations

from typing import Optional

import structlog
from confluent_kafka import Producer

from integration.models import Event

log = structlog.get_logger()


class EventProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "linger.ms": 20,
                "compression.type": "lz4",
                "client.id": "integration-service",
            }
        )
        self._delivery_errors = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors += 1
            log.error(
                "kafka_delivery_failed",
                topic=msg.topic(),
                key=msg.key().decode("utf-8") if msg.key() else None,
                error=str(err),
            )

    def publish(self, topic: str, event: Event) -> None:
        self._producer.produce(
            topic=topic,
            key=event.key().encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            headers={"event_type": event.event_type, "source": event.source},
            on_delivery=self._on_delivery,
        )
        # обслуживаем очередь доставки, не блокируясь
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Дожидается подтверждения всех сообщений. Возвращает число ошибок доставки."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            log.error("kafka_flush_timeout", remaining=remaining)
        return self._delivery_errors
