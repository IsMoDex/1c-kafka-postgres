"""Ядро consumer-service: consumer group → батч → транзакционный upsert → commit.

Гарантии и поведение:
  * consumer group, ручной commit offset ТОЛЬКО после успешной записи (at-least-once);
  * батчинг по количеству/времени, весь батч — в одной транзакции PostgreSQL;
  * идемпотентность обеспечивается upsert-ом ON CONFLICT (повтор не создаёт дублей);
  * retry с ограничением попыток на уровне батча (временные сбои БД);
  * poison-сообщения (невалидный JSON, устойчивый FK-конфликт) → DLQ, offset двигается;
  * порядок применения: формы собственности раньше контрагентов (FK).
"""
from __future__ import annotations

import signal
import time
from typing import Optional

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from consumer.config import Config
from consumer.db import Database
from consumer.health import HealthState
from consumer.models import Entity, ParsedEvent, parse_event

log = structlog.get_logger()


class DlqProducer:
    """Отправка «ядовитых» сообщений в dead-letter topic (<topic><suffix>)."""

    def __init__(self, bootstrap_servers: str, suffix: str) -> None:
        self._suffix = suffix
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "client.id": "consumer-dlq",
            }
        )

    def send(
        self, source_topic: str, key: Optional[bytes], value: Optional[bytes], reason: str
    ) -> bool:
        """Отправляет сообщение в DLQ и ДОЖИДАЕТСЯ подтверждения доставки.

        Возвращает True только если брокер подтвердил запись. При ошибке или
        таймауте возвращает False — вызывающий код НЕ должен коммитить offset
        такого сообщения, иначе poison-сообщение будет потеряно.
        """
        dlq_topic = f"{source_topic}{self._suffix}"
        delivered = {"ok": False, "err": None}

        def _on_delivery(err, _msg) -> None:
            if err is None:
                delivered["ok"] = True
            else:
                delivered["err"] = str(err)

        try:
            self._producer.produce(
                topic=dlq_topic,
                key=key,
                value=value,
                headers={"dlq_reason": reason[:512]},
                on_delivery=_on_delivery,
            )
            remaining = self._producer.flush(10)
        except (BufferError, KafkaException) as exc:
            log.error("dlq_produce_failed", topic=dlq_topic, error=str(exc))
            return False

        if remaining > 0 or not delivered["ok"]:
            log.error(
                "dlq_delivery_failed",
                topic=dlq_topic,
                remaining=remaining,
                error=delivered["err"],
            )
            return False

        log.warning("sent_to_dlq", topic=dlq_topic, reason=reason)
        return True


class Worker:
    def __init__(self, config: Config, health: HealthState) -> None:
        self._cfg = config
        self._health = health
        self._running = True

        self._consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.consumer_group,
                "enable.auto.commit": False,          # ручной commit после записи
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "client.id": "consumer-service",
            }
        )
        self._dlq = DlqProducer(config.kafka_bootstrap_servers, config.dlq_suffix)
        self._db = Database(config.pg_dsn)
        self._health.db_ok = self._db.ping()

    def stop(self, *_) -> None:
        log.info("shutdown_signal_received")
        self._running = False

    def run(self) -> None:
        self._consumer.subscribe(self._cfg.topics)
        self._health.ready = True
        self._health.kafka_ok = True
        log.info("consumer_started", topics=self._cfg.topics, group=self._cfg.consumer_group)

        try:
            while self._running:
                batch = self._poll_batch()
                if not batch:
                    continue
                self._process_batch(batch)
        finally:
            self._shutdown()

    # ── сбор батча ────────────────────────────────────────────────────────
    def _poll_batch(self) -> list:
        batch: list = []
        deadline = time.monotonic() + self._cfg.batch_max_seconds
        while self._running and len(batch) < self._cfg.batch_max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=min(remaining, 1.0))
            if msg is None:
                if batch:
                    break
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                # Транзиентные ошибки (топик ещё не прогрузился брокером,
                # ребаланс и т.п.) — логируем и продолжаем, не роняя сервис.
                if err.retriable() or err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    self._health.kafka_ok = False
                    self._health.last_kafka_error = str(err)
                    log.warning("kafka_transient_error", error=str(err), code=err.code())
                    time.sleep(1.0)
                    continue
                self._health.kafka_ok = False
                self._health.last_kafka_error = str(err)
                log.error("kafka_consume_error", error=str(err))
                raise KafkaException(err)
            self._health.kafka_ok = True
            batch.append(msg)
        return batch

    # ── обработка батча ─────────────────────────────────────────────────────
    def _process_batch(self, batch: list) -> None:
        ownership_rows: list[dict] = []
        counterparty_rows: list[dict] = []
        poison: list[tuple] = []  # (msg, reason)

        for msg in batch:
            try:
                event: ParsedEvent = parse_event(msg.value())
                if event.entity == Entity.OWNERSHIP_FORM:
                    ownership_rows.append(event.ownership_form_row())
                else:
                    counterparty_rows.append(event.counterparty_row())
            except Exception as exc:  # noqa: BLE001 — невалидное сообщение → DLQ
                poison.append((msg, f"parse_error: {exc}"))

        # ядовитые сообщения — сразу в DLQ (не блокируют батч).
        # Если DLQ-доставка не удалась, offset такого сообщения НЕ коммитим:
        # исключаем его из батча-для-commit, чтобы не потерять при следующем poll.
        committable = list(batch)
        for msg, reason in poison:
            if self._dlq.send(msg.topic(), msg.key(), msg.value(), reason):
                self._health.messages_dlq += 1
                self._health.last_error = reason
            else:
                # не подтверждено брокером — не коммитим этот offset
                committable.remove(msg)

        if ownership_rows or counterparty_rows:
            self._write_with_retry(committable, ownership_rows, counterparty_rows)
        elif committable:
            # только poison (успешно отправленный в DLQ) — коммитим, чтобы не зациклиться
            self._commit(committable)

    def _write_with_retry(self, batch: list, of_rows: list[dict], cp_rows: list[dict]) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                self._db.apply_batch(of_rows, cp_rows)
                self._health.db_ok = True
                self._health.rows_processed += len(of_rows) + len(cp_rows)
                self._health.messages_processed += len(batch)
                self._commit(batch)
                log.info(
                    "batch_committed",
                    ownership_forms=len(of_rows),
                    counterparties=len(cp_rows),
                    messages=len(batch),
                )
                return
            except Exception as exc:  # noqa: BLE001
                self._health.db_ok = self._db.ping()
                self._health.last_error = str(exc)
                log.error("batch_write_failed", attempt=attempt, error=str(exc))
                if attempt > self._cfg.max_retries:
                    # исчерпали попытки — весь батч в DLQ по соответствующим топикам.
                    # Коммитим только те сообщения, что брокер подтвердил в DLQ.
                    log.error("batch_exhausted_retries", messages=len(batch))
                    committable = []
                    for msg in batch:
                        if self._dlq.send(
                            msg.topic(), msg.key(), msg.value(),
                            f"db_write_failed_after_retries: {exc}",
                        ):
                            self._health.messages_dlq += 1
                            committable.append(msg)
                    if committable:
                        self._commit(committable)
                    return
                time.sleep(min(2 ** attempt, 30))

    def _commit(self, batch: list) -> None:
        """Коммитим максимальный offset+1 по каждой (topic, partition) батча."""
        max_offsets: dict[tuple[str, int], int] = {}
        for msg in batch:
            key = (msg.topic(), msg.partition())
            if msg.offset() > max_offsets.get(key, -1):
                max_offsets[key] = msg.offset()
        tps = [TopicPartition(t, p, off + 1) for (t, p), off in max_offsets.items()]
        self._consumer.commit(offsets=tps, asynchronous=False)

    def _shutdown(self) -> None:
        log.info("consumer_stopping")
        self._health.ready = False
        try:
            self._consumer.close()
        finally:
            self._db.close()


def main() -> None:
    from consumer.logging_setup import configure_logging

    configure_logging("consumer-service")
    cfg = Config.from_env()
    health = HealthState()

    from consumer.health import start_health_server

    start_health_server(cfg.health_port, health)

    worker = Worker(cfg, health)
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)
    worker.run()


if __name__ == "__main__":
    main()
