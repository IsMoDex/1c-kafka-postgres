"""
Ядро consumer-service: consumer group → батч → транзакционный upsert → commit.

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
import threading
import time
from typing import TYPE_CHECKING, cast

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from psycopg import InterfaceError, OperationalError
from psycopg.errors import CheckViolation, ForeignKeyViolation, NotNullViolation, UniqueViolation

from consumer.config import Config
from consumer.db import Database
from consumer.health import HealthState, start_health_server
from consumer.logging_setup import configure_logging
from consumer.models import Entity, ParsedEvent, parse_event

if TYPE_CHECKING:
    from types import FrameType

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

    def send(self, source_topic: str, key: bytes | None, value: bytes | None, reason: str) -> bool:
        """
        Отправляет сообщение в DLQ и ДОЖИДАЕТСЯ подтверждения доставки.

        Возвращает True только если брокер подтвердил запись. При ошибке или
        таймауте возвращает False — вызывающий код НЕ должен коммитить offset
        такого сообщения, иначе poison-сообщение будет потеряно.
        """
        dlq_topic = f"{source_topic}{self._suffix}"
        delivered = False
        delivery_error: str | None = None

        def _on_delivery(err: KafkaError | None, _msg: Message) -> None:
            nonlocal delivered, delivery_error
            if err is None:
                delivered = True
            else:
                delivery_error = str(err)

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
            log.exception("dlq_produce_failed", topic=dlq_topic, error=str(exc))
            return False

        if remaining > 0 or not delivered:
            log.error(
                "dlq_delivery_failed",
                topic=dlq_topic,
                remaining=remaining,
                error=delivery_error,
            )
            return False

        log.warning("sent_to_dlq", topic=dlq_topic, reason=reason)
        return True

    def close(self, timeout: float = 5.0) -> bool:
        """Flush queued messages before shutdown."""
        remaining = self._producer.flush(timeout)
        if remaining:
            log.error("dlq_close_incomplete", remaining=remaining)
            return False
        return True


class Worker:
    def __init__(self, config: Config, health: HealthState) -> None:
        self._cfg = config
        self._health = health
        self._stop_event = threading.Event()

        self._consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.consumer_group,
                "enable.auto.commit": False,  # ручной commit после записи
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "client.id": "consumer-service",
            }
        )
        self._dlq = DlqProducer(config.kafka_bootstrap_servers, config.dlq_suffix)
        self._db = Database(config.pg_dsn)
        if self._db.ping():
            self._health.mark_db_ok()

    def stop(self, _signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown_signal_received")
        self._health.set_stopping()
        self._stop_event.set()

    def run(self) -> None:
        self._consumer.subscribe(self._cfg.topics)
        self._health.set_running()
        log.info("consumer_started", topics=self._cfg.topics, group=self._cfg.consumer_group)

        next_db_ping = time.monotonic()
        next_kafka_probe = time.monotonic()
        try:
            while not self._stop_event.is_set():
                if time.monotonic() >= next_db_ping:
                    if self._db.ping():
                        self._health.mark_db_ok()
                    else:
                        self._health.mark_db_failed("PostgreSQL health probe failed")
                    next_db_ping = time.monotonic() + 10
                if time.monotonic() >= next_kafka_probe:
                    self._probe_kafka()
                    next_kafka_probe = time.monotonic() + 10
                batch = self._poll_batch()
                if not batch:
                    continue
                if self._stop_event.is_set():
                    self._rewind(batch)
                    break
                self._process_batch(batch)
        finally:
            self._shutdown()

    # ── сбор батча ────────────────────────────────────────────────────────
    def _poll_batch(self) -> list[Message]:
        batch: list[Message] = []
        deadline = time.monotonic() + self._cfg.batch_max_seconds
        while not self._stop_event.is_set() and len(batch) < self._cfg.batch_max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=min(remaining, 1.0))
            if msg is None:
                if batch:
                    break
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001 -- documented confluent-kafka EOF code.
                    continue
                # Транзиентные ошибки (топик ещё не прогрузился брокером,
                # ребаланс и т.п.) — логируем и продолжаем, не роняя сервис.
                if err.retriable() or err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    self._health.mark_kafka_failed(str(err))
                    log.warning("kafka_transient_error", error=str(err), code=err.code())
                    self._stop_event.wait(1.0)
                    continue
                self._health.mark_kafka_failed(str(err))
                log.error("kafka_consume_error", error=str(err))
                raise KafkaException(err)
            self._health.mark_kafka_ok()
            batch.append(msg)
        return batch

    def _probe_kafka(self) -> None:
        """Проверяет broker запросом metadata, а не фактом локального poll timeout."""
        try:
            self._consumer.list_topics(timeout=2)
            self._health.mark_kafka_ok()
        except Exception as exc:  # noqa: BLE001 -- probe failures of any origin must only affect health state.
            self._health.mark_kafka_failed(str(exc))
            log.warning("kafka_probe_failed", error=str(exc))

    # ── обработка батча ─────────────────────────────────────────────────────
    def _process_batch(self, batch: list[Message]) -> None:  # noqa: C901, PLR0912 -- one offset decision boundary.
        ownership_rows: list[dict[str, object]] = []
        counterparty_rows: list[dict[str, object]] = []
        valid_events: list[tuple[Message, ParsedEvent]] = []
        poison: list[tuple[Message, str]] = []

        for msg in batch:
            try:
                event: ParsedEvent = parse_event(cast("bytes", msg.value()))
                valid_events.append((msg, event))
                if event.entity == Entity.OWNERSHIP_FORM:
                    ownership_rows.append(event.ownership_form_row())
                else:
                    counterparty_rows.append(event.counterparty_row())
            except Exception as exc:  # noqa: BLE001 -- any malformed message must be isolated in DLQ.
                poison.append((msg, f"parse_error: {exc}"))

        applied_rows = 0
        if ownership_rows or counterparty_rows:
            try:
                result = self._apply_with_retry(
                    batch,
                    ownership_rows,
                    counterparty_rows,
                    rewind_on_data_error=False,
                )
            except (CheckViolation, ForeignKeyViolation, NotNullViolation, UniqueViolation):
                fallback = self._apply_individually(valid_events)
                if fallback is None:
                    return
                result, db_poison = fallback
                poison.extend(db_poison)
            if result is None:
                return
            applied_rows = result

        # Apply valid rows before DLQ delivery. If DLQ fails, replaying the DB
        # writes is safe because upserts are idempotent, while sending poison
        # before a transient DB failure would duplicate DLQ records on restart.
        dlq_failed = False
        for msg, reason in poison:
            if self._stop_event.is_set():
                self._rewind(batch)
                return
            if self._dlq.send(cast("str", msg.topic()), msg.key(), msg.value(), reason):
                self._health.record_dlq(reason)
            else:
                dlq_failed = True

        if dlq_failed:
            self._health.record_error("DLQ delivery failed; batch will be retried")
            log.error("batch_not_committed_due_to_dlq_failure", messages=len(batch))
            self._rewind(batch)
            return

        try:
            self._commit(batch)
            self._health.mark_kafka_ok()
        except Exception as exc:
            self._health.mark_kafka_failed(str(exc))
            log.exception("kafka_commit_failed", error=str(exc), messages=len(batch))
            self._rewind(batch)
            return

        self._health.record_batch(
            rows=applied_rows,
            messages=len(batch) - len(poison),
        )
        log.info(
            "batch_committed",
            ownership_forms=len(ownership_rows),
            counterparties=len(counterparty_rows),
            messages=len(batch),
            poison=len(poison),
        )

    def _apply_individually(
        self,
        valid_events: list[tuple[Message, ParsedEvent]],
    ) -> tuple[int, list[tuple[Message, str]]] | None:
        """Isolate permanent constraint errors after an atomic batch failed."""
        applied_rows = 0
        poison: list[tuple[Message, str]] = []
        ordered = sorted(valid_events, key=lambda item: item[1].entity != Entity.OWNERSHIP_FORM)
        for msg, event in ordered:
            if event.entity == Entity.OWNERSHIP_FORM:
                ownership_rows = [event.ownership_form_row()]
                counterparty_rows: list[dict[str, object]] = []
            else:
                ownership_rows = []
                counterparty_rows = [event.counterparty_row()]
            try:
                result = self._apply_with_retry(
                    [msg],
                    ownership_rows,
                    counterparty_rows,
                    rewind_on_data_error=False,
                )
            except (CheckViolation, ForeignKeyViolation, NotNullViolation, UniqueViolation) as exc:
                poison.append((msg, f"db_constraint_error: {exc}"))
                continue
            if result is None:
                return None
            applied_rows += result
        return applied_rows, poison

    def _apply_with_retry(  # noqa: C901 -- error classes intentionally have distinct delivery semantics.
        self,
        batch: list[Message],
        of_rows: list[dict[str, object]],
        cp_rows: list[dict[str, object]],
        *,
        rewind_on_data_error: bool = True,
    ) -> int | None:
        retries = 0
        while not self._stop_event.is_set():
            try:
                result = self._db.apply_batch(of_rows, cp_rows)
                self._health.mark_db_ok()
            except (OperationalError, InterfaceError) as exc:
                self._health.mark_db_failed(str(exc))
                log.exception("batch_write_transient_error", retry=retries, error=str(exc))
                if retries >= self._cfg.max_retries:
                    self._rewind(batch)
                    log.exception("batch_write_retries_exhausted", messages=len(batch))
                    raise
                retries += 1
                self._health.record_db_retry()
                if self._stop_event.wait(min(2**retries, 30)):
                    self._rewind(batch)
                    return None
            except ForeignKeyViolation as exc:
                log.warning("batch_write_fk_retry", retry=retries, error=str(exc))
                if retries >= self._cfg.max_retries:
                    if rewind_on_data_error:
                        self._rewind(batch)
                    raise
                retries += 1
                self._health.record_db_retry()
                if self._stop_event.wait(min(2**retries, 30)):
                    self._rewind(batch)
                    return None
            except (CheckViolation, NotNullViolation, UniqueViolation) as exc:
                if rewind_on_data_error:
                    self._rewind(batch)
                log.warning("batch_write_constraint_error", error=str(exc), messages=len(batch))
                raise
            except Exception as exc:
                self._health.mark_db_failed(str(exc))
                self._rewind(batch)
                log.exception("batch_write_fatal_error", error=str(exc), messages=len(batch))
                raise
            else:
                return result.total
        self._rewind(batch)
        return None

    def _rewind(self, batch: list[Message]) -> None:
        """
        Возвращает consumer к началу batch после неподтверждённой DLQ-доставки.

        Отсутствия commit недостаточно: poll уже передвинул локальную позицию.
        Seek обязателен для всех partition batch, иначе последующий commit может
        перескочить сообщения, которые мы намеренно не обработали.
        """
        min_offsets: dict[tuple[str, int], int] = {}
        for msg in batch:
            key = (cast("str", msg.topic()), cast("int", msg.partition()))
            offset = cast("int", msg.offset())
            min_offsets[key] = min(offset, min_offsets.get(key, offset))
        for (topic, partition), offset in min_offsets.items():
            self._consumer.seek(TopicPartition(topic, partition, offset))
        log.warning("batch_rewound", partitions=len(min_offsets), messages=len(batch))

    def _commit(self, batch: list[Message]) -> None:
        """Коммитим максимальный offset+1 по каждой (topic, partition) батча."""
        max_offsets: dict[tuple[str, int], int] = {}
        for msg in batch:
            key = (cast("str", msg.topic()), cast("int", msg.partition()))
            offset = cast("int", msg.offset())
            if offset > max_offsets.get(key, -1):
                max_offsets[key] = offset
        tps = [TopicPartition(t, p, off + 1) for (t, p), off in max_offsets.items()]
        self._consumer.commit(offsets=tps, asynchronous=False)

    def _shutdown(self) -> None:
        log.info("consumer_stopping")
        self._health.set_stopping()
        try:
            self._consumer.close()
        finally:
            try:
                self._dlq.close()
            finally:
                self._db.close()


def main() -> None:
    configure_logging("consumer-service")
    cfg = Config.from_env()
    health = HealthState()

    server = start_health_server(cfg.health_port, health)

    worker = Worker(cfg, health)
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)
    try:
        worker.run()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
