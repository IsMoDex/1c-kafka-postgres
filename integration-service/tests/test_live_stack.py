"""Интеграционные тесты живого контура 1С -> Kafka -> PostgreSQL."""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import TYPE_CHECKING

import httpx
import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from integration.config import Config
from integration.sources.onec_http import OneCHttpSource
from integration.sync import Synchronizer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import Any, LiteralString

pytestmark = pytest.mark.integration

TOPICS = ("1c.ownership_forms.v1", "1c.counterparties.v1")
DLQ_TOPICS = ("1c.ownership_forms.v1.dlq", "1c.counterparties.v1.dlq")


@pytest.fixture(scope="session")
def config() -> Config:
    cfg = Config.from_env()
    if cfg.source_type != "onec":
        pytest.fail("Integration tests require SOURCE_TYPE=onec")
    if "HOST_IPV4_NOT_SET" in cfg.onec_base_url or "<HOST_IPV4>" in cfg.onec_base_url:
        pytest.fail("Integration tests require a real ONEC_BASE_URL")
    return cfg


@pytest.fixture(scope="session")
def onec_client(config: Config) -> Iterator[httpx.Client]:
    auth = (config.onec_username, config.onec_password) if config.onec_username else None
    with httpx.Client(
        base_url=config.onec_base_url.rstrip("/"),
        auth=auth,
        timeout=config.onec_timeout,
        verify=config.onec_verify_ssl,
    ) as client:
        yield client


def _source(config: Config) -> OneCHttpSource:
    return OneCHttpSource(
        base_url=config.onec_base_url,
        username=config.onec_username,
        password=config.onec_password,
        timeout=config.onec_timeout,
        verify_ssl=config.onec_verify_ssl,
        retries=config.onec_http_retries,
        page_size=config.onec_page_size,
    )


def _sync(config: Config, mode: str) -> dict[str, Any]:
    source = _source(config)
    try:
        return Synchronizer(config, source).run(mode)
    finally:
        source.close()


def _fetchone(
    config: Config,
    query: LiteralString,
    params: tuple[Any, ...] = (),
) -> tuple[Any, ...]:
    with psycopg.connect(config.pg_dsn) as conn:
        row = conn.execute(query, params).fetchone()
        assert row is not None
        return row


def _wait_until(
    predicate: Callable[[], bool],
    message: str,
    timeout: float = 30,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    pytest.fail(message)


def _kafka_consumer(config: Config, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _watermarks(config: Config, topics: tuple[str, ...]) -> dict[tuple[str, int], int]:
    consumer = _kafka_consumer(config, f"integration-watermarks-{uuid.uuid4()}")
    try:
        result: dict[tuple[str, int], int] = {}
        metadata = consumer.list_topics(timeout=10)
        for topic in topics:
            topic_metadata = metadata.topics.get(topic)
            assert topic_metadata is not None, f"Kafka topic does not exist: {topic}"
            assert topic_metadata.error is None, str(topic_metadata.error)
            for partition in topic_metadata.partitions:
                _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                result[(topic, partition)] = high
        return result
    finally:
        consumer.close()


def _wait_for_main_consumer(
    config: Config,
    target_offsets: dict[tuple[str, int], int],
    timeout: float = 30,
) -> None:
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "integration-consumer")
    consumer = _kafka_consumer(config, group_id)
    partitions = [TopicPartition(topic, partition) for topic, partition in target_offsets]
    try:

        def caught_up() -> bool:
            committed = consumer.committed(partitions, timeout=10)
            offsets = {(tp.topic, tp.partition): tp.offset for tp in committed}
            return all(offsets.get(key, -1) >= value for key, value in target_offsets.items())

        _wait_until(caught_up, "consumer-service did not reach the produced Kafka offsets", timeout)
    finally:
        consumer.close()


def _read_range(
    config: Config,
    start: dict[tuple[str, int], int],
    end: dict[tuple[str, int], int],
) -> list[dict[str, Any]]:
    expected = sum(end[key] - offset for key, offset in start.items())
    consumer = _kafka_consumer(config, f"integration-events-{uuid.uuid4()}")
    consumer.assign([TopicPartition(topic, partition, offset) for (topic, partition), offset in start.items()])
    events: list[dict[str, Any]] = []
    deadline = time.monotonic() + 20
    try:
        while len(events) < expected and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                pytest.fail(str(message.error()))
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            value = message.value()
            message_key = message.key()
            if topic is None or partition is None or offset is None or value is None or message_key is None:
                pytest.fail("Kafka returned a message with missing metadata or payload")
            key = (topic, partition)
            if offset >= end[key]:
                continue
            event = json.loads(value)
            assert message_key.decode("utf-8") == str(event["payload"]["id"])
            events.append(event)
    finally:
        consumer.close()
    assert len(events) == expected
    return events


def _db_counts(config: Config) -> tuple[int, int, int]:
    return _fetchone(
        config,
        """
        SELECT
            (SELECT count(*) FROM ownership_forms),
            (SELECT count(*) FROM counterparties),
            (SELECT count(*) - count(DISTINCT id) FROM counterparties)
        """,
    )


def test_live_onec_contract(onec_client: httpx.Client) -> None:
    forms_response = onec_client.get("/ownership-forms", params={"limit": 500, "offset": 0})
    counterparties_response = onec_client.get("/counterparties", params={"limit": 500, "offset": 0})

    assert forms_response.status_code == 200
    assert counterparties_response.status_code == 200
    forms = forms_response.json()
    counterparties = counterparties_response.json()
    assert isinstance(forms, list)
    assert len(forms) >= 3
    assert isinstance(counterparties, list)
    assert len(counterparties) >= 5
    assert all({"id", "name", "deleted", "updated_at"} <= row.keys() for row in forms)
    assert all({"id", "name", "ownership_form_id", "deleted", "updated_at"} <= row.keys() for row in counterparties)

    invalid = onec_client.get("/counterparties", params={"changed_since": "not-a-date"})
    assert invalid.status_code == 400
    assert invalid.json()["error"] == "invalid changed_since; expected RFC3339"


def test_full_sync_reaches_kafka_and_postgres_idempotently(config: Config) -> None:
    dlq_before = _watermarks(config, DLQ_TOPICS)
    kafka_before = _watermarks(config, TOPICS)

    result = _sync(config, "full")
    kafka_after = _watermarks(config, TOPICS)
    assert result["ownership_forms"] >= 3
    assert result["counterparties"] >= 5
    assert sum(kafka_after[key] - value for key, value in kafka_before.items()) == (
        result["ownership_forms"] + result["counterparties"]
    )

    events = _read_range(config, kafka_before, kafka_after)
    assert all(event["source"] == "1c" for event in events)
    assert {event["event_type"].split(".")[0] for event in events} == {
        "ownership_form",
        "counterparty",
    }
    _wait_for_main_consumer(config, kafka_after)

    counts_after_first = _db_counts(config)
    assert counts_after_first[0] >= 3
    assert counts_after_first[1] >= 5
    assert counts_after_first[2] == 0
    assert (
        _fetchone(
            config,
            """
        SELECT count(*) FROM counterparties c
        LEFT JOIN ownership_forms o ON o.id = c.ownership_form_id
        WHERE c.ownership_form_id IS NOT NULL AND o.id IS NULL
        """,
        )[0]
        == 0
    )

    second_result = _sync(config, "full")
    second_offsets = _watermarks(config, TOPICS)
    _wait_for_main_consumer(config, second_offsets)
    assert second_result["ownership_forms"] == result["ownership_forms"]
    assert second_result["counterparties"] == result["counterparties"]
    assert _db_counts(config) == counts_after_first
    assert _watermarks(config, DLQ_TOPICS) == dlq_before


def test_incremental_update_propagates_and_restores(
    config: Config,
    onec_client: httpx.Client,
) -> None:
    _sync(config, "full")
    rows = onec_client.get("/counterparties", params={"limit": 500, "offset": 0}).json()
    target = next(row for row in rows if row["code"] == "000001")
    original_name = target["name"]
    changed_name = f"Integration {uuid.uuid4().hex[:8]}"

    try:
        response = onec_client.post("/touch", params={"id": target["id"], "name": changed_name})
        assert response.status_code == 200
        result = _sync(config, "incremental")
        offsets = _watermarks(config, TOPICS)
        _wait_for_main_consumer(config, offsets)
        _wait_until(
            lambda: (
                _fetchone(config, "SELECT name FROM counterparties WHERE id = %s", (target["id"],))[0] == changed_name
            ),
            "incremental update did not reach PostgreSQL",
        )
        assert result["counterparties"] >= 1
        assert _fetchone(config, "SELECT count(*) FROM counterparties WHERE id = %s", (target["id"],))[0] == 1
    finally:
        restore = onec_client.post("/touch", params={"id": target["id"], "name": original_name})
        assert restore.status_code == 200
        _sync(config, "incremental")
        restore_offsets = _watermarks(config, TOPICS)
        _wait_for_main_consumer(config, restore_offsets)
        _wait_until(
            lambda: (
                _fetchone(config, "SELECT name FROM counterparties WHERE id = %s", (target["id"],))[0] == original_name
            ),
            "test cleanup did not restore the original counterparty name",
        )


def test_soft_delete_health_and_dlq(
    config: Config,
    onec_client: httpx.Client,
) -> None:
    rows = onec_client.get("/counterparties", params={"limit": 500, "offset": 0}).json()
    target = next(row for row in rows if row["code"] == "000005")
    dlq_before = _watermarks(config, DLQ_TOPICS)

    response = onec_client.post("/delete", params={"id": target["id"]})
    assert response.status_code == 200
    result = _sync(config, "incremental")
    offsets = _watermarks(config, TOPICS)
    _wait_for_main_consumer(config, offsets)
    _wait_until(
        lambda: _fetchone(config, "SELECT deleted FROM counterparties WHERE id = %s", (target["id"],))[0] is True,
        "soft delete did not reach PostgreSQL",
    )
    assert result["counterparties"] >= 1
    assert _watermarks(config, DLQ_TOPICS) == dlq_before

    def health_is_green() -> bool:
        try:
            response = httpx.get("http://consumer-service:8081/health", timeout=5)
            if response.status_code != 200:
                return False
            payload = response.json()
            return payload["ready"] and payload["db_ok"] and payload["kafka_ok"]
        except httpx.HTTPError:
            return False

    _wait_until(health_is_green, "consumer health did not become green")
