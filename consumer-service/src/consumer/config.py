"""Validated consumer-service configuration loaded from environment variables."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass


def _required(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        msg = f"{name} must not be empty"
        raise ValueError(msg)
    return value


def _integer(name: str, default: int, *, minimum: int, maximum: int | None = None) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        msg = f"{name} must be an integer, got {raw!r}"
        raise ValueError(msg) from exc
    if value < minimum or (maximum is not None and value > maximum):
        bounds = f"between {minimum} and {maximum}" if maximum is not None else f">= {minimum}"
        msg = f"{name} must be {bounds}, got {value}"
        raise ValueError(msg)
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        msg = f"{name} must be a number, got {raw!r}"
        raise ValueError(msg) from exc
    if not math.isfinite(value) or value <= 0:
        msg = f"{name} must be a finite number > 0, got {raw!r}"
        raise ValueError(msg)
    return value


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    consumer_group: str
    topics: list[str]
    dlq_suffix: str
    pg_dsn: str
    max_retries: int
    batch_max_messages: int
    batch_max_seconds: float
    health_port: int

    @staticmethod
    def from_env() -> Config:
        topics = [
            topic.strip()
            for topic in os.getenv("KAFKA_TOPICS", "1c.ownership_forms.v1,1c.counterparties.v1").split(",")
            if topic.strip()
        ]
        if not topics:
            msg = "KAFKA_TOPICS must contain at least one topic"
            raise ValueError(msg)
        if len(topics) != len(set(topics)):
            msg = "KAFKA_TOPICS must not contain duplicates"
            raise ValueError(msg)

        dlq_suffix = _required("KAFKA_DLQ_SUFFIX", ".dlq")
        dlq_topics = {f"{topic}{dlq_suffix}" for topic in topics}
        overlap = set(topics) & dlq_topics
        if overlap:
            msg = f"KAFKA_TOPICS must not include generated DLQ topics: {sorted(overlap)}"
            raise ValueError(msg)

        return Config(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group=_required("KAFKA_CONSUMER_GROUP", "integration-consumer"),
            topics=topics,
            dlq_suffix=dlq_suffix,
            pg_dsn=_required(
                "PG_DSN",
                "postgresql://integration:integration@localhost:5432/integration",
            ),
            max_retries=_integer("MAX_RETRIES", 3, minimum=0),
            batch_max_messages=_integer("BATCH_MAX_MESSAGES", 500, minimum=1),
            batch_max_seconds=_positive_float("BATCH_MAX_SECONDS", 2),
            health_port=_integer("HEALTH_PORT", 8081, minimum=1, maximum=65535),
        )
