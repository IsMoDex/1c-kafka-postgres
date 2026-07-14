"""Конфигурация consumer-service из переменных окружения."""
from __future__ import annotations

import os
from dataclasses import dataclass


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
    def from_env() -> "Config":
        topics = [
            t.strip()
            for t in os.getenv("KAFKA_TOPICS", "1c.ownership_forms.v1,1c.counterparties.v1").split(",")
            if t.strip()
        ]
        return Config(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "integration-consumer"),
            topics=topics,
            dlq_suffix=os.getenv("KAFKA_DLQ_SUFFIX", ".dlq"),
            pg_dsn=os.getenv(
                "PG_DSN",
                "postgresql://integration:integration@localhost:5432/integration",
            ),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            batch_max_messages=int(os.getenv("BATCH_MAX_MESSAGES", "500")),
            batch_max_seconds=float(os.getenv("BATCH_MAX_SECONDS", "2")),
            health_port=int(os.getenv("HEALTH_PORT", "8081")),
        )
