"""
Конфигурация integration-service из переменных окружения.

Секреты (пароли, DSN) никогда не хранятся в коде — только в ENV/.env.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # Kafka
    kafka_bootstrap_servers: str
    topic_ownership_forms: str
    topic_counterparties: str

    # PostgreSQL (только для чтения/записи watermark в sync_state)
    pg_dsn: str

    # Источник данных
    source_type: str  # "mock" | "onec"
    onec_base_url: str
    onec_username: str
    onec_password: str
    onec_timeout: float
    onec_verify_ssl: bool
    onec_http_retries: int
    onec_page_size: int
    fk_barrier_timeout: float

    @staticmethod
    def from_env() -> Config:
        return Config(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_ownership_forms=os.getenv("TOPIC_OWNERSHIP_FORMS", "1c.ownership_forms.v1"),
            topic_counterparties=os.getenv("TOPIC_COUNTERPARTIES", "1c.counterparties.v1"),
            pg_dsn=os.getenv(
                "PG_DSN",
                "postgresql://integration:integration@localhost:5432/integration",
            ),
            source_type=os.getenv("SOURCE_TYPE", "mock").lower(),
            onec_base_url=os.getenv(
                "ONEC_BASE_URL",
                # По умолчанию не задан: адрес хоста явно прописывается в .env.
                "http://HOST_IPV4_NOT_SET/roshim/hs/integration",
            ),
            onec_username=os.getenv("ONEC_USERNAME", ""),
            onec_password=os.getenv("ONEC_PASSWORD", ""),
            onec_timeout=float(os.getenv("ONEC_TIMEOUT", "30")),
            onec_verify_ssl=os.getenv("ONEC_VERIFY_SSL", "true").lower() == "true",
            onec_http_retries=int(os.getenv("ONEC_HTTP_RETRIES", "3")),
            onec_page_size=int(os.getenv("ONEC_PAGE_SIZE", "500")),
            fk_barrier_timeout=float(os.getenv("FK_BARRIER_TIMEOUT", "30")),
        )
