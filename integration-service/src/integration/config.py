"""Validated integration-service configuration loaded from environment variables."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from urllib.parse import urlparse


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


def _boolean(name: str, *, default: bool) -> bool:
    raw = os.getenv(name, str(default).lower()).strip().lower()
    if raw not in {"true", "false"}:
        msg = f"{name} must be 'true' or 'false', got {raw!r}"
        raise ValueError(msg)
    return raw == "true"


def _validate_onec_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        msg = "ONEC_BASE_URL must be an absolute http(s) URL"
        raise ValueError(msg)
    if "HOST_IPV4_NOT_SET" in value or "<HOST_IPV4>" in value:
        msg = "ONEC_BASE_URL still contains a host placeholder"
        raise ValueError(msg)


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    topic_ownership_forms: str
    topic_counterparties: str
    pg_dsn: str
    source_type: str
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
        source_type = _required("SOURCE_TYPE", "mock").lower()
        if source_type not in {"mock", "onec"}:
            msg = f"SOURCE_TYPE must be 'mock' or 'onec', got {source_type!r}"
            raise ValueError(msg)

        ownership_topic = _required("TOPIC_OWNERSHIP_FORMS", "1c.ownership_forms.v1")
        counterparty_topic = _required("TOPIC_COUNTERPARTIES", "1c.counterparties.v1")
        if ownership_topic == counterparty_topic:
            msg = "TOPIC_OWNERSHIP_FORMS and TOPIC_COUNTERPARTIES must be different"
            raise ValueError(msg)

        onec_base_url = os.getenv(
            "ONEC_BASE_URL",
            "http://HOST_IPV4_NOT_SET/roshim/hs/integration",
        ).strip()
        if source_type == "onec":
            _validate_onec_url(onec_base_url)

        return Config(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_ownership_forms=ownership_topic,
            topic_counterparties=counterparty_topic,
            pg_dsn=_required(
                "PG_DSN",
                "postgresql://integration:integration@localhost:5432/integration",
            ),
            source_type=source_type,
            onec_base_url=onec_base_url,
            onec_username=os.getenv("ONEC_USERNAME", ""),
            onec_password=os.getenv("ONEC_PASSWORD", ""),
            onec_timeout=_positive_float("ONEC_TIMEOUT", 30),
            onec_verify_ssl=_boolean("ONEC_VERIFY_SSL", default=True),
            onec_http_retries=_integer("ONEC_HTTP_RETRIES", 3, minimum=0),
            onec_page_size=_integer("ONEC_PAGE_SIZE", 500, minimum=1, maximum=5000),
            fk_barrier_timeout=_positive_float("FK_BARRIER_TIMEOUT", 30),
        )
