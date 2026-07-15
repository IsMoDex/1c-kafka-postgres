"""Общие структуры логирования для сервисов (structlog, JSON-вывод)."""

from __future__ import annotations

import logging
import os

import structlog


def configure_logging(service: str) -> structlog.stdlib.BoundLogger:
    """
    Настраивает структурированное JSON-логирование.

    Уровень берётся из переменной окружения LOG_LEVEL (по умолчанию INFO).
    Все логи снабжаются полем ``service`` для трассировки в общем потоке.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(format="%(message)s", level=level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger().bind(service=service)
