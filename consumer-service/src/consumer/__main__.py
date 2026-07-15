"""
Точка входа consumer-service: python -m consumer [run].

Демон читает Kafka и пишет upsert в PostgreSQL. Настройки — из переменных
окружения (см. config.py / .env.example). Поддерживает `--help` без запуска
сервиса, чтобы диагностика не поднимала слушатель порта.
"""

from __future__ import annotations

import argparse

from consumer.worker import main


def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m consumer",
        description=(
            "Consumer-service: читает события Kafka (topics из KAFKA_TOPICS) и "
            "делает идемпотентный upsert в PostgreSQL. Конфигурация — через ENV: "
            "KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPICS, PG_DSN, "
            "MAX_RETRIES, BATCH_MAX_MESSAGES, BATCH_MAX_SECONDS, HEALTH_PORT."
        ),
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run"],
        help="run — запустить consumer-демон (по умолчанию).",
    )
    parser.parse_args()
    # сюда попадаем только если не был запрошен --help
    main()


if __name__ == "__main__":
    cli()
