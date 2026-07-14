"""Лёгкий HTTP-сервер healthcheck (/health) на stdlib.

Запускается в отдельном потоке. Возвращает 200, если consumer жив и последняя
проверка соединения с PostgreSQL успешна; иначе 503.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HealthState:
    """Разделяемое состояние здоровья сервиса."""

    def __init__(self) -> None:
        self.ready = False
        self.db_ok = False
        self.kafka_ok = False
        self.rows_processed = 0          # применённых строк upsert
        self.messages_processed = 0      # обработанных Kafka-сообщений
        self.messages_dlq = 0
        self.last_error: str | None = None
        self.last_kafka_error: str | None = None
        self.last_db_ok_at: float | None = None
        self.last_kafka_ok_at: float | None = None

    def mark_db_ok(self) -> None:
        self.db_ok = True
        self.last_db_ok_at = time.time()

    def mark_kafka_ok(self) -> None:
        self.kafka_ok = True
        self.last_kafka_error = None
        self.last_kafka_ok_at = time.time()

    def snapshot(self) -> dict:
        return {
            "ready": self.ready,
            "db_ok": self.db_ok,
            "kafka_ok": self.kafka_ok,
            "rows_processed": self.rows_processed,
            "messages_processed": self.messages_processed,
            "messages_dlq": self.messages_dlq,
            "last_error": self.last_error,
            "last_kafka_error": self.last_kafka_error,
            "last_db_ok_at": self.last_db_ok_at,
            "last_kafka_ok_at": self.last_kafka_ok_at,
        }

    def healthy(self) -> bool:
        now = time.time()
        db_fresh = self.last_db_ok_at is not None and now - self.last_db_ok_at < 30
        kafka_fresh = self.last_kafka_ok_at is not None and now - self.last_kafka_ok_at < 30
        return self.ready and self.db_ok and self.kafka_ok and db_fresh and kafka_fresh


def start_health_server(port: int, state: HealthState) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.rstrip("/") in ("/health", "/healthz", ""):
                payload = state.snapshot()
                code = 200 if state.healthy() else 503
                body = json.dumps(payload).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args) -> None:  # noqa: A002 — заглушаем лог
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
