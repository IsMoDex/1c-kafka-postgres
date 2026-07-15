"""Thread-safe liveness, readiness, and low-cardinality service metrics."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_HEARTBEAT_MAX_AGE_SECONDS = 30


class HealthState:
    """Shared state updated and read atomically across worker and HTTP threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = False
        self._db_ok = False
        self._kafka_ok = False
        self._rows_processed = 0
        self._messages_processed = 0
        self._messages_dlq = 0
        self._db_retries = 0
        self._last_error: str | None = None
        self._last_kafka_error: str | None = None
        self._last_db_ok_at: float | None = None
        self._last_kafka_ok_at: float | None = None

    def set_running(self) -> None:
        with self._lock:
            self._running = True

    def set_stopping(self) -> None:
        with self._lock:
            self._running = False

    def mark_db_ok(self) -> None:
        with self._lock:
            self._db_ok = True
            self._last_db_ok_at = time.time()

    def mark_db_failed(self, error: str) -> None:
        with self._lock:
            self._db_ok = False
            self._last_error = error

    def mark_kafka_ok(self) -> None:
        with self._lock:
            self._kafka_ok = True
            self._last_kafka_error = None
            self._last_kafka_ok_at = time.time()

    def mark_kafka_failed(self, error: str) -> None:
        with self._lock:
            self._kafka_ok = False
            self._last_kafka_error = error

    def record_dlq(self, reason: str) -> None:
        with self._lock:
            self._messages_dlq += 1
            self._last_error = reason

    def record_db_retry(self) -> None:
        with self._lock:
            self._db_retries += 1

    def record_batch(self, *, rows: int, messages: int) -> None:
        with self._lock:
            self._rows_processed += rows
            self._messages_processed += messages
            self._last_error = None

    def record_error(self, error: str) -> None:
        with self._lock:
            self._last_error = error

    def status(self) -> tuple[dict[str, object], bool]:
        with self._lock:
            now = time.time()
            db_fresh = self._last_db_ok_at is not None and now - self._last_db_ok_at < _HEARTBEAT_MAX_AGE_SECONDS
            kafka_fresh = (
                self._last_kafka_ok_at is not None and now - self._last_kafka_ok_at < _HEARTBEAT_MAX_AGE_SECONDS
            )
            ready = self._running and self._db_ok and self._kafka_ok and db_fresh and kafka_fresh
            return (
                {
                    "ready": ready,
                    "db_ok": self._db_ok and db_fresh,
                    "kafka_ok": self._kafka_ok and kafka_fresh,
                    "rows_processed": self._rows_processed,
                    "messages_processed": self._messages_processed,
                    "messages_dlq": self._messages_dlq,
                    "db_retries": self._db_retries,
                    "last_db_ok_at": self._last_db_ok_at,
                    "last_kafka_ok_at": self._last_kafka_ok_at,
                },
                ready,
            )

    def liveness(self) -> bool:
        with self._lock:
            return self._running

    def metrics(self) -> str:
        payload, ready = self.status()
        lines = [
            "# TYPE consumer_ready gauge",
            f"consumer_ready {int(ready)}",
            "# TYPE consumer_rows_processed_total counter",
            f"consumer_rows_processed_total {payload['rows_processed']}",
            "# TYPE consumer_messages_processed_total counter",
            f"consumer_messages_processed_total {payload['messages_processed']}",
            "# TYPE consumer_messages_dlq_total counter",
            f"consumer_messages_dlq_total {payload['messages_dlq']}",
            "# TYPE consumer_db_retries_total counter",
            f"consumer_db_retries_total {payload['db_retries']}",
        ]
        return "\n".join(lines) + "\n"

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    @property
    def last_kafka_error(self) -> str | None:
        with self._lock:
            return self._last_kafka_error

    @property
    def kafka_ok(self) -> bool:
        with self._lock:
            return self._kafka_ok

    @property
    def messages_processed(self) -> int:
        with self._lock:
            return self._messages_processed


def start_health_server(port: int, state: HealthState) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/livez":
                live = state.liveness()
                self._json({"live": live}, 200 if live else 503)
                return
            if self.path in {"/readyz", "/health", "/healthz"}:
                payload, ready = state.status()
                self._json(payload, 200 if ready else 503)
                return
            if self.path == "/metrics":
                body = state.metrics().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def _json(self, payload: dict[str, object], code: int) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 -- stdlib override name.
            del format, args

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),  # noqa: S104 -- container health endpoint must be reachable outside localhost.
        Handler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
